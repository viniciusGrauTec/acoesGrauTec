package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetFornecedoresCarga
  implements AcaoRotinaJava, ScheduledAction
 {
	
	private List<String> selectsParaInsert = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();
	
	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];
		
		String url = (String) registro.getCampo("URL");
		String token = (String) registro.getCampo("TOKEN");
		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");
		
		String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
		
		//String integracaoAutomatica = Optional.ofNullable(registro.getCampo("INTEGRACAO")).orElse("N").toString();
		//String fazIntegracao = integracaoAutomatica.equalsIgnoreCase("S") ? "S" : "N";
		
		
		try {

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {

				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];
				String idExterno = (String) obj[2];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfParceiros.get(cpf_cnpj) == null) {
					mapaInfParceiros.put(cpf_cnpj, codParc);
				}
			}

			// Id Forncedor
			Map<String, BigDecimal> mapaInfIdParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {

				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];
				String idExterno = (String) obj[2];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfIdParceiros.get(idExterno + "###" + cpf_cnpj + "###"
						+ codemp) == null) {
					mapaInfIdParceiros.put(idExterno + "###" + cpf_cnpj + "###"
							+ codemp, codParc);
				}
			}
			
			
			//if((fazIntegracao != null && fazIntegracao.equals("S"))){
				
		//	}
			
			iterarEndpoint(mapaInfIdParceiros, mapaInfParceiros, url,
					token, codEmp, dataInicio, dataFim);
			
			
			
			contexto.setMensagemRetorno("Periodo Processado!");
			
		}catch(Exception e){
			e.printStackTrace();
			contexto.mostraErro(e.getMessage());
		}finally{

			if(selectsParaInsert.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
				
				//BigDecimal idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsert.size();
				
				int i = 1;
				for (String sqlInsert : selectsParaInsert) {
					String sql = sqlInsert;
					int nuFin = util.getMaxNumLog();
					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);

					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}
				
				System.out.println("Consulta de log: \n" + msgError);
				insertLogList(msgError.toString());
				
			}
		
		}
		
	}
	
	@Override
	public void onTime(ScheduledActionContext arg0) {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";

		System.out.println("Iniciou o cadastro dos fornecedores empresa 4");
		try {

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {

				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];
				String idExterno = (String) obj[2];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfParceiros.get(cpf_cnpj) == null) {
					mapaInfParceiros.put(cpf_cnpj, codParc);
				}
			}

			// Id Forncedor
			Map<String, BigDecimal> mapaInfIdParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {

				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];
				String idExterno = (String) obj[2];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfIdParceiros.get(idExterno + "###" + cpf_cnpj + "###"
						+ codemp) == null) {
					mapaInfIdParceiros.put(idExterno + "###" + cpf_cnpj + "###"
							+ codemp, codParc);
				}
			}

			jdbc.openSession();

			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";
			//String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			//String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 4";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();
			while (rs.next()) {
				System.out.println("While principal");

				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");

				iterarEndpoint(mapaInfIdParceiros, mapaInfParceiros, url,
						token, codEmp);
			}
			System.out
					.println("Finalizou o cadastro dos fornecedores empresa 4");
		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar Fornecedores, Mensagem de erro: "
								+ e.getMessage(), "Erro", "");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			jdbc.closeSession();
			

			if(selectsParaInsert.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
				
				//BigDecimal idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsert.size();
				
				int i = 1;
				for (String sqlInsert : selectsParaInsert) {
					String sql = sqlInsert;
					int nuFin = 0;
					
					try {
						nuFin = util.getMaxNumLog();
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);

					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}
				
				System.out.println("Consulta de log: \n" + msgError);
				try {
					insertLogList(msgError.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				msgError = null;
				this.selectsParaInsert = new ArrayList<String>();
				
			}
		
		}
	}

	public void iterarEndpoint(Map<String, BigDecimal> mapaInfIdParceiros,
			Map<String, BigDecimal> mapaInfParceiros, String url, String token,
			BigDecimal codEmp, String dataInicio, String dataFim) throws Exception {


		try {
			
			// Convertendo as Strings para LocalDate
	        LocalDate inicio = LocalDate.parse(dataInicio);
	        LocalDate fim = LocalDate.parse(dataFim);

	        // Loop para percorrer o intervalo de dias
	        LocalDate atual = inicio;
	        
			while (!atual.isAfter(fim)) {
				
				String dataAtual = atual.toString();
				
				System.out.println("While de iteraÁ„o");

				String[] response = apiGet2(url
						+ "/financeiro/clientes/fornecedores?" + "quantidade=0"
						+ "&dataInicial=" + dataAtual
						+ " 00:00:00&dataFinal=" + dataAtual + " 23:59:59"
						,token);

				int status = Integer.parseInt(response[0]);
				System.out.println("Status teste: " + status);

				String responseString = response[1];
				System.out.println("response string: " + responseString);

				cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros,
						response, codEmp);
				
				// Incrementa para o prÛximo dia
				atual = atual.plusDays(1);
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void iterarEndpoint(Map<String, BigDecimal> mapaInfIdParceiros,
			Map<String, BigDecimal> mapaInfParceiros, String url, String token,
			BigDecimal codEmp) throws Exception {
		
		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);
		
		try {

			System.out.println("While de iteraÁ„o");

			String[] response = apiGet2(url
					+ "/financeiro/clientes/fornecedores?" + "quantidade=0"
					+ "&dataInicial=" + dataFormatada + " 00:00:00&dataFinal="
					+ dataFormatada + " 23:59:59", token);

			int status = Integer.parseInt(response[0]);
			System.out.println("Status teste: " + status);

			String responseString = response[1];
			System.out.println("response string: " + responseString);

			cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros,
					response, codEmp);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cadastrarFornecedor(Map<String, BigDecimal> mapaInfIdParceiros,
	        Map<String, BigDecimal> mapaInfParceiros, String[] response,
	        BigDecimal codEmp) {
	    System.out.println("Cadastro principal");

	    EnviromentUtils util = new EnviromentUtils();

	    String fornecedorId = "";

	    try {
	        String responseString = response[1];
	        String status = response[0];

	        if (status.equalsIgnoreCase("200")) {
	            JsonParser parser = new JsonParser();
	            JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();
	            int count = 0;
	            System.out.println("contagem: " + count);

	            for (JsonElement jsonElement : jsonArray) {
	                System.out.println("contagem2: " + count);
	                JsonObject jsonObject = jsonElement.getAsJsonObject();

	                // ExtraÁ„o dos campos do JSON
	                String fornecedorTipo = jsonObject.get("fornecedor_tipo").isJsonNull() ? null : jsonObject.get("fornecedor_tipo").getAsString();
	                fornecedorId = jsonObject.get("fornecedor_id").isJsonNull() ? null : jsonObject.get("fornecedor_id").getAsString();
	                String fornecedorNome = jsonObject.get("fornecedor_nome").isJsonNull() ? null : jsonObject.get("fornecedor_nome").getAsString();
	                String fornecedorNomeFantasia = jsonObject.get("fornecedor_nomefantasia").isJsonNull() ? null : jsonObject.get("fornecedor_nomefantasia").getAsString();
	                if (fornecedorNomeFantasia == null) {
	                    fornecedorNomeFantasia = fornecedorNome;
	                }
	                String fornecedorEndereco = jsonObject.get("fornecedor_endereco").isJsonNull() ? null : jsonObject.get("fornecedor_endereco").getAsString();
	                String fornecedorBairro = jsonObject.get("fornecedor_bairro").isJsonNull() ? null : jsonObject.get("fornecedor_bairro").getAsString();
	                String fornecedorCidade = jsonObject.get("fornecedor_cidade").isJsonNull() ? null : jsonObject.get("fornecedor_cidade").getAsString();
	                String fornecedorUf = jsonObject.get("fornecedor_uf").isJsonNull() ? null : jsonObject.get("fornecedor_uf").getAsString();
	                String fornecedorCep = jsonObject.get("fornecedor_cep").isJsonNull() ? null : jsonObject.get("fornecedor_cep").getAsString();
	                String fornecedorInscMunicipal = jsonObject.get("fornecedor_isncmunicipal").isJsonNull() ? null : jsonObject.get("fornecedor_isncmunicipal").getAsString();
	                String fornecedorInscestadual = jsonObject.get("fornecedor_inscestadual").isJsonNull() ? null : jsonObject.get("fornecedor_inscestadual").getAsString();
	                String fornecedorFone1 = jsonObject.get("fornecedor_fone1").isJsonNull() ? null : jsonObject.get("fornecedor_fone1").getAsString();
	                String fornecedorFone2 = jsonObject.get("fornecedor_fone2").isJsonNull() ? null : jsonObject.get("fornecedor_fone2").getAsString();
	                String fornecedorFax = jsonObject.get("fornecedor_fax").isJsonNull() ? null : jsonObject.get("fornecedor_fax").getAsString();
	                String fornecedorCelular = jsonObject.get("fornecedor_celular").isJsonNull() ? null : jsonObject.get("fornecedor_celular").getAsString();
	                String fornecedorContato = jsonObject.get("fornecedor_contato").isJsonNull() ? null : jsonObject.get("fornecedor_contato").getAsString();
	                String fornecedorCpfcnpj = jsonObject.get("fornecedor_cpfcnpj").isJsonNull() ? null : jsonObject.get("fornecedor_cpfcnpj").getAsString();
	                String fornecedorEmail = jsonObject.get("fornecedor_email").isJsonNull() ? null : jsonObject.get("fornecedor_email").getAsString();
	                String fornecedorHomepage = jsonObject.get("fornecedor_homepage").isJsonNull() ? null : jsonObject.get("fornecedor_homepage").getAsString();
	                String fornecedorAtivo = jsonObject.get("fornecedor_ativo").isJsonNull() ? null : jsonObject.get("fornecedor_ativo").getAsString();
	                String dataAtualizacao = jsonObject.get("data_atualizacao").isJsonNull() ? null : jsonObject.get("data_atualizacao").getAsString();

	                // ValidaÁ„o dos campos obrigatÛrios CEP  DESCRICAO CPF/CNPJ  ATIVO
	                if (fornecedorCpfcnpj == null || fornecedorAtivo == null || fornecedorCidade == null) {
	                    StringBuilder mensagemErro = new StringBuilder("Fornecedor N„o Ser· Cadastrado. InformaÁıes Faltando: ");
	                    if (fornecedorCpfcnpj == null) mensagemErro.append("CPF/CNPJ, ");
	                    if (fornecedorAtivo == null) mensagemErro.append("Ativo, ");
	                    if (fornecedorCidade == null) mensagemErro.append("Cidade, ");
	                    if (fornecedorNome == null) mensagemErro.append("Descricao, ");
	                    	    

	                    // Remove a ˙ltima vÌrgula e espaÁo
	                    mensagemErro.setLength(mensagemErro.length() - 2);

	                    // Adiciona a mensagem ao selectsParaInsert
	                    selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + mensagemErro.toString() + "', SYSDATE, 'Aviso', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
	                } else {
	                    // Verifica se o fornecedor j· existe
	                    boolean fornecedorExiste = mapaInfParceiros.get(fornecedorCpfcnpj) != null;

	                    if (!fornecedorExiste) {
	                        System.out.println("Entrou no cadastro");
	                        insertFornecedor(
	                                fornecedorTipo, fornecedorId, fornecedorNome,
	                                fornecedorNomeFantasia, fornecedorEndereco,
	                                fornecedorBairro, fornecedorCidade,
	                                fornecedorUf, fornecedorCep,
	                                fornecedorInscMunicipal, fornecedorCpfcnpj,
	                                fornecedorHomepage, fornecedorAtivo,
	                                dataAtualizacao, fornecedorInscestadual,
	                                fornecedorFone1, fornecedorFone2,
	                                fornecedorFax, fornecedorCelular,
	                                fornecedorContato, fornecedorNome,
	                                fornecedorEmail, codEmp);

	                        System.out.println("Fornecedor cadastrado");
	                    } else {
	                        boolean idFornecedorExiste = mapaInfIdParceiros.get(fornecedorId + "###" + fornecedorCpfcnpj + "###" + codEmp) != null;

	                        if (!idFornecedorExiste) {
	                            insertIdForn(fornecedorId, fornecedorCpfcnpj, codEmp);
	                        }
	                    }

	                    count++;
	                }
	            }
	        } else {
	            selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Status de Retorno da API Diferente de Sucesso, Status Retornado: " + status + "', SYSDATE, 'Aviso', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	        selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro no chamado do endpoint: " + e.getMessage() + "', SYSDATE, 'Erro', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
	    }
	}

	public boolean getIfFornecedorExist(String fornecedorCpfcnpj)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int fornecedor = 0;
		try {

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS FORNECEDOR FROM TGFPAR WHERE CGC_CPF = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, fornecedorCpfcnpj);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				fornecedor = rs.getInt("FORNECEDOR");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		if (fornecedor > 0) {
			return false;
		}
		return true;
	}

	public boolean getIfIdFornecedorExist(String fornecedorCpfcnpj,
			String idAcad, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int fornecedor = 0;
		try {

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS C FROM AD_IDFORNACAD WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
					+ fornecedorCpfcnpj
					+ "') AND IDACADWEB = '"
					+ idAcad
					+ "' AND CODEMP = " + codemp;

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				fornecedor = rs.getInt("C");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		if (fornecedor > 0) {
			return false;
		}
		return true;
	}

	public void updateTgfNumParc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TGFPAR'";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public BigDecimal getMaxNumParc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal bd = BigDecimal.ZERO;
		try {
			updateTgfNumParc();

			jdbc.openSession();

			String sqlUpd = "SELECT MAX (ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFPAR'";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				bd = rs.getBigDecimal("ULTCOD");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		return bd;
	}

	public BigDecimal insertFornecedor(String fornecedorTipo,
	        String fornecedorId, String fornecedorNome,
	        String fornecedorNomeFantasia, String fornecedorEndereco,
	        String fornecedorBairro, String fornecedorCidade,
	        String fornecedorUf, String fornecedorCep,
	        String fornecedorInscMunicipal, String fornecedorCpfcnpj,
	        String fornecedorHomepage, String fornecedorAtivo,
	        String dataAtualizacao, String fornecedorInscestadual,
	        String fornecedorFone1, String fornecedorFone2,
	        String fornecedorFax, String fornecedorCelular,
	        String fornecedorContato, String fornecedorNome2,
	        String fornecedorEmail, BigDecimal codEmp) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    
	    EnviromentUtils util = new EnviromentUtils();
	    
	    BigDecimal atualCodparc = util.getMaxNumParc();

	    String tipPessoa = "";
	    if (fornecedorCpfcnpj.length() == 11) {
	        tipPessoa = "F";
	    } else if (fornecedorCpfcnpj.length() == 14) {
	        tipPessoa = "J";
	    }
	    try {
	        jdbc.openSession();

	        String sqlP = "INSERT INTO TGFPAR(CODPARC, AD_ID_EXTERNO_FORN, AD_IDENTINSCMUNIC, AD_TIPOFORNECEDOR, FORNECEDOR, IDENTINSCESTAD, HOMEPAGE, ATIVO, NOMEPARC, RAZAOSOCIAL ,TIPPESSOA, AD_ENDCREDOR, CODBAI, CODCID, CEP, CGC_CPF, DTCAD, DTALTER, CODEMP) \t\tVALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NVL((select max(codbai) from tsibai where TRANSLATE( \t\t\t    upper(nomebai), \t\t\t    '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  ) like TRANSLATE( \t\t\t    upper(?), \t\t\t    '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  )), 0), NVL((SELECT max(codcid) FROM tsicid WHERE TRANSLATE(              UPPER(descricaocorreio),               '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               LIKE TRANSLATE(UPPER(?),               '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               OR SUBSTR(UPPER(descricaocorreio),               1, INSTR(UPPER(descricaocorreio), ' ') - 1)               LIKE TRANSLATE(UPPER(?),               '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')),0),  ?, ?, SYSDATE, SYSDATE, ?)";

	        pstmt = jdbc.getPreparedStatement(sqlP);
	        pstmt.setBigDecimal(1, atualCodparc);
	        pstmt.setString(2, fornecedorId);
	        pstmt.setString(3, fornecedorInscMunicipal);
	        pstmt.setString(4, fornecedorTipo);
	        pstmt.setString(5, "S");
	        pstmt.setString(6, fornecedorInscestadual);
	        pstmt.setString(7, fornecedorHomepage);
	        pstmt.setString(8, fornecedorAtivo);
	        pstmt.setString(9, fornecedorNome.toUpperCase());
	        pstmt.setString(10, fornecedorNomeFantasia.toUpperCase());
	        pstmt.setString(11, tipPessoa);
	        pstmt.setString(12, fornecedorEndereco);

	        pstmt.setString(13, fornecedorBairro);

	        pstmt.setString(14, fornecedorCidade.trim());
	        pstmt.setString(15, fornecedorCidade.trim());

	        pstmt.setString(16, fornecedorCep);

	        pstmt.setString(17, fornecedorCpfcnpj);
	        pstmt.setBigDecimal(18, codEmp);

	        pstmt.executeUpdate();
	        if ((fornecedorFone1 != null) && (fornecedorFone2 != null)
	                && (fornecedorFax != null) && (fornecedorCelular != null)
	                && (fornecedorContato != null) && (fornecedorNome != null)
	                && (fornecedorEmail != null) && (atualCodparc != null)) {
	            insertContatoFornecedor(fornecedorFone1, fornecedorFone2,
	                    fornecedorFax, fornecedorCelular, fornecedorContato,
	                    fornecedorNome, fornecedorEmail, atualCodparc, fornecedorId,
	                    codEmp);
	        } else {
	        	//CEP DESCRICAO CPF/CNPJ ATIVO
	        	
	        	//talvez nao seja necessario filtrar dentro do insert
	        	
	            StringBuilder mensagemErro = new StringBuilder("Campos faltando: ");
	           // if (fornecedorFone1 == null) mensagemErro.append("fornecedorFone1, ");
	           // if (fornecedorFone2 == null) mensagemErro.append("fornecedorFone2, ");
	          //  if (fornecedorFax == null) mensagemErro.append("fornecedorFax, ");
	           // if (fornecedorCelular == null) mensagemErro.append("fornecedorCelular, ");
	           // if (fornecedorContato == null) mensagemErro.append("fornecedorContato, ");
	          //  if (fornecedorNome == null) mensagemErro.append("fornecedorNome, ");
	          //  if (fornecedorEmail == null) mensagemErro.append("fornecedorEmail, ");
	          //  if (atualCodparc == null) mensagemErro.append("atualCodparc, ");

	            // Remove a ˙ltima vÌrgula e espaÁo
	            mensagemErro.setLength(mensagemErro.length() - 2);

	            selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + mensagemErro.toString() + "', SYSDATE, 'Aviso', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	        }

	        insertIdForn(fornecedorId, fornecedorCpfcnpj, codEmp);

	    } catch (SQLException e) {
	        selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar fornecedor: " + e.getMessage().replace("'", "\"")+"', SYSDATE, 'Erro', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	        
	        e.printStackTrace();
	    } finally {
	        if (pstmt != null) {
	            pstmt.close();
	        }
	        jdbc.closeSession();
	    }
	    return atualCodparc;
	}

	public void updateIdForn(String idForn, String cgc) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();

			String sqlP = "UPDATE TGFPAR SET AD_ID_EXTERNO_FORN = '"
					+ idForn
					+ "' WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
					+ cgc + "')";

			pstmt = jdbc.getPreparedStatement(sqlP);

			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void insertIdForn(String idForn, String cgc, BigDecimal codemp) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;

	    try {
	        jdbc.openSession();

	        // Monta a query SQL
	        String sqlP = "INSERT INTO AD_IDFORNACAD (CODPARC, ID, IDACADWEB, CODEMP) " +
	                      "VALUES ((SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?), " +
	                      "(SELECT NVL(MAX(ID), 0) + 1 FROM AD_IDFORNACAD), ?, ?)";

	        pstmt = jdbc.getPreparedStatement(sqlP);

	        // Define os par‚metros da query
	        pstmt.setString(1, cgc);
	        pstmt.setString(2, idForn);
	        pstmt.setBigDecimal(3, codemp);

	        // Executa a query
	        pstmt.executeUpdate();

	    } catch (SQLException e) {
	        e.printStackTrace();

	        // Mensagem de erro detalhada
	        String mensagemErro = "Erro ao Cadastrar Id de Fornecedor. Detalhes: ";
	        if (cgc == null || cgc.isEmpty()) {
	            mensagemErro += "CPF/CNPJ do fornecedor È nulo ou vazio. ";
	        }
	        if (idForn == null || idForn.isEmpty()) {
	            mensagemErro += "ID do fornecedor È nulo ou vazio. ";
	        }
	        if (codemp == null) {
	            mensagemErro += "CÛdigo da empresa È nulo. ";
	        }
	        mensagemErro += "ExceÁ„o: " + e.getMessage();

	        // Adiciona a mensagem de erro ao selectsParaInsert
	        selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + mensagemErro + "', SYSDATE, 'Erro', " + codemp + ", '" + idForn + "' FROM DUAL");

	    } finally {
	        // Fecha os recursos
	        if (pstmt != null) {
	            pstmt.close();
	        }
	        jdbc.closeSession();
	    }
	}

	private void insertContatoFornecedor(String fornecedorFone1,
			String fornecedorFone2, String fornecedorFax,
			String fornecedorCelular, String fornecedorContato,
			String fornecedorNome, String fornecedorEmail,
			BigDecimal credotAtual, String fornecedorId,
			BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO TGFCTT (CODCONTATO, CODPARC, NOMECONTATO, CELULAR, TELEFONE, TELRESID, FAX, EMAIL) VALUES ((SELECT MAX(NVL(CODCONTATO, 0)) + 1 FROM TGFCTT WHERE CODPARC = ?), ?, ?, ?, ?, ?, ?, ?)";

			pstmt = jdbc.getPreparedStatement(sqlP);
			pstmt.setBigDecimal(1, credotAtual);
			pstmt.setBigDecimal(2, credotAtual);
			pstmt.setString(3, fornecedorNome);
			pstmt.setString(4, fornecedorCelular);
			pstmt.setString(5, fornecedorFone1);
			pstmt.setString(6, fornecedorFone2);
			pstmt.setString(7, fornecedorFax);
			pstmt.setString(8, fornecedorEmail);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			/*insertLogIntegracao("Erro ao cadastrar contatos do fornecedor: "
					+ e.getMessage(), "Erro", fornecedorNome);*/

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar contatos do fornecedor: "
					+ e.getMessage()+"', SYSDATE, 'Erro', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
			
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void insertLogIntegracao(String descricao, String status,
			String fornecedorNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String descricaoCompleta = null;
			if (fornecedorNome.equals("")) {
				descricaoCompleta = descricao;
			} else if (!fornecedorNome.isEmpty()) {
				descricaoCompleta = descricao + " " + " Fornecedor:"
						+ fornecedorNome;
			} else if (!fornecedorNome.isEmpty()) {
				descricaoCompleta = descricao + " " + " Fornecedor:"
						+ fornecedorNome;
			}
			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setString(1, descricaoCompleta);
			pstmt.setString(2, status);
			pstmt.executeUpdate();
		} catch (Exception se) {
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public String[] apiGet2(String ur, String token) throws Exception {
		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();

		// Codificando a URL corretamente
		String encodedUrl = ur.replace(" ", "%20"); // Alternativa ao
													// URLEncoder.encode()
		URL obj = new URL(encodedUrl);
		HttpURLConnection https = (HttpURLConnection) obj.openConnection();

		System.out.println("Entrou na API");
		System.out.println("URL: " + encodedUrl);
		System.out.println("Token Enviado: [" + token + "]");

		// ConfiguraÁ„o da requisiÁ„o
		https.setRequestMethod("GET");
		https.setRequestProperty("User-Agent",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Accept", "application/json");
		https.setRequestProperty("Authorization", "Bearer " + token);
		https.setDoInput(true); // Somente entrada, j· que È GET

		// Obtendo resposta
		int status = https.getResponseCode();
		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					https.getErrorStream()));
		} else {
			reader = new BufferedReader(new InputStreamReader(
					https.getInputStream()));
		}
		while ((line = reader.readLine()) != null) {
			responseContent.append(line);
		}
		reader.close();

		System.out.println("Output from Server .... \n" + status);
		String response = responseContent.toString();
		https.disconnect();

		return new String[] { Integer.toString(status), response };
	}

	private List<Object[]> retornarInformacoesParceiros() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();

		try {
			jdbc.openSession();
			String sql = "SELECT p.CODPARC, p.CGC_CPF, "
					+ "a.IDACADWEB, a.codemp " + "FROM TGFPAR p "
					+ "LEFT join AD_IDFORNACAD a "
					+ "on a.codparc = p.codparc " + "where fornecedor = 'S'";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("CGC_CPF");
				ret[2] = rs.getString("IDACADWEB");
				ret[3] = rs.getBigDecimal("codemp");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}

	public void insertLogList(String listInsert) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();
			
			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, "
							 + "	STATUS, CODEMP, MATRICULA_IDFORN) " + listInsert;
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			//pstmt.setString(1, listInsert);
			pstmt.executeUpdate();
		} catch (Exception se) {
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
}

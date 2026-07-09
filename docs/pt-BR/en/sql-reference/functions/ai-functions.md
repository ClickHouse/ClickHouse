---
description: 'Documentação das Funções de IA'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'Funções de IA'
doc_type: 'reference'
---

As Funções de IA são funções nativas do ClickHouse que você pode usar para chamar IA ou gerar embeddings para trabalhar com seus dados, extrair informações, classificar dados etc...

:::note
As funções de IA são experimentais. Defina [`allow_experimental_ai_functions`](/pt-BR/operations/settings/settings#allow_experimental_ai_functions) para ativá-las.
:::

:::note
As funções de IA podem retornar saídas imprevisíveis. O resultado dependerá bastante da qualidade do prompt e do modelo usado.
:::

Todas as funções compartilham uma infraestrutura comum que oferece:

* **Aplicação de cotas**: Limites por consulta para tokens ([`ai_function_max_input_tokens_per_query`](/pt-BR/operations/settings/settings#ai_function_max_input_tokens_per_query), [`ai_function_max_output_tokens_per_query`](/pt-BR/operations/settings/settings#ai_function_max_output_tokens_per_query)) e chamadas de API ([`ai_function_max_api_calls_per_query`](/pt-BR/operations/settings/settings#ai_function_max_api_calls_per_query)).
* **Retry com backoff**: Falhas transitórias passam por novas tentativas ([`ai_function_max_retries`](/pt-BR/operations/settings/settings#ai_function_max_retries)) com backoff exponencial ([`ai_function_retry_initial_delay_ms`](/pt-BR/operations/settings/settings#ai_function_retry_initial_delay_ms)).

<div id="configuration">
  ## Configuração
</div>

As funções de IA fazem referência a uma **coleção nomeada** que armazena as credenciais do provedor e a configuração. É possível criar e usar diferentes coleções nomeadas para diferentes funções ou chamadas de função. Por exemplo, você pode definir uma coleção nomeada diferente para usar com as funções de texto (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`) em vez da função `aiEmbed`, que requer endpoints diferentes e normalmente usa modelos diferentes.

Exemplo de instrução para criar uma coleção nomeada com credenciais do provedor: uma com um endpoint de chat e outra com um endpoint de embedding:

```sql
CREATE NAMED COLLECTION ai_text_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/chat/completions',
    model = 'gpt-4o-mini',
    api_key = 'sk-...';

CREATE NAMED COLLECTION ai_embedding_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/embeddings',
    model = 'text-embedding-3-small',
    api_key = 'sk-...';
```

<div id="named-collection-parameters">
  ### Parâmetros da coleção nomeada
</div>

| Parâmetro     | Tipo   | Padrão | Descrição                                                                                                                                                                                         |
| ------------- | ------ | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `provider`    | String | —      | Provedor do modelo. Compatível com: `'openai'`, `'anthropic'`. Veja a nota abaixo.                                                                                                                |
| `endpoint`    | String | —      | URL do endpoint da API.                                                                                                                                                                           |
| `model`       | String | —      | Nome do modelo (por exemplo, `'gpt-4o-mini'`, `'text-embedding-3-small'`).                                                                                                                        |
| `api_key`     | String | —      | Chave de autenticação do provedor. Opcional: quando omitida, o cabeçalho de autenticação não é enviado, o que permite apontar para servidores compatíveis com OpenAI que não exigem autenticação. |
| `max_tokens`  | UInt64 | `1024` | Número máximo de tokens de saída por chamada à API.                                                                                                                                               |
| `api_version` | String | —      | String de versão da API. Usada pelo Anthropic (`'2023-06-01'`).                                                                                                                                   |

:::note
Qualquer API compatível com OpenAI (por exemplo, vLLM, Ollama, LiteLLM) pode ser usada definindo `provider = 'openai'` e apontando o `endpoint` para o seu serviço.
:::

<div id="selecting-credentials">
  ### Selecionando credenciais
</div>

Uma função determina a coleção nomeada a ser usada, nesta ordem:

1. a chave `credentials` do seu mapa de parâmetros, quando presente;
2. caso contrário, a configuração aplicável de credenciais padrão:
   * [`ai_function_text_default_credentials`](/pt-BR/operations/settings/settings#ai_function_text_default_credentials) para as funções de texto (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`);
   * [`ai_function_embedding_default_credentials`](/pt-BR/operations/settings/settings#ai_function_embedding_default_credentials) para `aiEmbed`.

Se nenhum dos dois estiver definido, a chamada falhará. As funções de texto e de embedding usam configurações padrão separadas porque o endpoint e o modelo de chat completions são diferentes dos usados para embeddings.

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### Mapa de parâmetros
</div>

Cada função aceita, opcionalmente, um `Map(String, String)` de parâmetros ao final. Todos os valores são strings (coloque números entre aspas, por exemplo, `'0.2'`). Chaves desconhecidas são rejeitadas. Uma chave presente substitui o valor correspondente da coleção nomeada; uma chave ausente usa a coleção nomeada como fallback (para `model`/`max_tokens`) ou o valor padrão interno.

Os parâmetros a seguir são comuns a todas as funções de IA:

| Chave         | Descrição                                  |
| ------------- | ------------------------------------------ |
| `credentials` | Coleção nomeada a ser usada (veja acima). |
| `model`       | Substitui o `model` da collection.         |

Funções individuais aceitam parâmetros adicionais específicos de cada função (como `max_tokens`, `temperature`, `system_prompt`, `instructions` e `dimensions`). Consulte a referência de cada função abaixo para ver quais parâmetros ela aceita e seus valores padrão.

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### Configurações em nível de consulta
</div>

Todas as configurações relacionadas à IA estão listadas em [Configurações](/pt-BR/operations/settings/settings) com o prefixo `ai_function_`.

<div id="restricting-endpoint-hosts">
  ### Restringindo hosts de endpoint
</div>

A URL de `endpoint` em uma coleção nomeada de IA é um destino de saída ao qual o servidor se conecta com sua própria identidade, podendo incluir (se especificada) a `api_key` da coleção nomeada nos cabeçalhos da solicitação. Por padrão, o ClickHouse permite qualquer host. Para restringir as funções a um conjunto específico de provedores, configure [`remote_url_allow_hosts`](/pt-BR/operations/server-configuration-parameters/settings#remote_url_allow_hosts) na configuração do servidor, por exemplo:

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

Observe que essa configuração vale para todo o servidor e se aplica a todas as funcionalidades que usam HTTP.

<div id="transport-security">
  ### Segurança de transporte (HTTP vs HTTPS)
</div>

O transporte é determinado exclusivamente pelo esquema da URL do `endpoint`. Não há criptografia do `payload` da requisição no nível da aplicação; a proteção dos dados em trânsito depende inteiramente do esquema:

* `https://` — a conexão usa TLS. O corpo da requisição (texto de entrada, prompts) e a `api_key` nos cabeçalhos da requisição são criptografados em trânsito, e o certificado do provedor é validado. Use-o com qualquer provedor remoto.
* `http://` — a conexão **não é criptografada**. O corpo da requisição e a `api_key` são enviados em texto simples. Use-o apenas com um provedor confiável em uma rede privada (por exemplo, uma instância local do `vLLM` ou `Ollama`).

As funções de IA não exigem HTTPS: um `endpoint` `http://` é aceito e envia dados sem criptografia. No momento, não há nenhuma configuração no servidor que rejeite endpoints de IA em texto simples — [`remote_url_allow_hosts`](/pt-BR/operations/server-configuration-parameters/settings#remote_url_allow_hosts) restringe apenas o host de destino e não inspeciona o esquema da URL, portanto um `endpoint` `http://` para um host permitido ainda é aceito. Para garantir transporte criptografado, configure coleções nomeadas com endpoints `https://`.

Observe que, em ambos os casos, o provedor recebe os dados de entrada em texto simples após a terminação do TLS; o TLS protege os dados apenas no caminho de rede entre o servidor e o provedor.

<div id="supported-providers">
  ## Provedores compatíveis
</div>

| Provedor  | Valor de `provider` | Funções de chat | Observações                    |
| --------- | ------------------- | --------------- | ------------------------------ |
| OpenAI    | `'openai'`          | Sim             | Provedor padrão.               |
| Anthropic | `'anthropic'`       | Sim             | Usa o endpoint `/v1/messages`. |

<div id="observability">
  ## Observabilidade
</div>

A atividade da AI function é rastreada por meio dos [ProfileEvents](/pt-BR/operations/system-tables/query_log) do ClickHouse:

| ProfileEvent      | Descrição                                                                                |
| ----------------- | ---------------------------------------------------------------------------------------- |
| `AIAPICalls`      | Número de requisições HTTP feitas ao provedor de IA.                                     |
| `AIInputTokens`   | Total de tokens de entrada consumidos.                                                   |
| `AIOutputTokens`  | Total de tokens de saída consumidos.                                                     |
| `AIRowsProcessed` | Número de linhas que receberam um resultado.                                             |
| `AIRowsSkipped`   | Número de linhas ignoradas (cota excedida ou erro com `ai_function_throw_on_error = 0`). |

Consulte estes eventos:

```sql
SELECT
    ProfileEvents['AIAPICalls'] AS api_calls,
    ProfileEvents['AIInputTokens'] AS input_tokens,
    ProfileEvents['AIOutputTokens'] AS output_tokens
FROM system.query_log
WHERE query_id = 'query_id'
AND type = 'QueryFinish'
ORDER BY event_time DESC;
```

{/*
  O conteúdo interno das tags abaixo é substituído no momento da compilação da estrutura de documentação por
  documentação gerada a partir de system.functions. Não modifique nem remova as tags.
  Consulte: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }
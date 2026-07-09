---
title: Configurações de servidor fora da Source
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

Ativado por padrão nas implantações do ClickHouse Cloud.

Se essa configuração não estiver ativada por padrão no seu ambiente, dependendo de como o ClickHouse foi instalado, você pode seguir as instruções abaixo para ativá-la ou desativá-la.

**Habilitando**

Para ativar manualmente a coleta do histórico de logs de métricas assíncronas em [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md), crie `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` com o seguinte conteúdo:

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**Desativando**

Para desativar a configuração `asynchronous_metric_log`, crie o seguinte arquivo `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` com o conteúdo abaixo:

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

Use o endereço de origem na autenticação de clientes conectados por meio de proxy.

:::note
Essa configuração deve ser usada com cautela redobrada, pois endereços encaminhados podem ser facilmente falsificados — servidores que aceitam esse tipo de autenticação não devem ser acessados diretamente, mas exclusivamente por meio de um proxy confiável.
:::

<div id="backups">
  ## backups
</div>

Configurações de backups, usadas ao executar as instruções [`BACKUP` e `RESTORE`](/pt-BR/operations/backup/overview).

As configurações a seguir podem ser definidas por subtags:

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','Determina se várias operações de backup podem ser executadas simultaneamente no mesmo host.', 'true'),
    ('allow_concurrent_restores', 'Bool', 'Determina se várias operações de restauração podem ser executadas simultaneamente no mesmo host.', 'true'),
    ('allowed_disk', 'String', 'Disco para o qual fazer backup ao usar `File()`. Esta configuração deve ser definida para usar `File`.', ''),
    ('allowed_path', 'String', 'Caminho para o qual fazer backup ao usar `File()`. Esta configuração deve ser definida para usar `File`.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', 'Número de tentativas de coletar metadados antes de aguardar em caso de inconsistência após comparar os metadados coletados.', '2'),
    ('collect_metadata_timeout', 'UInt64', 'Tempo limite em milissegundos para coletar metadados durante o backup.', '600000'),
    ('compare_collected_metadata', 'Bool', 'Se true, compara os metadados coletados com os metadados existentes para garantir que não sejam alterados durante o backup.', 'true'),
    ('create_table_timeout', 'UInt64', 'Tempo limite em milissegundos para criar tabelas durante a restauração.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', 'Número máximo de tentativas após encontrar um erro de versão inválida durante o backup/restauração coordenado.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Tempo máximo de espera, em milissegundos, antes da próxima tentativa de coletar metadados.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Tempo mínimo de espera, em milissegundos, antes da próxima tentativa de coletar metadados.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', 'Se o comando `BACKUP` falhar, o ClickHouse tentará remover os arquivos já copiados para o backup antes da falha; caso contrário, deixará os arquivos copiados como estão.', 'true'),
    ('sync_period_ms', 'UInt64', 'Período de sincronização, em milissegundos, para backup/restauração coordenado.', '5000'),
    ('test_inject_sleep', 'Bool', 'Espera relacionada a testes', 'false'),
    ('test_randomize_order', 'Bool', 'Se true, randomiza a ordem de determinadas operações para fins de teste.', 'false'),
    ('zookeeper_path', 'String', 'Caminho no ZooKeeper em que os metadados de backup e restauração são armazenados ao usar a cláusula `ON CLUSTER`.', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS Configuração, t.2 AS Tipo, t.3 AS Descrição, concat('`', t.4, '`') AS Padrão FROM settings FORMAT Markdown
  */ }

| Configuração                                        | Tipo   | Descrição                                                                                                                                                                 | Padrão                |
| :-------------------------------------------------- | :----- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | Determina se várias operações de backup podem ser executadas simultaneamente no mesmo host.                                                                               | `true`                |
| `allow_concurrent_restores`                         | Bool   | Determina se várias operações de restauração podem ser executadas simultaneamente no mesmo host.                                                                          | `true`                |
| `allowed_disk`                                      | String | Disco para o qual fazer backup ao usar `File()`. Essa configuração deve ser definida para usar `File`.                                                                    | &#96;&#96;            |
| `allowed_path`                                      | String | Caminho para o qual fazer backup ao usar `File()`. Essa configuração deve ser definida para usar `File`.                                                                  | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | Número de tentativas de coletar metadados antes de aguardar em caso de inconsistência após comparar os metadados coletados.                                               | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | Tempo limite, em milissegundos, para coletar metadados durante o backup.                                                                                                  | `600000`              |
| `compare_collected_metadata`                        | Bool   | Se true, compara os metadados coletados com os metadados existentes para garantir que não sejam alterados durante o backup.                                               | `true`                |
| `create_table_timeout`                              | UInt64 | Tempo limite, em milissegundos, para criar tabelas durante a restauração.                                                                                                 | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | Número máximo de tentativas de nova execução após encontrar um erro de versão inválida durante backup/restauração coordenados.                                            | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Tempo máximo de espera, em milissegundos, antes da próxima tentativa de coletar metadados.                                                                                | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Tempo mínimo de espera, em milissegundos, antes da próxima tentativa de coletar metadados.                                                                                | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | Se o comando `BACKUP` falhar, o ClickHouse tentará remover os arquivos já copiados para o backup antes da falha; caso contrário, deixará os arquivos copiados como estão. | `true`                |
| `sync_period_ms`                                    | UInt64 | Período de sincronização, em milissegundos, para backup/restauração coordenados.                                                                                          | `5000`                |
| `test_inject_sleep`                                 | Bool   | Pausa relacionada a testes                                                                                                                                                | `false`               |
| `test_randomize_order`                              | Bool   | Se true, embaralha a ordem de determinadas operações para fins de teste.                                                                                                  | `false`               |
| `zookeeper_path`                                    | String | Caminho no ZooKeeper onde os metadados de backup e restauração são armazenados ao usar a cláusula `ON CLUSTER`.                                                           | `/clickhouse/backups` |

Essa configuração é definida por padrão como:

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

Contém informações sobre todas as tarefas em segundo plano executadas por meio de diferentes pools em segundo plano.

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

<div id="bcrypt_workfactor">
  ## bcrypt_workfactor
</div>

Fator de trabalho para o tipo de autenticação `bcrypt_password`, que usa o [algoritmo Bcrypt](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/).
O fator de trabalho define a quantidade de processamento e o tempo necessários para calcular o hash e verificar a senha.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
Para aplicações com um alto volume de autenticações,
considere métodos de autenticação alternativos devido à
sobrecarga computacional do bcrypt com fatores de trabalho mais altos.
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

Se definido como true, os usuários precisam de um grant para criar uma tabela com um motor específico, por exemplo `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
Por padrão, para manter a compatibilidade retroativa, a criação de uma tabela com um motor de tabela específico ignora o grant. No entanto, você pode alterar esse comportamento definindo esta opção como true.
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

O intervalo, em segundos, antes de recarregar os dicionários integrados.

O ClickHouse recarrega os dicionários integrados a cada x segundos. Isso permite editar os dicionários &quot;dinamicamente&quot; sem reiniciar o servidor.

**Exemplo**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## compressão
</div>

Configurações de compressão de dados para tabelas com o motor [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

:::note
Recomendamos não alterar esta configuração se você começou a usar o ClickHouse há pouco tempo.
:::

**Modelo de configuração**:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

**Campos de `<case>`**:

* `min_part_size` – O tamanho mínimo de uma parte de dados.
* `min_part_size_ratio` – A proporção entre o tamanho da parte de dados e o tamanho da tabela.
* `method` – Método de compressão. Valores aceitos: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
* `level` – Nível de compressão. Consulte [Codecs](/pt-BR/sql-reference/statements/create/table#general-purpose-codecs).

:::note
Você pode configurar várias seções `<case>`.
:::

**Ações quando as condições são atendidas**:

* Se uma parte de dados corresponder a um conjunto de condições, o ClickHouse usa o método de compressão especificado.
* Se uma parte de dados corresponder a vários conjuntos de condições, o ClickHouse usa o primeiro conjunto de condições correspondente.

:::note
Se nenhuma condição for atendida para uma parte de dados, o ClickHouse usa a compressão `lz4`.
:::

**Exemplo**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

<div id="encryption">
  ## criptografia
</div>

Configura um comando para obter uma chave a ser usada por [codecs de criptografia](/pt-BR/sql-reference/statements/create/table#encryption-codecs). A chave (ou as chaves) deve ser armazenada em variáveis de ambiente ou definida no arquivo de configuração.

As chaves podem ser hexadecimais ou strings com comprimento de 16 bytes.

**Exemplo**

Carregando da configuração:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Não é recomendável armazenar chaves no arquivo de configuração. Isso não é seguro. Você pode mover as chaves para um arquivo de configuração separado em um disco seguro e criar um link simbólico para esse arquivo de configuração na pasta `config.d/`.
:::

Carregando a partir da configuração, quando a chave está em hexadecimal:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Carregando a chave a partir da variável de ambiente:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Aqui, `current_key_id` define a chave de criptografia atual, e todas as chaves especificadas podem ser usadas para descriptografar.

Cada um desses métodos pode ser aplicado a várias chaves:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Aqui, `current_key_id` mostra a chave atual de criptografia.

Além disso, os usuários podem adicionar um nonce, que deve ter 12 bytes de comprimento (por padrão, os processos de criptografia e descriptografia usam um nonce composto por bytes de valor zero):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Ou pode ser definido em hexadecimal:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Tudo o que foi mencionado acima também pode ser aplicado a `aes_256_gcm_siv` (mas a chave deve ter 32 bytes).
:::

<div id="error_log">
  ## error_log
</div>

Ele vem desativado por padrão.

**Habilitando**

Para ativar manualmente a coleta do histórico de erros em [`system.error_log`](../../operations/system-tables/error_log.md), crie `/etc/clickhouse-server/config.d/error_log.xml` com o seguinte conteúdo:

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**Desativando**

Para desativar a configuração `error_log`, crie o arquivo `/etc/clickhouse-server/config.d/disable_error_log.xml` com o seguinte conteúdo:

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

Lista dos prefixos usados para [configurações personalizadas](/pt-BR/operations/settings/query-level#custom_settings).
Vários prefixos devem ser separados por vírgulas.

**Exemplo**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**Veja também**

* [Configurações personalizadas](/pt-BR/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

Define o limite flexível para o tamanho do arquivo de core dump.

:::note
O limite rígido é definido por ferramentas do sistema
:::

**Exemplo**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

Perfil de configurações padrão. Os perfis de configurações ficam no arquivo especificado na configuração `user_config`.

**Exemplo**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

O caminho para o arquivo de configuração dos dicionários.

Caminho:

* Especifique o caminho absoluto ou um caminho relativo ao arquivo de configuração do servidor.
* O caminho pode conter caracteres curinga * e ?.

Ver também:

* &quot;[Dicionários](../../sql-reference/statements/create/dictionary/overview.md)&quot;.

**Exemplo**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

O caminho para o arquivo de configuração das funções executáveis definidas pelo usuário.

Caminho:

* Especifique o caminho absoluto ou o caminho relativo ao arquivo de configuração do servidor.
* O caminho pode conter caracteres curinga * e ?.

Veja também:

* &quot;[Funções executáveis definidas pelo usuário](/pt-BR/sql-reference/functions/udf#executable-user-defined-functions).&quot;.

**Exemplo**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

Envio de dados para o [Graphite](https://github.com/graphite-project).

Configurações:

* `host` – O servidor Graphite.
* `port` – A porta do servidor Graphite.
* `interval` – O intervalo de envio, em segundos.
* `timeout` – O tempo limite para envio de dados, em segundos.
* `root_path` – Prefixo das chaves.
* `metrics` – Envio de dados da tabela [system.metrics](/pt-BR/operations/system-tables/metrics).
* `events` – Envio de dados delta acumulados no período de tempo a partir da tabela [system.events](/pt-BR/operations/system-tables/events).
* `events_cumulative` – Envio de dados cumulativos da tabela [system.events](/pt-BR/operations/system-tables/events).
* `asynchronous_metrics` – Envio de dados da tabela [system.asynchronous&#95;metrics](/pt-BR/operations/system-tables/asynchronous_metrics).

Você pode configurar várias cláusulas `<graphite>`. Por exemplo, pode usá-las para enviar dados diferentes em intervalos diferentes.

**Exemplo**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

<div id="graphite_rollup">
  ## graphite_rollup
</div>

Configurações para reduzir o volume de dados do Graphite.

Para mais detalhes, consulte [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Exemplo**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

<div id="http_handlers">
  ## http_handlers
</div>

Permite usar handlers HTTP personalizados.
Para adicionar um novo handler HTTP, basta adicionar uma nova `<rule>`.
As regras são verificadas de cima para baixo, na ordem em que são definidas,
e a primeira correspondência executa o handler.
Uma regra sem condições de correspondência (apenas `handler`) corresponde a qualquer requisição; como as regras são verificadas em ordem,
essa regra só é útil como fallback, colocada no final.

As configurações a seguir podem ser definidas por subtags (todas essas subtags são opcionais, exceto `handler`):

| Sub-tags             | Definição                                                                                                                                                                                                                                                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | Para corresponder ao caminho da URL da requisição. A string de consulta é ignorada na correspondência                                                                                                                                                                                                        |
| `url_prefix`         | Para corresponder ao caminho da URL da requisição com um caminho base: o próprio caminho ou qualquer coisa abaixo dele em um limite de segmento de caminho (por exemplo, &#39;/api/v1&#39; corresponde a /api/v1, /api/v1/ e /api/v1/write, mas não a /api/v1beta). A string de consulta é ignorada na correspondência |
| `url_regexp`         | Para corresponder ao caminho da URL da requisição com uma expressão regular. A string de consulta é ignorada na correspondência                                                                                                                                                                              |
| `full_url`           | Para corresponder à URL completa da requisição `scheme://host:port/path`. A string de consulta é ignorada na correspondência, e o host é o endereço IP da connection (não o header `Host`)                                                                                                                    |
| `full_url_prefix`    | Para corresponder à URL completa da requisição `scheme://host:port/path` com a base URL `scheme://host:port/base_path`, em um limite de segmento de caminho (veja `url_prefix`). A string de consulta é ignorada na correspondência                                                                          |
| `full_url_regexp`    | Para corresponder à URL completa da requisição `scheme://host:port/path` com uma expressão regular. A string de consulta é ignorada na correspondência                                                                                                                                                        |
| `methods`            | Para corresponder aos métodos da requisição, você pode usar vírgulas para separar vários métodos                                                                                                                                                                                                              |
| `headers`            | Para corresponder aos headers da requisição, faça a correspondência de cada elemento filho (o nome do elemento filho é o nome do header)                                                                                                                                                                      |
| `headers_regexp`     | Como em `headers`, mas o valor de cada elemento filho é comparado com uma expressão regular                                                                                                                                                                                                                   |
| `empty_query_string` | Verifica se não há string de consulta na URL                                                                                                                                                                                                                                                                  |
| `handler`            | O handler da requisição (obrigatório)                                                                                                                                                                                                                                                                         |

:::note
Em vez de `url_regexp`, `full_url_regexp` e `headers_regexp`, você também pode escrever uma expressão regular em `url`, `full_url` ou `headers` usando o prefixo `regex:` (por exemplo, `<url>regex:/api/.*</url>`). Isso ainda é aceito por compatibilidade retroativa, mas está obsoleto: prefira as subtags dedicadas `url_regexp`, `full_url_regexp` e `headers_regexp`.
:::

`handler` contém as configurações a seguir, que podem ser definidas por subtags:

| Sub-tags           | Definição                                                                                                                                                                                           |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | Um local para redirecionamento                                                                                                                                                                      |
| `type`             | Tipos compatíveis: static, dynamic&#95;query&#95;handler, predefined&#95;query&#95;handler, redirect                                                                                                |
| `status`           | Usado com o tipo static, código de status da resposta                                                                                                                                               |
| `query_param_name` | Usado com o tipo dynamic&#95;query&#95;handler, extrai e executa o valor correspondente ao valor de `<query_param_name>` nos params da requisição HTTP                                              |
| `query`            | Usado com o tipo predefined&#95;query&#95;handler, executa a consulta quando o handler é chamado                                                                                                    |
| `content_type`     | Usado com o tipo static, content-type da resposta                                                                                                                                                   |
| `response_content` | Usado com o tipo static, conteúdo da Response enviado ao client; ao usar o prefixo &#39;file://&#39; ou &#39;config://&#39;, localiza o conteúdo no arquivo ou na configuration e o envia ao client |

Junto com uma lista de regras, você pode especificar `<defaults/>`, que habilita todos os handlers padrão.

Exemplo:

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

<div id="http_server_default_response">
  ## http_server_default_response
</div>

A página exibida por padrão ao acessar o servidor HTTP(s) do ClickHouse.
O valor padrão é &quot;Ok.&quot; (com uma quebra de linha no final)

**Exemplo**

Abre `https://tabix.io/` ao acessar `http://localhost: http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

Usado para adicionar cabeçalhos à resposta de uma requisição HTTP `OPTIONS`.
O método `OPTIONS` é usado ao fazer requisições preflight de CORS.

Para mais informações, consulte [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS).

Exemplo:

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

<div id="hsts_max_age">
  ## hsts_max_age
</div>

Tempo de expiração do HSTS, em segundos.

:::note
Um valor de `0` significa que o ClickHouse desativa o HSTS. Se você definir um número positivo, o HSTS será ativado e o max-age será o número definido.
:::

**Exemplo**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

Restrição aos hosts que podem trocar dados entre os servidores ClickHouse.
Se o Keeper for usado, a mesma restrição será aplicada à comunicação entre diferentes instâncias do Keeper.

:::note
Por padrão, o valor é igual à configuração [`listen_host`](#listen_host).
:::

**Exemplo**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

Tipo:

Padrão:

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

Um nome de usuário e uma senha usados para se conectar a outros servidores durante a [replicação](../../engines/table-engines/mergetree-family/replication.md). Além disso, o servidor autentica outras réplicas usando essas credenciais.
Portanto, `interserver_http_credentials` deve ser o mesmo para todas as réplicas em um cluster.

:::note

* Por padrão, se a seção `interserver_http_credentials` for omitida, a autenticação não será usada durante a replicação.
* As configurações de `interserver_http_credentials` não estão relacionadas a uma configuração de credenciais do cliente ClickHouse [configuration](../../interfaces/client.md#configuration_files).
* Essas credenciais são comuns para replicação via `HTTP` e `HTTPS`.
  :::

As configurações a seguir podem ser definidas por subtags:

* `user` — Nome de usuário.
* `password` — Senha.
* `allow_empty` — Se `true`, outras réplicas poderão se conectar sem autenticação, mesmo que as credenciais estejam definidas. Se `false`, conexões sem autenticação serão recusadas. Padrão: `false`.
* `old` — Contém o `user` e o `password` antigos usados durante a rotação de credenciais. Várias seções `old` podem ser especificadas.

**Rotação de credenciais**

O ClickHouse oferece suporte à rotação dinâmica de credenciais entre servidores sem interromper todas as réplicas ao mesmo tempo para atualizar a configuração delas. As credenciais podem ser alteradas em várias etapas.

Para habilitar a autenticação, defina `interserver_http_credentials.allow_empty` como `true` e adicione credenciais. Isso permite conexões com e sem autenticação.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

Depois de configurar todas as réplicas, defina `allow_empty` como `false` ou remova essa configuração. Isso torna obrigatória a autenticação com as novas credenciais.

Para alterar as credenciais existentes, mova o nome de usuário e a senha para a seção `interserver_http_credentials.old` e atualize `user` e `password` com os novos valores. Nesse momento, o servidor usa as novas credenciais para se conectar a outras réplicas e aceita conexões com as credenciais novas ou antigas.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

Quando novas credenciais forem aplicadas a todas as réplicas, as credenciais antigas poderão ser removidas.

<div id="ldap_servers">
  ## ldap_servers
</div>

Liste aqui os servidores LDAP com seus parâmetros de conexão para:

* usá-los como autenticadores para usuários locais dedicados, que têm o mecanismo de autenticação `ldap` especificado em vez de `password`
* usá-los como diretório de usuários remotos.

As seguintes configurações podem ser definidas por subtags:

| Setting                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bind_dn`                      | Modelo usado para construir o DN a ser usado no bind. O DN resultante será construído substituindo todas as substrings `\{user_name\}` do modelo pelo nome de usuário real durante cada tentativa de autenticação.                                                                                                                                                                                                                                                                     |
| `enable_tls`                   | Flag para ativar o uso de uma conexão segura com o servidor LDAP. Especifique `no` para o protocolo em texto simples (`ldap://`) (não recomendado). Especifique `yes` para LDAP sobre SSL/TLS (`ldaps://`) (recomendado e padrão). Especifique `starttls` para o protocolo StartTLS legacy (protocolo em texto simples (`ldap://`), atualizado para TLS).                                                                                                                                 |
| `host`                         | Hostname ou IP do servidor LDAP; esse parâmetro é obrigatório e não pode estar vazio.                                                                                                                                                                                                                                                                                                                                                                                                     |
| `port`                         | Porta do servidor LDAP; o padrão é 636 se `enable_tls` estiver definido como true, `389` caso contrário.                                                                                                                                                                                                                                                                                                                                                                                  |
| `tls_ca_cert_dir`              | path para o diretório que contém os certificados de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `tls_ca_cert_file`             | path para o arquivo do certificado de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tls_cert_file`                | path para o arquivo do certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `tls_cipher_suite`             | conjunto de cifras permitido (na notação do OpenSSL).                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `tls_key_file`                 | path para o arquivo da chave do certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `tls_minimum_protocol_version` | A versão mínima do protocolo SSL/TLS. Os valores aceitos são: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (padrão).                                                                                                                                                                                                                                                                                                                                                                      |
| `tls_require_cert`             | Comportamento da verificação de certificados do peer SSL/TLS. Os valores aceitos são: `never`, `allow`, `try`, `demand` (padrão).                                                                                                                                                                                                                                                                                                                                                         |
| `user_dn_detection`            | Seção com parâmetros de LDAP search para detectar o user DN real do usuário autenticado via bind. Isso é usado principalmente em search filters para mapeamento de função adicional quando o servidor é Active Directory. O user DN resultante será usado ao substituir as substrings `\{user_dn\}` onde elas forem permitidas. Por padrão, o user DN é definido como igual ao bind DN, mas, assim que a search for realizada, ele será atualizado com o valor real do user DN detectado. |
| `verification_cooldown`        | Um período de tempo, em segundos, após uma tentativa de bind bem-sucedida, durante o qual será assumido que um usuário foi autenticado com sucesso para todas as solicitações consecutivas sem contatar o servidor LDAP. Especifique `0` (padrão) para desativar o Caching e forçar o contato com o servidor LDAP em cada solicitação de autenticação.                                                                                                                                   |

A configuração `user_dn_detection` pode ser definida com subtags:

| Setting         | Description                                                                                                                                                                                                                                                                                                                                    |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | modelo usado para construir o base DN para a LDAP search. O DN resultante será construído substituindo todas as substrings `\{user_name\}` e `\{bind_dn\}` do modelo pelo nome de usuário real e pelo bind DN durante a LDAP search.                                                                                                           |
| `scope`         | escopo da LDAP search. Os valores aceitos são: `base`, `one_level`, `children`, `subtree` (padrão).                                                                                                                                                                                                                                            |
| `search_filter` | modelo usado para construir o search filter para a LDAP search. O filtro resultante será construído substituindo todas as substrings `\{user_name\}`, `\{bind_dn\}` e `\{base_dn\}` do modelo pelo nome de usuário real, bind DN e base DN durante a LDAP search. Observe que os caracteres especiais devem ser escapados corretamente em XML. |

Exemplo:

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

Exemplo (Active Directory típico com detecção do DN do usuário configurada para posterior mapeamento de função):

```xml
<my_ad_server>
    <host>localhost</host>
    <port>389</port>
    <bind_dn>EXAMPLE\{user_name}</bind_dn>
    <user_dn_detection>
        <base_dn>CN=Users,DC=example,DC=com</base_dn>
        <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
    </user_dn_detection>
    <enable_tls>no</enable_tls>
</my_ad_server>
```

<div id="listen_host">
  ## listen_host
</div>

Restrição aos hosts de onde as solicitações podem vir. Se você quiser que o servidor responda a todos eles, especifique `::`.

Exemplos:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## logger
</div>

A localização e o formato das mensagens de log.

**Chaves**:

| Key                          | Description                                                                                                                                                                                                                                                                                                                            |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async`                      | Quando `true` (padrão), o logging ocorre de forma assíncrona (uma thread em segundo plano por canal de saída). Caso contrário, o registro é feito na thread que chama LOG                                                                                                                                                              |
| `async_queue_max_size`       | Ao usar logging assíncrono, número máximo de mensagens mantidas na fila aguardando flush. Mensagens excedentes serão descartadas                                                                                                                                                                                                       |
| `console`                    | Habilita o logging no console. Defina como `1` ou `true` para habilitar. O padrão é `1` se o ClickHouse não estiver em execução no modo daemon; caso contrário, `0`.                                                                                                                                                                   |
| `console_log_level`          | Nível de log para a saída no console. O padrão é `level`.                                                                                                                                                                                                                                                                              |
| `console_shutdown_log_level` | Shutdown level é usado para definir o nível de log do console no desligamento do servidor.                                                                                                                                                                                                                                             |
| `console_startup_log_level`  | Startup level é usado para definir o nível de log do console na inicialização do servidor. Após a inicialização, o nível de log volta para a configuração `console_log_level`                                                                                                                                                          |
| `count`                      | Rotation policy: quantos arquivos de log históricos o ClickHouse mantém no máximo.                                                                                                                                                                                                                                                     |
| `errorlog`                   | O caminho para o error log file.                                                                                                                                                                                                                                                                                                       |
| `formatting.type`            | Formato de log para a saída no console. Atualmente, apenas `json` é compatível                                                                                                                                                                                                                                                         |
| `level`                      | Nível de log. Valores aceitos: `none` (desativa o logging), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,`debug`, `trace`, `test`                                                                                                                                                                                  |
| `log`                        | O caminho para o log file.                                                                                                                                                                                                                                                                                                             |
| `rotation`                   | Rotation policy: controla quando os log files são rotacionados. A rotação pode ser baseada em tamanho, tempo ou uma combinação de ambos. Exemplos: 100M, daily, 100M,daily. Quando o log file excede o tamanho especificado ou o intervalo de tempo especificado é atingido, ele é renomeado e arquivado, e um novo log file é criado. |
| `shutdown_level`             | Shutdown level é usado para definir o nível do root logger no desligamento do servidor.                                                                                                                                                                                                                                                |
| `size`                       | Rotation policy: tamanho máximo dos log files em bytes. Quando o tamanho do log file excede esse threshold, ele é renomeado e arquivado, e um novo log file é criado.                                                                                                                                                                  |
| `startup_level`              | Startup level é usado para definir o nível do root logger na inicialização do servidor. Após a inicialização, o nível de log volta para a configuração `level`                                                                                                                                                                         |
| `stream_compress`            | Comprime mensagens de log usando LZ4. Defina como `1` ou `true` para habilitar.                                                                                                                                                                                                                                                        |
| `syslog_level`               | Nível de log para logging no syslog.                                                                                                                                                                                                                                                                                                   |
| `use_syslog`                 | Também encaminha a saída de log para o syslog.                                                                                                                                                                                                                                                                                         |

**Especificadores de formato de log**

Os nomes de arquivo nos caminhos `log` e `errorLog` aceitam os especificadores de formato abaixo para o nome de arquivo resultante (a parte do diretório não os aceita).

A coluna &quot;Example&quot; mostra a saída em `2023-07-06 18:32:07`.

| Especificador | Descrição                                                                                                                                                                                  | Exemplo                    |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------- |
| `%%`          | % literal                                                                                                                                                                                  | `%`                        |
| `%n`          | Caractere de nova linha                                                                                                                                                                    |                            |
| `%t`          | Caractere de tabulação horizontal                                                                                                                                                          |                            |
| `%Y`          | Ano como número decimal, por exemplo 2017                                                                                                                                                  | `2023`                     |
| `%y`          | Últimos 2 dígitos do ano como número decimal (intervalo [00,99])                                                                                                                           | `23`                       |
| `%C`          | Primeiros 2 dígitos do ano como número decimal (intervalo [00,99])                                                                                                                         | `20`                       |
| `%G`          | [Ano baseado em semanas ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) com quatro dígitos, ou seja, o ano que contém a semana especificada. Normalmente útil apenas com `%V` | `2023`                     |
| `%g`          | Últimos 2 dígitos do [ano baseado em semanas ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), ou seja, o ano que contém a semana especificada                                 | `23`                       |
| `%b`          | Nome abreviado do mês, por exemplo Oct (dependente da localidade)                                                                                                                          | `Jul`                      |
| `%h`          | Sinônimo de %b                                                                                                                                                                             | `Jul`                      |
| `%B`          | Nome completo do mês, por exemplo October (dependente da localidade)                                                                                                                       | `July`                     |
| `%m`          | Mês como número decimal (intervalo [01,12])                                                                                                                                                | `07`                       |
| `%U`          | Número da semana do ano em formato decimal (domingo é o primeiro dia da semana) (intervalo [00,53])                                                                                        | `27`                       |
| `%W`          | Número da semana do ano em formato decimal (segunda-feira é o primeiro dia da semana) (intervalo [00,53])                                                                                  | `27`                       |
| `%V`          | Número da semana ISO 8601 (intervalo [01,53])                                                                                                                                              | `27`                       |
| `%j`          | Dia do ano como número decimal (intervalo [001,366])                                                                                                                                       | `187`                      |
| `%d`          | Dia do mês como número decimal com zero à esquerda (intervalo [01,31]). Um único dígito é precedido por zero.                                                                              | `06`                       |
| `%e`          | Dia do mês como número decimal com espaço à esquerda (intervalo [1,31]). Um único dígito é precedido por um espaço.                                                                        | `&nbsp; 6`                 |
| `%a`          | Nome abreviado do dia da semana, por exemplo Fri (dependente da localidade)                                                                                                                | `Thu`                      |
| `%A`          | Nome completo do dia da semana, por exemplo Friday (dependente da localidade)                                                                                                              | `Thursday`                 |
| `%w`          | Dia da semana como número inteiro, com domingo como 0 (intervalo [0-6])                                                                                                                    | `4`                        |
| `%u`          | Dia da semana como número decimal, em que segunda-feira é 1 (formato ISO 8601) (intervalo [1-7])                                                                                           | `4`                        |
| `%H`          | Hora como número decimal, no formato de 24 horas (intervalo [00-23])                                                                                                                       | `18`                       |
| `%I`          | Hora como número decimal, no formato de 12 horas (intervalo [01,12])                                                                                                                       | `06`                       |
| `%M`          | Minuto como número decimal (intervalo [00,59])                                                                                                                                             | `32`                       |
| `%S`          | Segundo como número decimal (intervalo [00,60])                                                                                                                                            | `07`                       |
| `%c`          | String padrão de data e hora, por exemplo Sun Oct 17 04:41:13 2010 (dependente da localidade)                                                                                              | `Thu Jul  6 18:32:07 2023` |
| `%x`          | Representação localizada da data (dependente da localidade)                                                                                                                                | `07/06/23`                 |
| `%X`          | Representação localizada da hora, por exemplo 18:40:20 ou 6:40:20 PM (dependente da localidade)                                                                                            | `18:32:07`                 |
| `%D`          | Data curta no formato MM/DD/YY, equivalente a %m/%d/%y                                                                                                                                     | `07/06/23`                 |
| `%F`          | Data curta no formato AAAA-MM-DD, equivalente a %Y-%m-%d                                                                                                                                   | `2023-07-06`               |
| `%r`          | Hora local no formato de 12 horas (dependente da localidade)                                                                                                                               | `06:32:07 PM`              |
| `%R`          | Equivalente a &quot;%H:%M&quot;                                                                                                                                                            | `18:32`                    |
| `%T`          | Equivalente a &quot;%H:%M:%S&quot; (o formato de hora ISO 8601)                                                                                                                            | `18:32:07`                 |
| `%p`          | Indicador local de a.m. ou p.m. (dependente da localidade)                                                                                                                                 | `PM`                       |
| `%z`          | Deslocamento em relação a UTC no formato ISO 8601 (por exemplo, -0430), ou nenhum caractere se as informações de fuso horário não estiverem disponíveis                                    | `+0800`                    |
| `%Z`          | Nome ou abreviação do fuso horário dependente da localidade, ou nenhum caractere se as informações de fuso horário não estiverem disponíveis                                               | `Z AWST `                  |

**Exemplo**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

Para imprimir mensagens de log somente no console:

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**Sobrescritas por nível**

É possível sobrescrever o nível de log de loggers individuais. Por exemplo, para silenciar todas as mensagens dos loggers &quot;Backup&quot; e &quot;RBAC&quot;.

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

Para gravar também mensagens de log no syslog:

```xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

Chaves para `<syslog>`:

| Chave      | Descrição                                                                                                                                                                                                                                                                                                       |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | O endereço do syslog no formato `host\[:port\]`. Se omitido, o daemon local será usado.                                                                                                                                                                                                                         |
| `hostname` | O nome do host de onde os logs são enviados (opcional).                                                                                                                                                                                                                                                         |
| `facility` | A [palavra-chave](https://en.wikipedia.org/wiki/Syslog#Facility) de facility no syslog. Deve ser especificada em letras maiúsculas com o prefixo &quot;LOG&#95;&quot;, por exemplo, `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` etc. Padrão: `LOG_USER` se `address` for especificado; caso contrário, `LOG_DAEMON`. |
| `format`   | Formato da mensagem de log. Possíveis valores: `bsd` e `syslog.`                                                                                                                                                                                                                                                |

**Formatos de log**

Você pode especificar o formato de log que será usado na saída do log do console. Atualmente, apenas JSON é compatível.

**Exemplo**

Veja um exemplo de log JSON de saída:

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

Para habilitar o suporte a logs em JSON, use o trecho a seguir:

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**Renomeando chaves em logs JSON**

Os nomes das chaves podem ser modificados alterando os valores das tags dentro da tag `<names>`. Por exemplo, para alterar `DATE_TIME` para `MY_DATE_TIME`, você pode usar `<date_time>MY_DATE_TIME</date_time>`.

**Omitindo chaves em logs JSON**

As propriedades do log podem ser omitidas comentando a propriedade. Por exemplo, se você não quiser que o log imprima `query_id`, pode comentar a tag `<query_id>`.

<div id="send_crash_reports">
  ## send_crash_reports
</div>

Configurações para o envio de relatórios de falha à equipe principal de desenvolvedores do ClickHouse.

A ativação dessa configuração, especialmente em ambientes de pré-produção, é muito apreciada.

Chaves:

| Key                   | Description                                                                                                                       |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`             | Flag booleana para habilitar o recurso, `true` por padrão. Defina como `false` para evitar o envio de relatórios de falha.        |
| `endpoint`            | Você pode substituir a endpoint URL usada para enviar relatórios de falha.                                                        |
| `send_logical_errors` | `LOGICAL_ERROR` é como um `assert`; é um bug no ClickHouse. Esta flag booleana habilita o envio dessas exceções (Padrão: `true`). |

**Uso recomendado**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

A parte pública da chave do host será gravada no arquivo known&#95;hosts
no lado do cliente SSH na primeira conexão.

As configurações de chave de host ficam inativas por padrão.
Remova os comentários das configurações de chave de host e forneça o caminho para a respectiva chave SSH para ativá-las:

Exemplo:

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

<div id="tcp_ssh_port">
  ## tcp_ssh_port
</div>

Porta do servidor SSH que permite ao usuário se conectar e executar consultas de forma interativa usando o cliente embutido via PTY.

Exemplo:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## storage_configuration
</div>

Permite a configuração de armazenamento em vários discos.

A configuração de armazenamento segue a estrutura:

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

<div id="configuration-of-disks">
  ### Configuração de disks
</div>

A configuração de `disks` segue a estrutura abaixo:

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

As subtags acima definem as seguintes configurações para `disks`:

| Configuração            | Descrição                                                                                                     |
| ----------------------- | ------------------------------------------------------------------------------------------------------------- |
| `<disk_name_N>`         | O nome do disco, que deve ser exclusivo.                                                                      |
| `path`                  | O caminho em que os dados do servidor serão armazenados (diretórios `data` e `shadow`). Deve terminar com `/` |
| `keep_free_space_bytes` | Tamanho do espaço livre reservado no disco.                                                                   |

:::note
A ordem dos `disks` não importa.
:::

<div id="configuration-of-policies">
  ### Configuração de políticas
</div>

As subtags acima definem as seguintes configurações para `policies`:

| Configuração                 | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy_name_N`              | Nome da política. Os nomes das políticas devem ser únicos.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `volume_name_N`              | Nome do volume. Os nomes dos volumes devem ser únicos.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `disk`                       | O disco localizado dentro do volume.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `max_data_part_size_bytes`   | O tamanho máximo de um fragmento de dados que pode ficar em qualquer um dos discos deste volume. Se a mesclagem resultar em um fragmento com tamanho previsto maior que max&#95;data&#95;part&#95;size&#95;bytes, o fragmento será gravado no próximo volume. Basicamente, esse recurso permite armazenar fragmentos novos/pequenos em um volume quente (SSD) e movê-los para um volume frio (HDD) quando atingirem um tamanho maior. Não use esta opção se a política tiver apenas um volume.                                                                         |
| `move_factor`                | A parcela do espaço livre disponível no volume. Se o espaço ficar abaixo desse limite, os dados começarão a ser transferidos para o próximo volume, se houver. Para a transferência, os fragmentos são ordenados por tamanho, do maior para o menor (ordem decrescente), e são selecionados os fragmentos cujo tamanho total seja suficiente para atender à condição `move_factor`; se o tamanho total de todos os fragmentos for insuficiente, todos os fragmentos serão movidos.                                                                                     |
| `perform_ttl_move_on_insert` | Desabilita a movimentação, durante a inserção, de dados com TTL expirado. Por padrão (se habilitado), se inserirmos um dado que já expirou de acordo com a regra de movimentação por TTL, ele será imediatamente movido para o volume/disco especificado na regra de movimentação. Isso pode tornar a inserção significativamente mais lenta caso o volume/disco de destino seja lento (por exemplo, S3). Se desabilitado, a parte expirada dos dados é gravada no volume padrão e então imediatamente movida para o volume especificado na regra para o TTL expirado. |
| `load_balancing`             | Política de balanceamento entre discos, `round_robin` ou `least_used`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `least_used_ttl_ms`          | Define o timeout (em milissegundos) para atualizar o espaço disponível em todos os discos (`0` - sempre atualizar, `-1` - nunca atualizar, o valor padrão é `60000`). Observe que, se o disco for usado apenas pelo ClickHouse e não estiver sujeito a redimensionamento dinâmico do sistema de arquivos, você pode usar o valor `-1`. Em todos os outros casos, isso não é recomendado, pois acabará levando a uma alocação incorreta de espaço.                                                                                                                      |
| `prefer_not_to_merge`        | Desabilita a mesclagem de partes dos dados neste volume. Observação: isso é potencialmente prejudicial e pode causar lentidão. Quando esta configuração está habilitada (não faça isso), a mesclagem de dados neste volume é proibida (o que é ruim). Isso permite controlar como o ClickHouse interage com discos lentos. Recomendamos não usar isso de forma alguma.                                                                                                                                                                                                 |
| `volume_priority`            | Define a prioridade (ordem) em que os volumes são preenchidos. Quanto menor o valor, maior a prioridade. Os valores do parâmetro devem ser números naturais e cobrir o intervalo de 1 a N (N é o maior valor de parâmetro especificado), sem lacunas.                                                                                                                                                                                                                                                                                                                  |

Para `volume_priority`:

* Se todos os volumes tiverem esse parâmetro, eles serão priorizados na ordem especificada.
* Se apenas *alguns* volumes o tiverem, os volumes que não o tiverem terão a menor prioridade. Aqueles que o tiverem serão priorizados de acordo com o valor da tag; a prioridade dos demais é determinada pela ordem em que aparecem no arquivo de configuração, em relação uns aos outros.
* Se *nenhum* volume receber esse parâmetro, sua ordem será determinada pela ordem em que aparecem no arquivo de configuração.
* A prioridade dos volumes não pode ser igual.

<div id="macros">
  ## macros
</div>

Substituições de parâmetros para tabelas replicadas.

Pode ser omitido caso tabelas replicadas não sejam usadas.

Para mais informações, consulte a seção [Criação de tabelas replicadas](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Exemplo**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

Nome do grupo de réplicas do banco de dados Replicated.

O cluster criado pelo banco de dados Replicated será composto por réplicas do mesmo grupo.
As consultas DDL aguardarão apenas as réplicas do mesmo grupo.

Vazio por padrão.

**Exemplo**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

Tempo limite máximo da sessão, em segundos.

Exemplo:

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

Ajustes finos para tabelas no [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Para mais informações, consulte o arquivo de cabeçalho MergeTreeSettings.h.

**Exemplo**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

Está desabilitado por padrão.

**Habilitando**

Para ativar manualmente a coleta do histórico de métricas [`system.metric_log`](../../operations/system-tables/metric_log.md), crie `/etc/clickhouse-server/config.d/metric_log.xml` com o seguinte conteúdo:

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**Desabilitação**

Para desabilitar a configuração `metric_log`, crie o arquivo `/etc/clickhouse-server/config.d/disable_metric_log.xml` com o seguinte conteúdo:

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

Ajustes finos para tabelas no [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Esta configuração tem prioridade mais alta.

Para mais informações, consulte o arquivo de cabeçalho MergeTreeSettings.h.

**Exemplo**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

Configurações da tabela do sistema [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md).

<SystemLogParameters />

Exemplo:

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

<div id="openSSL">
  ## openSSL
</div>

Configuração SSL de cliente/servidor.

O suporte a SSL é fornecido pela biblioteca `libpoco`. As opções de configuração disponíveis são explicadas em [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Os valores padrão podem ser encontrados em [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Chaves das configurações de servidor/cliente:

| Opção                         | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Valor padrão                                                                               |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | Ativa ou desativa o cache de sessões. Deve ser usado em conjunto com `sessionIdContext`. Valores aceitáveis: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                             | `false`                                                                                    |
| `caConfig`                    | Caminho para o arquivo ou diretório que contém certificados de CA confiáveis. Se apontar para um arquivo, ele deve estar no formato PEM e pode conter vários certificados de CA. Se apontar para um diretório, ele deve conter um arquivo .pem para cada certificado de CA. Os nomes dos arquivos são pesquisados com base no valor de hash do nome do subject da CA. Detalhes podem ser encontrados na página de manual de [SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html). |                                                                                            |
| `certificateFile`             | Caminho para o arquivo de certificado de cliente/servidor no formato PEM. Você pode omiti-lo se `privateKeyFile` contiver o certificado.                                                                                                                                                                                                                                                                                                                                                                                                                  |                                                                                            |
| `cipherList`                  | Cifras OpenSSL compatíveis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | Protocolos cujo uso não é permitido.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |                                                                                            |
| `extendedVerification`        | Se ativado, verifica se o CN ou SAN do certificado corresponde ao hostname do peer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `false`                                                                                    |
| `fips`                        | Ativa o modo FIPS do OpenSSL. Compatível se a versão da biblioteca OpenSSL oferecer suporte a FIPS.                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `false`                                                                                    |
| `invalidCertificateHandler`   | Classe (uma subclasse de CertificateHandler) usada para verificar certificados inválidos. Por exemplo: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .                                                                                                                                                                                                                                                                                                                                                 | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | Se os certificados de CA integrados do OpenSSL serão usados. O ClickHouse presume que os certificados de CA integrados estejam no arquivo `/etc/ssl/cert.pem` (respectivamente, no diretório `/etc/ssl/certs`) ou no arquivo (respectivamente, no diretório) especificado pela variável de ambiente `SSL_CERT_FILE` (respectivamente, `SSL_CERT_DIR`).                                                                                                                                                                                                    | `true`                                                                                     |
| `preferServerCiphers`         | Cifras de servidor preferidas pelo cliente.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `false`                                                                                    |
| `privateKeyFile`              | Caminho para o arquivo com a chave privada do certificado PEM. O arquivo pode conter a chave e o certificado ao mesmo tempo.                                                                                                                                                                                                                                                                                                                                                                                                                              |                                                                                            |
| `privateKeyPassphraseHandler` | Classe (subclasse de PrivateKeyPassphraseHandler) que solicita a senha para acessar a chave privada. Por exemplo: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                                                                                                                                                                       | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | Requer uma conexão TLSv1. Valores aceitos: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `false`                                                                                    |
| `requireTLSv1_1`              | Exige uma conexão TLSv1.1. Valores aceitos: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `false`                                                                                    |
| `requireTLSv1_2`              | Requer uma conexão TLSv1.2. Valores aceitos: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `false`                                                                                    |
| `sessionCacheSize`            | O número máximo de sessões que o servidor mantém em cache. Um valor de `0` significa um número ilimitado de sessões.                                                                                                                                                                                                                                                                                                                                                                                                                                      | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | Um conjunto exclusivo de caracteres aleatórios que o servidor acrescenta a cada identificador gerado. O comprimento da string não deve exceder `SSL_MAX_SSL_SESSION_ID_LENGTH`. Este parâmetro é sempre recomendado, pois ajuda a evitar problemas tanto quando o servidor armazena a sessão em cache quanto quando o cliente solicita cache.                                                                                                                                                                                                             | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | Tempo de cache da sessão no servidor, em horas.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `2`                                                                                        |
| `verificationDepth`           | O comprimento máximo da cadeia de verificação. A verificação falhará se o comprimento da cadeia de certificados exceder o valor definido.                                                                                                                                                                                                                                                                                                                                                                                                                 | `9`                                                                                        |
| `verificationMode`            | O método de verificação dos certificados do nó. Os detalhes estão na descrição da classe [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h). Valores possíveis: `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                                                                                                                                              | `relaxed`                                                                                  |

**Exemplo de configurações:**

```xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

<div id="part_log">
  ## part_log
</div>

Eventos de log associados ao [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), como adição ou mesclagem de dados. Você pode usar o log para simular algoritmos de mesclagem e comparar suas características. Também é possível visualizar o processo de mesclagem.

As consultas são registradas na tabela [system.part&#95;log](/pt-BR/operations/system-tables/part_log), não em um arquivo separado. Você pode configurar o nome dessa tabela no parâmetro `table` (veja abaixo).

<SystemLogParameters />

**Exemplo**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

<div id="processors_profile_log">
  ## processors_profile_log
</div>

Configurações da tabela de sistema [`processors_profile_log`](../system-tables/processors_profile_log.md).

<SystemLogParameters />

As configurações padrão são:

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

<div id="prometheus">
  ## prometheus
</div>

Disponibiliza dados de métricas para coleta pelo [Prometheus](https://prometheus.io).

Configurações:

* `endpoint` – endpoint HTTP para a coleta de métricas pelo servidor Prometheus. Deve começar com &#39;/&#39;.
* `port` – Porta do `endpoint`.
* `metrics` – Expõe métricas da tabela [system.metrics](/pt-BR/operations/system-tables/metrics).
* `events` – Expõe métricas da tabela [system.events](/pt-BR/operations/system-tables/events).
* `asynchronous_metrics` – Expõe os valores atuais das métricas da tabela [system.asynchronous&#95;metrics](/pt-BR/operations/system-tables/asynchronous_metrics).
* `errors` - Expõe o número de erros, por código de erro, ocorridos desde a última reinicialização do servidor. Essas informações também podem ser obtidas na tabela [system.errors](/pt-BR/operations/system-tables/errors).

**Exemplo**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

Verifique (substitua `127.0.0.1` pelo endereço IP ou hostname do seu servidor ClickHouse):

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

Configuração para registrar as consultas recebidas com a configuração [log&#95;queries=1](../../operations/settings/settings.md).

As consultas são registradas na tabela [system.query&#95;log](/pt-BR/operations/system-tables/query_log), e não em um arquivo separado. Você pode alterar o nome da tabela no parâmetro `table` (veja abaixo).

<SystemLogParameters />

Se a tabela não existir, o ClickHouse a criará. Se a estrutura do query log tiver sido alterada quando o servidor ClickHouse foi atualizado, a tabela com a estrutura antiga será renomeada, e uma nova tabela será criada automaticamente.

**Exemplo**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

<div id="query_metric_log">
  ## query_metric_log
</div>

Ele vem desabilitado por padrão.

**Habilitação**

Para ativar manualmente a coleta do histórico de métricas [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md), crie `/etc/clickhouse-server/config.d/query_metric_log.xml` com o seguinte conteúdo:

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**Desativando**

Para desativar a configuração `query_metric_log`, crie o seguinte arquivo `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` com o conteúdo a seguir:

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

Configuração do [cache de consultas](../query-cache.md).

As seguintes configurações estão disponíveis:

| Configuração              | Descrição                                                                                                 | Valor padrão |
| ------------------------- | --------------------------------------------------------------------------------------------------------- | ------------ |
| `max_entries`             | O número máximo de resultados de consultas `SELECT` armazenados no cache.                                 | `1024`       |
| `max_entry_size_in_bytes` | O tamanho máximo, em bytes, que os resultados de consultas `SELECT` podem ter para serem salvos no cache. | `1048576`    |
| `max_entry_size_in_rows`  | O número máximo de linhas que os resultados de consultas `SELECT` podem ter para serem salvos no cache.   | `30000000`   |
| `max_size_in_bytes`       | O tamanho máximo do cache em bytes. `0` significa que o cache de consultas está desabilitado.             | `1073741824` |

:::note

* As configurações alteradas entram em vigor imediatamente.
* Os dados do cache de consultas são alocados na DRAM. Se houver pouca memória disponível, defina um valor baixo para `max_size_in_bytes` ou desabilite completamente o cache de consultas.
  :::

**Exemplo**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

<div id="query_thread_log">
  ## query_thread_log
</div>

Configuração para registrar as threads de consultas recebidas com a configuração [log&#95;query&#95;threads=1](/pt-BR/operations/settings/settings#log_query_threads).

As consultas são registradas na tabela [system.query&#95;thread&#95;log](/pt-BR/operations/system-tables/query_thread_log), e não em um arquivo separado. Você pode alterar o nome da tabela no parâmetro `table` (veja abaixo).

<SystemLogParameters />

Se a tabela não existir, o ClickHouse a criará. Se a estrutura do log de threads de consulta tiver sido alterada após a atualização do servidor ClickHouse, a tabela com a estrutura antiga será renomeada, e uma nova tabela será criada automaticamente.

**Exemplo**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

<div id="query_views_log">
  ## query_views_log
</div>

Configuração para registrar views (live, materialized etc.) com base nas consultas recebidas com a configuração [log&#95;query&#95;views=1](/pt-BR/operations/settings/settings#log_query_views).

As consultas são registradas na tabela [system.query&#95;views&#95;log](/pt-BR/operations/system-tables/query_views_log), e não em um arquivo separado. Você pode alterar o nome da tabela no parâmetro `table` (veja abaixo).

<SystemLogParameters />

Se a tabela não existir, o ClickHouse a criará. Se a estrutura do log de query views tiver mudado quando o servidor ClickHouse for atualizado, a tabela com a estrutura antiga será renomeada, e uma nova tabela será criada automaticamente.

**Exemplo**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

<div id="text_log">
  ## text_log
</div>

Configurações da tabela de sistema [text&#95;log](/pt-BR/operations/system-tables/text_log) para registrar mensagens de texto.

<SystemLogParameters />

Além disso:

| Configuração | Descrição                                                                     | Valor padrão |
| ------------ | ----------------------------------------------------------------------------- | ------------ |
| `level`      | Nível máximo de mensagem (por padrão, `Trace`) que será armazenado na tabela. | `Trace`      |

**Exemplo**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

<div id="trace_log">
  ## trace_log
</div>

Configurações para a operação da tabela de sistema [trace&#95;log](/pt-BR/operations/system-tables/trace_log).

<SystemLogParameters />

O arquivo de configuração padrão do servidor `config.xml` contém a seguinte seção de configurações:

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

<div id="asynchronous_insert_log">
  ## asynchronous_insert_log
</div>

Configurações da tabela de sistema [asynchronous&#95;insert&#95;log](/pt-BR/operations/system-tables/asynchronous_insert_log) para o registro de inserções assíncronas.

<SystemLogParameters />

**Exemplo**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

<div id="crash_log">
  ## crash_log
</div>

Configurações para a operação da tabela de sistema [crash&#95;log](../../operations/system-tables/crash_log.md).

As configurações a seguir podem ser definidas por subtags:

| Setting                            | Description                                                                                                                                                                 | Default             | Note                                                                                                                                               |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `buffer_size_rows_flush_threshold` | Limiar para a quantidade de linhas. Se o limiar for atingido, a gravação dos logs no disco será iniciada em segundo plano.                                                  | `max_size_rows / 2` |                                                                                                                                                    |
| `database`                         | Nome do banco de dados.                                                                                                                                                     |                     |                                                                                                                                                    |
| `engine`                           | [Definição do motor MergeTree](/pt-BR/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) para uma tabela de sistema.                       |                     | Não pode ser usado se `partition_by` ou `order_by` estiver definido. Se não for especificado, `MergeTree` será selecionado por padrão              |
| `flush_interval_milliseconds`      | Intervalo para gravar os dados do buffer em memória na tabela.                                                                                                              | `7500`              |                                                                                                                                                    |
| `flush_on_crash`                   | Define se os logs devem ser gravados no disco em caso de falha.                                                                                                             | `false`             |                                                                                                                                                    |
| `max_size_rows`                    | Tamanho máximo em linhas para os logs. Quando a quantidade de logs ainda não gravados atinge `max_size`, os logs são gravados no disco.                                     | `1024`              |                                                                                                                                                    |
| `order_by`                         | [Chave de ordenação personalizada](/pt-BR/engines/table-engines/mergetree-family/mergetree#order_by) para uma tabela de sistema. Não pode ser usada se `engine` estiver definido. |                     | Se `engine` for especificado para a tabela de sistema, o parâmetro `order_by` deverá ser especificado diretamente dentro de &#39;engine&#39;       |
| `partition_by`                     | [Chave de particionamento personalizada](/pt-BR/engines/table-engines/mergetree-family/custom-partitioning-key.md) para uma tabela de sistema.                                    |                     | Se `engine` for especificado para a tabela de sistema, o parâmetro `partition_by` deverá ser especificado diretamente dentro de &#39;engine&#39;   |
| `reserved_size_rows`               | Tamanho da memória pré-alocada, em linhas, para os logs.                                                                                                                    | `1024`              |                                                                                                                                                    |
| `settings`                         | [Parâmetros adicionais](/pt-BR/engines/table-engines/mergetree-family/mergetree/#settings) que controlam o comportamento do MergeTree (opcional).                                 |                     | Se `engine` for especificado para a tabela de sistema, o parâmetro `settings` deverá ser especificado diretamente dentro de &#39;engine&#39;       |
| `storage_policy`                   | Nome da política de armazenamento a ser usada para a tabela (opcional).                                                                                                     |                     | Se `engine` for especificado para a tabela de sistema, o parâmetro `storage_policy` deverá ser especificado diretamente dentro de &#39;engine&#39; |
| `table`                            | Nome da tabela de sistema.                                                                                                                                                  |                     |                                                                                                                                                    |
| `ttl`                              | Especifica o [TTL](/pt-BR/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) da tabela.                                                                 |                     | Se `engine` for especificado para a tabela de sistema, o parâmetro `ttl` deverá ser especificado diretamente dentro de &#39;engine&#39;            |

O arquivo de configuração do servidor `config.xml` padrão contém a seguinte seção de configurações:

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

<div id="custom_cached_disks_base_directory">
  ## custom_cached_disks_base_directory
</div>

Esta configuração especifica o caminho do cache para discos com cache personalizados (criados via SQL).
`custom_cached_disks_base_directory` tem prioridade sobre `filesystem_caches_path` para discos personalizados (encontrado em `filesystem_caches_path.xml`),
que é usado se o primeiro não estiver definido.
O caminho da configuração do filesystem cache deve estar dentro desse diretório,
caso contrário, será lançada uma exceção, impedindo a criação do disco.

:::note
Isso não afetará discos criados em uma versão mais antiga para os quais o servidor foi atualizado.
Nesse caso, nenhuma exceção será lançada, para permitir que o servidor seja iniciado com êxito.
:::

Exemplo:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

Configurações da tabela de sistema [backup&#95;log](../../operations/system-tables/backup_log.md) para registrar as operações `BACKUP` e `RESTORE`.

<SystemLogParameters />

**Exemplo**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

<div id="blob_storage_log">
  ## blob_storage_log
</div>

Configurações da tabela de sistema [`blob_storage_log`](../system-tables/blob_storage_log.md).

<SystemLogParameters />

Exemplo:

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

<div id="query_masking_rules">
  ## query_masking_rules
</div>

Regras baseadas em Regexp, aplicadas às consultas e também a todas as mensagens de log antes de serem armazenadas nos logs do servidor,
nas tabelas [`system.query_log`](/pt-BR/operations/system-tables/query_log), [`system.text_log`](/pt-BR/operations/system-tables/text_log), [`system.processes`](/pt-BR/operations/system-tables/processes) e nos logs enviados ao cliente. Isso ajuda a evitar
o vazamento de dados sensíveis de consultas SQL, como nomes, e-mails, identificadores pessoais ou números de cartão de crédito, para os logs.

**Exemplo**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**Campos de configuração**:

| Configuração | Descrição                                                                           |
| ------------ | ----------------------------------------------------------------------------------- |
| `name`       | nome da regra (opcional)                                                            |
| `regexp`     | expressão regular compatível com RE2 (obrigatório)                                  |
| `replace`    | string de substituição para dados sensíveis (opcional, por padrão: seis asteriscos) |

As regras de mascaramento são aplicadas à consulta inteira (para evitar o vazamento de dados sensíveis de consultas malformadas / que não podem ser analisadas).

A tabela [`system.events`](/pt-BR/operations/system-tables/events) tem o contador `QueryMaskingRulesMatch`, que representa o número total de correspondências com as regras de mascaramento de consultas.

Para consultas distribuídas, cada servidor precisa ser configurado separadamente; caso contrário, as subconsultas enviadas a outros
nós serão armazenadas sem mascaramento.

<div id="remote_servers">
  ## remote_servers
</div>

Configuração dos clusters usados pelo motor de tabela [Distributed](../../engines/table-engines/special/distributed.md) e pela função de tabela `cluster`.

**Exemplo**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

Para o valor do atributo `incl`, consulte a seção &quot;[Arquivos de configuração](/pt-BR/operations/configuration-files)&quot;.

**Veja também**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [Cluster Discovery](../../operations/cluster-discovery.md)
* [Replicated database engine](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

Lista de hosts permitidos para uso em motores de armazenamento relacionados a URL e em funções de tabela.

Ao adicionar um host com a tag XML `\<host\>`:

* ele deve ser especificado exatamente como na URL, pois o nome é verificado antes da resolução de DNS. Por exemplo: `<host>clickhouse.com</host>`
* se a porta for especificada explicitamente na URL, então host:port será verificado como um todo. Por exemplo: `<host>clickhouse.com:80</host>`
* se o host for especificado sem porta, qualquer porta desse host será permitida. Por exemplo: se `<host>clickhouse.com</host>` for especificado, então `clickhouse.com:20` (FTP), `clickhouse.com:80` (HTTP), `clickhouse.com:443` (HTTPS) etc. serão permitidos.
* se o host for especificado como um endereço IP, ele será verificado conforme especificado na URL. Por exemplo: `[2a02:6b8:a::a]`.
* se houver redirecionamentos e o suporte a redirecionamentos estiver habilitado, cada redirecionamento (o campo location) será verificado.

Por exemplo:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## timezone
</div>

O fuso horário do servidor.

Especificado como um identificador IANA do fuso horário UTC ou de uma localização geográfica (por exemplo, Africa/Abidjan).

O fuso horário é necessário para conversões entre os formatos String e DateTime quando campos DateTime são exibidos em formato de texto (na tela ou em um arquivo) e ao obter um valor DateTime a partir de uma string. Além disso, o fuso horário é usado em funções que trabalham com hora e data, caso ele não tenha sido informado nos parâmetros de entrada.

**Exemplo**

```xml
<timezone>Asia/Istanbul</timezone>
```

**Veja também**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

Porta usada para a comunicação com clientes pelo protocolo TCP.

**Exemplo**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

Porta TCP para comunicação segura com clientes. Use-a com as configurações do [OpenSSL](#openssl).

**Valor padrão**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

Porta para comunicação com clientes via protocolo MySQL.

:::note

* Inteiros positivos especificam o número da porta na qual se deve escutar
* Valores vazios são usados para desativar a comunicação com clientes via protocolo MySQL.
  :::

**Exemplo**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

Porta para comunicação com clientes via protocolo PostgreSQL.

:::note

* Inteiros positivos especificam o número da porta na qual o serviço deve escutar
* Valores vazios são usados para desativar a comunicação com clientes via protocolo PostgreSQL.
  :::

**Exemplo**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

Configuração para converter prefixos de URL abreviados ou simbólicos em URLs completas.

Exemplo:

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

<div id="user_defined_path">
  ## user_defined_path
</div>

O diretório que contém arquivos definidos pelo usuário. Usado para funções definidas pelo usuário em SQL [Funções definidas pelo usuário em SQL](/pt-BR/sql-reference/functions/udf).

**Exemplo**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

Caminho para o arquivo que contém:

* Configurações de usuários.
* Permissões de acesso.
* Perfis de configuração.
* Configurações de QUOTA.

**Exemplo**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

Configurações para melhorias opcionais no sistema de controle de acesso.

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Default |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `on_cluster_queries_require_cluster_grant`      | Define se consultas `ON CLUSTER` exigem o privilégio `CLUSTER`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `true`  |
| `role_cache_expiration_time_seconds`            | Define por quantos segundos desde o último acesso uma função permanece armazenada no cache de funções.                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `600`   |
| `select_from_information_schema_requires_grant` | Define se `SELECT * FROM information_schema.<table>` exige algum privilégio ou pode ser executado por qualquer usuário. Se definido como true, essa consulta exige `GRANT SELECT ON information_schema.<table>`, assim como em tabelas comuns.                                                                                                                                                                                                                                                                                                                    | `true`  |
| `select_from_system_db_requires_grant`          | Define se `SELECT * FROM system.<table>` exige algum privilégio ou pode ser executado por qualquer usuário. Se definido como true, essa consulta exige `GRANT SELECT ON system.<table>`, assim como em tabelas que não são do sistema. Exceções: algumas tabelas do sistema (`tables`, `columns`, `databases` e algumas tabelas constantes, como `one` e `contributors`) continuam acessíveis a todos; e, se um privilégio `SHOW` (por exemplo, `SHOW USERS`) tiver sido concedido, a tabela do sistema correspondente (isto é, `system.users`) ficará acessível. | `true`  |
| `settings_constraints_replace_previous`         | Define se uma restrição em um perfil de configurações para uma determinada configuração substituirá os efeitos da restrição anterior (definida em outros perfis) para essa configuração, incluindo campos que não forem definidos pela nova restrição. Isso também habilita o tipo de restrição `changeable_in_readonly`.                                                                                                                                                                                                                                         | `true`  |
| `table_engines_require_grant`                   | Define se criar uma tabela com um mecanismo de tabela específico exige um privilégio.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `false` |
| `throw_on_unmatched_row_policies`               | Define se a leitura de uma tabela deve gerar uma exceção caso a tabela tenha políticas de linha, mas nenhuma delas se aplique ao usuário atual                                                                                                                                                                                                                                                                                                                                                                                                                    | `false` |
| `users_without_row_policies_can_read_rows`      | Define se usuários sem políticas de linha permissivas ainda podem ler linhas usando uma consulta `SELECT`. Por exemplo, se houver dois usuários, A e B, e uma política de linha estiver definida apenas para A, então, se essa configuração for true, o usuário B verá todas as linhas. Se essa configuração for false, o usuário B não verá nenhuma linha.                                                                                                                                                                                                       | `true`  |

Exemplo:

```xml
<access_control_improvements>
    <throw_on_unmatched_row_policies>true</throw_on_unmatched_row_policies>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

<div id="s3queue_log">
  ## s3queue_log
</div>

Configurações para a tabela do sistema `s3queue_log`.

<SystemLogParameters />

As configurações padrão são:

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

<div id="dead_letter_queue">
  ## dead_letter_queue
</div>

Configuração da tabela do sistema &#39;dead&#95;letter&#95;queue&#39;.

<SystemLogParameters />

As configurações padrão são:

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

<div id="zookeeper">
  ## zookeeper
</div>

Contém configurações que permitem ao ClickHouse interagir com um cluster [ZooKeeper](http://zookeeper.apache.org/). O ClickHouse usa o ZooKeeper para armazenar metadados de réplicas ao usar tabelas replicadas. Se tabelas replicadas não forem usadas, esta seção de parâmetros pode ser omitida.

As configurações a seguir podem ser definidas por subtags:

| Configuração                                    | Descrição                                                                                                                                                                                                                                                                                                                                                                              |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node`                                          | Endpoint do ZooKeeper. Você pode definir vários endpoints. Ex.: `<node index="1"><host>example_host</host><port>2181</port></node>`. O atributo `index` especifica a ordem dos nodes ao tentar se conectar ao cluster ZooKeeper.                                                                                                                                                       |
| `operation_timeout_ms`                          | Tempo limite máximo para uma operação, em milissegundos.                                                                                                                                                                                                                                                                                                                               |
| `session_timeout_ms`                            | Tempo limite máximo para a sessão do cliente, em milissegundos.                                                                                                                                                                                                                                                                                                                        |
| `root` (optional)                               | O znode usado como raiz para os znodes usados pelo servidor ClickHouse.                                                                                                                                                                                                                                                                                                                |
| `fallback_session_lifetime.min` (optional)      | Limite mínimo para o ciclo de vida de uma sessão do ZooKeeper no node de fallback quando o primário estiver indisponível (balanceamento de carga). Definido em segundos. Padrão: 3 horas.                                                                                                                                                                                              |
| `fallback_session_lifetime.max` (optional)      | Limite máximo para o ciclo de vida de uma sessão do ZooKeeper no node de fallback quando o primário estiver indisponível (balanceamento de carga). Definido em segundos. Padrão: 6 horas.                                                                                                                                                                                              |
| `identity` (optional)                           | Usuário e senha exigidos pelo ZooKeeper para acessar os znodes solicitados.                                                                                                                                                                                                                                                                                                            |
| `use_compression` (optional)                    | Habilita a compressão no protocolo Keeper se definido como true.                                                                                                                                                                                                                                                                                                                       |
| `use_xid_64` (optional)                         | Habilita IDs de transação de 64 bits. Defina como `true` para ativar o formato estendido de ID de transação. Padrão: `false`.                                                                                                                                                                                                                                                          |
| `pass_opentelemetry_tracing_context` (optional) | Habilita a propagação do contexto de tracing do OpenTelemetry para solicitações ao Keeper. Quando habilitado, serão criados spans de tracing para operações do Keeper, permitindo rastreamento distribuído entre ClickHouse e Keeper. Consulte [Tracing ClickHouse Keeper Requests](/pt-BR/operations/opentelemetry#tracing-clickhouse-keeper-requests) para mais detalhes. Padrão: `false`. |

Também há a configuração `zookeeper_load_balancing` (opcional), que permite selecionar o algoritmo de seleção de node do ZooKeeper:

| Nome do algoritmo                | Descrição                                                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `random`                         | Seleciona aleatoriamente um dos nodes do ZooKeeper.                                                                                  |
| `in_order`                       | Seleciona o primeiro node do ZooKeeper; se ele não estiver disponível, seleciona o segundo, e assim por diante.                      |
| `nearest_hostname`               | Seleciona um node do ZooKeeper com hostname mais semelhante ao hostname do servidor; o hostname é comparado com o prefixo do nome.   |
| `hostname_levenshtein_distance`  | Assim como `nearest_hostname`, mas compara o hostname usando a distância de Levenshtein.                                             |
| `hostname_longest_common_prefix` | Assim como `nearest_hostname`, mas prefere o node cujo hostname compartilha o prefixo comum mais longo com o hostname do servidor.   |
| `hostname_longest_common_suffix` | Assim como `nearest_hostname`, mas prefere o node cujo hostname compartilha o sufixo comum mais longo com o hostname do servidor.    |
| `first_or_random`                | Seleciona o primeiro node do ZooKeeper; se ele não estiver disponível, seleciona aleatoriamente um dos nodes restantes do ZooKeeper. |
| `round_robin`                    | Seleciona o primeiro node do ZooKeeper; se houver reconexão, seleciona o próximo.                                                    |

**Exemplo de configuração**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / hostname_longest_common_prefix / hostname_longest_common_suffix / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation. -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**Veja também**

* [Replicação](../../engines/table-engines/mergetree-family/replication.md)
* [Guia do programador do ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [Comunicação segura opcional entre ClickHouse e Zookeeper](/pt-BR/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

Método de armazenamento para cabeçalhos de partes de dados no ZooKeeper. Esta configuração se aplica apenas à família [`MergeTree`](/pt-BR/engines/table-engines/mergetree-family). Ela pode ser especificada:

**Globalmente na seção [merge&#95;tree](#merge_tree) do arquivo `config.xml`**

O ClickHouse usa essa configuração para todas as tabelas no servidor. Você pode alterá-la a qualquer momento. As tabelas existentes mudam de comportamento quando a configuração é alterada.

**Para cada tabela**

Ao criar uma tabela, especifique a [configuração do mecanismo](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) correspondente. O comportamento de uma tabela existente com essa configuração não muda, mesmo que a configuração global seja alterada.

**Valores possíveis**

* `0` — A funcionalidade está desativada.
* `1` — A funcionalidade está ativada.

Se [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper), então as tabelas [replicadas](../../engines/table-engines/mergetree-family/replication.md) armazenam os cabeçalhos das partes de dados de forma compacta usando um único `znode`. Se a tabela contiver muitas colunas, esse método de armazenamento reduz significativamente o volume de dados armazenados no ZooKeeper.

:::note
Depois de aplicar `use_minimalistic_part_header_in_zookeeper = 1`, você não pode fazer downgrade do servidor ClickHouse para uma versão que não ofereça suporte a essa configuração. Tenha cuidado ao atualizar o ClickHouse nos servidores de um cluster. Não atualize todos os servidores de uma só vez. É mais seguro testar novas versões do ClickHouse em um ambiente de teste ou em apenas alguns servidores do cluster.

Os cabeçalhos de partes de dados já armazenados com essa configuração não podem ser restaurados para a representação anterior (não compacta).
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

Gerencia a execução de [consultas de DDL distribuído](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`) no cluster.
Funciona somente se o [ZooKeeper](/pt-BR/operations/server-configuration-parameters/settings#zookeeper) estiver habilitado.

As configurações de `<distributed_ddl>` incluem:

| Configuração           | Descrição                                                                                                                                         | Valor padrão                                |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| `cleanup_delay_period` | a limpeza começa após o recebimento de um evento de novo nó se a última limpeza não tiver sido feita há menos de `cleanup_delay_period` segundos. | `60` segundos                               |
| `max_tasks_in_queue`   | o número máximo de tarefas que podem estar na fila.                                                                                               | `1,000`                                     |
| `path`                 | o caminho no Keeper para a `task_queue` de consultas DDL                                                                                          |                                             |
| `pool_size`            | quantas consultas `ON CLUSTER` podem ser executadas simultaneamente                                                                               |                                             |
| `profile`              | o perfil usado para executar as consultas DDL                                                                                                     |                                             |
| `task_max_lifetime`    | exclui o nó se sua idade for maior que esse valor.                                                                                                | `7 * 24 * 60 * 60` (uma semana em segundos) |

**Exemplo**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

<div id="access_control_path">
  ## access_control_path
</div>

Caminho para a pasta em que o servidor ClickHouse armazena as configurações de usuários e funções criadas por comandos SQL.

**Veja também**

* [Controle de acesso e gerenciamento de contas](/pt-BR/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

Define se os tipos de senha em texto simples (inseguros) são permitidos.

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

Define se o tipo de senha inseguro no&#95;password é permitido ou não.

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

Proíbe a criação de um usuário sem senha, a menos que &#39;IDENTIFIED WITH no&#95;password&#39; seja especificado de forma explícita.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

Tempo limite padrão da sessão, em segundos.

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

Define o tipo de senha a ser definido automaticamente em consultas como `CREATE USER u IDENTIFIED BY 'p'`.

Os valores aceitos são:

* `plaintext_password`
* `sha256_password`
* `double_sha1_password`
* `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

<div id="user_directories">
  ## user_directories
</div>

Seção do arquivo de configuração que contém as seguintes configurações:

* Caminho para o arquivo de configuração com usuários predefinidos.
* Caminho para a pasta onde são armazenados os usuários criados por comandos SQL.
* Caminho do nó no ZooKeeper onde são armazenados e replicados os usuários criados por comandos SQL.

Se esta seção for especificada, os caminhos de [users&#95;config](/pt-BR/operations/server-configuration-parameters/settings#users_config) e [access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path) não serão usados.

A seção `user_directories` pode conter qualquer número de itens; a ordem deles determina sua precedência (quanto mais acima o item, maior a precedência).

**Exemplos**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

Usuários, funções, políticas de linha, cotas e perfis também podem ser armazenados no ZooKeeper:

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

Você também pode definir as seções `memory` — que significa armazenar informações apenas na memória, sem gravá-las em disco — e `ldap` — que significa armazenar informações em um servidor LDAP.

Para adicionar um servidor LDAP como diretório remoto para usuários que não estão definidos localmente, defina uma única seção `ldap` com as seguintes configurações:

| Configuração | Descrição                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `roles`      | seção com uma lista de funções definidas localmente que serão atribuídas a cada usuário recuperado do servidor LDAP. Se nenhuma função for especificada, o usuário não poderá executar nenhuma ação após a autenticação. Se qualquer uma das funções listadas não estiver definida localmente no momento da autenticação, a tentativa de autenticação falhará como se a senha fornecida estivesse incorreta. |
| `server`     | um dos nomes de servidor LDAP definidos na seção de config `ldap_servers`. Este parâmetro é obrigatório e não pode ficar vazio.                                                                                                                                                                                                                                                                            |

**Exemplo**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

<div id="top_level_domains_list">
  ## top_level_domains_list
</div>

Define uma lista de domínios de nível superior personalizados a serem adicionados, em que cada entrada tem o formato `<name>/path/to/file</name>`.

Por exemplo:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

Veja também:

* a função [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) e suas variações,
  que aceita o nome de uma lista de TLDs personalizada e retorna a parte do domínio que inclui os subdomínios de nível superior até o primeiro subdomínio significativo.

<div id="proxy">
  ## proxy
</div>

Defina servidores proxy para solicitações HTTP e HTTPS, atualmente compatíveis com armazenamento S3, funções de tabela do S3 e funções de URL.

Há três maneiras de definir servidores proxy:

* variáveis de ambiente
* listas de proxy
* resolvedores remotos de proxy.

Também há suporte para ignorar servidores proxy para hosts específicos usando `no_proxy`.

**Variáveis de ambiente**

As variáveis de ambiente `http_proxy` e `https_proxy` permitem especificar um
servidor proxy para um determinado protocolo. Se elas estiverem definidas no seu sistema, deverão funcionar sem problemas.

Essa é a abordagem mais simples se um determinado protocolo tiver
apenas um servidor proxy e esse servidor proxy não mudar.

**Listas de proxy**

Essa abordagem permite especificar um ou mais
servidores proxy para um protocolo. Se mais de um servidor proxy estiver definido,
o ClickHouse usa os diferentes proxies em esquema round-robin, distribuindo a
carga entre os servidores. Essa é a abordagem mais simples se houver mais de
um servidor proxy para um protocolo e a lista de servidores proxy não mudar.

**Modelo de configuração**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

Selecione um campo pai nas abas abaixo para ver os campos filhos correspondentes:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Campo     | Descrição                             |
    | --------- | ------------------------------------- |
    | `<http>`  | Uma lista de um ou mais proxies HTTP  |
    | `<https>` | Uma lista de um ou mais proxies HTTPS |
  </TabItem>

  <TabItem value="http_https" label="<http> e <https>">
    | Campo   | Descrição      |
    | ------- | -------------- |
    | `<uri>` | O URI do proxy |
  </TabItem>
</Tabs>

**Resolvedores remotos de proxy**

É possível que os servidores proxy mudem dinamicamente. Nesse
caso, você pode definir o endpoint de um resolvedor. O ClickHouse envia
uma solicitação GET vazia para esse endpoint, e o resolvedor remoto deve retornar o host do proxy.
O ClickHouse o usará para compor o URI do proxy usando o seguinte modelo: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**Modelo de configuração**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

Selecione um campo pai nas abas abaixo para ver os campos filhos:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Campo     | Descrição                               |
    | --------- | --------------------------------------- |
    | `<http>`  | Uma lista com um ou mais resolvers* |
    | `<https>` | Uma lista com um ou mais resolvers* |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | Campo        | Descrição                                   |
    | ------------ | ------------------------------------------- |
    | `<resolver>` | O endpoint e outros detalhes de um resolver |

    :::note
    Você pode ter vários elementos `<resolver>`, mas apenas o primeiro
    `<resolver>` de um determinado protocolo é usado. Os demais elementos `<resolver>`
    desse protocolo são ignorados. Isso significa que o balanceamento de carga
    (se necessário) deve ser implementado pelo resolver remoto.
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | Campo                | Descrição                                                                                                                                                                                                                          |
    | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
    | `<endpoint>`         | O URI do resolver de proxy                                                                                                                                                                                                         |
    | `<proxy_scheme>`     | O protocolo do URI final do proxy. Pode ser `http` ou `https`.                                                                                                                                                                     |
    | `<proxy_port>`       | O número da porta do resolver de proxy                                                                                                                                                                                             |
    | `<proxy_cache_time>` | O tempo, em segundos, durante o qual os valores do resolver devem ser armazenados em cache pelo ClickHouse. Definir esse valor como `0` faz com que o ClickHouse entre em contato com o resolver em cada requisição HTTP ou HTTPS. |
  </TabItem>
</Tabs>

**Precedência**

As configurações de proxy são determinadas na seguinte ordem:

| Ordem | Configuração               |
| ----- | -------------------------- |
| 1.    | Resolvers remotos de proxy |
| 2.    | Listas de proxy            |
| 3.    | Variáveis de ambiente      |

O ClickHouse verificará o tipo de resolver de maior prioridade para o protocolo da solicitação. Se ele não estiver definido,
verificará o próximo tipo de resolver na ordem de prioridade, até chegar ao resolver de ambiente.
Isso também permite usar uma combinação de tipos de resolver.

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

Por padrão, o tunelamento (isto é, `HTTP CONNECT`) é usado para fazer requisições `HTTPS` por meio de um proxy `HTTP`. Essa configuração pode ser usada para desativá-lo.

**no&#95;proxy**

Por padrão, todas as requisições passarão pelo proxy. Para desativá-lo para hosts específicos, a variável `no_proxy` deve ser definida.
Ela pode ser definida dentro da cláusula `<proxy>` para resolvedores de lista e remotos e como variável de ambiente para o resolvedor de ambiente.
Ela oferece suporte a endereços IP, domínios, subdomínios e ao curinga `'*'` para ignorar totalmente o proxy. Os pontos à esquerda são removidos, assim como o curl faz.

**Exemplo**

A configuração abaixo faz com que as requisições para `clickhouse.cloud` e todos os seus subdomínios (por exemplo, `auth.clickhouse.cloud`) ignorem o proxy.
O mesmo se aplica ao GitLab, mesmo com um ponto à esquerda. Tanto `gitlab.com` quanto `about.gitlab.com` ignorariam o proxy.

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

<div id="workload_path">
  ## workload_path
</div>

O diretório usado para armazenar todas as consultas `CREATE WORKLOAD` e `CREATE RESOURCE`. Por padrão, é usada a pasta `/workload/` no diretório de trabalho do servidor.

**Exemplo**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**Veja também**

* [Hierarquia de workloads](/pt-BR/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

O caminho para um nó do ZooKeeper, usado como armazenamento para todas as consultas `CREATE WORKLOAD` e `CREATE RESOURCE`. Para garantir a consistência, todas as definições SQL são armazenadas como o valor desse único znode. Por padrão, o ZooKeeper não é usado, e as definições são armazenadas em [disk](#workload_path).

**Exemplo**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**Veja também**

* [Hierarquia de workloads](/pt-BR/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

Configurações da tabela do sistema [`zookeeper_log`](/pt-BR/operations/system-tables/zookeeper_log).

As configurações a seguir podem ser definidas por meio de subtags:

<SystemLogParameters />

**Exemplo**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```
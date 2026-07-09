---
description: 'Este motor oferece integração com o ecossistema Apache Hadoop ao
  permitir o gerenciamento de dados no HDFS via ClickHouse. Este motor é semelhante aos motores File
  e URL, mas oferece funcionalidades específicas do Hadoop.'
sidebar_label: 'HDFS'
sidebar_position: 80
slug: /engines/table-engines/integrations/hdfs
title: 'Motor de tabela HDFS'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-engine">
  # Motor de tabela HDFS
</div>

<CloudNotSupportedBadge />

Este motor fornece integração com o ecossistema [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop), permitindo gerenciar dados no [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) por meio do ClickHouse. Este motor é semelhante aos motores [File](/pt-BR/engines/table-engines/special/file) e [URL](/pt-BR/engines/table-engines/special/url), mas oferece recursos específicos do Hadoop.

Esse recurso não é suportado pelos engenheiros do ClickHouse, e sua qualidade é reconhecidamente questionável. Em caso de problemas, corrija-os você mesmo e envie um pull request.

<div id="usage">
  ## Uso
</div>

```sql
ENGINE = HDFS(URI, format)
```

**Parâmetros do motor**

* `URI` - URI completa do arquivo no HDFS. A parte do caminho de `URI` pode conter globs. Nesse caso, a tabela seria somente leitura.
* `format` - especifica um dos formatos de arquivo disponíveis. Para executar
  consultas `SELECT`, o `format` deve ter suporte para entrada e, para executar
  consultas `INSERT` – para saída. Os formatos disponíveis estão listados na
  seção [Formatos](/pt-BR/sql-reference/formats#formats-overview).
* [PARTITION BY expr]

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — Opcional. Na maioria dos casos, você não precisa de uma chave de partição e, se precisar de uma, em geral ela não deve ser mais granular do que por mês. O particionamento não acelera consultas (ao contrário do que ocorre com a expressão ORDER BY). Você nunca deve usar um particionamento granular demais. Não particione seus dados por identificadores ou nomes de clientes (em vez disso, use o identificador ou nome do cliente como a primeira coluna na expressão ORDER BY).

Para particionar por mês, use a expressão `toYYYYMM(date_column)`, em que `date_column` é uma coluna com uma data do tipo [Date](/pt-BR/sql-reference/data-types/date.md). Os nomes das partições aqui têm o formato `"YYYYMM"`.

**Exemplo:**

**1.** Configure a tabela `hdfs_engine_table`:

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Preencher o arquivo:

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Consulte os dados:

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="implementation-details">
  ## Detalhes de implementação
</div>

* Leituras e gravações podem ser paralelas.
* Não há suporte a:

  * operações `ALTER` e `SELECT...SAMPLE`.
  * Índices.
  * A [replicação zero-copy](../../../operations/storing-data.md#zero-copy) é possível, mas não é recomendada.

  :::note A replicação zero-copy não está pronta para produção
  A replicação zero-copy vem desativada por padrão na versão 22.8 do ClickHouse e posteriores. Esta funcionalidade não é recomendada para uso em produção.
  :::

**Globs no caminho**

Vários componentes do caminho podem ter globs. Para ser processado, o arquivo deve existir e corresponder ao padrão do caminho completo. A listagem dos arquivos ocorre durante o `SELECT` (e não no momento do `CREATE`).

* `*` — Substitui qualquer número de quaisquer caracteres, exceto `/`, incluindo a string vazia.
* `?` — Substitui qualquer caractere único.
* `{some_string,another_string,yet_another_one}` — Substitui qualquer uma das strings `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — Substitui qualquer número no intervalo de N a M, incluindo ambos os limites.

Construções com `{}` são semelhantes à função de tabela [remote](../../../sql-reference/table-functions/remote.md).

**Exemplo**

1. Suponha que temos vários arquivos no formato TSV com os seguintes URIs no HDFS:

   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Há várias maneiras de criar uma tabela composta pelos seis arquivos:

{/* */ }

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Outra forma:

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

A tabela é composta por todos os arquivos em ambos os diretórios (todos os arquivos devem estar em conformidade com o formato e o esquema descritos na consulta):

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
Se a listagem de arquivos contiver intervalos numéricos com zeros à esquerda, use a construção com chaves para cada dígito separadamente ou `?`.
:::

**Exemplo**

Crie uma tabela com arquivos chamados `file000`, `file001`, ... , `file999`:

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

<div id="configuration">
  ## Configuração
</div>

Assim como o GraphiteMergeTree, o motor HDFS oferece suporte a configurações estendidas por meio do arquivo de configuração do ClickHouse. Há duas chaves de configuração que você pode usar: global (`hdfs`) e de nível de usuário (`hdfs_*`). A configuração global é aplicada primeiro, e depois a configuração de nível de usuário é aplicada (se existir).

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
  <hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
  <hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  <hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
  <hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

<div id="configuration-options">
  ### Opções de configuração
</div>

<div id="supported-by-libhdfs3">
  #### Compatível com libhdfs3
</div>

| **parâmetro**                                                           | **valor padrão**                  |
| ----------------------------------------------------------------------- | --------------------------------- |
| rpc&#95;client&#95;connect&#95;tcpnodelay                               | true                              |
| dfs&#95;client&#95;read&#95;shortcircuit                                | true                              |
| output&#95;replace-datanode-on-failure                                  | true                              |
| input&#95;notretry-another-node                                         | false                             |
| input&#95;localread&#95;mappedfile                                      | true                              |
| dfs&#95;client&#95;use&#95;legacy&#95;blockreader&#95;local             | false                             |
| rpc&#95;client&#95;ping&#95;interval                                    | 10  * 1000                        |
| rpc&#95;client&#95;connect&#95;timeout                                  | 600 * 1000                        |
| rpc&#95;client&#95;read&#95;timeout                                     | 3600 * 1000                       |
| rpc&#95;client&#95;write&#95;timeout                                    | 3600 * 1000                       |
| rpc&#95;client&#95;socket&#95;linger&#95;timeout                        | -1                                |
| rpc&#95;client&#95;connect&#95;retry                                    | 10                                |
| rpc&#95;client&#95;timeout                                              | 3600 * 1000                       |
| dfs&#95;default&#95;replica                                             | 3                                 |
| input&#95;connect&#95;timeout                                           | 600 * 1000                        |
| input&#95;read&#95;timeout                                              | 3600 * 1000                       |
| input&#95;write&#95;timeout                                             | 3600 * 1000                       |
| input&#95;localread&#95;default&#95;buffersize                          | 1 * 1024 * 1024                   |
| dfs&#95;prefetchsize                                                    | 10                                |
| input&#95;read&#95;getblockinfo&#95;retry                               | 3                                 |
| input&#95;localread&#95;blockinfo&#95;cachesize                         | 1000                              |
| input&#95;read&#95;max&#95;retry                                        | 60                                |
| output&#95;default&#95;chunksize                                        | 512                               |
| output&#95;default&#95;packetsize                                       | 64 * 1024                         |
| output&#95;default&#95;write&#95;retry                                  | 10                                |
| output&#95;connect&#95;timeout                                          | 600 * 1000                        |
| output&#95;read&#95;timeout                                             | 3600 * 1000                       |
| output&#95;write&#95;timeout                                            | 3600 * 1000                       |
| output&#95;close&#95;timeout                                            | 3600 * 1000                       |
| output&#95;packetpool&#95;size                                          | 1024                              |
| output&#95;heartbeat&#95;interval                                       | 10 * 1000                         |
| dfs&#95;client&#95;failover&#95;max&#95;attempts                        | 15                                |
| dfs&#95;client&#95;read&#95;shortcircuit&#95;streams&#95;cache&#95;size | 256                               |
| dfs&#95;client&#95;socketcache&#95;expiryMsec                           | 3000                              |
| dfs&#95;client&#95;socketcache&#95;capacity                             | 16                                |
| dfs&#95;default&#95;blocksize                                           | 64 * 1024 * 1024                  |
| dfs&#95;default&#95;uri                                                 | &quot;hdfs://localhost:9000&quot; |
| hadoop&#95;security&#95;authentication                                  | &quot;simple&quot;                |
| hadoop&#95;security&#95;kerberos&#95;ticket&#95;cache&#95;path          | &quot;&quot;                      |
| dfs&#95;client&#95;log&#95;severity                                     | &quot;INFO&quot;                  |
| dfs&#95;domain&#95;socket&#95;path                                      | &quot;&quot;                      |

[A referência de configuração do HDFS](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) pode explicar alguns parâmetros.

<div id="clickhouse-extras">
  #### Extras do ClickHouse
</div>

| **parâmetro**                     | **valor padrão** |
| --------------------------------- | ---------------- |
| hadoop&#95;kerberos&#95;keytab    | &quot;&quot;     |
| hadoop&#95;kerberos&#95;principal | &quot;&quot;     |
| libhdfs3&#95;conf                 | &quot;&quot;     |

<div id="limitations">
  ### Limitações
</div>

* `hadoop_security_kerberos_ticket_cache_path` e `libhdfs3_conf` podem ser apenas globais, não específicos para cada usuário

<div id="kerberos-support">
  ## Suporte ao Kerberos
</div>

Se o parâmetro `hadoop_security_authentication` tiver o valor `kerberos`, o ClickHouse fará a autenticação via Kerberos.
Os parâmetros estão [aqui](#clickhouse-extras), e `hadoop_security_kerberos_ticket_cache_path` pode ser útil.
Observe que, devido a limitações da libhdfs3, apenas a abordagem antiga é compatível:
as comunicações com o datanode não são protegidas por SASL (`HADOOP_SECURE_DN_USER` é um indicador confiável dessa
abordagem de segurança). Use `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` como referência.

Se `hadoop_kerberos_keytab`, `hadoop_kerberos_principal` ou `hadoop_security_kerberos_ticket_cache_path` forem especificados, a autenticação via Kerberos será usada. Nesse caso, `hadoop_kerberos_keytab` e `hadoop_kerberos_principal` são obrigatórios.

<div id="namenode-ha">
  ## Suporte a alta disponibilidade do namenode do HDFS
</div>

O libhdfs3 oferece suporte à alta disponibilidade do namenode do HDFS.

* Copie `hdfs-site.xml` de um nó do HDFS para `/etc/clickhouse-server/`.
* Adicione o trecho a seguir ao arquivo de configuração do ClickHouse:

```xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

* Em seguida, use o valor da tag `dfs.nameservices` em `hdfs-site.xml` como endereço do namenode na URI do HDFS. Por exemplo, substitua `hdfs://appadmin@192.168.101.11:8020/abc/` por `hdfs://appadmin@my_nameservice/abc/`.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho for desconhecido, o valor será `NULL`.
* `_time` — Hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se a hora for desconhecida, o valor será `NULL`.

<div id="storage-settings">
  ## Configurações de armazenamento
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/pt-BR/operations/settings/settings.md#hdfs_truncate_on_insert) - permite truncar o arquivo antes de inserir dados nele. Desativado por padrão.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/pt-BR/operations/settings/settings.md#hdfs_create_new_file_on_insert) - permite criar um novo arquivo a cada inserção se o formato tiver um sufixo. Desativado por padrão.
* [hdfs&#95;skip&#95;empty&#95;files](/pt-BR/operations/settings/settings.md#hdfs_skip_empty_files) - permite ignorar arquivos vazios durante a leitura. Desativado por padrão.

**Veja também**

* [Colunas virtuais](../../../engines/table-engines/index.md#table_engines-virtual_columns)
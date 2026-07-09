---
description: 'Documentação sobre coleções nomeadas'
sidebar_label: 'Coleções nomeadas'
sidebar_position: 69
slug: /operations/named-collections
title: 'Coleções nomeadas'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

Coleções nomeadas oferecem uma maneira de armazenar coleções de pares chave-valor para
serem usadas na configuração de integrações com fontes externas. Você pode usar coleções nomeadas com
dicionários, tabelas, funções de tabela e armazenamento de objetos.

As coleções nomeadas podem ser configuradas com DDL ou em arquivos de configuração, e são aplicadas
quando o ClickHouse é iniciado. Elas simplificam a criação de objetos e a ocultação de credenciais
de usuários sem acesso administrativo.

As chaves em uma coleção nomeada devem corresponder aos nomes dos parâmetros da
função, mecanismo de tabela, banco de dados etc. correspondentes. Nos exemplos abaixo, há um link para a lista de parâmetros
de cada tipo.

Os parâmetros definidos em uma coleção nomeada podem ser substituídos em SQL, como mostrado nos exemplos
abaixo. Essa capacidade pode ser limitada usando as palavras-chave `[NOT] OVERRIDABLE`, atributos XML
e/ou a opção de configuração `allow_named_collection_override_by_default`.

:::warning
Se a substituição for permitida, pode ser possível que usuários sem acesso administrativo
descubram as credenciais que você está tentando ocultar.
Se você estiver usando coleções nomeadas com esse objetivo, deverá desabilitar
`allow_named_collection_override_by_default` (que é habilitada por padrão).
:::

<div id="storing-named-collections-in-the-system-database">
  ## Armazenando coleções nomeadas no banco de dados do sistema
</div>

<div id="ddl-example">
  ### Exemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value' OVERRIDABLE,
key_2 = 'value2' NOT OVERRIDABLE,
url = 'https://connection.url/'
```

No exemplo acima:

* `key_1` sempre pode ser sobrescrita.
* `key_2` nunca pode ser sobrescrita.
* `url` pode ou não ser sobrescrita, dependendo do valor de `allow_named_collection_override_by_default`.

<div id="permissions-to-create-named-collections-with-ddl">
  ### Permissões para criar coleções nomeadas com DDL
</div>

Para gerenciar coleções nomeadas com DDL, o usuário deve ter o privilégio `named_collection_control`. Isso pode ser atribuído adicionando um arquivo a `/etc/clickhouse-server/users.d/`. O exemplo concede ao usuário `default` os privilégios `access_management` e `named_collection_control`:

```xml title='/etc/clickhouse-server/users.d/user_default.xml'
<clickhouse>
  <users>
    <default>
      <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex replace=true>
      <access_management>1</access_management>
      <!-- highlight-start -->
      <named_collection_control>1</named_collection_control>
      <!-- highlight-end -->
    </default>
  </users>
</clickhouse>
```

:::tip
No exemplo acima, o valor `password_sha256_hex` é a representação hexadecimal do hash SHA256 da senha. Esta configuração do usuário `default` tem o atributo `replace=true`, pois a configuração padrão já define uma `password` em texto simples, e não é possível definir, ao mesmo tempo, senhas em texto simples e senhas SHA256 hex para um usuário.
:::

<div id="storage-for-named-collections">
  ### Armazenamento de coleções nomeadas
</div>

As coleções nomeadas podem ser armazenadas em disco local ou no ZooKeeper/Keeper. Por padrão, o armazenamento local é usado.
Elas também podem ser armazenadas com criptografia, usando os mesmos algoritmos da [criptografia de disco](storing-data#encrypted-virtual-file-system),
sendo `aes_128_ctr` usado por padrão.

Para configurar o armazenamento de coleções nomeadas, você precisa especificar um `type`. Ele pode ser `local` ou `keeper`/`zookeeper`. Para armazenamento criptografado,
você pode usar `local_encrypted` ou `keeper_encrypted`/`zookeeper_encrypted`.

Para usar ZooKeeper/Keeper, também é necessário configurar um `path` (caminho no ZooKeeper/Keeper onde as coleções nomeadas serão armazenadas) na
seção `named_collections_storage` do arquivo de configuração. O exemplo a seguir usa criptografia e ZooKeeper/Keeper:

```xml
<clickhouse>
  <named_collections_storage>
    <type>zookeeper_encrypted</type>
    <key_hex>bebec0cabebec0cabebec0cabebec0ca</key_hex>
    <algorithm>aes_128_ctr</algorithm>
    <path>/named_collections_path/</path>
    <update_timeout_ms>1000</update_timeout_ms>
  </named_collections_storage>
</clickhouse>
```

Um parâmetro de configuração opcional, `update_timeout_ms`, tem o valor padrão de `5000`.

<div id="storing-named-collections-in-configuration-files">
  ## Armazenar coleções nomeadas em arquivos de configuração
</div>

<div id="xml-example">
  ### Exemplo em XML
</div>

```xml title='/etc/clickhouse-server/config.d/named_collections.xml'
<clickhouse>
     <named_collections>
        <name>
            <key_1 overridable="true">value</key_1>
            <key_2 overridable="false">value_2</key_2>
            <url>https://connection.url/</url>
        </name>
     </named_collections>
</clickhouse>
```

No exemplo acima:

* `key_1` sempre pode ser sobrescrita.
* `key_2` nunca pode ser sobrescrita.
* `url` pode ou não ser sobrescrita, dependendo do valor de `allow_named_collection_override_by_default`.

<div id="modifying-named-collections">
  ## Modificando coleções nomeadas
</div>

As coleções nomeadas criadas com consultas DDL podem ser alteradas ou excluídas com DDL. As coleções nomeadas criadas com arquivos XML podem ser gerenciadas por meio da edição ou exclusão do XML correspondente.

<div id="alter-a-ddl-named-collection">
  ### Alterar uma coleção nomeada por DDL
</div>

Altere ou adicione as chaves `key1` e `key3` da coleção `collection2`
(isso não alterará o valor da flag `overridable` dessas chaves):

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

Altere ou adicione a chave `key1` e permita que ela seja sempre sobrescrita:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4 OVERRIDABLE
```

Remova a chave `key2` da coleção `collection2`:

```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

Altere ou adicione a chave `key1` e remova a chave `key3` da coleção `collection2`:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

Para forçar uma chave a usar as configurações padrão da flag `overridable`, é preciso
removê-la e adicioná-la novamente.

```sql
ALTER NAMED COLLECTION collection2 DELETE key1;
ALTER NAMED COLLECTION collection2 SET key1=4;
```

<div id="drop-the-ddl-named-collection-collection2">
  ### Exclua a coleção nomeada DDL `collection2`:
</div>

```sql
DROP NAMED COLLECTION collection2
```

<div id="named-collections-for-accessing-s3">
  ## Coleções nomeadas para acessar o S3
</div>

A descrição dos parâmetros está em [função de tabela S3](../sql-reference/table-functions/s3.md).

<div id="ddl-example">
  ### Exemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

<div id="xml-example">
  ### Exemplo em XML
</div>

```xml
<clickhouse>
    <named_collections>
        <s3_mydata>
            <access_key_id>AKIAIOSFODNN7EXAMPLE</access_key_id>
            <secret_access_key>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</secret_access_key>
            <format>CSV</format>
            <url>https://s3.us-east-1.amazonaws.com/yourbucket/mydata/</url>
        </s3_mydata>
    </named_collections>
</clickhouse>
```

<div id="s3-function-and-s3-table-named-collection-examples">
  ### função s3() e exemplos de coleção nomeada da tabela S3
</div>

Ambos os exemplos a seguir usam a mesma coleção nomeada `s3_mydata`:

<div id="s3-function">
  #### Função s3()
</div>

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
O primeiro argumento da função `s3()` acima é o nome da coleção, `s3_mydata`. Sem coleções nomeadas, o ID da chave de acesso, a chave secreta, o formato e a URL teriam de ser informados em cada chamada da função `s3()`.
:::

<div id="s3-table">
  #### Tabela S3
</div>

```sql
CREATE TABLE s3_engine_table (number Int64)
ENGINE=S3(s3_mydata, url='https://s3.us-east-1.amazonaws.com/yourbucket/mydata/test_file.tsv.gz', format = 'TSV')
SETTINGS input_format_with_names_use_header = 0;

SELECT * FROM s3_engine_table LIMIT 3;
┌─number─┐
│      0 │
│      1 │
│      2 │
└────────┘
```

<div id="named-collections-for-accessing-mysql-database">
  ## Coleções nomeadas para acessar o banco de dados MySQL
</div>

Para ver a descrição dos parâmetros, consulte [mysql](../sql-reference/table-functions/mysql.md).

<div id="ddl-example">
  ### Exemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION mymysql AS
user = 'myuser',
password = 'mypass',
host = '127.0.0.1',
port = 3306,
database = 'test',
connection_pool_size = 8,
replace_query = 1
```

<div id="xml-example">
  ### Exemplo em XML
</div>

```xml
<clickhouse>
    <named_collections>
        <mymysql>
            <user>myuser</user>
            <password>mypass</password>
            <host>127.0.0.1</host>
            <port>3306</port>
            <database>test</database>
            <connection_pool_size>8</connection_pool_size>
            <replace_query>1</replace_query>
        </mymysql>
    </named_collections>
</clickhouse>
```

<div id="mysql-function-mysql-table-mysql-database-and-dictionary-named-collection-examples">
  ### Exemplos de coleção nomeada para mysql(), tabela MySQL, banco de dados MySQL e Dicionário
</div>

Os quatro exemplos a seguir usam a mesma coleção nomeada `mymysql`:

<div id="mysql-function">
  #### função mysql()
</div>

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

:::note
A coleção nomeada não especifica o parâmetro `table`, então ele é informado na chamada da função como `table = 'test'`.
:::

<div id="mysql-table">
  #### Tabela MySQL
</div>

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
O DDL substitui a configuração connection&#95;pool&#95;size da coleção nomeada.
:::

<div id="mysql-database">
  #### Banco de dados MySQL
</div>

```sql
CREATE DATABASE mydatabase ENGINE = MySQL(mymysql);

SHOW TABLES FROM mydatabase;

┌─name───┐
│ source │
│ test   │
└────────┘
```

<div id="mysql-dictionary">
  #### Dicionário MySQL
</div>

```sql
CREATE DICTIONARY dict (A Int64, B String)
PRIMARY KEY A
SOURCE(MYSQL(NAME mymysql TABLE 'source'))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'B', 2);

┌─dictGet('dict', 'B', 2)─┐
│ two                     │
└─────────────────────────┘
```

<div id="named-collections-for-accessing-postgresql-database">
  ## Coleções nomeadas para acessar o banco de dados PostgreSQL
</div>

Consulte a descrição dos parâmetros em [postgresql](../sql-reference/table-functions/postgresql.md). Além disso, há aliases:

* `username` para `user`
* `db` para `database`.

O parâmetro `addresses_expr` é usado em uma coleção em vez de `host:port`. Esse parâmetro é opcional, pois há outros parâmetros opcionais: `host`, `hostname`, `port`. O pseudocódigo a seguir explica a prioridade:

```sql
CASE
    WHEN collection['addresses_expr'] != '' THEN collection['addresses_expr']
    WHEN collection['host'] != ''           THEN collection['host'] || ':' || if(collection['port'] != '', collection['port'], '5432')
    WHEN collection['hostname'] != ''       THEN collection['hostname'] || ':' || if(collection['port'] != '', collection['port'], '5432')
END
```

Exemplo de criação:

```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema'
```

Exemplo de configuração:

```xml
<clickhouse>
    <named_collections>
        <mypg>
            <user>pguser</user>
            <password>jw8s0F4</password>
            <host>127.0.0.1</host>
            <port>5432</port>
            <database>test</database>
            <schema>test_schema</schema>
        </mypg>
    </named_collections>
</clickhouse>
```

<div id="example-of-using-named-collections-with-the-postgresql-function">
  ### Exemplo de uso de coleções nomeadas com a função postgresql
</div>

```sql
SELECT * FROM postgresql(mypg, table = 'test');

┌─a─┬─b───┐
│ 2 │ two │
│ 1 │ one │
└───┴─────┘
SELECT * FROM postgresql(mypg, table = 'test', schema = 'public');

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

<div id="example-of-using-named-collections-with-database-with-engine-postgresql">
  ### Exemplo de uso de coleções nomeadas com banco de dados usando o mecanismo PostgreSQL
</div>

```sql
CREATE TABLE mypgtable (a Int64) ENGINE = PostgreSQL(mypg, table = 'test', schema = 'public');

SELECT * FROM mypgtable;

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

:::note
O PostgreSQL copia os dados da coleção nomeada no momento da criação da tabela. Alterações na coleção não afetam as tabelas existentes.
:::

<div id="example-of-using-named-collections-with-database-with-engine-postgresql-1">
  ### Exemplo de uso de coleções nomeadas com banco de dados com mecanismo PostgreSQL
</div>

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-postgresql">
  ### Exemplo de uso de coleções nomeadas com um dicionário com origem no PostgreSQL
</div>

```sql
CREATE DICTIONARY dict (a Int64, b String)
PRIMARY KEY a
SOURCE(POSTGRESQL(NAME mypg TABLE test))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'b', 2);

┌─dictGet('dict', 'b', 2)─┐
│ two                     │
└─────────────────────────┘
```

<div id="named-collections-for-accessing-a-remote-clickhouse-database">
  ## Coleções nomeadas para acessar um banco de dados ClickHouse remoto
</div>

Consulte a descrição dos parâmetros em [remote](../sql-reference/table-functions/remote.md/#parameters).

Exemplo de configuração:

```sql
CREATE NAMED COLLECTION remote1 AS
host = 'remote_host',
port = 9000,
database = 'system',
user = 'foo',
password = 'secret',
secure = 1
```

```xml
<clickhouse>
    <named_collections>
        <remote1>
            <host>remote_host</host>
            <port>9000</port>
            <database>system</database>
            <user>foo</user>
            <password>secret</password>
            <secure>1</secure>
        </remote1>
    </named_collections>
</clickhouse>
```

`secure` não é necessário para a conexão devido a `remoteSecure`, mas pode ser usado para dicionários.

<div id="example-of-using-named-collections-with-the-remoteremotesecure-functions">
  ### Exemplo de uso de coleções nomeadas com as funções `remote`/`remoteSecure`
</div>

```sql
SELECT * FROM remote(remote1, table = one);
┌─dummy─┐
│     0 │
└───────┘

SELECT * FROM remote(remote1, database = merge(system, '^one'));
┌─dummy─┐
│     0 │
└───────┘

INSERT INTO FUNCTION remote(remote1, database = default, table = test) VALUES (1,'a');

SELECT * FROM remote(remote1, database = default, table = test);
┌─a─┬─b─┐
│ 1 │ a │
└───┴───┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-clickhouse">
  ### Exemplo de uso de coleções nomeadas com um dicionário de origem ClickHouse
</div>

```sql
CREATE DICTIONARY dict(a Int64, b String)
PRIMARY KEY a
SOURCE(CLICKHOUSE(NAME remote1 TABLE test DB default))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'b', 1);
┌─dictGet('dict', 'b', 1)─┐
│ a                       │
└─────────────────────────┘
```

<div id="named-collections-for-accessing-kafka">
  ## Coleções nomeadas para acessar o Kafka
</div>

Consulte a descrição dos parâmetros em [Kafka](../engines/table-engines/integrations/kafka.md).

<div id="ddl-example">
  ### Exemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION my_kafka_cluster AS
kafka_broker_list = 'localhost:9092',
kafka_topic_list = 'kafka_topic',
kafka_group_name = 'consumer_group',
kafka_format = 'JSONEachRow',
kafka_max_block_size = '1048576';

```

<div id="xml-example">
  ### Exemplo em XML
</div>

```xml
<clickhouse>
    <named_collections>
        <my_kafka_cluster>
            <kafka_broker_list>localhost:9092</kafka_broker_list>
            <kafka_topic_list>kafka_topic</kafka_topic_list>
            <kafka_group_name>consumer_group</kafka_group_name>
            <kafka_format>JSONEachRow</kafka_format>
            <kafka_max_block_size>1048576</kafka_max_block_size>
        </my_kafka_cluster>
    </named_collections>
</clickhouse>
```

<div id="example-of-using-named-collections-with-a-kafka-table">
  ### Exemplo de uso de coleções nomeadas com uma tabela do Kafka
</div>

Ambos os exemplos a seguir usam a mesma coleção nomeada `my_kafka_cluster`:

```sql
CREATE TABLE queue
(
    timestamp UInt64,
    level String,
    message String
)
ENGINE = Kafka(my_kafka_cluster)

CREATE TABLE queue
(
    timestamp UInt64,
    level String,
    message String
)
ENGINE = Kafka(my_kafka_cluster)
SETTINGS kafka_num_consumers = 4,
         kafka_thread_per_consumer = 1;
```

<div id="named-collections-for-backups">
  ## Coleções nomeadas para backups
</div>

Para a descrição dos parâmetros, consulte [Backup e restauração](/pt-BR/operations/backup/overview).

<div id="ddl-example">
  ### Exemplo de DDL
</div>

```sql
BACKUP TABLE default.test to S3(named_collection_s3_backups, 'directory')
```

<div id="xml-example">
  ### Exemplo em XML
</div>

```xml
<clickhouse>
    <named_collections>
        <named_collection_s3_backups>
            <url>https://my-s3-bucket.s3.amazonaws.com/backup-S3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </named_collection_s3_backups>
    </named_collections>
</clickhouse>
```

<div id="named-collections-for-accessing-mongodb-table-and-dictionary">
  ## Coleções nomeadas para acessar a tabela e o dicionário do MongoDB
</div>

Para ver a descrição dos parâmetros, consulte [mongodb](../sql-reference/table-functions/mongodb.md).

<div id="ddl-example">
  ### Exemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION mymongo AS
user = '',
password = '',
host = '127.0.0.1',
port = 27017,
database = 'test',
collection = 'my_collection',
options = 'connectTimeoutMS=10000'
```

<div id="xml-example">
  ### Exemplo em XML
</div>

```xml
<clickhouse>
    <named_collections>
        <mymongo>
            <user></user>
            <password></password>
            <host>127.0.0.1</host>
            <port>27017</port>
            <database>test</database>
            <collection>my_collection</collection>
            <options>connectTimeoutMS=10000</options>
        </mymongo>
    </named_collections>
</clickhouse>
```

<div id="mongodb-table">
  #### Tabela do MongoDB
</div>

```sql
CREATE TABLE mytable(log_type VARCHAR, host VARCHAR, command VARCHAR) ENGINE = MongoDB(mymongo, options='connectTimeoutMS=10000&compressors=zstd')
SELECT count() FROM mytable;

┌─count()─┐
│       2 │
└─────────┘
```

:::note
A DDL substitui as opções definidas na coleção nomeada.
:::

<div id="mongodb-dictionary">
  #### Dicionário MongoDB
</div>

```sql
CREATE DICTIONARY dict
(
    `a` Int64,
    `b` String
)
PRIMARY KEY a
SOURCE(MONGODB(NAME mymongo COLLECTION my_dict))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED())

SELECT dictGet('dict', 'b', 2);

┌─dictGet('dict', 'b', 2)─┐
│ two                     │
└─────────────────────────┘
```

:::note
A coleção nomeada define `my_collection` como o nome da coleção. Na chamada da função, esse valor é substituído por `collection = 'my_dict'` para selecionar outra coleção.
:::
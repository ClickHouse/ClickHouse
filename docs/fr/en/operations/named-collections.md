---
description: 'Documentation sur les collections nommées'
sidebar_label: 'Collections nommées'
sidebar_position: 69
slug: /operations/named-collections
title: 'Collections nommées'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

Les collections nommées permettent de stocker des ensembles de paires clé-valeur
utilisés pour configurer des intégrations avec des sources externes. Vous pouvez utiliser des collections nommées avec
des dictionnaires, des tables, des fonctions de table et le stockage objet.

Les collections nommées peuvent être configurées via le DDL ou dans des fichiers de configuration, et elles sont appliquées
au démarrage de ClickHouse. Elles simplifient la création d&#39;objets et permettent de masquer les identifiants
aux utilisateurs ne disposant pas d&#39;un accès administratif.

Les clés d&#39;une collection nommée doivent correspondre aux noms des paramètres de la
fonction, du moteur de table, de la base de données, etc. correspondants. Dans les exemples ci-dessous, un lien vers la liste des paramètres
est fourni pour chaque type.

Les paramètres définis dans une collection nommée peuvent être surchargés en SQL, comme le montrent les exemples
ci-dessous. Cette possibilité peut être limitée à l&#39;aide des mots-clés `[NOT] OVERRIDABLE` et des attributs XML,
et/ou de l&#39;option de configuration `allow_named_collection_override_by_default`.

:::warning
Si la surcharge est autorisée, il peut être possible pour des utilisateurs ne disposant pas d&#39;un accès administratif de
découvrir les identifiants que vous essayez de masquer.
Si vous utilisez des collections nommées à cette fin, vous devez désactiver
`allow_named_collection_override_by_default` (qui est activée par défaut).
:::

<div id="storing-named-collections-in-the-system-database">
  ## Stocker des collections nommées dans la base de données système
</div>

<div id="ddl-example">
  ### Exemple de DDL
</div>

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value' OVERRIDABLE,
key_2 = 'value2' NOT OVERRIDABLE,
url = 'https://connection.url/'
```

Dans l’exemple ci-dessus :

* `key_1` peut toujours être surchargé.
* `key_2` ne peut jamais être surchargé.
* `url` peut ou non être surchargé selon la valeur de `allow_named_collection_override_by_default`.

<div id="permissions-to-create-named-collections-with-ddl">
  ### Autorisations pour créer des collections nommées avec DDL
</div>

Pour gérer des collections nommées avec DDL, un utilisateur doit disposer du privilège `named_collection_control`. Cela peut être accordé en ajoutant un fichier dans `/etc/clickhouse-server/users.d/`. L’exemple ci-dessous accorde à l’utilisateur `default` les privilèges `access_management` et `named_collection_control` :

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
Dans l’exemple ci-dessus, la valeur `password_sha256_hex` est la représentation hexadécimale du hash SHA256 du mot de passe. Cette configuration de l’utilisateur `default` possède l’attribut `replace=true`, car la configuration par défaut définit un `password` en clair, et il n’est pas possible de définir à la fois un mot de passe en clair et un mot de passe SHA256 hexadécimal pour un même utilisateur.
:::

<div id="storage-for-named-collections">
  ### Stockage des collections nommées
</div>

Les collections nommées peuvent être stockées soit sur le disque local, soit dans ZooKeeper/Keeper. Par défaut, le stockage local est utilisé.
Elles peuvent aussi être stockées sous forme chiffrée, à l’aide des mêmes algorithmes que ceux utilisés pour le [chiffrement du disque](storing-data#encrypted-virtual-file-system),
`aes_128_ctr` étant utilisé par défaut.

Pour configurer le stockage des collections nommées, vous devez spécifier un `type`. Il peut s’agir de `local` ou de `keeper`/`zookeeper`. Pour le stockage chiffré,
vous pouvez utiliser `local_encrypted` ou `keeper_encrypted`/`zookeeper_encrypted`.

Pour utiliser ZooKeeper/Keeper, vous devez également définir un `path` (chemin dans ZooKeeper/Keeper où seront stockées les collections nommées) dans la
section `named_collections_storage` du fichier de configuration. L’exemple suivant utilise le chiffrement et ZooKeeper/Keeper :

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

Le paramètre de configuration facultatif `update_timeout_ms` vaut `5000` par défaut.

<div id="storing-named-collections-in-configuration-files">
  ## Stocker les collections nommées dans les fichiers de configuration
</div>

<div id="xml-example">
  ### Exemple en XML
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

Dans l’exemple ci-dessus :

* `key_1` peut toujours être surchargé.
* `key_2` ne peut jamais être surchargé.
* `url` peut être surchargé ou non selon la valeur de `allow_named_collection_override_by_default`.

<div id="modifying-named-collections">
  ## Modification des collections nommées
</div>

Les collections nommées créées à l’aide de requêtes DDL peuvent être modifiées ou supprimées au moyen de DDL. Les collections nommées créées à partir de fichiers XML peuvent être gérées en modifiant ou en supprimant le fichier XML correspondant.

<div id="alter-a-ddl-named-collection">
  ### Modifier une collection nommée via DDL
</div>

Modifiez ou ajoutez les clés `key1` et `key3` de la collection `collection2`
(cela ne modifiera pas la valeur du paramètre `overridable` pour ces clés) :

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

Modifiez ou ajoutez la clé `key1` et autorisez qu’elle soit toujours surchargée :

```sql
ALTER NAMED COLLECTION collection2 SET key1=4 OVERRIDABLE
```

Supprimez la clé `key2` de `collection2` :

```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

Modifiez ou ajoutez la clé `key1` et supprimez la clé `key3` de la collection `collection2` :

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

Pour qu’une clé utilise les paramètres par défaut du paramètre `overridable`, vous devez
supprimer la clé, puis l’ajouter à nouveau.

```sql
ALTER NAMED COLLECTION collection2 DELETE key1;
ALTER NAMED COLLECTION collection2 SET key1=4;
```

<div id="drop-the-ddl-named-collection-collection2">
  ### Supprimez la collection nommée `collection2` définie en DDL :
</div>

```sql
DROP NAMED COLLECTION collection2
```

<div id="named-collections-for-accessing-s3">
  ## Collections nommées pour accéder à S3
</div>

Pour la description des paramètres, voir [fonction de table S3](../sql-reference/table-functions/s3.md).

<div id="ddl-example">
  ### Exemple de DDL
</div>

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

<div id="xml-example-1">
  ### Exemple XML
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
  ### Exemples de collection nommée avec la fonction s3() et la table S3
</div>

Les deux exemples suivants utilisent la même collection nommée `s3_mydata` :

<div id="s3-function">
  #### fonction s3()
</div>

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
Le premier argument de la fonction `s3()` ci-dessus est le nom de la collection, `s3_mydata`. Sans collections nommées, l&#39;ID de la clé d&#39;accès, le secret, le format et l&#39;URL devraient être passés à chaque appel de la fonction `s3()`.
:::

<div id="s3-table">
  #### Table S3
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
  ## Collections nommées pour accéder à une base de données MySQL
</div>

Pour la description des paramètres, voir [mysql](../sql-reference/table-functions/mysql.md).

<div id="ddl-example">
  ### Exemple de DDL
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

<div id="xml-example-1">
  ### Exemple XML
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
  ### Exemples de collections nommées pour la fonction mysql(), la table MySQL, la base de données MySQL et le dictionnaire
</div>

Les quatre exemples suivants utilisent la même collection nommée `mymysql` :

<div id="mysql-function">
  #### fonction mysql()
</div>

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

:::note
La collection nommée ne précise pas le paramètre `table` ; il est donc indiqué dans l&#39;appel de fonction sous la forme `table = 'test'`.
:::

<div id="mysql-table">
  #### Table MySQL
</div>

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
L’instruction DDL remplace le paramètre `connection_pool_size` défini dans la collection nommée.
:::

<div id="mysql-database">
  #### Base de données MySQL
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
  #### Dictionnaire MySQL
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
  ## Collections nommées pour accéder à une base de données PostgreSQL
</div>

Pour la description des paramètres, voir [postgresql](../sql-reference/table-functions/postgresql.md). En outre, il existe des alias :

* `username` pour `user`
* `db` pour `database`.

Le paramètre `addresses_expr` est utilisé dans une collection nommée à la place de `host:port`. Ce paramètre est facultatif, car d&#39;autres paramètres facultatifs existent également : `host`, `hostname`, `port`. Le pseudo-code suivant explique la priorité :

```sql
CASE
    WHEN collection['addresses_expr'] != '' THEN collection['addresses_expr']
    WHEN collection['host'] != ''           THEN collection['host'] || ':' || if(collection['port'] != '', collection['port'], '5432')
    WHEN collection['hostname'] != ''       THEN collection['hostname'] || ':' || if(collection['port'] != '', collection['port'], '5432')
END
```

Exemple de création :

```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema'
```

Exemple de configuration :

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
  ### Exemple d’utilisation des collections nommées avec la fonction postgresql
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
  ### Exemple d’utilisation de collections nommées avec une base de données utilisant le moteur PostgreSQL
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
PostgreSQL copie les données de la collection nommée lors de la création de la table. Toute modification de la collection n’affecte pas les tables existantes.
:::

<div id="example-of-using-named-collections-with-database-with-engine-postgresql">
  ### Exemple d’utilisation de collections nommées avec une base de données utilisant le moteur PostgreSQL
</div>

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-postgresql">
  ### Exemple d’utilisation de collections nommées avec un dictionnaire de source POSTGRESQL
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
  ## Collections nommées pour accéder à une base de données ClickHouse distante
</div>

Pour une description des paramètres, voir [remote](../sql-reference/table-functions/remote.md/#parameters).

Exemple de configuration :

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

`secure` n’est pas nécessaire pour la connexion, car `remoteSecure` est utilisé, mais il peut servir pour les dictionnaires.

<div id="example-of-using-named-collections-with-the-remoteremotesecure-functions">
  ### Exemple d’utilisation de collections nommées avec les fonctions `remote`/`remoteSecure`
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
  ### Exemple d’utilisation de collections nommées avec un dictionnaire dont la source est ClickHouse
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
  ## Collections nommées pour accéder à Kafka
</div>

Voir la description des paramètres dans [Kafka](../engines/table-engines/integrations/kafka.md).

<div id="ddl-example">
  ### Exemple de DDL
</div>

```sql
CREATE NAMED COLLECTION my_kafka_cluster AS
kafka_broker_list = 'localhost:9092',
kafka_topic_list = 'kafka_topic',
kafka_group_name = 'consumer_group',
kafka_format = 'JSONEachRow',
kafka_max_block_size = '1048576';

```

<div id="xml-example-1">
  ### Exemple XML
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
  ### Exemple d’utilisation de collections nommées avec une table Kafka
</div>

Les deux exemples suivants utilisent la même collection nommée `my_kafka_cluster` :

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
  ## Collections nommées pour les sauvegardes
</div>

Pour la description des paramètres, voir [Sauvegarde et restauration](/fr/operations/backup/overview).

<div id="ddl-example">
  ### Exemple de DDL
</div>

```sql
BACKUP TABLE default.test to S3(named_collection_s3_backups, 'directory')
```

<div id="xml-example-1">
  ### Exemple XML
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
  ## Collections nommées pour accéder à la table et au dictionnaire MongoDB
</div>

Pour la description des paramètres, voir [mongodb](../sql-reference/table-functions/mongodb.md).

<div id="ddl-example">
  ### Exemple de DDL
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

<div id="xml-example-1">
  ### Exemple XML
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
  #### Table MongoDB
</div>

```sql
CREATE TABLE mytable(log_type VARCHAR, host VARCHAR, command VARCHAR) ENGINE = MongoDB(mymongo, options='connectTimeoutMS=10000&compressors=zstd')
SELECT count() FROM mytable;

┌─count()─┐
│       2 │
└─────────┘
```

:::note
Le DDL remplace le paramètre « options » de la collection nommée.
:::

<div id="mongodb-dictionary">
  #### Dictionnaire MongoDB
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
La collection nommée définit `my_collection` comme nom de collection. Dans l’appel de fonction, cette valeur est remplacée par `collection = 'my_dict'` afin de sélectionner une autre collection.
:::
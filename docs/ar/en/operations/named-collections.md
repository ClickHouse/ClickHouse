---
description: 'توثيق المجموعات المُسمّاة'
sidebar_label: 'المجموعات المُسمّاة'
sidebar_position: 69
slug: /operations/named-collections
title: 'المجموعات المُسمّاة'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

توفّر المجموعات المُسمّاة طريقة لتخزين مجموعات من أزواج المفتاح والقيمة لاستخدامها
في تهيئة عمليات التكامل مع المصادر الخارجية. يمكنك استخدام المجموعات المُسمّاة مع
القواميس والجداول ودوال الجداول والتخزين الكائني.

يمكن تهيئة المجموعات المُسمّاة باستخدام DDL أو ضمن ملفات التهيئة، ويُعمل بها
عند بدء تشغيل ClickHouse. وهي تُسهّل إنشاء الكائنات وإخفاء بيانات الاعتماد
عن المستخدمين الذين لا يملكون صلاحيات إدارية.

يجب أن تتطابق المفاتيح في المجموعة المُسمّاة مع أسماء المعلَمات الخاصة
بالدالة أو محرك الجدول أو قاعدة البيانات المقابلة، وما إلى ذلك. في الأمثلة أدناه،
توجد روابط إلى قائمة المعلَمات لكل نوع.

يمكن تجاوز المعلَمات المحددة في مجموعة مُسمّاة داخل SQL، كما هو موضّح في الأمثلة
أدناه. ويمكن تقييد هذه الإمكانية باستخدام الكلمتين المفتاحيتين `[NOT] OVERRIDABLE` وسمات XML
و/أو خيار التهيئة `allow_named_collection_override_by_default`.

:::warning
إذا كان التجاوز مسموحًا، فقد يتمكن المستخدمون الذين لا يملكون صلاحيات إدارية من
معرفة بيانات الاعتماد التي تحاول إخفاءها.
إذا كنت تستخدم المجموعات المُسمّاة لهذا الغرض، فينبغي عليك تعطيل
`allow_named_collection_override_by_default` (وهو مفعّل افتراضيًا).
:::

<div id="storing-named-collections-in-the-system-database">
  ## تخزين المجموعات المُسمّاة في قاعدة بيانات النظام
</div>

<div id="ddl-example">
  ### مثال على DDL
</div>

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value' OVERRIDABLE,
key_2 = 'value2' NOT OVERRIDABLE,
url = 'https://connection.url/'
```

في المثال أعلاه:

* يمكن دائمًا تجاوز `key_1`.
* لا يمكن تجاوز `key_2` مطلقًا.
* تعتمد إمكانية تجاوز `url` على قيمة `allow_named_collection_override_by_default`.

<div id="permissions-to-create-named-collections-with-ddl">
  ### الأذونات المطلوبة لإنشاء المجموعات المُسمّاة باستخدام DDL
</div>

لإدارة المجموعات المُسمّاة باستخدام DDL، يجب أن يمتلك المستخدم امتياز `named_collection_control`. ويمكن منح هذا الامتياز بإضافة ملف إلى `/etc/clickhouse-server/users.d/`. يوضّح المثال منح المستخدم `default` امتيازي `access_management` و`named_collection_control`:

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
في المثال أعلاه، تمثّل القيمة `password_sha256_hex` التمثيل السداسي العشري لتجزئة SHA256 الخاصة بكلمة المرور. تحتوي تهيئة المستخدم `default` هذه على السمة `replace=true` لأن التهيئة الافتراضية تتضمن تعيين `password` كنص عادي، ولا يمكن للمستخدم نفسه أن يملك في الوقت ذاته كلمة مرور بنص عادي وكلمة مرور بصيغة sha256 hex.
:::

<div id="storage-for-named-collections">
  ### تخزين المجموعات المُسمّاة
</div>

يمكن تخزين المجموعات المُسمّاة إما على القرص المحلي أو في ZooKeeper/Keeper. ويُستخدم التخزين المحلي افتراضيًا.
ويمكن أيضًا تخزينها بشكل مشفّر باستخدام الخوارزميات نفسها المستخدمة في [تشفير القرص](storing-data#encrypted-virtual-file-system)،
حيث يُستخدم `aes_128_ctr` افتراضيًا.

لإعداد تخزين المجموعات المُسمّاة، تحتاج إلى تحديد `type`. ويمكن أن تكون قيمته إما `local` أو `keeper`/`zookeeper`. أما بالنسبة إلى التخزين المشفّر،
فيمكنك استخدام `local_encrypted` أو `keeper_encrypted`/`zookeeper_encrypted`.

لاستخدام ZooKeeper/Keeper، نحتاج أيضًا إلى إعداد `path` (المسار في ZooKeeper/Keeper الذي ستُخزَّن فيه المجموعات المُسمّاة) ضمن
قسم `named_collections_storage` في ملف الإعداد. يستخدم المثال التالي التشفير وZooKeeper/Keeper:

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

مَعْلَمة إعداد اختيارية `update_timeout_ms` تساوي `5000` افتراضيًا.

<div id="storing-named-collections-in-configuration-files">
  ## تخزين المجموعات المُسمّاة في ملفات التهيئة
</div>

<div id="xml-example">
  ### مثال بتنسيق XML
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

في المثال أعلاه:

* يمكن دائمًا تجاوز `key_1`.
* لا يمكن أبدًا تجاوز `key_2`.
* يمكن تجاوز `url` أو عدم تجاوزه، بحسب قيمة `allow_named_collection_override_by_default`.

<div id="modifying-named-collections">
  ## تعديل المجموعات المُسمّاة
</div>

يمكن تعديل المجموعات المُسمّاة التي أُنشئت باستخدام استعلامات DDL أو حذفها باستخدام DDL. أما المجموعات المُسمّاة التي أُنشئت باستخدام ملفات XML، فيمكن إدارتها عبر تحرير ملف XML المقابل أو حذفه.

<div id="alter-a-ddl-named-collection">
  ### تعديل مجموعة DDL مُسمّاة
</div>

غيّر أو أضف المفتاحين `key1` و`key3` في المجموعة `collection2`
(لن يغيّر ذلك قيمة الخاصية `overridable` لهذين المفتاحين):

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

غيّر المفتاح `key1` أو أضِفه، واجعل من الممكن دائمًا تجاوز قيمته:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4 OVERRIDABLE
```

أزل المفتاح `key2` من `collection2`:

```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

غيّر المفتاح `key1` أو أضِفه، واحذف المفتاح `key3` من المجموعة `collection2`:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

لإجبار مفتاحٍ ما على استخدام الإعدادات الافتراضية للخيار `overridable`، يجب عليك
إزالة المفتاح ثم إعادة إضافته.

```sql
ALTER NAMED COLLECTION collection2 DELETE key1;
ALTER NAMED COLLECTION collection2 SET key1=4;
```

<div id="drop-the-ddl-named-collection-collection2">
  ### احذف المجموعة المسماة `collection2` الخاصة بـ DDL:
</div>

```sql
DROP NAMED COLLECTION collection2
```

<div id="named-collections-for-accessing-s3">
  ## المجموعات المُسمّاة للوصول إلى S3
</div>

للاطلاع على وصف المعلمات، راجع [دالة الجدول S3](../sql-reference/table-functions/s3.md).

<div id="ddl-example">
  ### مثال على DDL
</div>

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

<div id="xml-example">
  ### مثال بتنسيق XML
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
  ### أمثلة على الدالة s3() والمجموعة المسماة لجدول S3
</div>

يستخدم المثالان التاليان المجموعة المسماة نفسها `s3_mydata`:

<div id="s3-function">
  #### دالة s3()
</div>

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
أول وسيط للدالة `s3()` أعلاه هو اسم المجموعة المسماة، `s3_mydata`. ومن دون المجموعات المسماة، سيتعيّن تمرير معرّف مفتاح الوصول، والسر، والتنسيق، وURL في كل استدعاء للدالة `s3()`.
:::

<div id="s3-table">
  #### جدول S3
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
  ## المجموعات المُسمّاة للوصول إلى قاعدة بيانات MySQL
</div>

راجع وصف المعلمات في [mysql](../sql-reference/table-functions/mysql.md).

<div id="ddl-example">
  ### مثال على DDL
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

<div id="xml-example-2">
  ### مثال بصيغة XML
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
  ### أمثلة على الدالة `mysql()`، وجدول MySQL، وقاعدة بيانات MySQL، ومجموعة مُسمّاة لـ قاموس
</div>

تستخدم الأمثلة الأربعة التالية المجموعة المسماة نفسها `mymysql`:

<div id="mysql-function">
  #### الدالة mysql()
</div>

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

:::note
لا تتضمن مجموعة مُسمّاة المَعلمة `table`، لذا يُحدَّد هذا المَعْلم في استدعاء الدالة بالشكل `table = 'test'`.
:::

<div id="mysql-table">
  #### جدول MySQL
</div>

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
يحلّ تعريف DDL محل إعداد connection&#95;pool&#95;size في مجموعة مُسمّاة.
:::

<div id="mysql-database">
  #### قاعدة بيانات MySQL
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
  #### قاموس MySQL
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
  ## المجموعات المُسمّاة للوصول إلى قاعدة بيانات PostgreSQL
</div>

للاطلاع على وصف المَعلمات، راجع [postgresql](../sql-reference/table-functions/postgresql.md). بالإضافة إلى ذلك، تتوفر أسماء مستعارة:

* `username` بدلًا من `user`
* `db` بدلًا من `database`.

تُستخدم المَعلمة `addresses_expr` في مجموعة بدلًا من `host:port`. وهذه المَعلمة اختيارية، لأن هناك مَعلمات اختيارية أخرى، وهي: `host` و`hostname` و`port`. توضّح الشيفرة الزائفة التالية ترتيب الأولوية:

```sql
CASE
    WHEN collection['addresses_expr'] != '' THEN collection['addresses_expr']
    WHEN collection['host'] != ''           THEN collection['host'] || ':' || if(collection['port'] != '', collection['port'], '5432')
    WHEN collection['hostname'] != ''       THEN collection['hostname'] || ':' || if(collection['port'] != '', collection['port'], '5432')
END
```

مثال على كيفية الإنشاء:

```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema'
```

مثال على التهيئة:

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
  ### مثال على استخدام المجموعات المُسمّاة مع الدالة postgresql
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
  ### مثال على استخدام المجموعات المُسمّاة مع قاعدة بيانات تستخدم محرك PostgreSQL
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
ينسخ PostgreSQL البيانات من المجموعة المُسمّاة عند إنشاء الجدول. وأي تغيير يطرأ على المجموعة لا يؤثر في الجداول الحالية.
:::

<div id="example-of-using-named-collections-with-database-with-engine-postgresql">
  ### مثال على استخدام المجموعات المُسمّاة مع قاعدة بيانات تستخدم محرك PostgreSQL
</div>

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-postgresql">
  ### مثال على استخدام المجموعات المسماة مع قاموس مصدره POSTGRESQL
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
  ## المجموعات المُسمّاة للوصول إلى قاعدة بيانات ClickHouse بعيدة
</div>

للاطّلاع على وصف المعاملات، راجع [remote](../sql-reference/table-functions/remote.md/#parameters).

مثال على التهيئة:

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

`secure` ليس مطلوبًا للاتصال بسبب `remoteSecure`، ولكن يمكن استخدامه مع القواميس.

<div id="example-of-using-named-collections-with-the-remoteremotesecure-functions">
  ### مثال على استخدام المجموعات المُسمّاة مع الدالتين `remote`/`remoteSecure`
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
  ### مثال على استخدام المجموعات المسماة مع قاموس مصدره ClickHouse
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
  ## المجموعات المُسمّاة للوصول إلى Kafka
</div>

للاطلاع على وصف المَعلمات، راجع [Kafka](../engines/table-engines/integrations/kafka.md).

<div id="ddl-example">
  ### مثال على DDL
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
  ### مثال بتنسيق XML
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
  ### مثال على استخدام المجموعات المُسمّاة مع جدول Kafka
</div>

يستخدم المثالان التاليان المجموعة المُسمّاة نفسها `my_kafka_cluster`:

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
  ## المجموعات المُسمّاة للنسخ الاحتياطية
</div>

للاطلاع على وصف المعلمات، راجع [النسخ الاحتياطي والاستعادة](/ar/operations/backup/overview).

<div id="ddl-example">
  ### مثال على DDL
</div>

```sql
BACKUP TABLE default.test to S3(named_collection_s3_backups, 'directory')
```

<div id="xml-example-2">
  ### مثال بصيغة XML
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
  ## المجموعات المُسمّاة للوصول إلى جدول MongoDB والقاموس
</div>

للاطلاع على وصف المعلمات، راجع [mongodb](../sql-reference/table-functions/mongodb.md).

<div id="ddl-example">
  ### مثال على DDL
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
  ### مثال بتنسيق XML
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
  #### جدول MongoDB
</div>

```sql
CREATE TABLE mytable(log_type VARCHAR, host VARCHAR, command VARCHAR) ENGINE = MongoDB(mymongo, options='connectTimeoutMS=10000&compressors=zstd')
SELECT count() FROM mytable;

┌─count()─┐
│       2 │
└─────────┘
```

:::note
تتجاوز عبارة DDL إعدادات الخيارات المحددة في مجموعة مُسمّاة.
:::

<div id="mongodb-dictionary">
  #### قاموس MongoDB
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
تحدّد المجموعة المُسمّاة `my_collection` اسم المجموعة. وفي استدعاء الدالة، يُستبدَل هذا الإعداد بـ `collection = 'my_dict'` لاختيار مجموعة أخرى.
:::
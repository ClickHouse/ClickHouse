---
description: 'توثيق دوال العمل مع القواميس'
sidebar_label: 'القواميس'
slug: /sql-reference/functions/ext-dict-functions
title: 'دوال العمل مع القواميس'
doc_type: 'مرجع'
---

:::note
بالنسبة إلى القواميس التي أُنشئت باستخدام [استعلامات DDL](../statements/create/dictionary/overview.md)، يجب تحديد المعلَمة `dict_name` بالكامل، مثل `<database>.<dict_name>`. وإلا، فستُستخدم قاعدة البيانات الحالية.
:::

للاطلاع على معلومات حول ربط القواميس وتهيئتها، راجع [القواميس](../statements/create/dictionary/overview.md).

<div id="example-dictionary">
  ## قواميس أمثلة
</div>

تستخدم الأمثلة الواردة في هذا القسم القواميس التالية. يمكنك إنشاؤها في ClickHouse
لتشغيل أمثلة الدالة الموضحة أدناه.

<details>
  <summary>قاموس مثال للدالتين dictGet&lt;T&gt; وdictGet&lt;T&gt;OrDefault</summary>

  ```sql
  -- إنشاء جدول بجميع أنواع البيانات المطلوبة
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- نوع String
      `String_value` String,
      
      -- أنواع الأعداد الصحيحة غير الموقعة
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- أنواع الأعداد الصحيحة الموقعة
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- أنواع الأعداد ذات الفاصلة العائمة
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- أنواع التاريخ/الوقت
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- أنواع الشبكة
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- نوع UUID
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- إدراج بيانات اختبار
  INSERT INTO all_types_test VALUES
  (
      1,                              -- id
      'ClickHouse',                   -- String
      100,                            -- UInt8
      5000,                           -- UInt16
      1000000,                        -- UInt32
      9223372036854775807,            -- UInt64
      -100,                           -- Int8
      -5000,                          -- Int16
      -1000000,                       -- Int32
      -9223372036854775808,           -- Int64
      123.45,                         -- Float32
      987654.123456,                  -- Float64
      '2024-01-15',                   -- Date
      '2024-01-15 10:30:00',          -- DateTime
      '192.168.1.1',                  -- IPv4
      '2001:db8::1',                  -- IPv6
      '550e8400-e29b-41d4-a716-446655440000' -- UUID
  )
  ```

  ```sql
  -- إنشاء قاموس
  CREATE DICTIONARY all_types_dict
  (
      id UInt32,
      String_value String,
      UInt8_value UInt8,
      UInt16_value UInt16,
      UInt32_value UInt32,
      UInt64_value UInt64,
      Int8_value Int8,
      Int16_value Int16,
      Int32_value Int32,
      Int64_value Int64,
      Float32_value Float32,
      Float64_value Float64,
      Date_value Date,
      DateTime_value DateTime,
      IPv4_value IPv4,
      IPv6_value IPv6,
      UUID_value UUID
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'all_types_test' DB 'default'))
  LAYOUT(HASHED())
  LIFETIME(MIN 300 MAX 600);
  ```
</details>

<details>
  <summary>قاموس مثال لـ dictGetAll</summary>

  أنشئ جدولًا لتخزين بيانات قاموس regexp tree:

  ```sql
  CREATE TABLE regexp_os(
      id UInt64,
      parent_id UInt64,
      regexp String,
      keys Array(String),
      values Array(String)
  )
  ENGINE = Memory;
  ```

  أدرج البيانات في الجدول:

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  أنشئ قاموس regexp tree:

  ```sql
  CREATE DICTIONARY regexp_tree
  (
      regexp String,
      os_replacement String DEFAULT 'Other',
      os_v1_replacement String DEFAULT '0',
      os_v2_replacement String DEFAULT '0',
      os_v3_replacement String DEFAULT '0',
      os_v4_replacement String DEFAULT '0'
  )
  PRIMARY KEY regexp
  SOURCE(CLICKHOUSE(TABLE 'regexp_os'))
  LIFETIME(MIN 0 MAX 0)
  LAYOUT(REGEXP_TREE);
  ```
</details>

<details>
  <summary>مثال على قاموس بمفتاح نطاق</summary>

  أنشئ جدول الإدخال:

  ```sql
  CREATE TABLE range_key_dictionary_source_table
  (
      key UInt64,
      start_date Date,
      end_date Date,
      value String,
      value_nullable Nullable(String)
  )
  ENGINE = TinyLog();
  ```

  أدرج البيانات في جدول الإدخال:

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  أنشئ القاموس:

  ```sql
  CREATE DICTIONARY range_key_dictionary
  (
      key UInt64,
      start_date Date,
      end_date Date,
      value String,
      value_nullable Nullable(String)
  )
  PRIMARY KEY key
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_dictionary_source_table'))
  LIFETIME(MIN 1 MAX 1000)
  LAYOUT(RANGE_HASHED())
  RANGE(MIN start_date MAX end_date);
  ```
</details>

<details>
  <summary>مثال على قاموس بمفتاح مركب</summary>

  أنشئ الجدول المصدر:

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  أدرج البيانات في الجدول المصدر:

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  أنشئ القاموس:

  ```sql
  CREATE DICTIONARY ext_dict_mult
  (
      id UInt32,
      c1 UInt32,
      c2 String
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dict_mult_source' DB 'default'))
  LAYOUT(FLAT())
  LIFETIME(MIN 0 MAX 0);
  ```
</details>

<details>
  <summary>مثال على قاموس هرمي</summary>

  أنشئ الجدول المصدر:

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  أدرج البيانات في الجدول المصدر:

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0 (الجذر)
  -- └── 1 (المستوى 1 - العقدة 1)
  --     ├── 2 (المستوى 2 - العقدة 2)
  --     │   ├── 4 (المستوى 3 - العقدة 4)
  --     │   └── 5 (المستوى 3 - العقدة 5)
  --     └── 3 (المستوى 2 - العقدة 3)
  --         └── 6 (المستوى 3 - العقدة 6)
  ```

  أنشئ القاموس:

  ```sql
  CREATE DICTIONARY hierarchical_dictionary
  (
      id UInt64,
      parent_id UInt64 HIERARCHICAL,
      name String
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'hierarchy_source' DB 'default'))
  LAYOUT(HASHED())
  LIFETIME(MIN 300 MAX 600);
  ```
</details>

<div id="passing-keys">
  ## تمرير المفاتيح إلى دالة القاموس
</div>

تعتمد وسيطة المفتاح (`id_expr`) في الدالة مثل `dictGet` و`dictGetOrDefault` و`dictGetOrNull` و`dictHas` على مفتاح القاموس:

* في القاموس ذي **المفتاح البسيط** (`UInt64`)، مرّر قيمة المفتاح مباشرةً:

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* بالنسبة إلى قاموس ذي **مفتاح مركّب (معقّد)** يتكوّن من أكثر من سمة واحدة، مرّر قيم المفتاح على هيئة tuple:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* عندما **يتكوّن المفتاح المركب من سمة واحدة**، يمكن تمرير قيمة المفتاح مباشرةً، من دون وضعها داخل `tuple`. وكلتا الطريقتين التاليتين صحيحتان ومتكافئتان:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

ينطبق هذا أيضًا على قواميس `ip_trie`، إذ يكون مفتاحها سمة واحدة. ويمكن تمرير عنوان IP المراد البحث عنه مباشرةً:

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  يُستبدل المحتوى الداخلي للوسوم أدناه، وقت بناء إطار عمل التوثيق،
  بوثائق مُولَّدة من system.functions. يُرجى عدم تعديل الوسوم أو إزالتها.
  انظر: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }
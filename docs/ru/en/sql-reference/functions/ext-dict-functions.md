---
description: 'Документация по функциям для работы со словарями'
sidebar_label: 'Словари'
slug: /sql-reference/functions/ext-dict-functions
title: 'Функции для работы со словарями'
doc_type: 'reference'
---

:::note
Для словарей, созданных с помощью [DDL-запросов](../statements/create/dictionary/overview.md), параметр `dict_name` должен быть указан полностью, в виде `<database>.<dict_name>`. В противном случае будет использоваться текущая база данных.
:::

Информацию о подключении и настройке словарей см. в разделе [Словари](../statements/create/dictionary/overview.md).

<div id="example-dictionary">
  ## Примеры словарей
</div>

В примерах этого раздела используются следующие словари. Вы можете создать их в ClickHouse,
чтобы запустить примеры для функций, описанных ниже.

<details>
  <summary>Пример словаря для функций dictGet&lt;T&gt; и dictGet&lt;T&gt;OrDefault</summary>

  ```sql
  -- Создать таблицу со всеми необходимыми типами данных
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- Тип String
      `String_value` String,
      
      -- Беззнаковые целочисленные типы
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- Знаковые целочисленные типы
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- Типы с плавающей запятой
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- Типы даты и времени
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- Сетевые типы
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- Тип UUID
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- Вставить тестовые данные
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
  -- Создать словарь
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
  <summary>Пример словаря для dictGetAll</summary>

  Создайте таблицу для хранения данных словаря regexp tree:

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

  Вставьте данные в таблицу:

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  Создайте словарь regexp tree:

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
  <summary>Пример словаря с диапазонным ключом</summary>

  Создайте исходную таблицу:

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

  Вставьте данные в исходную таблицу:

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  Создайте словарь:

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
  <summary>Пример словаря с составным ключом</summary>

  Создайте исходную таблицу:

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  Вставьте данные в исходную таблицу:

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  Создайте словарь:

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
  <summary>Пример иерархического словаря</summary>

  Создайте исходную таблицу:

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  Вставьте данные в исходную таблицу:

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0 (Корень)
  -- └── 1 (Уровень 1 — узел 1)
  --     ├── 2 (Уровень 2 — узел 2)
  --     │   ├── 4 (Уровень 3 — узел 4)
  --     │   └── 5 (Уровень 3 — узел 5)
  --     └── 3 (Уровень 2 — узел 3)
  --         └── 6 (Уровень 3 — узел 6)
  ```

  Создайте словарь:

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
  ## Передача ключей в функции словаря
</div>

Аргумент ключа (`id_expr`) в таких функциях, как `dictGet`, `dictGetOrDefault`, `dictGetOrNull` и `dictHas`, зависит от типа ключа словаря:

* Для словаря с **простым ключом** (`UInt64`) передавайте значение ключа напрямую:

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* Для словаря с **составным ключом** из нескольких атрибутов передавайте значения ключа в виде кортежа:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* Если **составной ключ состоит из одного атрибута**, значение ключа можно передавать напрямую, не оборачивая его в `tuple`. Оба следующих варианта допустимы и эквивалентны:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

Это также относится к словарям `ip_trie`, у которых ключ состоит из одного атрибута. Искомый IP-адрес можно передать напрямую:

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  Внутреннее содержимое тегов ниже во время сборки документации заменяется
  документацией, сгенерированной из system.functions. Пожалуйста, не изменяйте и не удаляйте эти теги.
  См.: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }
---
description: '딕셔너리 관련 함수 문서'
sidebar_label: '딕셔너리'
slug: /sql-reference/functions/ext-dict-functions
title: '딕셔너리 관련 함수'
doc_type: 'reference'
---

:::note
[DDL queries](../statements/create/dictionary/overview.md)로 생성한 딕셔너리의 경우 `dict_name` 매개변수는 `<database>.<dict_name>`처럼 전체 이름을 지정해야 합니다. 그렇지 않으면 현재 데이터베이스가 사용됩니다.
:::

딕셔너리 연결 및 구성에 대한 자세한 내용은 [딕셔너리](../statements/create/dictionary/overview.md)를 참조하십시오.

<div id="example-dictionary">
  ## 예시 딕셔너리
</div>

이 섹션의 예시에서는 다음 딕셔너리를 사용합니다. 아래에 설명된 함수 예시를 실행하려면 ClickHouse에서
이 딕셔너리를 생성하십시오.

<details>
  <summary>dictGet&lt;T&gt; 및 dictGet&lt;T&gt;OrDefault 함수용 예시 딕셔너리</summary>

  ```sql
  -- 필요한 모든 데이터 타입이 포함된 테이블 생성
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- String 타입
      `String_value` String,
      
      -- 부호 없는 정수 타입
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- 부호 있는 정수 타입
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- 부동소수점 타입
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- 날짜/시간 타입
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- 네트워크 타입
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- UUID 타입
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- 테스트 데이터 삽입
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
  -- 딕셔너리 생성
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
  <summary>dictGetAll용 예시 딕셔너리</summary>

  regexp 트리 딕셔너리의 데이터를 저장할 테이블을 생성합니다.

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

  테이블에 데이터를 삽입합니다.

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  regexp 트리 딕셔너리를 생성합니다.

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
  <summary>예시 범위 키 딕셔너리</summary>

  입력 테이블을 생성합니다:

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

  입력 테이블에 데이터를 삽입합니다:

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  딕셔너리를 생성합니다:

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
  <summary>예시 복합 키 딕셔너리</summary>

  원본 테이블을 생성합니다:

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  원본 테이블에 데이터를 삽입합니다:

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  딕셔너리를 생성합니다:

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
  <summary>예시 계층형 딕셔너리</summary>

  원본 테이블을 생성합니다:

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  원본 테이블에 데이터를 삽입합니다:

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0 (루트)
  -- └── 1 (수준 1 - 노드 1)
  --     ├── 2 (수준 2 - 노드 2)
  --     │   ├── 4 (수준 3 - 노드 4)
  --     │   └── 5 (수준 3 - 노드 5)
  --     └── 3 (수준 2 - 노드 3)
  --         └── 6 (수준 3 - 노드 6)
  ```

  딕셔너리를 생성합니다:

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
  ## 딕셔너리 함수에 키 전달하기
</div>

`dictGet`, `dictGetOrDefault`, `dictGetOrNull`, `dictHas`와 같은 함수의 키 인수(`id_expr`)는 딕셔너리 키 유형에 따라 달라집니다.

* **단순 키**(`UInt64`)를 사용하는 딕셔너리에서는 키 값을 직접 전달합니다:

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* 둘 이상의 속성으로 구성된 **복합(복잡한) 키**를 사용하는 딕셔너리에서는 키 값을 튜플로 전달합니다:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* **복합 키가 단일 속성 하나로 이루어진 경우**, 키 값은 `tuple`로 감싸지 않고 그대로 전달할 수 있습니다. 다음 두 방식은 모두 유효하며 동일합니다:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

이는 키가 단일 속성인 `ip_trie` 딕셔너리에도 적용됩니다. 조회할 IP 주소를 직접 전달할 수 있습니다:

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  아래 태그의 내부 내용은 문서 프레임워크 빌드 시 
  system.functions에서 생성된 문서로 대체됩니다. 태그를 수정하거나 제거하지 마십시오.
  참고: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }
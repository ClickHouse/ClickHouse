---
description: '테이블 문서'
keywords: ['압축', '코덱', '스키마', 'DDL']
sidebar_label: '테이블'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

새 테이블을 생성합니다. 이 쿼리는 사용 사례에 따라 다양한 구문 형식으로 작성할 수 있습니다.

기본적으로 테이블은 현재 서버에서만 생성됩니다. 분산 DDL 쿼리는 `ON CLUSTER` 절을 통해 구현되며, 이에 대해서는 [별도로 설명합니다](../../../sql-reference/distributed-ddl.md).

<div id="syntax-forms">
  ## 구문 형식
</div>

<div id="with-explicit-schema">
  ### 명시적 스키마 사용
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

`db`가 설정되지 않은 경우 `db` 데이터베이스 또는 현재 데이터베이스에, 대괄호 안에 지정된 구조와 `engine` 엔진을 사용하여 `table_name`이라는 이름의 테이블을 생성합니다.
테이블 구조는 컬럼 설명, 보조 인덱스, 프로젝션, 제약 조건의 목록으로 이루어집니다. 엔진이 [기본 키](#primary-key)를 지원하는 경우, 이는 테이블 엔진의 매개변수로 지정됩니다.

가장 단순한 경우 컬럼 설명은 `name type` 형식입니다. 예시: `RegionID UInt32`.

기본값에 대한 표현식도 정의할 수 있습니다(아래 참조).

필요한 경우 하나 이상의 키 표현식과 함께 기본 키를 지정할 수 있습니다.

컬럼과 테이블에 주석을 추가할 수 있습니다.

<div id="with-a-schema-similar-to-other-table">
  ### 기존 테이블의 스키마 사용
</div>

ClickHouse는 기존 테이블의 스키마와 데이터를 복사할 수 있습니다.

기존 테이블의 스키마를 복제하려면 다음을 수행하십시오:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

이 구문은 다른 테이블과 동일한 구조의 테이블을 생성합니다.

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### 기존 테이블의 스키마 및 데이터 복제
</div>

기존 테이블의 스키마와 데이터를 복제하려면 다음과 같습니다:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

기존 테이블과 동일한 스키마와 데이터를 갖는 테이블을 생성합니다. 새 테이블이 생성되면 `db.table`의 모든 파티션이 여기에 ATTACH됩니다. 즉, 생성 시점에 `db.table`의 데이터가 `db2.table_clone`으로 복제됩니다. 이 쿼리는 다음과 동일합니다:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

두 기능 모두에서 테이블에 대해 서로 다른 엔진을 지정할 수 있습니다. 엔진을 지정하지 않으면 원본 테이블(`db.table`)에 사용된 것과 동일한 엔진이 사용됩니다.

<div id="from-a-table-function">
  ### 테이블 함수에서
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

지정된 [테이블 함수](/ko/sql-reference/table-functions)와 동일한 결과를 반환하는 테이블을 생성합니다. 생성된 테이블도 지정된 해당 테이블 함수와 같은 방식으로 동작합니다.

<div id="from-select-query">
  ### SELECT 쿼리에서
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

`SELECT` 쿼리 결과와 같은 구조의 테이블을 `engine` 엔진으로 생성하고, `SELECT`의 데이터로 채웁니다. 또한 컬럼 설명을 명시적으로 지정할 수도 있습니다.

테이블이 이미 존재하고 `IF NOT EXISTS`가 지정된 경우, 쿼리는 아무 작업도 수행하지 않습니다.

쿼리에서 `ENGINE` 절 뒤에 다른 절이 올 수도 있습니다. 테이블 생성 방법에 대한 자세한 내용은 [테이블 엔진](/ko/engines/table-engines) 설명을 참조하십시오.

**예시**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## NULL 또는 NOT NULL 수정자
</div>

컬럼 정의에서 데이터 타입 뒤에 오는 `NULL` 및 `NOT NULL` 수정자는 해당 타입을 [널 허용](/ko/sql-reference/data-types/nullable)으로 지정할지 여부를 결정합니다.

타입이 `Nullable`이 아닌 상태에서 `NULL`이 지정되면 `Nullable`로 처리되고, `NOT NULL`이 지정되면 그렇지 않습니다. 예를 들어 `INT NULL`은 `Nullable(INT)`와 같습니다. 타입이 이미 `Nullable`인데 `NULL` 또는 `NOT NULL` 수정자를 지정하면 예외가 발생합니다.

관련 항목: [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable) 설정.

<div id="default_values">
  ## 기본값
</div>

컬럼 설명에는 `DEFAULT expr`, `MATERIALIZED expr`, 또는 `ALIAS expr` 형식으로 기본값 표현식을 지정할 수 있습니다. 예시: `URLDomain String DEFAULT domain(URL)`.

표현식 `expr`은 선택 사항입니다. 이를 생략하면 컬럼 타입을 명시적으로 지정해야 하며, 기본값은 숫자 컬럼은 `0`, 문자열 컬럼은 `''`(빈 문자열), 배열 컬럼은 `[]`(빈 배열), 날짜 컬럼은 `1970-01-01`, 널 허용 컬럼은 `NULL`입니다.

기본값 컬럼의 컬럼 타입은 생략할 수 있으며, 이 경우 `expr`의 타입으로부터 자동으로 추론됩니다. 예를 들어 `EventDate DEFAULT toDate(EventTime)` 컬럼의 타입은 Date가 됩니다.

데이터 타입과 기본값 표현식을 모두 지정하면 표현식을 지정된 타입으로 변환하는 암시적 타입 캐스팅 함수가 삽입됩니다. 예시: `Hits UInt32 DEFAULT 0`은 내부적으로 `Hits UInt32 DEFAULT toUInt32(0)`으로 표현됩니다.

기본값 표현식 `expr`은 임의의 테이블 컬럼과 상수를 참조할 수 있습니다. ClickHouse는 테이블 구조 변경으로 인해 표현식 계산에 루프가 생기지 않는지 확인합니다. INSERT의 경우에는 표현식을 해석할 수 있는지, 즉 표현식을 계산하는 데 필요한 모든 컬럼이 전달되었는지 확인합니다.

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

일반 기본값입니다. 이러한 컬럼의 값이 `INSERT` 쿼리에서 지정되지 않으면 `expr`를 기반으로 계산됩니다.

예시:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

구체화된 표현식입니다. 이러한 컬럼의 값은 행이 삽입될 때 지정된 구체화된 표현식에 따라 자동으로 계산됩니다. `INSERT` 시에는 값을 명시적으로 지정할 수 없습니다.

또한 이 유형의 기본값 컬럼은 `SELECT *` 결과에 포함되지 않습니다. 이는 `SELECT *`의 결과를 언제나 `INSERT`를 사용해 다시 테이블에 삽입할 수 있다는 불변식을 유지하기 위한 것입니다. 이 동작은 `asterisk_include_materialized_columns` 설정으로 비활성화할 수 있습니다.

예시:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

임시 컬럼입니다. 이 유형의 컬럼은 테이블(table)에 저장되지 않으며, 이 컬럼을 대상으로 SELECT할 수 없습니다. 임시 컬럼의 유일한 목적은 이를 바탕으로 다른 컬럼의 기본값 표현식을 만드는 것입니다.

컬럼을 명시적으로 지정하지 않은 INSERT에서는 이 유형의 컬럼이 제외됩니다. 이는 `SELECT *`의 결과를 언제나 `INSERT`를 사용해 다시 테이블에 삽입할 수 있다는 불변식을 유지하기 위한 것입니다.

예시:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

계산된 컬럼(동의어)입니다. 이 유형의 컬럼은 테이블에 저장되지 않으며, 여기에 값을 INSERT할 수 없습니다.

SELECT 쿼리에서 이 유형의 컬럼을 명시적으로 참조하면 값은 `expr`로부터 쿼리 시점에 계산됩니다. 기본적으로 `SELECT *`에는 ALIAS 컬럼이 포함되지 않습니다. 이 동작은 설정 `asterisk_include_alias_columns`로 비활성화할 수 있습니다.

ALTER 쿼리를 사용해 새 컬럼을 추가해도 해당 컬럼의 기존 데이터는 기록되지 않습니다. 대신 새 컬럼 값이 없는 기존 데이터를 읽을 때는 기본적으로 표현식이 즉시 계산됩니다. 다만 표현식을 실행하는 데 쿼리에 지정되지 않은 다른 컬럼이 필요하면 해당 컬럼도 추가로 읽지만, 필요한 데이터 블록에 대해서만 읽습니다.

테이블에 새 컬럼을 추가한 후 나중에 해당 컬럼의 기본 표현식을 변경하면 기존 데이터에 사용되는 값도 변경됩니다(디스크에 값이 저장되지 않은 데이터의 경우). 백그라운드 머지가 실행될 때 머지되는 파트 중 하나에 없는 컬럼의 데이터는 병합된 파트에 기록된다는 점에 유의하십시오.

중첩 데이터 구조의 요소에는 기본값을 설정할 수 없습니다.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

<div id="primary-key">
  ## 기본 키
</div>

테이블을 생성할 때 [기본 키](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)를 정의할 수 있습니다. 기본 키는 두 가지 방식으로 지정할 수 있습니다.

* 컬럼 목록 내에서

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* 컬럼 목록 밖

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
하나의 쿼리에서 두 방식을 함께 사용할 수는 없습니다.
:::

<div id="constraints">
  ## 제약 조건
</div>

컬럼 설명과 함께 제약 조건도 정의할 수 있습니다:

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1`은 임의의 불리언 표현식일 수 있습니다. 테이블(table)에 제약 조건이 정의되어 있으면 `INSERT` 쿼리의 모든 행마다 각 제약 조건이 검사됩니다. 제약 조건 중 하나라도 충족되지 않으면 서버는 제약 조건 이름과 검사 표현식을 포함한 예외를 발생시킵니다.

제약 조건을 많이 추가하면 대규모 `INSERT` 쿼리의 성능에 부정적인 영향을 줄 수 있습니다.

모든 테이블에 존재하는 기존 제약 조건은 [`system.constraints`](/ko/operations/system-tables/constraints) 테이블을 통해 확인할 수 있습니다.

<div id="assume">
  ### ASSUME
</div>

`ASSUME` 절은 참이라고 가정되는 테이블(table)의 `CONSTRAINT`를 정의하는 데 사용됩니다. 이렇게 정의된 제약 조건은 이후 옵티마이저가 SQL 쿼리 성능을 향상시키는 데 활용할 수 있습니다.

다음은 `users_a` 테이블을 생성할 때 `ASSUME CONSTRAINT`를 사용하는 예시입니다:

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

여기서 `ASSUME CONSTRAINT`는 `length(name)` 함수의 값이 항상 `name_len` 컬럼 값과 같다고 간주하도록 지정하는 데 사용됩니다. 즉, 쿼리에서 `length(name)`이 호출될 때마다 ClickHouse는 이를 `name_len`으로 대체할 수 있으며, `length()` 함수를 호출하지 않아도 되므로 더 빠를 수 있습니다.

그런 다음 `SELECT name FROM users_a WHERE length(name) < 5;` 쿼리를 실행할 때 ClickHouse는 `ASSUME CONSTRAINT`를 기반으로 이를 `SELECT name FROM users_a WHERE name_len < 5`;로 최적화할 수 있습니다. 이렇게 하면 각 행마다 `name`의 길이를 계산하지 않아도 되므로 쿼리가 더 빠르게 실행될 수 있습니다.

`ASSUME CONSTRAINT`는 **제약 조건을 강제하지 않으며**, 단지 해당 제약 조건이 참이라고 옵티마이저에 알려줄 뿐입니다. 제약 조건이 실제로 참이 아니라면 쿼리 결과가 올바르지 않을 수 있습니다. 따라서 제약 조건이 참이라고 확신할 수 있을 때만 `ASSUME CONSTRAINT`를 사용해야 합니다.

<div id="ttl-expression">
  ## TTL 표현식
</div>

값의 저장 기간을 정의합니다. MergeTree 계열 테이블에서만 지정할 수 있습니다. 자세한 내용은 [컬럼과 테이블의 TTL](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)을 참조하십시오.

<div id="column_compression_codec">
  ## 컬럼 압축 코덱
</div>

기본적으로 ClickHouse는 자가 관리형 버전에서 `lz4` 압축을 적용하며, ClickHouse Cloud에서는 `zstd`를 적용합니다.

`MergeTree` 엔진 계열에서는 서버 구성의 [compression](/ko/operations/server-configuration-parameters/settings#compression) 섹션에서 기본 압축 방식을 변경할 수 있습니다.

또한 각 컬럼별 압축 방식을 `CREATE TABLE` 쿼리에서 정의할 수도 있습니다.

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

`Default` 코덱은 기본 압축을 가리키도록 지정할 수 있으며, 이 기본 압축은 런타임의 여러 설정(및 데이터 속성)에 따라 달라질 수 있습니다.
예시: `value UInt64 CODEC(Default)` — 코덱을 지정하지 않은 것과 같습니다.

또한 컬럼에서 현재 CODEC을 제거하고 config.xml에 정의된 기본 압축을 사용할 수도 있습니다:

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

코덱은 파이프라인으로 조합할 수 있습니다. 예를 들어 `CODEC(Delta, Default)`와 같습니다.

:::tip
ClickHouse 데이터베이스 파일은 `lz4`와 같은 외부 유틸리티로 압축 해제할 수 없습니다. 대신 전용 [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) 유틸리티를 사용하십시오.
:::

다음 테이블 엔진에서 압축을 지원합니다.

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) 계열. 컬럼 압축 코덱을 지원하며 [compression](/ko/operations/server-configuration-parameters/settings#compression) 설정에서 기본 압축 방식을 선택할 수 있습니다.
* [Log](../../../engines/table-engines/log-family/index.md) 계열. 기본적으로 `lz4` 압축 방식을 사용하며 컬럼 압축 코덱을 지원합니다.
* [Set](../../../engines/table-engines/special/set.md). 기본 압축만 지원합니다.
* [Join](../../../engines/table-engines/special/join.md). 기본 압축만 지원합니다.

ClickHouse는 범용 코덱과 특수 코덱을 지원합니다.

<div id="general-purpose-codecs">
  ### 범용 코덱
</div>

<div id="none">
  #### NONE
</div>

`NONE` — 압축을 사용하지 않습니다.

<div id="lz4">
  #### LZ4
</div>

`LZ4` — 기본적으로 사용되는 무손실 [데이터 압축 알고리즘](https://github.com/lz4/lz4)입니다. LZ4의 고속 압축을 적용합니다.

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — 레벨을 조정할 수 있는 LZ4 HC(고압축) 알고리즘입니다. 기본 레벨: 9입니다. `level <= 0`으로 설정하면 기본 레벨이 적용됩니다. 가능한 레벨: [1, 12]. 권장 레벨 범위: [4, 9].

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — `level`을 설정할 수 있는 [ZSTD 압축 알고리즘](https://en.wikipedia.org/wiki/Zstandard)입니다. 가능한 수준: [1, 22]. 기본 수준: 1입니다.

높은 압축 수준은 한 번 압축한 뒤 여러 번 압축 해제하는 것과 같은 비대칭 시나리오에서 유용합니다. 수준이 높을수록 압축률은 더 좋아지지만 CPU 사용량도 더 증가합니다.

<div id="zstd_qat">
  #### 사용 중단: ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### 사용 중단: DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### 특수 코덱
</div>

이러한 코덱은 데이터의 특정 특성을 활용해 압축을 더욱 효과적으로 수행하도록 설계되었습니다. 이러한 코덱 중 일부는 자체적으로 데이터를 압축하지 않고, 대신 데이터를 전처리하여 범용 코덱을 사용하는 두 번째 압축 단계에서 더 높은 데이터 압축률을 얻을 수 있도록 합니다.

<div id="delta">
  #### Delta
</div>

`Delta(delta_bytes)` — 첫 번째 값은 그대로 두고, 원시 값을 인접한 두 값의 차이로 대체하는 압축 방식입니다. `delta_bytes`는 원시 값의 최대 크기이며, 기본값은 `sizeof(type)`입니다. `delta_bytes`를 인수로 지정하는 방식은 더 이상 권장되지 않으며, 향후 릴리스에서 지원이 제거될 예정입니다. Delta는 데이터 준비 코덱이므로 단독으로 사용할 수 없습니다.

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — 델타의 델타를 계산하여 이를 컴팩트한 바이너리 형식으로 기록합니다. `bytes_size`는 [Delta](#delta) 코덱의 `delta_bytes`와 유사한 의미를 가집니다. `bytes_size`를 인수로 지정하는 방식은 더 이상 권장되지 않으며, 향후 릴리스에서 지원이 제거될 예정입니다. 시계열 데이터와 같이 스트라이드가 일정한 단조 시퀀스에서 최적의 압축률을 얻을 수 있습니다. 모든 숫자 타입과 함께 사용할 수 있습니다. Gorilla TSDB에서 사용되는 알고리즘을 구현하며, 이를 확장해 64비트 타입도 지원합니다. 32비트 델타에는 추가로 1비트를 사용합니다. 즉, 4비트 프리픽스 대신 5비트 프리픽스를 사용합니다. 자세한 내용은 [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)의 Compressing Time Stamps를 참조하십시오. DoubleDelta는 데이터 준비 코덱이므로 단독으로 사용할 수 없습니다.

<div id="gcd">
  #### GCD
</div>

`GCD()` - - 컬럼 값의 최대공약수(GCD)를 계산한 다음 각 값을 GCD로 나눕니다. 정수, Decimal, 날짜/시간 컬럼에 사용할 수 있습니다. 이 코덱은 값이 GCD의 배수 단위로 변하는(증가하거나 감소하는) 컬럼에 특히 적합합니다. 예를 들어 24, 28, 16, 24, 8, 24의 경우 GCD는 4입니다. GCD는 데이터 준비 코덱이므로 단독으로 사용할 수 없습니다.

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — 현재 부동 소수점 값과 이전 부동 소수점 값의 XOR을 계산하고, 이를 컴팩트한 binary form으로 기록합니다. 연속된 값 사이의 차이가 작을수록, 즉 시계열 값의 변화가 느릴수록 압축률이 더 좋아집니다. Gorilla TSDB에서 사용되는 알고리즘을 구현하며, 이를 확장해 64비트 타입도 지원합니다. 가능한 `bytes_size` 값은 1, 2, 4, 8이며, `sizeof(type)`이 1, 2, 4, 8 중 하나이면 기본값은 `sizeof(type)`입니다. 그 외의 모든 경우에는 1입니다. 자세한 내용은 [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078)의 4.1절을 참조하십시오.

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — 부동소수점 데이터용 적응형 무손실 압축입니다. `Float32` 및 `Float64`를 지원합니다. 자세한 내용은 [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334)을 참조하십시오.

이 코덱은 선택적으로 variant 인수를 받을 수 있습니다.

* `ALP()` 또는 `ALP(AUTO)` (기본값) — 추정된 압축된 크기에 따라 STD를 사용하고, 필요하면 RD로 전환합니다.
* `ALP(STD)` — 표준 ALP variant입니다. 각 값을 10의 거듭제곱을 사용하는 정확한 스케일 정수로 표현한 다음, 결과 정수를 Frame-of-Reference와 비트 패킹으로 압축합니다. 표현할 수 없는 값은 원시 예외로 저장됩니다. Decimal에서 비롯된 숫자(예: 측정값, 가격)에 가장 적합합니다.
* `ALP(RD)` — Real Doubles variant입니다. 각 값의 비트 패턴을 reinterpret하여 상위 파트(부호 + 지수 + 상위 가수 비트)와 하위 파트로 나눕니다. 상위 파트는 딕셔너리 인코딩되며(최대 8개 엔트리), 하위 파트는 비트 패킹됩니다. 많은 값이 동일한 상위 비트를 공유할 때 가장 효과적입니다.

:::note
이 코덱은 Experimental 기능이므로, 사용하려면 `SET allow_experimental_codecs = 1`을 설정해야 합니다.
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` - 두 가지 예측기 중 더 적합한 예측기를 사용해 시퀀스의 다음 부동소수점 값을 반복적으로 예측한 뒤, 실제 값과 예측값에 XOR를 수행하고 그 결과를 선행 0 압축합니다. Gorilla와 유사하게, 천천히 변하는 일련의 부동소수점 값을 저장할 때 효율적입니다. 64비트 값(double)의 경우 FPC가 Gorilla보다 빠르지만, 32비트 값의 경우 성능은 상황에 따라 달라질 수 있습니다. 가능한 `level` 값은 1-28이며, 기본값은 12입니다. 가능한 `float_size` 값은 4, 8이며, type이 Float인 경우 기본값은 `sizeof(type)`입니다. 그 외의 모든 경우에는 4입니다. 알고리즘에 대한 자세한 설명은 [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf)를 참조하십시오.

<div id="t64">
  #### T64
</div>

`T64` — 정수 데이터 타입(`Enum`, `Date`, `DateTime` 포함)에서 값의 사용되지 않는 상위 비트를 잘라내는 압축 방식입니다. 이 알고리즘의 각 단계에서 코덱은 64개의 값으로 이루어진 블록을 가져와 64x64 비트 행렬에 배치하고, 이를 전치한 뒤 값에서 사용되지 않는 비트를 잘라내고 남은 부분을 시퀀스로 반환합니다. 사용되지 않는 비트란 압축이 적용되는 전체 데이터 파트에서 최댓값과 최솟값이 서로 차이를 보이지 않는 비트입니다.

`DoubleDelta` 및 `Gorilla` 코덱은 Gorilla TSDB에서 해당 압축 알고리즘의 구성 요소로 사용됩니다. Gorilla 방식은 타임스탬프와 함께 값이 서서히 변하는 시퀀스가 있는 경우에 효과적입니다. 타임스탬프는 `DoubleDelta` 코덱으로 효율적으로 압축되고, 값은 `Gorilla` 코덱으로 효율적으로 압축됩니다. 예를 들어, 테이블을 효율적으로 저장하려면 다음과 같은 구성으로 생성할 수 있습니다:

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### 암호화 코덱
</div>

이 코덱들은 실제로 데이터를 압축하는 대신 디스크에 저장된 데이터를 암호화합니다. 이러한 코덱은 [encryption](/ko/operations/server-configuration-parameters/settings#encryption) 설정에서 암호화 키를 지정한 경우에만 사용할 수 있습니다. 일반적으로 암호화된 데이터는 의미 있게 압축할 수 없으므로, 암호화는 코덱 파이프라인의 마지막 단계에서만 사용하는 것이 적절합니다.

암호화 코덱:

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — [RFC 8452](https://tools.ietf.org/html/rfc8452)의 GCM-SIV 모드에서 AES-128로 데이터를 암호화합니다.

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — AES-256의 GCM-SIV 모드로 데이터를 암호화합니다.

이 코덱은 고정된 nonce를 사용하므로 암호화 결과가 결정적입니다. 따라서 [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md)와 같은 중복 제거 엔진과 호환되지만, 약점도 있습니다. 동일한 데이터 블록을 두 번 암호화하면 결과 암호문(ciphertext)이 완전히 같아지므로, 디스크를 읽을 수 있는 공격자는 내용은 알 수 없더라도 둘이 동일하다는 사실은 확인할 수 있습니다.

:::note
&quot;*MergeTree&quot; 계열을 포함한 대부분의 엔진은 코덱을 적용하지 않은 상태로 디스크에 인덱스 파일을 생성합니다. 즉, 암호화된 컬럼에 인덱스가 있으면 평문(plaintext)이 디스크에 나타납니다.
:::

:::note
암호화된 컬럼의 특정 값을 명시하는 SELECT 쿼리(예: WHERE 절)를 수행하면 해당 값이 [system.query&#95;log](../../../operations/system-tables/query_log.md)에 기록될 수 있습니다. 로깅을 비활성화하는 것이 좋을 수 있습니다.
:::

**예시**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
압축을 적용해야 한다면 명시적으로 지정해야 합니다. 그렇지 않으면 데이터에는 암호화만 적용됩니다.
:::

**예시**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## 임시 테이블
</div>

:::note
임시 테이블은 복제되지 않는다는 점에 유의하십시오. 따라서 임시 테이블에 삽입된 데이터가 다른 레플리카에서 사용할 수 있다고 보장할 수 없습니다. 임시 테이블이 특히 유용한 대표적인 사용 사례는 단일 세션 동안 작은 외부 데이터셋을 쿼리하거나 조인하는 경우입니다.
:::

ClickHouse는 다음과 같은 특성을 가진 임시 테이블을 지원합니다.

* 임시 테이블은 연결이 끊어진 경우를 포함하여 세션이 종료되면 사라집니다.
* 임시 테이블은 엔진을 지정하지 않으면 Memory 엔진을 사용하며, Replicated 및 `KeeperMap` 엔진을 제외한 모든 테이블 엔진을 사용할 수 있습니다.
* 임시 테이블에는 DB를 지정할 수 없습니다. 데이터베이스 외부에 생성됩니다.
* `ON CLUSTER`를 사용해 모든 클러스터 서버에서 분산 DDL 쿼리로 임시 테이블을 생성할 수 없습니다. 이 테이블은 현재 세션에만 존재합니다.
* 임시 테이블의 이름이 다른 테이블과 같고, 쿼리에서 DB를 지정하지 않고 테이블 이름만 지정하면 임시 테이블이 사용됩니다.
* 분산 쿼리 처리에서는 쿼리에서 사용되는 Memory 엔진 임시 테이블이 원격 서버로 전달됩니다.

임시 테이블을 생성하려면 다음 구문을 사용하십시오:

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

대부분의 경우 임시 테이블은 수동으로 생성하지 않지만, 쿼리에서 외부 데이터를 사용하거나 분산 `(GLOBAL) IN`을 사용할 때는 생성됩니다. 자세한 내용은 해당 섹션을 참조하십시오

임시 테이블 대신 [ENGINE = Memory](../../../engines/table-engines/special/memory.md) 테이블을 사용할 수도 있습니다.

<div id="replace-table">
  ## REPLACE TABLE
</div>

`REPLACE` 문을 사용하면 테이블을 [원자적으로](/ko/concepts/glossary#atomicity) 업데이트할 수 있습니다.

:::note
이 문은 [`Atomic`](../../../engines/database-engines/atomic.md) 및 [`Replicated`](../../../engines/database-engines/replicated.md) 데이터베이스 엔진에서 지원됩니다.
이들은 각각 ClickHouse와 ClickHouse Cloud의 기본 데이터베이스 엔진입니다.
:::

일반적으로 테이블에서 일부 데이터를 삭제해야 하는 경우,
원하지 않는 데이터를 조회하지 않는 `SELECT` 문으로 새 테이블을 만들고 데이터를 채운 다음,
기존 테이블을 삭제하고 새 테이블로 이름을 변경할 수 있습니다.
이 방법은 아래 예시에서 확인할 수 있습니다:

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

위 방법 대신(기본 데이터베이스 엔진을 사용하는 경우) `REPLACE`를 사용해 동일한 결과를 얻을 수도 있습니다:

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### 구문
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
`CREATE` 문의 모든 구문 형식은 이 문에도 동일하게 적용됩니다. 존재하지 않는 테이블(table)에 `REPLACE`를 실행하면 오류가 발생합니다.
:::

<div id="examples">
  ### 예시:
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="로컬" default>
    다음 테이블을 살펴보겠습니다.

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    `REPLACE` 문을 사용하여 모든 데이터를 지울 수 있습니다.

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    또는 `REPLACE` 문을 사용하여 테이블 구조를 변경할 수 있습니다.

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    ClickHouse Cloud에서 다음 테이블을 살펴보겠습니다.

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    `REPLACE` 문을 사용하여 모든 데이터를 지울 수 있습니다.

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    또는 `REPLACE` 문을 사용하여 테이블 구조를 변경할 수 있습니다.

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## COMMENT 절
</div>

테이블을 생성할 때 해당 테이블에 주석을 추가할 수 있습니다.

**구문**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
`COMMENT` 절은 `PARTITION BY`, `ORDER BY`, 스토리지별 `SETTINGS`와 같은 스토리지 관련 절 **뒤에** 지정해야 합니다.

`COMMENT` 절 뒤에서는 스토리지 관련 설정이 아니라 쿼리별 `SETTINGS`(`max_threads` 등)만 구문 분석됩니다.

즉, 올바른 절 순서는 다음과 같습니다.

* `ENGINE`
* 스토리지 절
* `COMMENT`
* 쿼리 설정(있는 경우)
  :::

**예시**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [스키마와 코덱으로 ClickHouse 최적화하기](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* 블로그: [ClickHouse에서 시계열 데이터와 함수 다루기](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
---
description: 'YTsaurus 클러스터의 데이터를 가져올 수 있는 테이블 엔진입니다.'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'YTsaurus 테이블 엔진'
keywords: ['YTsaurus', '테이블 엔진']
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-engine">
  # YTsaurus 테이블 엔진
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

YTsaurus 테이블 엔진을 사용하면 YTsaurus 클러스터에서 데이터를 가져올 수 있습니다.

<div id="creating-a-table">
  ## 테이블 생성하기
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
이는 향후 릴리스에서 하위 호환되지 않는 방식으로 변경될 수 있는 실험적 기능입니다.
설정 [`allow_experimental_ytsaurus_table_engine`](/ko/operations/settings/settings#allow_experimental_ytsaurus_table_engine)을 사용하여
YTsaurus 테이블 엔진 사용을 활성화하십시오.

다음과 같이 설정할 수 있습니다.

`SET allow_experimental_ytsaurus_table_engine = 1`.
:::

**엔진 매개변수**

* `http_proxy_url` — YTsaurus http 프록시의 URL입니다.
* `cypress_path` — 데이터 소스의 Cypress 경로입니다.
* `oauth_token` — OAuth 토큰입니다.

<div id="usage-example">
  ## 사용 예시
</div>

다음은 YTsaurus 테이블을 생성하는 쿼리입니다:

```sql title="Query"
SHOW CREATE TABLE yt_saurus;
```

```sql title="Response"
CREATE TABLE yt_saurus
(
    `a` UInt32,
    `b` String
)
ENGINE = YTsaurus('http://localhost:8000', '//tmp/table', 'password')
```

테이블의 데이터를 조회하려면 다음을 실행하세요:

```sql title="Query"
SELECT * FROM yt_saurus;
```

```response title="Response"
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

<div id="data-types">
  ## 데이터 타입
</div>

<div id="primitive-data-types">
  ### 기본 데이터 타입
</div>

| YTsaurus 데이터 타입         | ClickHouse 데이터 타입  |
| ----------------------- | ------------------ |
| `int8`                  | `Int8`             |
| `int16`                 | `Int16`            |
| `int32`                 | `Int32`            |
| `int64`                 | `Int64`            |
| `uint8`                 | `UInt8`            |
| `uint16`                | `UInt16`           |
| `uint32`                | `UInt32`           |
| `uint64`                | `UInt64`           |
| `float`                 | `Float32`          |
| `double`                | `Float64`          |
| `boolean`               | `Bool`             |
| `string`                | `String`           |
| `utf8`                  | `String`           |
| `json`                  | `JSON`             |
| `yson(type_v3)`         | `JSON`             |
| `uuid`                  | `UUID`             |
| `date32`                | `Date`(아직 지원되지 않음) |
| `datetime64`            | `Int64`            |
| `timestamp64`           | `Int64`            |
| `interval64`            | `Int64`            |
| `date`                  | `Date`(아직 지원되지 않음) |
| `datetime`              | `DateTime`         |
| `timestamp`             | `DateTime64(6)`    |
| `interval`              | `UInt64`           |
| `any`                   | `String`           |
| `null`                  | `Nothing`          |
| `void`                  | `Nothing`          |
| `required = False`인 `T` | `Nullable(T)`      |

<div id="composite-data-types">
  ### 복합 데이터 타입
</div>

| YTsaurus 데이터 타입 | ClickHouse 데이터 타입      |
| --------------- | ---------------------- |
| `decimal`       | `Decimal`              |
| `optional`      | `Nullable`             |
| `list`          | `Array`                |
| `struct`        | `NamedTuple`           |
| `tuple`         | `Tuple`                |
| `variant`       | `Variant`              |
| `dict`          | &#96;Array(Tuple(...)) |
| `tagged`        | `T`                    |

**관련 항목**

* [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md) 테이블 함수
* [ytsaurus 데이터 스키마](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
* [ytsaurus 데이터 타입](https://ytsaurus.tech/docs/en/user-guide/storage/data-types)
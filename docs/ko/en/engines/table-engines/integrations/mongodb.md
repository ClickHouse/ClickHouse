---
description: 'MongoDB 엔진은 원격 컬렉션에서 데이터를 읽을 수 있는 읽기 전용 테이블 엔진입니다.'
sidebar_label: 'MongoDB'
sidebar_position: 135
slug: /engines/table-engines/integrations/mongodb
title: 'MongoDB 테이블 엔진'
doc_type: 'reference'
---

MongoDB 엔진은 원격 [MongoDB](https://www.mongodb.com/) 컬렉션에서 데이터를 읽을 수 있는 읽기 전용 테이블 엔진입니다.

MongoDB v3.6 이상 서버만 지원합니다.
[시드 목록(`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list)은 아직 지원되지 않습니다.

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password[, options[, oid_columns]]);
```

**엔진 매개변수**

| 매개변수     | Description                                                                                                                                                                            |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | MongoDB 서버 주소입니다.                                                                                                                                                                      |
| `database`    | 원격 데이터베이스 이름입니다.                                                                                                                                                                       |
| `collection`  | 원격 컬렉션 이름입니다.                                                                                                                                                                          |
| `user`        | MongoDB 사용자입니다.                                                                                                                                                                        |
| `password`    | 사용자 비밀번호입니다.                                                                                                                                                                           |
| `options`     | 선택 사항입니다. URL 형식의 문자열로 지정하는 MongoDB 연결 문자열 [options](https://www.mongodb.com/docs/manual/reference/connection-string-options/#connection-options)입니다. 예: `'authSource=admin&ssl=true'` |
| `oid_columns` | WHERE 절에서 `oid`로 처리해야 하는 컬럼의 쉼표로 구분된 목록입니다. 기본값은 `_id`입니다.                                                                                                                             |

:::tip
MongoDB Atlas Cloud 연결 URL은 &#39;Atlas SQL&#39; 옵션에서 확인할 수 있습니다.
시드 목록(`mongodb**+srv**`)은 아직 지원되지 않지만, 향후 릴리스에서 지원될 예정입니다.
:::

또는 URI를 전달할 수 있습니다:

```sql
ENGINE = MongoDB(uri, collection[, oid_columns]);
```

**엔진 매개변수**

| 매개변수          | 설명                                                         |
| ------------- | ---------------------------------------------------------- |
| `uri`         | MongoDB 서버의 연결 URI입니다.                                     |
| `collection`  | 원격 컬렉션 이름입니다.                                              |
| `oid_columns` | WHERE 절에서 `oid`로 처리해야 하는 컬럼의 쉼표로 구분된 목록입니다. 기본값은 `_id`입니다. |

<div id="types-mappings">
  ## 타입 매핑
</div>

| MongoDB                 | ClickHouse                                           |
| ----------------------- | ---------------------------------------------------- |
| bool, int32, int64      | *Decimal을 제외한 모든 숫자 타입*, Boolean, String             |
| double                  | Float64, String                                      |
| date                    | Date, Date32, DateTime, DateTime64, String           |
| string                  | String, *올바른 포맷인 경우 Decimal을 제외한 모든 숫자 타입*           |
| document                | String(JSON 형식)                                      |
| array                   | Array, String(JSON 형식)                               |
| oid                     | String                                               |
| binary                  | 컬럼에 있으면 String, 배열 또는 document에 있으면 base64로 인코딩된 문자열 |
| uuid (binary subtype 4) | UUID                                                 |
| *기타 모든 타입*              | String                                               |

MongoDB 문서에서 키를 찾을 수 없는 경우(예: 컬럼 이름이 일치하지 않는 경우) 기본값 또는 `NULL`(컬럼이 널 허용인 경우)이 삽입됩니다.

<div id="oid">
  ### OID
</div>

`String`을 WHERE 절에서 `oid`로 처리하려면 테이블 엔진의 마지막 인수에 해당 컬럼 이름을 지정하면 됩니다.
기본적으로 MongoDB에서 `_id` 컬럼은 `oid` 유형이므로 `_id` 컬럼으로 레코드를 쿼리할 때 이 설정이 필요할 수 있습니다.
테이블의 `_id` 필드가 예를 들어 `uuid`처럼 다른 유형인 경우에는 `oid_columns`를 비워 두어야 합니다. 그렇지 않으면 이 매개변수의 기본값인 `_id`가 사용됩니다.

```javascript
db.sample_oid.insertMany([
    {"another_oid_column": ObjectId()},
]);

db.sample_oid.find();
[
    {
        "_id": {"$oid": "67bf6cc44ebc466d33d42fb2"},
        "another_oid_column": {"$oid": "67bf6cc40000000000ea41b1"}
    }
]
```

기본적으로 `_id`만 `oid` 컬럼으로 처리됩니다.

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid');

SELECT count() FROM sample_oid WHERE _id = '67bf6cc44ebc466d33d42fb2'; --will output 1.
SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; --will output 0
```

이 경우 출력값은 `0`입니다. ClickHouse는 `another_oid_column`의 유형이 `oid`라는 것을 알지 못하므로, 이를 수정해 보겠습니다:

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid', '_id,another_oid_column');

-- or

CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('host', 'db', 'sample_oid', 'user', 'pass', '', '_id,another_oid_column');

SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; -- will output 1 now
```

<div id="supported-clauses">
  ## 지원되는 절
</div>

단순한 표현식이 포함된 쿼리만 지원됩니다(예: `WHERE field = <constant> ORDER BY field2 LIMIT <constant>`).
이러한 표현식은 MongoDB 쿼리 언어로 변환되어 서버 측에서 실행됩니다.
[mongodb&#95;throw&#95;on&#95;unsupported&#95;query](../../../operations/settings/settings.md#mongodb_throw_on_unsupported_query)를 사용하면 이러한 제한을 모두 비활성화할 수 있습니다.
이 경우 ClickHouse는 가능한 범위에서 쿼리 변환을 시도하지만, 전체 테이블 스캔과 ClickHouse 측 처리가 발생할 수 있습니다.

:::note
Mongo는 타입이 엄격한 필터를 요구하므로, 리터럴의 유형을 명시적으로 지정하는 것이 항상 더 좋습니다.
예를 들어 `Date`로 필터링하려는 경우입니다:

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```

Mongo는 문자열을 `Date`로 변환하지 않으므로 이 방식은 작동하지 않습니다. 따라서 수동으로 변환해야 합니다:

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```

이는 `Date`, `Date32`, `DateTime`, `Bool`, `UUID`에 적용됩니다.

:::

<div id="usage-example">
  ## 사용 예시
</div>

MongoDB에 [sample&#95;mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix) 데이터셋이 로드되어 있다고 가정합니다

MongoDB 컬렉션의 데이터를 읽을 수 있도록 ClickHouse에 테이블을 생성합니다:

```sql title="Query"
CREATE TABLE sample_mflix_table
(
    _id String,
    title String,
    plot String,
    genres Array(String),
    directors Array(String),
    writers Array(String),
    released Date,
    imdb String,
    year String
) ENGINE = MongoDB('mongodb://<USERNAME>:<PASSWORD>@atlas-sql-6634be87cefd3876070caf96-98lxs.a.query.mongodb.net/sample_mflix?ssl=true&authSource=admin', 'movies');
```

```sql title="Query"
SELECT count() FROM sample_mflix_table
```

```text title="Response"
   ┌─count()─┐
1. │   21349 │
   └─────────┘
```

```sql title="Query"
-- JSONExtractString cannot be pushed down to MongoDB
SET mongodb_throw_on_unsupported_query = 0;

-- Find all 'Back to the Future' sequels with rating > 7.5
SELECT title, plot, genres, directors, released FROM sample_mflix_table
WHERE title IN ('Back to the Future', 'Back to the Future Part II', 'Back to the Future Part III')
    AND toFloat32(JSONExtractString(imdb, 'rating')) > 7.5
ORDER BY year
FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
title:     Back to the Future
plot:      A young man is accidentally sent 30 years into the past in a time-traveling DeLorean invented by his friend, Dr. Emmett Brown, and must make sure his high-school-age parents unite in order to save his own existence.
genres:    ['Adventure','Comedy','Sci-Fi']
directors: ['Robert Zemeckis']
released:  1985-07-03

Row 2:
──────
title:     Back to the Future Part II
plot:      After visiting 2015, Marty McFly must repeat his visit to 1955 to prevent disastrous changes to 1985... without interfering with his first trip.
genres:    ['Action','Adventure','Comedy']
directors: ['Robert Zemeckis']
released:  1989-11-22
```

```sql title="Query"
-- Find top 3 movies based on Cormac McCarthy's books
SELECT title, toFloat32(JSONExtractString(imdb, 'rating')) AS rating
FROM sample_mflix_table
WHERE arrayExists(x -> x LIKE 'Cormac McCarthy%', writers)
ORDER BY rating DESC
LIMIT 3;
```

```text title="Response"
   ┌─title──────────────────┬─rating─┐
1. │ No Country for Old Men │    8.1 │
2. │ The Sunset Limited     │    7.4 │
3. │ The Road               │    7.3 │
   └────────────────────────┴────────┘
```

<div id="troubleshooting">
  ## 문제 해결
</div>

생성된 MongoDB 쿼리는 DEBUG 수준의 로그에서 확인할 수 있습니다.

구현 세부 사항은 [mongocxx](https://github.com/mongodb/mongo-cxx-driver) 및 [mongoc](https://github.com/mongodb/mongo-c-driver) 문서에서 확인할 수 있습니다.
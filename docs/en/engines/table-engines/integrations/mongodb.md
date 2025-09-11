---
description: 'MongoDB engine is read-only table engine which allows to read data from
  a remote collection.'
sidebar_label: 'MongoDB'
sidebar_position: 135
slug: /engines/table-engines/integrations/mongodb
title: 'MongoDB'
---

# MongoDB

MongoDB engine is read-only table engine which allows to read data from a remote [MongoDB](https://www.mongodb.com/) collection.

Only MongoDB v3.6+ servers are supported.
[Seed list(`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list) is not yet supported.

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password[, options[, oid_columns]]);
```

**Engine Parameters**

| Parameter     | Description                                                                                                                                                                                              |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `host:port`   | MongoDB server address.                                                                                                                                                                                  |
| `database`    | Remote database name.                                                                                                                                                                                    |
| `collection`  | Remote collection name.                                                                                                                                                                                  |
| `user`        | MongoDB user.                                                                                                                                                                                            |
| `password`    | User password.                                                                                                                                                                                           |
| `options`     | Optional. MongoDB connection string [options](https://www.mongodb.com/docs/manual/reference/connection-string-options/#connection-options) as a URL formatted string. e.g. `'authSource=admin&ssl=true'` |
| `oid_columns` | Comma-separated list of columns that should be treated as `oid` in the WHERE clause. `_id` by default.                                                                                                   |

:::tip
If you are using the MongoDB Atlas cloud offering connection url can be obtained from 'Atlas SQL' option.
Seed list(`mongodb**+srv**`) is not yet supported, but will be added in future releases.
:::

Alternatively, you can pass a URI:

```sql
ENGINE = MongoDB(uri, collection[, oid_columns]);
```

**Engine Parameters**

| Parameter     | Description                                                                                            |
|---------------|--------------------------------------------------------------------------------------------------------|
| `uri`         | MongoDB server's connection URI.                                                                       |
| `collection`  | Remote collection name.                                                                                |
| `oid_columns` | Comma-separated list of columns that should be treated as `oid` in the WHERE clause. `_id` by default. |

## Types mappings {#types-mappings}

| MongoDB                 | ClickHouse                                                            |
|-------------------------|-----------------------------------------------------------------------|
| bool, int32, int64      | *any numeric type*, String                                            |
| double                  | Float64, String                                                       |
| date                    | Date, Date32, DateTime, DateTime64, String                            |
| string                  | String                                                                |
| document                | String(as JSON)                                                       |
| array                   | Array, String(as JSON)                                                |
| oid                     | String                                                                |
| binary                  | String if in column, base64 encoded string if in an array or document |
| uuid (binary subtype 4) | UUID                                                                  |
| *any other*             | String                                                                |

If key is not found in MongoDB document (for example, column name doesn't match), default value or `NULL` (if the column is nullable) will be inserted.

### OID {#oid}

If you want a `String` to be treated as `oid` in the WHERE clause, just put the column's name in the last argument of the table engine.
This may be necessary when querying a record by the `_id` column, which by default has `oid` type in MongoDB.
If the `_id` field in the table has other type, for example `uuid`, you need to specify empty `oid_columns`, otherwise the default value for this parameter `_id` is used.

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

By default, only `_id` is treated as `oid` column.

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid');

SELECT count() FROM sample_oid WHERE _id = '67bf6cc44ebc466d33d42fb2'; --will output 1.
SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; --will output 0
```

In this case the output will be `0`, because ClickHouse doesn't know that `another_oid_column` has `oid` type, so let's fix it:

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

## Supported clauses {#supported-clauses}

Only queries with simple expressions are supported (for example, `WHERE field = <constant> ORDER BY field2 LIMIT <constant>`).
Such expressions are translated to MongoDB query language and executed on the server side.
You can disable all these restriction, using [mongodb_throw_on_unsupported_query](../../../operations/settings/settings.md#mongodb_throw_on_unsupported_query).
In that case ClickHouse tries to convert query on best effort basis, but it can lead to full table scan and processing on ClickHouse side.

:::note
It's always better to explicitly set type of literal because Mongo requires strict typed filters.\
For example you want to filter by `Date`:

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```

This will not work because Mongo will not cast string to `Date`, so you need to cast it manually:

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```

This applied for `Date`, `Date32`, `DateTime`, `Bool`, `UUID`.

:::


## Usage Example {#usage-example}


Assuming MongoDB has [sample_mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix) dataset loaded

Create a table in ClickHouse which allows to read data from MongoDB collection:

```sql
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
    year String,
) ENGINE = MongoDB('mongodb://<USERNAME>:<PASSWORD>@atlas-sql-6634be87cefd3876070caf96-98lxs.a.query.mongodb.net/sample_mflix?ssl=true&authSource=admin', 'movies');
```

Query:

```sql
SELECT count() FROM sample_mflix_table
```

```text
   ┌─count()─┐
1. │   21349 │
   └─────────┘
```

```sql
-- JSONExtractString cannot be pushed down to MongoDB
SET mongodb_throw_on_unsupported_query = 0;

-- Find all 'Back to the Future' sequels with rating > 7.5
SELECT title, plot, genres, directors, released FROM sample_mflix_table
WHERE title IN ('Back to the Future', 'Back to the Future Part II', 'Back to the Future Part III')
    AND toFloat32(JSONExtractString(imdb, 'rating')) > 7.5
ORDER BY year
FORMAT Vertical;
```

```text
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

```sql
-- Find top 3 movies based on Cormac McCarthy's books
SELECT title, toFloat32(JSONExtractString(imdb, 'rating')) AS rating
FROM sample_mflix_table
WHERE arrayExists(x -> x LIKE 'Cormac McCarthy%', writers)
ORDER BY rating DESC
LIMIT 3;
```

```text
   ┌─title──────────────────┬─rating─┐
1. │ No Country for Old Men │    8.1 │
2. │ The Sunset Limited     │    7.4 │
3. │ The Road               │    7.3 │
   └────────────────────────┴────────┘
```

## Troubleshooting {#troubleshooting}
You can see the generated MongoDB query in DEBUG level logs.

Implementation details can be found in [mongocxx](https://github.com/mongodb/mongo-cxx-driver) and [mongoc](https://github.com/mongodb/mongo-c-driver) documentations.

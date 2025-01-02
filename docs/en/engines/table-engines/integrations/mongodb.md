---
slug: /en/engines/table-engines/integrations/mongodb
sidebar_position: 135
sidebar_label: MongoDB
---

# MongoDB

MongoDB engine is read-only table engine which allows to read data from remote [MongoDB](https://www.mongodb.com/) collection.

Only MongoDB v3.6+ servers are supported.
[Seed list(`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list) is not yet supported.

:::note
If you're facing troubles, please report the issue, and try to use [the legacy implementation](../../../operations/server-configuration-parameters/settings.md#use_legacy_mongodb_integration).
Keep in mind that it is deprecated, and will be removed in next releases.
:::

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password [, options]);
```

**Engine Parameters**

- `host:port` — MongoDB server address.

- `database` — Remote database name.

- `collection` — Remote collection name.

- `user` — MongoDB user.

- `password` — User password.

- `options` — MongoDB connection string options (optional parameter).

:::tip
If you are using the MongoDB Atlas cloud offering connection url can be obtained from 'Atlas SQL' option.
Seed list(`mongodb**+srv**`) is not yet supported, but will be added in future releases.
:::

Also, you can simply pass a URI:

``` sql
ENGINE = MongoDB(uri, collection);
```

**Engine Parameters**

- `uri` — MongoDB server's connection URI

- `collection` — Remote collection name.


## Types mappings

| MongoDB            | ClickHouse                                                            |
|--------------------|-----------------------------------------------------------------------|
| bool, int32, int64 | *any numeric type*, String                                            |
| double             | Float64, String                                                       |
| date               | Date, Date32, DateTime, DateTime64, String                            |
| string             | String, UUID                                                          |
| document           | String(as JSON)                                                       |
| array              | Array, String(as JSON)                                                |
| oid                | String                                                                |
| binary             | String if in column, base64 encoded string if in an array or document |
| *any other*        | String                                                                |

If key is not found in MongoDB document (for example, column name doesn't match), default value or `NULL` (if the column is nullable) will be inserted.

## Supported clauses

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

``` sql
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

``` sql
SELECT count() FROM sample_mflix_table
```

``` text
   ┌─count()─┐
1. │   21349 │
   └─────────┘
```

```SQL
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

```SQL
-- Find top 3 movies based on Cormac McCarthy's books
SELECT title, toFloat32(JSONExtractString(imdb, 'rating')) as rating
FROM sample_mflix_table
WHERE arrayExists(x -> x like 'Cormac McCarthy%', writers)
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

## Troubleshooting
You can see the generated MongoDB query in DEBUG level logs.

Implementation details can be found in [mongocxx](https://github.com/mongodb/mongo-cxx-driver) and [mongoc](https://github.com/mongodb/mongo-c-driver) documentations.

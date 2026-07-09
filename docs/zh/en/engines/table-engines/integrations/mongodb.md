---
description: 'MongoDB 引擎是一种只读表引擎，可用于从远程集合中读取数据。'
sidebar_label: 'MongoDB'
sidebar_position: 135
slug: /engines/table-engines/integrations/mongodb
title: 'MongoDB 表引擎'
doc_type: 'reference'
---

MongoDB 引擎是一种只读表引擎，可用于从远程 [MongoDB](https://www.mongodb.com/) 集合中读取数据。

仅支持 MongoDB v3.6+ 服务器。
暂不支持 [Seed list(`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list)。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, 集合, user, password[, options[, oid_columns]]);
```

**引擎参数**

| Parameter     | Description                                                                                                                                                                         |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | MongoDB 服务器地址。                                                                                                                                                                      |
| `database`    | 远程数据库名称。                                                                                                                                                                            |
| `collection`  | 远程 集合 名称。                                                                                                                                                                   |
| `user`        | MongoDB 用户。                                                                                                                                                                         |
| `password`    | 用户密码。                                                                                                                                                                               |
| `options`     | 可选。以 URL 格式字符串表示的 MongoDB connection string [options](https://www.mongodb.com/docs/manual/reference/connection-string-options/#connection-options)。例如：`'authSource=admin&ssl=true'` |
| `oid_columns` | 在 WHERE 子句 中应视为 `oid` 的列的逗号分隔列表。默认为 `_id`。                                                                                                                                      |

:::tip
如果你使用的是 MongoDB Atlas 云服务，connection url 可从 &#39;Atlas SQL&#39; 选项中获取。
Seed list (`mongodb**+srv**`) 目前尚不支持，但将在未来的发行版中添加。
:::

或者，你也可以传入一个 URI：

```sql
ENGINE = MongoDB(uri, collection[, oid_columns]);
```

**引擎参数**

| 参数            | 说明                                        |
| ------------- | ----------------------------------------- |
| `uri`         | MongoDB 服务器的连接 URI。                       |
| `collection`  | 远程 集合 名称。                         |
| `oid_columns` | 在 WHERE 子句中应视为 `oid` 的列的逗号分隔列表。默认为 `_id`。 |

<div id="types-mappings">
  ## 类型映射
</div>

| MongoDB            | ClickHouse                                    |
| ------------------ | --------------------------------------------- |
| bool, int32, int64 | *除 Decimal 外的任意数值类型*、布尔值、String               |
| double             | Float64、String                                |
| date               | Date、Date32、日期时间、DateTime64、String            |
| string             | String、*如果格式正确，也可以是任意数值类型 (Decimal 除外)&#x20;* |
| 文档                 | String (JSON 格式)                              |
| array              | Array、String (JSON 格式)                        |
| oid                | String                                        |
| binary             | 在列中时为 String；在数组或文档中时为 base64 编码字符串           |
| uuid (二进制子类型 4)    | UUID                                          |
| *任何其他类型*           | String                                        |

如果在 MongoDB 文档中找不到对应的键 (例如列名不匹配) ，则会插入默认值；如果该列为 Nullable，则会插入 `NULL`。

<div id="oid">
  ### OID
</div>

如果你希望在 WHERE 子句中将 `String` 视为 `oid`，只需将该列的名称填入表引擎的最后一个参数即可。
当按 `_id` 列查询记录时，这可能是必需的，因为在 MongoDB 中，该列默认类型为 `oid`。
如果表中的 `_id` 字段是其他类型，例如 `uuid`，则需要将 `oid_columns` 指定为空；否则，将使用该参数的默认值 `_id`。

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

默认情况下，只有 `_id` 会被视为 `oid` 列。

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid');

SELECT count() FROM sample_oid WHERE _id = '67bf6cc44ebc466d33d42fb2'; --will output 1.
SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; --will output 0
```

在这种情况下，输出将是 `0`，因为 ClickHouse 不知道 `another_oid_column` 的类型是 `oid`，所以我们来修正一下：

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
  ## 支持的子句
</div>

仅支持包含简单表达式的查询 (例如，`WHERE field = <constant> ORDER BY field2 LIMIT <constant>`) 。
此类表达式会被转换为 MongoDB 查询语言，并在服务端执行。
你可以使用 [mongodb&#95;throw&#95;on&#95;unsupported&#95;query](../../../operations/settings/settings.md#mongodb_throw_on_unsupported_query) 禁用所有这些限制。
在这种情况下，ClickHouse 会尽力转换查询，但这可能导致全表扫描，并在 ClickHouse 端进行处理。

:::note
由于 Mongo 要求过滤器具有严格的类型，因此最好始终显式设置字面量的类型。
例如，你想按 `Date` 进行过滤：

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```

这行不通，因为 Mongo 不会将字符串转换为 `Date`，所以需要手动转换：

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```

这适用于 `Date`、`Date32`、`DateTime`、`Bool`、`UUID`。

:::

<div id="usage-example">
  ## 使用示例
</div>

假设 MongoDB 中已加载 [sample&#95;mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix) 数据集

在 ClickHouse 中创建一个表，用于从 MongoDB 集合中读取数据：

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
  ## 故障排查
</div>

你可以在 DEBUG 级别的日志中查看生成的 MongoDB 查询。

具体实现细节可参见 [mongocxx](https://github.com/mongodb/mongo-cxx-driver) 和 [mongoc](https://github.com/mongodb/mongo-c-driver) 的文档。
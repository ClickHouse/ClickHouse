---
description: 'Documentation for the MongoDB wire protocol and the MongoDB query dialect'
sidebar_label: 'MongoDB Interface'
sidebar_position: 22
slug: /interfaces/mongo
title: 'MongoDB Interface'
doc_type: 'reference'
---

ClickHouse can accept queries written for MongoDB in two ways:

- as a **wire protocol endpoint**, so that MongoDB drivers and tools connect to ClickHouse as if it were a MongoDB server;
- as a **query dialect**, so that MongoDB shell syntax can be sent over the usual ClickHouse interfaces.

Both are experimental and cover only a subset of MongoDB. They are useful for pointing an existing MongoDB application at ClickHouse without rewriting its queries, not as a MongoDB replacement.

## MongoDB wire protocol {#mongodb-wire-protocol}

Add a `mongo_port` to the server configuration to accept MongoDB connections:

```xml
<clickhouse>
    <mongo_port>27017</mongo_port>
</clickhouse>
```

Then connect with any MongoDB client. Only the `PLAIN` authentication mechanism is supported, because it is the only one that provides the cleartext password that ClickHouse needs to authenticate a user:

```bash
mongosh "mongodb://default:@localhost:27017/default?authMechanism=PLAIN"
```

```python
import pymongo
client = pymongo.MongoClient("mongodb://default:@localhost:27017/default?authMechanism=PLAIN")
client["my_database"]["my_collection"].find({"age": {"$gt": 20}})
```

The user name is taken from the authentication payload sent by the client, not from the authentication database, so the database in the connection string may be any value.

:::note
The wire protocol port is plaintext. Do not expose it outside a trusted network.
:::

### Mapping of MongoDB concepts {#mapping-of-mongodb-concepts}

| MongoDB     | ClickHouse                                    |
|-------------|-----------------------------------------------|
| database    | database                                      |
| collection  | table                                         |
| document    | row                                           |
| field       | column (nested fields become `a.b` columns)   |
| index       | data skipping index of type `bloom_filter`    |

Collections with the same name in different MongoDB databases are different ClickHouse tables, and the database is created on demand by the first insert into it.

### Supported commands {#supported-commands}

`insert`, `find`, `aggregate`, `count`, `update`, `delete`, `create`, `drop`, `createIndexes`, `listDatabases`, `listCollections`, `isMaster` and `saslStart`.

A `find` supports a filter, a projection, `limit` and `sort`. A projection may compute `$add`, `$sub`, `$mul` and `$div`, and an `update` supports `$set` and `$inc`.

#### Filters {#filters}

A filter - of a `find`, a `delete`, an `update` or a `$match` stage - supports:

- the comparisons `$eq`, `$ne`, `$lt`, `$lte`, `$gt` and `$gte`;
- the set membership tests `$in` and `$nin`;
- the connectives `$and`, `$or`, `$nor` and `$not`;
- `$regex` with the options `i`, `m`, `s` and `x`, which is matched as a regular expression rather than as a `LIKE` pattern;
- the Extended JSON wrappers `$numberInt`, `$numberLong`, `$numberDouble`, `$numberDecimal`, `$oid` and `$date`, which the drivers send for the types JSON cannot represent.

Several operators on the same field all have to hold, so a range is written the way MongoDB writes it:

```javascript
{"EventDate": {"$gte": {"$date": "2013-07-01"}, "$lte": {"$date": "2013-07-31"}}}
```

#### Aggregation pipelines {#aggregation-pipelines}

An `aggregate` translates its pipeline into a chain of `SELECT`s: each stage fills a clause of the query being built, and a stage that needs a clause already filled continues on top of a subquery. The stages `$match`, `$group`, `$project`, `$set` (`$addFields`), `$sort`, `$skip`, `$limit`, `$count` and `$unionWith` are supported.

```javascript
db.hits.aggregate([
    {"$match": {"SearchPhrase": {"$ne": ""}}},
    {"$group": {"_id": "$SearchPhrase", "c": {"$sum": 1}}},
    {"$sort": {"c": -1}},
    {"$limit": 10}
])
```

`$group` supports the accumulators `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$count`, `$stdDevPop` and `$stdDevSamp`. A `_id` of `null` aggregates the whole stream into one document, and a `_id` that is a document groups by each of its fields, which become the `_id.<field>` columns of the result.

Inside a stage, an expression may use `$literal`, the arithmetic `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$pow`, `$abs`, `$ceil`, `$floor`, `$round`, `$sqrt`, `$exp`, `$ln` and `$log10`, the comparisons and connectives listed above in their expression form, the conditionals `$cond`, `$switch` and `$ifNull`, the conversions `$toString`, `$toInt`, `$toLong`, `$toDouble`, `$toDecimal`, `$toBool` and `$toDate`, the string operators `$concat`, `$strLenBytes`, `$strLenCP`, `$toUpper`, `$toLower`, `$split`, `$substrBytes`, `$substrCP`, `$regexMatch` and `$regexFind`, the array operators `$size`, `$first`, `$last`, `$arrayElemAt`, `$in` and `$reverseArray`, and the date parts `$year`, `$month`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$week`, `$hour`, `$minute`, `$second`, `$millisecond` and `$dateTrunc`.

A `$regexFind` becomes the `match`, `idx` and `captures` fields of its result document, following the same mapping of a nested field onto an `a.b` column as everywhere else:

```javascript
db.hits.aggregate([
    {"$set": {"k": {"$regexFind": {"input": "$Referer", "regex": "^https?://([^/]+)/"}}}},
    {"$group": {"_id": {"$first": "$k.captures"}, "c": {"$sum": 1}}}
])
```

### Schemas {#schemas}

MongoDB collections have no schema, while ClickHouse tables do. A collection created by the first `insert` gets one column per field of the first inserted document, and the field types of that document decide the column types. Later documents in the same collection:

- may omit a field, in which case the column gets its default value;
- may **not** contain a field that is not in the schema, which is rejected instead of being written to the wrong column.

`_id` is generated by the client and is not stored.

A collection created explicitly with `createCollection` has no document to infer a schema from, so it gets a single `JSON` column named `json`.

### Limitations {#limitations}

- An `update` is translated into `ALTER TABLE ... UPDATE`, which is asynchronous: the new value does not have to be visible to the next `find`.
- The number of documents affected by `update` and `delete` is always reported as `0`.
- Cursors are not implemented, so the whole result of a `find` or an `aggregate` is returned in the first batch.
- A projection lists exactly the fields it names: `_id` is not added to it implicitly, because a ClickHouse table has no implicit `_id` column.
- `$lookup`, `$unwind`, `$facet` and the other pipeline stages not listed above are not supported, and neither are transactions, change streams and the `OP_COMPRESSED` message.
- Database and collection names must consist of letters, digits, `_` and `-`.

## MongoDB dialect {#mongodb-dialect}

Set the [`dialect`](/operations/settings/settings#dialect) setting to `mongo` to write MongoDB shell syntax over any ClickHouse interface:

```sql
SET dialect = 'mongo';

db.users.find({"age" : {"$gt" : 20}});
db.users.find({"$projection" : {"name" : "name", "total" : {"$add" : ["price", "tax"]}}});
db.users.find({}).limit(10).sort({"age" : 1});
db.users.aggregate([{"$group" : {"_id" : "$city", "c" : {"$sum" : 1}}}, {"$sort" : {"c" : -1}}]);
```

The first name of a query is the database. The literal `db`, as written by the MongoDB shell, means the current database; any other name addresses that database explicitly:

```sql
SET dialect = 'mongo';

analytics.users.find({"age" : 20});
```

The dialect is available only in builds that include `rapidjson`; otherwise setting `dialect = 'mongo'` fails with `SUPPORT_IS_DISABLED`.

## See also {#see-also}

- [MongoDB table engine](/engines/table-engines/integrations/mongodb) - to read from a real MongoDB server from ClickHouse.

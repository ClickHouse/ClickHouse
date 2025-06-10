---
description: 'The engine allows to import data from the YTsaurus cluster.'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'YTsaurus'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# YTsaurus

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

Experimental table
The engine allows to import data from the YTsaurus cluster.

## Creating a Table {#creating-a-table}

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases.
Enable usage of the YTsaurus table engine
with [allow_experimental_ytsaurus_table_engine](/operations/settings/settings#allow_experimental_ytsaurus_table_engine) setting.
Input the command `set allow_experimental_ytsaurus_table_engine = 1`.
:::


**Engine Parameters**

- `http_proxy_url` — URL to the YTsaurus http proxy.
- `cypress_path` — Cypress path to the data source.
- `oauth_token` — OAuth token.


## Usage Example {#usage-example}

Shows a query creating the YTsaurus table:

```sql
SHOW CREATE TABLE yt_saurus;
```

```text
CREATE TABLE yt_saurus
(
    `a` UInt32,
    `b` String
)
ENGINE = YTsaurus('http://localhost:8000', '//tmp/table', 'password')

```

Returns the data from the table:

```sql
SELECT * FROM yt_saurus;
```

```text
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

## Data types
### Primitive data types
| YTsaurus data type | Clickhouse data type    |
| ------------------ | ----------------------- |
| `int8`             | `Int8`                  |
| `int16`            | `Int16`                 |
| `int32`            | `Int32`                 |
| `int64`            | `Int64`                 |
| `uint8`            | `UInt8`                 |
| `uint16`           | `UInt16`                |
| `uint32`           | `UInt32`                |
| `uint64`           | `UInt64`                |
| `float`            | `Float32`               |
| `double`           | `Float64`               |
| `boolean`          | `Boolean`               |
| `bool(type_v3)`    | `Boolean`               |
| `string`           | `String`                |
| `utf8`             | `String`                |
| `json`             | `JSON`                  |
| `yson(type_v3)`    | `JSON`                  |
| `uuid`             | `UUID`                  |
| `date32`           | `Int32`                 |
| `datetime64`       | `Int64`                 |
| `timestamp64`      | `Int64`                 |
| `interval64`       | `Int64`                 |
| `date`             | `Date`(Not supported yet)|
| `datetime`         | `DateTime`              |
| `timestamp`        | `DateTime64(6)`         |
| `interval`         | `UInt64`                |
| `any`              | `String`                |
| `null`             | `Nothing`               |
| `void`             | `Nothing`               |
| `T` with `required = False`| `Nullable(T)`   |

### Composite types
| YTsaurus data type | Clickhouse data type |
| ------------------ | -------------------- |
| `decimal`          | `Decimal`            |
| `optional`         | `Nullable`           |
| `list`             | `Array`              |
| `struct`           | -                    |
| `tuple`            | `Tuple`              |
| `variant`          | `Variant`            |
| `dict`             | -                    |
| `tagged`           | `T`                  |

### Restrictions
#### Nullable
Since ClickHouse doesn't support types `Nullable<Nullable<T>>`, `Nullable<Array>` and `Nullable<Tuple>`, this types have alternative convertation.

- `optional<optional<T>>` converts to `optional<T>`, which is `Nullable<T>` in ClickHouse
- `optional<list<T>>` converts to `Array<T>` in ClickHouse with default value `[]`, which represents `null`
- `optional<tuple<T1, T2, ...>>` converts to `Tuple<T1, T2, ...>` in ClickHouse with default value `(T1(), T2(), ...)`(each value in tuple is default constructor of type `TN`), which represents `null`.


**See Also**

- [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md) table function
- [ytsaurus data schema](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
- [ytsaurus data types](https://ytsaurus.tech/docs/en/user-guide/storage/data-types)

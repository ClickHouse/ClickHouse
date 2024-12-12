---
slug: /ja/integrations/postgresql/data-type-mappings
title: PostgreSQL のデータ型マッピング
keywords: [postgres, postgresql, データ型, 型]
---

以下の表は、Postgres に対する ClickHouse の同等のデータ型を示しています。

| Postgres データ型 | ClickHouse 型 |
| --- | --- |
| DATE | [Date](/ja/sql-reference/data-types/date) |
| TIMESTAMP | [DateTime](/ja/sql-reference/data-types/datetime) |
| REAL | [Float32](/ja/sql-reference/data-types/float) |
| DOUBLE | [Float64](/ja/sql-reference/data-types/float) |
| DECIMAL, NUMERIC | [Decimal](/ja/sql-reference/data-types/decimal) |
| SMALLINT | [Int16](/ja/sql-reference/data-types/int-uint) |
| INTEGER | [Int32](/ja/sql-reference/data-types/int-uint) |
| BIGINT | [Int64](/ja/sql-reference/data-types/int-uint) |
| SERIAL | [UInt32](/ja/sql-reference/data-types/int-uint) |
| BIGSERIAL | [UInt64](/ja/sql-reference/data-types/int-uint) |
| TEXT, CHAR | [String](/ja/sql-reference/data-types/string) |
| INTEGER | Nullable([Int32](/ja/sql-reference/data-types/int-uint)) |
| ARRAY | [Array](/ja/sql-reference/data-types/array) |
| FLOAT4 | [Float32](/ja/sql-reference/data-types/float) |
| BOOLEAN | [Bool](/ja/sql-reference/data-types/boolean) |
| VARCHAR | [String](/ja/sql-reference/data-types/string) |
| BIT | [String](/ja/sql-reference/data-types/string) |
| BIT VARYING | [String](/ja/sql-reference/data-types/string) |
| BYTEA | [String](/ja/sql-reference/data-types/string) |
| NUMERIC | [Decimal](/ja/sql-reference/data-types/decimal) |
| GEOGRAPHY | [Point](/ja/sql-reference/data-types/geo#point), [Ring](/ja/sql-reference/data-types/geo#ring), [Polygon](/ja/sql-reference/data-types/geo#polygon), [MultiPolygon](/ja/sql-reference/data-types/geo#multipolygon) |
| GEOMETRY | [Point](/ja/sql-reference/data-types/geo#point), [Ring](/ja/sql-reference/data-types/geo#ring), [Polygon](/ja/sql-reference/data-types/geo#polygon), [MultiPolygon](/ja/sql-reference/data-types/geo#multipolygon) |
| INET | [IPv4](/ja/sql-reference/data-types/ipv4), [IPv6](/ja/sql-reference/data-types/ipv6) |
| MACADDR | [String](/ja/sql-reference/data-types/string) |
| CIDR | [String](/ja/sql-reference/data-types/string) |
| HSTORE | [Map(K, V)](/ja/sql-reference/data-types/map), [Map](/ja/sql-reference/data-types/map)(K,[Variant](/ja/sql-reference/data-types/variant)) |
| UUID | [UUID](/ja/sql-reference/data-types/uuid) |
| ARRAY<T\> | [ARRAY(T)](/ja/sql-reference/data-types/array) |
| JSON* | [String](/ja/sql-reference/data-types/string), [Variant](/ja/sql-reference/data-types/variant), [Nested](/ja/sql-reference/data-types/nested-data-structures/nested#nestedname1-type1-name2-type2-), [Tuple](/ja/sql-reference/data-types/tuple) |
| JSONB | [String](/ja/sql-reference/data-types/string) |

*\* ClickHouse での JSON の本格的なサポートは現在開発中です。現時点では JSON を String にマッピングし、[JSON 関数](/ja/sql-reference/functions/json-functions)を使用するか、構造が予測可能な場合は JSON を直接 [Tuples](/ja/sql-reference/data-types/tuple) と [Nested](/ja/sql-reference/data-types/nested-data-structures/nested) にマッピングすることができます。JSON についての詳細は[こちら](/ja/integrations/data-formats/json#handle-as-structured-data)をお読みください。*

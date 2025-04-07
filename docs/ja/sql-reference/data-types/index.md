---
slug: /ja/sql-reference/data-types/
sidebar_label: データ型の一覧
sidebar_position: 1
---

# ClickHouseのデータ型

ClickHouseは様々な種類のデータをテーブルのセルに格納できます。このセクションでは、サポートされているデータ型と、それを使用および/または実装する際の特別な考慮事項について説明します。

:::note
データ型名が大文字と小文字を区別するかどうかは、[system.data_type_families](../../operations/system-tables/data_type_families.md#system_tables-data_type_families)テーブルで確認できます。
:::

ClickHouseのデータ型には次のものがあります：

- **整数型**：[符号付きおよび符号なし整数](./int-uint.md) (`UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`, `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`)
- **浮動小数点数**：[浮動小数点数](./float.md) (`Float32` と `Float64`) と [`Decimal` 値](./decimal.md)
- **ブール型**：ClickHouseには[`Boolean`型](./boolean.md)があります
- **文字列**：[文字列型 `String`](./string.md) と [`FixedString`](./fixedstring.md)
- **日付**：日付には[`Date`](./date.md) と [`Date32`](./date32.md)を、時間には[`DateTime`](./datetime.md) と [`DateTime64`](./datetime64.md)を使用
- **オブジェクト**：[`Object`](./json.md)は1つのカラムでJSONドキュメントを保存（非推奨）
- **JSON**：[`JSON`オブジェクト](./newjson.md)は1つのカラムでJSONドキュメントを保存
- **UUID**：[`UUID`値](./uuid.md)を効率的に保存するための選択肢
- **低いカーディナリティ型**：少数のユニークな値がある場合は[`Enum`](./enum.md)を、最大10,000のユニークなカラム値がある場合は[`LowCardinality`](./lowcardinality.md)を使用
- **配列**：任意のカラムは[`Array` 型](./array.md)として定義可能
- **マップ**：キーと値のペアを保存するには[`Map`](./map.md)を使用
- **集約関数型**：集約関数の中間状態を保存するには[`SimpleAggregateFunction`](./simpleaggregatefunction.md) と [`AggregateFunction`](./aggregatefunction.md)を使用
- **ネストされたデータ構造**：[`Nested`データ構造](./nested-data-structures/index.md)はセル内のテーブルのようなもの
- **タプル**：個別の型をもつ要素の[`Tuple`](./tuple.md)
- **Nullable**：値が"欠けている"場合にデータ型のデフォルト値ではなく`NULL`として保存する場合に[`Nullable`](./nullable.md)を使用
- **IPアドレス**：IPアドレスを効率的に保存するには[`IPv4`](./ipv4.md) と [`IPv6`](./ipv6.md)を使用
- **ジオタイプ**：[地理データ](./geo.md)用の`Point`, `Ring`, `Polygon`, `MultiPolygon`
- **特別なデータ型**：[`Expression`](./special-data-types/expression.md), [`Set`](./special-data-types/set.md), [`Nothing`](./special-data-types/nothing.md), [`Interval`](./special-data-types/interval.md)など含む

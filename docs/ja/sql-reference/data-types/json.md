---
slug: /ja/sql-reference/data-types/object-data-type
sidebar_position: 26
sidebar_label: Object Data Type
keywords: [object, data type]
---

# Object Data Type (非推奨)

**この機能は本番環境で利用可能な状態ではなく、現在非推奨です。** JSON ドキュメントを扱う必要がある場合は、[このガイド](/docs/ja/integrations/data-formats/json/overview)を参照してください。JSON オブジェクトをサポートする新しい実装が進行中であり、[こちら](https://github.com/ClickHouse/ClickHouse/issues/54864)で追跡できます。

<hr />

JavaScript Object Notation (JSON) ドキュメントを単一のカラムに格納します。

`JSON` は、[use_json_alias_for_old_object_type](../../operations/settings/settings.md#usejsonaliasforoldobjecttype) が有効の場合、`Object('json')` のエイリアスとして使用できます。

## 例

**例 1**

`JSON` カラムを持つテーブルを作成し、データを挿入する例:

```sql
CREATE TABLE json
(
    o JSON
)
ENGINE = Memory
```

```sql
INSERT INTO json VALUES ('{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}')
```

```sql
SELECT o.a, o.b.c, o.b.d[3] FROM json
```

```text
┌─o.a─┬─o.b.c─┬─arrayElement(o.b.d, 3)─┐
│   1 │     2 │                      3 │
└─────┴───────┴────────────────────────┘
```

**例 2**

Orderedな `MergeTree` ファミリのテーブルを作成するためには、ソートキーをそのカラムに抽出する必要があります。例えば、圧縮された JSON 形式の HTTP アクセスログファイルを挿入するには:

```sql
CREATE TABLE logs
(
	timestamp DateTime,
	message JSON
)
ENGINE = MergeTree
ORDER BY timestamp
```

```sql
INSERT INTO logs
SELECT parseDateTimeBestEffort(JSONExtractString(json, 'timestamp')), json
FROM file('access.json.gz', JSONAsString)
```

## JSON カラムの表示

`JSON` カラムを表示する際、ClickHouse はデフォルトでフィールド値のみを表示します（内部的にはタプルとして表現されるため）。フィールド名も表示するには、`output_format_json_named_tuples_as_objects = 1` を設定してください:

```sql
SET output_format_json_named_tuples_as_objects = 1

SELECT * FROM json FORMAT JSONEachRow
```

```text
{"o":{"a":1,"b":{"c":2,"d":[1,2,3]}}}
```

## 関連コンテンツ

- [ClickHouseでのJSON利用](/ja/integrations/data-formats/json/overview)
- [ClickHouseへのデータ取得 - パート2 - JSONの迂回](https://clickhouse.com/blog/getting-data-into-clickhouse-part-2-json)

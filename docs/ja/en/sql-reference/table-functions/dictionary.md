---
description: 'dictionary のデータを ClickHouse テーブルとして表示します。Dictionary エンジンと同様に
  動作します。'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

[dictionary](../statements/create/dictionary/overview.md) のデータを ClickHouse テーブルとして表示します。[Dictionary](../../engines/table-engines/special/dictionary.md) エンジンと同様に動作します。

<div id="syntax">
  ## 構文
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## 引数
</div>

* `dict` — Dictionary名。[String](../../sql-reference/data-types/string.md)。

<div id="returned_value">
  ## 戻り値
</div>

ClickHouse のテーブル。

<div id="examples">
  ## 例
</div>

入力元のテーブル `dictionary_source_table`:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Dictionaryを作成します:

```sql title="Query"
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

```sql title="Query"
SELECT * FROM dictionary('new_dictionary');
```

```text title="Response"
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

<div id="related">
  ## 関連
</div>

* [Dictionary engine](/ja/engines/table-engines/special/dictionary)
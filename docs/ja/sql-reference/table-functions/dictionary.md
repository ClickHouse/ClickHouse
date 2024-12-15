---
slug: /ja/sql-reference/table-functions/dictionary
sidebar_position: 47
sidebar_label: dictionary
title: dictionary
---

[Dictionary](../../sql-reference/dictionaries/index.md) のデータを ClickHouse テーブルとして表示します。この機能は [Dictionary](../../engines/table-engines/special/dictionary.md) エンジンと同様に動作します。

**構文**

``` sql
dictionary('dict')
```

**引数**

- `dict` — Dictionary の名前。[String](../../sql-reference/data-types/string.md)。

**返される値**

ClickHouse テーブル。

**例**

入力テーブル `dictionary_source_table`:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Dictionary の作成:

``` sql
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

クエリ:

``` sql
SELECT * FROM dictionary('new_dictionary');
```

結果:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

**関連項目**

- [Dictionary エンジン](../../engines/table-engines/special/dictionary.md#dictionary)

---
description: '`Merge` エンジン（`MergeTree` と混同しないでください）は、それ自体では
  データを格納しませんが、任意の数の他のテーブルを同時に読み取ることができます。'
sidebar_label: 'Merge'
sidebar_position: 30
slug: /engines/table-engines/special/merge
title: 'Merge テーブルエンジン'
doc_type: 'reference'
---

`Merge` エンジン (`MergeTree` と混同しないでください) は、それ自体ではデータを格納しませんが、任意の数の他のテーブルを同時に読み取ることができます。

読み取りは自動的に並列化されます。テーブルへの書き込みはサポートされていません。読み取り時には、実際に読み取られるテーブルに索引が存在する場合、その索引が使用されます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

<div id="engine-parameters">
  ## エンジンパラメータ
</div>

<div id="db_name">
  ### `db_name`
</div>

`db_name` — 設定可能な値:

* データベース名、
  * データベース名を返す文字列の定数式。たとえば `currentDatabase()`、
  * `REGEXP(expression)`。ここで `expression` は DB 名に一致する正規表現です。

<div id="tables_regexp">
  ### `tables_regexp`
</div>

`tables_regexp` — 指定したDBまたは複数のDB内のテーブル名に一致する正規表現です。

正規表現 — [re2](https://github.com/google/re2) (PCREの一部をサポート) 、大文字と小文字を区別します。
正規表現内での記号のエスケープについては、「match」セクションの注記を参照してください。

<div id="usage">
  ## 使い方
</div>

読み取り対象のテーブルを選択する際、`Merge` テーブル自身は、たとえ正規表現に一致していても選択されません。これは、ループを避けるためです。
互いのデータを延々と読み取ろうとする 2 つの `Merge` テーブルを作成することは可能ですが、これはよい考えではありません。

`Merge` エンジンの一般的な使い方は、多数の `TinyLog` テーブルを 1 つのテーブルであるかのように扱うことです。

<div id="examples">
  ## 例
</div>

**例 1**

2 つのデータベース `ABC_corporate_site` と `ABC_store` について考えてみましょう。`all_visitors` テーブルには、両方のデータベースの `visitors` テーブルの ID が含まれます。

```sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**例 2**

古いテーブル `WatchLog_old` があり、データを新しいテーブル `WatchLog_new` に移動せずにパーティション化を変更することにし、両方のテーブルのデータを参照する必要があるとします。

```sql
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
ORDER BY (date, UserId, EventType);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
PARTITION BY date
ORDER BY (UserId, EventType)
SETTINGS index_granularity=8192;

INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog AS WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

```text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_table` — データの読み取り元であるテーブル名。型: [String](../../../sql-reference/data-types/string.md)。

  `_table` でフィルタすると (たとえば `WHERE _table='xyz'`) 、フィルタ条件に一致するテーブルだけが読み取られます。

* `_database` — データの読み取り元であるデータベース名を含みます。型: [String](../../../sql-reference/data-types/string.md)。

**関連項目**

* [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [merge](../../../sql-reference/table-functions/merge.md) テーブル関数
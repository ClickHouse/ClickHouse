---
slug: /ja/engines/table-engines/special/merge
sidebar_position: 30
sidebar_label: Merge
---

# Merge テーブルエンジン

`Merge` エンジン（`MergeTree`と混同しないでください）は、データ自体を保存するのではなく、他の任意の数のテーブルから同時に読み取ることができます。

読み取りは自動的に並列化されます。テーブルへの書き込みはサポートされていません。読み取りの際に、実際に読み取られるテーブルのインデックスが存在すれば使用されます。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

## エンジンパラメータ

### db_name

`db_name` — 使用可能な値:
   - データベース名
   - データベース名を文字列で返す定数式、例：`currentDatabase()`
   - DB名にマッチする正規表現の `REGEXP(expression)`

### tables_regexp

`tables_regexp` — 指定されたDBまたは複数のDBで、テーブル名にマッチする正規表現。

正規表現は、[re2](https://github.com/google/re2)（PCREのサブセットをサポート）、大文字と小文字を区別します。正規表現における記号のエスケープについては、"match"セクションのノートをご参照ください。

## 使用方法 {#usage}

読み取り用のテーブルを選択する際、たとえ正規表現に一致したとしても、`Merge` テーブル自体は選択されません。これはループを避けるためです。2つの `Merge` テーブルが互いのデータを無限に読み取ろうとするシナリオを作ることは可能ですが、これは推奨されません。

`Merge` エンジンの典型的な使用方法は、多数の `TinyLog` テーブルを単一のテーブルのように扱う場合です。

## 例 {#examples}

**例 1**

2つのデータベース `ABC_corporate_site` と `ABC_store` を考えます。`all_visitors` テーブルには、両方のデータベースにある `visitors` テーブルのIDが含まれます。

``` sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**例 2**

古いテーブル `WatchLog_old` があり、データを新しいテーブル `WatchLog_new` に移さずにパーティショニングを変更することを決めたとします。そして、両方のテーブルからデータを表示する必要があります。

``` sql
CREATE TABLE WatchLog_old(date Date, UserId Int64, EventType String, Cnt UInt64)
    ENGINE=MergeTree(date, (UserId, EventType), 8192);
INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(date Date, UserId Int64, EventType String, Cnt UInt64)
    ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

``` text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## 仮想カラム {#virtual-columns}

- `_table` — データが読み取られたテーブルの名前を含む。タイプ: [String](../../../sql-reference/data-types/string.md)。

  `WHERE/PREWHERE` 句で `_table` に定数条件を設定することができます（例：`WHERE _table='xyz'`）。この場合、読み取り操作は `_table` の条件を満たすテーブルのみに対して行われるため、カラム `_table` はインデックスとして機能します。

**関連項目**

- [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns)
- [merge](../../../sql-reference/table-functions/merge.md) テーブル関数

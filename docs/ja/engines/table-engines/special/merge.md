---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 36
toc_title: "\u30DE\u30FC\u30B8"
---

# マージ {#merge}

その `Merge` エンジン（と混同しないように `MergeTree`）データ自体を格納しませんが、同時に他のテーブルの任意の数からの読み取りを可能にします。
読書は自動的に平行である。 表への書き込みはサポートされません。 読み取り時には、実際に読み取られているテーブルのインデックスが存在する場合に使用されます。
その `Merge` テーブルのデータベース名と正規表現です。

例えば:

``` sql
Merge(hits, '^WatchLog')
```

データはテーブルから読み込まれます。 `hits` 正規表現に一致する名前を持つデータベース ‘`^WatchLog`’.

データベース名の代わりに、文字列を返す定数式を使用できます。 例えば, `currentDatabase()`.

Regular expressions — [re2unit description in lists](https://github.com/google/re2) (PCREのサブセットをサポート)、大文字と小文字を区別します。
正規表現のエスケープシンボルに関する注意事項を参照してください。 “match” セクション。

読み込むテーブルを選択するとき、 `Merge` 正規表現と一致していても、テーブル自体は選択されません。 これはループを避けるためです。
それは可能に作成二つ `Merge` お互いのデータを無限に読み取ろうとするテーブルですが、これは良い考えではありません。

使用する典型的な方法 `Merge` エンジンは多数を使用のためです `TinyLog` 単一のテーブルと同様にテーブル。

例2:

古いテーブル（watchlog\_old）があり、データを新しいテーブル（watchlog\_new）に移動せずにパーティション分割を変更することにしたとしましょう。

``` sql
CREATE TABLE WatchLog_old(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree(date, (UserId, EventType), 8192);
INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT *
FROM WatchLog
```

``` text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## 仮想列 {#virtual-columns}

-   `_table` — Contains the name of the table from which data was read. Type: [文字列](../../../sql-reference/data-types/string.md).

    定数条件を設定することができます `_table` で `WHERE/PREWHERE` 句(例えば, `WHERE _table='xyz'`). この場合、読み取り操作はそのテーブルに対してのみ実行されます。 `_table` は満足しているので、 `_table` 列は索引として機能します。

**また見なさい**

-   [仮想列](index.md#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/merge/) <!--hide-->

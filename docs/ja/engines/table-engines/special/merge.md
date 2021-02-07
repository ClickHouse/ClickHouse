---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "\u30DE\u30FC\u30B8"
---

# マージ {#merge}

その `Merge` エンジン（と混同しないように `MergeTree`)を格納していないデータそのものができるから、他のテーブルを同時に
読み取りは自動的に並列化されます。 表への書き込みはサポートされません。 読み取り時には、実際に読み取られているテーブルのインデックスが存在する場合に使用されます。
その `Merge` データベース名とテーブルの正規表現です。

例:

``` sql
Merge(hits, '^WatchLog')
```

データはのテーブルから読まれます `hits` 正規表現に一致する名前を持つデータベース ‘`^WatchLog`’.

データベース名の代わりに、文字列を返す定数式を使用できます。 例えば, `currentDatabase()`.

Regular expressions — [re2](https://github.com/google/re2) (PCREのサブセットをサポート)、大文字と小文字を区別します。
正規表現におけるシンボルのエスケープに関する注意 “match” セクション

読み取るテーブルを選択するとき、 `Merge` 正規表現と一致しても、テーブル自体は選択されません。 これはループを避けるためです。
二つを作成することが可能です `Merge` 無限にお互いのデータを読み込もうとするテーブルですが、これは良い考えではありません。

使用する典型的な方法 `Merge` エンジンは多数を使用のためです `TinyLog` 単一のテーブルを持つかのようにテーブル。

例2:

古いテーブル（WatchLog_old）があり、データを新しいテーブル（WatchLog_new）に移動せずにパーティション分割を変更することにしたとしましょう。

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

    定数条件は次のように設定できます `_table` で `WHERE/PREWHERE` 句(例えば, `WHERE _table='xyz'`). この場合、読み取り操作は、条件がオンのテーブルに対してのみ実行されます `_table` は満足しているので、 `_table` 列は索引として機能します。

**も参照。**

-   [仮想列](index.md#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/merge/) <!--hide-->

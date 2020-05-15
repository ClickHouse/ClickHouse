---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 33
toc_title: "\uFF82\u3064\uFF68\uFF82\u59EA\"\uFF82\u3064\"\uFF82\u50B5\uFF82\u3064\
  \uFF79"
---

# ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂつｹ {#replacingmergetree}

エンジンは [MergeTree](mergetree.md#table_engines-mergetree) それは、同じ主キー値（またはより正確には同じ値）を持つ重複エントリを削除するという点で [ソートキー](mergetree.md) 値）。

データ重複除去は、マージ中にのみ行われます。 マージは未知の時間にバックグラウンドで行われるため、計画することはできません。 一部のデータは未処理のままです。 スケジュールされていないマージを実行するには `OPTIMIZE` クエリは、それを使用してカウントされません。 `OPTIMIZE` クエリは大量のデータを読み書きします。

したがって, `ReplacingMergeTree` に適した清算出重複データを背景に保存するための空間が保証するものではありませんが重複している。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

説明リクエストパラメータの参照 [要求の説明](../../../sql-reference/statements/create.md).

**ReplacingMergeTreeパラメータ**

-   `ver` — column with version. Type `UInt*`, `Date` または `DateTime`. 省略可能なパラメータ。

    マージ時, `ReplacingMergeTree` 同じ主キーを持つすべての行から一つだけを残します:

    -   選択の最後の場合 `ver` 設定されていません。
    -   最大バージョンでは、 `ver` 指定します。

**クエリ句**

作成するとき `ReplacingMergeTree` テーブル同じ [句](mergetree.md) 作成するときと同じように、必須です。 `MergeTree` テーブル。

<details markdown="1">

<summary>テーブルを作成する非推奨の方法</summary>

!!! attention "注意"
    可能であれば、古いプロジェクトを上記の方法に切り替えてください。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

すべてのパラメーターを除く `ver` と同じ意味を持つ `MergeTree`.

-   `ver` -バージョンの列。 省略可能なパラメータ。 説明は上記のテキストを参照してください。

</details>

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->

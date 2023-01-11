---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: "\u7F6E\u63DB\u30DE\u30FC\u30B8\u30C4\u30EA\u30FC"
---

# 置換マージツリー {#replacingmergetree}

エンジンは [メルゲツリー](mergetree.md#table_engines-mergetree) 同じ主キー値を持つ重複したエントリを削除するという点で、より正確には同じです [ソートキー](mergetree.md) 値）。

データ重複除外は、マージ中にのみ発生します。 マージは未知の時間にバックグラウンドで発生するため、計画することはできません。 一部のデータは未処理のままになる場合があります。 スケジュールされていないマージを実行するには `OPTIMIZE` クエリは、それを使用してカウントされませんので、 `OPTIMIZE` クエリは大量のデータを読み書きします。

従って, `ReplacingMergeTree` に適した清算出重複データを背景に保存するための空間が保証するものではありませんが重複している。

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

-   `ver` — column with version. Type `UInt*`, `Date` または `DateTime`. 任意パラメータ。

    マージ時, `ReplacingMergeTree` 同じ主キーを持つすべての行から、一つだけを残します:

    -   選択範囲の最後にある場合 `ver` 設定されていません。
    -   最大バージョンでは、次の場合 `ver` 指定。

**クエリ句**

を作成するとき `ReplacingMergeTree` 同じテーブル [句](mergetree.md) を作成するときのように必要です。 `MergeTree` テーブル。

<details markdown="1">

<summary>推奨されていません法テーブルを作成する</summary>

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

以下を除くすべてのパラメータ `ver` と同じ意味を持つ `MergeTree`.

-   `ver` -バージョンの列。 任意パラメータ。 説明は、上記のテキストを参照してください。

</details>

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->

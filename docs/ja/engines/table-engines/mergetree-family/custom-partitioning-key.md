---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 32
toc_title: "\u30AB\u30B9\u30BF\u30E0\u5206\u5272\u30AD\u30FC"
---

# カスタム分割キー {#custom-partitioning-key}

パーティション分割は [MergeTree](mergetree.md) ファミリーテーブル [複製された](replication.md) テーブル）。 [マテリアライズ表示](../special/materializedview.md#materializedview) に基づくMergeTreeテーブル支援を分割します。

パーティションが論理的に組み合わせの記録テーブルに指定された評価のポイントになります。 パーティションは、月別、日別、またはイベントタイプ別など、任意の基準によって設定できます。 各パーティションは別に保存される簡単操作のデータです。 アクセス時のデータclickhouseの最小サブセットのパーティションは可能です。

パーティションは `PARTITION BY expr` 節とき [テーブルの作成](mergetree.md#table_engine-mergetree-creating-a-table). これはパーティションキーにすることはでき表現からのテーブル列あります。 例えば、指定ョ月の表現を使用 `toYYYYMM(date_column)`:

``` sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

これはパーティションキーもできるタプルの表現を [主キー](mergetree.md#primary-keys-and-indexes-in-queries)). 例えば:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

この例では、現在の週に発生したイベントタイプによるパーティション分割を設定します。

テーブルに新しいデータを挿入すると、このデータは主キーでソートされた別の部分（チャンク）として格納されます。 挿入後10-15分で、同じパーティションのパーツがパート全体にマージされます。

!!! info "情報"
    Mergeは、パーティショニング式と同じ値を持つデータパーツに対してのみ機能します。 これは **なんかを過度に粒状仕切り** （約千パーティション以上）。 それ以外の場合は、 `SELECT` クエリは、ファイルシステムとオープンファイル記述子のファイルの不当に大きな数のために不十分な実行します。

を使用 [システム。パーツ](../../../operations/system-tables.md#system_tables-parts) テーブルのテーブル部品およびパーテッション. たとえば、我々が持っていると仮定しましょう `visits` テーブルを分割する。 のは、実行してみましょう `SELECT` のための問い合わせ `system.parts` テーブル:

``` sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      1 │
│ 201902    │ 201902_10_10_0 │      1 │
│ 201902    │ 201902_11_11_0 │      1 │
└───────────┴────────────────┴────────┘
```

その `partition` 列にはパーティションの名前が含まれます。 この例には二つの区画があります: `201901` と `201902`. この列の値を使用して、パーティション名を指定できます。 [ALTER … PARTITION](#alter_manipulations-with-partitions) クエリ。

その `name` カラムの名前を格納して、パーティションのデータ部品です。 この列を使用して、パートの名前を指定することができます。 [ALTER ATTACH PART](#alter_attach-partition) クエリ。

最初の部分の名前を分解してみましょう: `201901_1_3_1`:

-   `201901` パーティション名です。
-   `1` データブロックの最小数です。
-   `3` データブロックの最大数です。
-   `1` チャンクレベル(マージツリーの深さ)です。

!!! info "情報"
    古いタイプのテーブルの部分には名前があります: `20190117_20190123_2_2_0` （最小日付-最大日付-最小ブロック番号-最大ブロック番号-レベル）。

その `active` コラムは部品の状態を示します。 `1` アクティブです; `0` 非アクティブです。 に不活性部品、例えば、ソース部品の残りの後の合併によります。 破損したデータ部分も非アクティブとして示されます。

この例でわかるように、同じパーティションのいくつかの分離された部分があります（たとえば, `201901_1_3_1` と `201901_1_9_2`). つまり、これらの部分はまだマージされていません。 ClickHouseは、挿入してから約15分後に、データの挿入された部分を定期的にマージします。 さらに、スケジュールされていないマージを実行するには [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) クエリ。 例えば:

``` sql
OPTIMIZE TABLE visits PARTITION 201902;
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      0 │
│ 201902    │ 201902_4_11_2  │      1 │
│ 201902    │ 201902_10_10_0 │      0 │
│ 201902    │ 201902_11_11_0 │      0 │
└───────────┴────────────────┴────────┘
```

不活性パーツを削除する約10分後の統合.

部品やパーティションのセットを表示する別の方法は、テーブルのディレクトリに移動することです: `/var/lib/clickhouse/data/<database>/<table>/`. 例えば:

``` bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

フォルダ ‘201901\_1\_1\_0’, ‘201901\_1\_7\_1’ というように部品のディレクトリです。 各部に関する対応する分割データが含まれまで一定の月のテーブルこの例では、分割による。

その `detached` ディレクト [DETACH](../../../sql-reference/statements/alter.md#alter_detach-partition) クエリ。 破損した部分も削除されるのではなく、このディレクトリに移動されます。 サーバーはサーバーからの部品を使用しません `detached` directory. You can add, delete, or modify the data in this directory at any time – the server will not know about this until you run the [ATTACH](../../../sql-reference/statements/alter.md#alter_attach-partition) クエリ。

オペレーティングサーバーでは、ファイルシステム上の部品またはそのデータのセットを手動で変更することはできません。 非複製のテーブル、これを実行する事ができます。サーバが停止中でないお勧めします。 レプリケートされたテーブルの場合、パートのセットは変更できません。

ClickHouseを使用すると、パーティションを削除したり、テーブル間でコピーしたり、バックアップを作成したりできます。 セクションのすべての操作の一覧を参照してください [パーティションとパーツの操作](../../../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->

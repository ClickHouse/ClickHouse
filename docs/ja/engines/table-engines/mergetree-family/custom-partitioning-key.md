---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: "\u30AB\u30B9\u30BF\u30E0\u5206\u5272\u30AD\u30FC"
---

# カスタム分割キー {#custom-partitioning-key}

パーティション分割は、 [メルゲツリー](mergetree.md) 家族テーブル（含む [複製](replication.md) テーブル）。 [実体化ビュー](../special/materializedview.md#materializedview) に基づくMergeTreeテーブル支援を分割します。

パーティションは、指定された条件によるテーブル内のレコードの論理的な組合せです。 パーティションは、月別、日別、イベントタイプ別など、任意の条件で設定できます。 各パーティションは別に保存される簡単操作のデータです。 アクセス時のデータClickHouseの最小サブセットのパーティションは可能です。

パーティションは `PARTITION BY expr` 句とき [テーブルの作成](mergetree.md#table_engine-mergetree-creating-a-table). これはパーティションキーにすることはでき表現からのテーブル列あります。 例えば、指定ョ月の表現を使用 `toYYYYMM(date_column)`:

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

パーティションキーは、式のタプルにすることもできます。 [主キー](mergetree.md#primary-keys-and-indexes-in-queries)). 例えば:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

この例では、現在の週に発生したイベントの種類によってパーティション分割を設定します。

挿入する際に新しいデータテーブルにこのデータを保存することがで別パーツとして（個）-field-list順にソートその有効なタイプを利用します。 挿入後10-15分で、同じパーティションの部分が部分全体にマージされます。

!!! info "情報"
    マージは、パーティション分割式の値が同じデータパーツに対してのみ機能します。 つまり **なんかを過度に粒状仕切り** （千約以上のパーティション）。 それ以外の場合は、 `SELECT` ファイルシステムおよびオープンファイル記述子に不当に多数のファイルがあるため、クエリの実行が不十分です。

使用する [システム部品](../../../operations/system-tables.md#system_tables-parts) 表パーツとパーティションを表示する表。 たとえば、のは、我々が持っていると仮定しましょう `visits` テーブルを分割する。 のは、実行してみましょう `SELECT` のクエリ `system.parts` テーブル:

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

その `partition` 列にはパーティションの名前が含まれます。 あるパーティション例: `201901` と `201902`. この列の値を使用して、パーティション名を指定できます [ALTER … PARTITION](#alter_manipulations-with-partitions) クエリ。

その `name` カラムの名前を格納して、パーティションのデータ部品です。 この列を使用して、パーツの名前を指定することができます。 [ALTER ATTACH PART](#alter_attach-partition) クエリ。

最初の部分の名前を分解しましょう: `201901_1_3_1`:

-   `201901` パーティション名です。
-   `1` データブロックの最小数です。
-   `3` データブロックの最大数です。
-   `1` チャンクレベル(形成されるマージツリーの深さ)です。

!!! info "情報"
    古いタイプのテーブルの部分には名前があります: `20190117_20190123_2_2_0` (最小日-最大日-最小ブロック番号-最大ブロック番号-レベル)。

その `active` 列は部品の状態を示します。 `1` アクティブです; `0` 非アクティブです。 非アクティブな部分は、たとえば、より大きな部分にマージした後に残るソース部分です。 破損したデータ部分も非アクティブとして示されます。

この例でわかるように、同じパーティションにはいくつかの分離された部分があります（たとえば, `201901_1_3_1` と `201901_1_9_2`). つまり、これらの部分はまだマージされていません。 ClickHouseは、データの挿入された部分を定期的にマージし、挿入の約15分後にマージします。 また、スケジュールされていないマージを実行するには [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) クエリ。 例:

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

非アクティブな部分は、マージ後約10分で削除されます。

パーツとパーティションのセットを表示する別の方法は、テーブルのディレクトリに移動します: `/var/lib/clickhouse/data/<database>/<table>/`. 例えば:

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

フォルダ ‘201901_1_1_0’, ‘201901_1_7_1’ そして、部品のディレクトリです。 各部に関する対応する分割データが含まれまで一定の月のテーブルこの例では、分割による。

その `detached` ディレクトリに含まれる部品のこともあったかを使って、テーブル [DETACH](../../../sql-reference/statements/alter.md#alter_detach-partition) クエリ。 破損した部分も、削除されるのではなく、このディレクトリに移動されます。 サーバーは、サーバーからの部品を使用しません。 `detached` directory. You can add, delete, or modify the data in this directory at any time – the server will not know about this until you run the [ATTACH](../../../sql-reference/statements/alter.md#alter_attach-partition) クエリ。

オペレーティングサーバーでは、ファイルシステム上の部品のセットまたはそのデータを手動で変更することはできません。 非複製のテーブル、これを実行する事ができます。サーバが停止中でないお勧めします。 のための複製のテーブルはパーツのセットの変更はできません。

ClickHouseでは、パーティションの削除、テーブル間のコピー、またはバックアップの作成などの操作を実行できます。 セクションのすべての操作の一覧を参照してください [パーティションとパーツの操作](../../../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->

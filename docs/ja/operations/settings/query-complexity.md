---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: "\u30AF\u30A8\u30EA\u306E\u8907\u96D1\u3055\u306E\u5236\u9650"
---

# クエリの複雑さの制限 {#restrictions-on-query-complexity}

クエリの複雑さに関する制限は、設定の一部です。
これらをより安全な実行のユーザーインターフェースです。
ほとんどすべての制限が適用されます `SELECT`. 分散クエリ処理では、各サーバーに個別に制限が適用されます。

ClickHouseは、各行ではなく、データ部分の制限をチェックします。 これは、データ部分のサイズで制限の値を超えることができることを意味します。

の制限 “maximum amount of something” 値0を取ることができます。 “unrestricted”.
ほとんどの制限には ‘overflow\_mode’ 設定、制限を超えたときに何をすべきかを意味します。
での値: `throw` または `break`. 集計の制限(group\_by\_overflow\_mode)にも値があります `any`.

`throw` – Throw an exception (default).

`break` – Stop executing the query and return the partial result, as if the source data ran out.

`any (only for group_by_overflow_mode)` – Continuing aggregation for the keys that got into the set, but don't add new keys to the set.

## max\_memory\_usage {#settings_max_memory_usage}

単一サーバーでクエリを実行するために使用するRAMの最大量。

既定の構成ファイルでは、最大10GBです。

この設定では、使用可能なメモリの容量やマシン上のメモリの合計容量は考慮されません。
この制限は、単一サーバー内の単一のクエリに適用されます。
以下を使用できます `SHOW PROCESSLIST` 各クエリの現在のメモリ消費量を確認します。
さらに、ピークメモリ消費は各クエリに対して追跡され、ログに書き込まれます。

特定の集計関数の状態については、メモリ使用量は監視されません。

集計関数の状態に対してメモリ使用量が完全に追跡されません `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` から `String` と `Array` 引数。

メモリ消費もパラメータによって制限されます `max_memory_usage_for_user` と `max_memory_usage_for_all_queries`.

## max\_memory\_usage\_for\_user {#max-memory-usage-for-user}

単一サーバー上でユーザーのクエリを実行するために使用するRAMの最大量。

デフォルト値は [設定。h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L288). デフォルトでは、金額は制限されません (`max_memory_usage_for_user = 0`).

の説明も参照してください [max\_memory\_usage](#settings_max_memory_usage).

## max\_memory\_usage\_for\_all\_queries {#max-memory-usage-for-all-queries}

単一サーバー上ですべてのクエリを実行するために使用するRAMの最大量。

デフォルト値は [設定。h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L289). デフォルトでは、金額は制限されません (`max_memory_usage_for_all_queries = 0`).

の説明も参照してください [max\_memory\_usage](#settings_max_memory_usage).

## max\_rows\_to\_read {#max-rows-to-read}

次の制限は、各ブロック（各行ではなく）で確認できます。 つまり、制限は少し壊れる可能性があります。
複数のスレッドでクエリを実行する場合、次の制限は各スレッドに個別に適用されます。

クエリの実行時にテーブルから読み取ることができる最大行数。

## max\_bytes\_to\_read {#max-bytes-to-read}

クエリの実行時にテーブルから読み取ることができる最大バイト数(非圧縮データ)。

## read\_overflow\_mode {#read-overflow-mode}

読み込まれるデータ量がいずれかの制限を超えた場合の対処方法: ‘throw’ または ‘break’. デフォルトでは、throw。

## max\_rows\_to\_group\_by {#settings-max-rows-to-group-by}

集計から受け取った一意のキーの最大数。 この設定を使用すると、集計時のメモリ消費量を制限できます。

## group\_by\_overflow\_mode {#group-by-overflow-mode}

集計の一意キーの数が制限を超えた場合の対処方法: ‘throw’, ‘break’,または ‘any’. デフォルトでは、throw。
を使用して ‘any’ valueでは、GROUP BYの近似を実行できます。 この近似の品質は、データの統計的性質に依存します。

## max\_bytes\_before\_external\_group\_by {#settings-max_bytes_before_external_group_by}

の実行を有効または無効にします。 `GROUP BY` 外部メモリ内の句。 見る [外部メモリのGROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory).

可能な値:

-   シングルで使用できるRAMの最大ボリューム(バイト単位) [GROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-clause) 作戦だ
-   0 — `GROUP BY` 外部メモリでは無効です。

デフォルト値は0です。

## max\_rows\_to\_sort {#max-rows-to-sort}

並べ替え前の最大行数。 これにより、ソート時のメモリ消費を制限できます。

## max\_bytes\_to\_sort {#max-bytes-to-sort}

並べ替え前の最大バイト数。

## sort\_overflow\_mode {#sort-overflow-mode}

ソート前に受信した行数がいずれかの制限を超えた場合の対処方法: ‘throw’ または ‘break’. デフォルトでは、throw。

## max\_result\_rows {#setting-max_result_rows}

結果の行数を制限します。 またチェックサブクエリは、windowsアプリケーションの実行時にパーツの分散を返します。

## max\_result\_bytes {#max-result-bytes}

結果のバイト数を制限します。 前の設定と同じです。

## result\_overflow\_mode {#result-overflow-mode}

結果の量が制限のいずれかを超えた場合の対処方法: ‘throw’ または ‘break’. デフォルトでは、throw。

を使用して ‘break’ LIMITの使用に似ています。 `Break` ブロックレベルでのみ実行を中断します。 これは、返される行の量が [max\_result\_rows](#setting-max_result_rows) の倍数 [max\_block\_size](settings.md#setting-max_block_size) そして依存する [max\_threads](settings.md#settings-max_threads).

例:

``` sql
SET max_threads = 3, max_block_size = 3333;
SET max_result_rows = 3334, result_overflow_mode = 'break';

SELECT *
FROM numbers_mt(100000)
FORMAT Null;
```

結果:

``` text
6666 rows in set. ...
```

## max\_execution\_time {#max-execution-time}

クエリの最大実行時間を秒単位で指定します。
現時点では、ソートステージのいずれか、または集計関数のマージおよびファイナライズ時にはチェックされません。

## timeout\_overflow\_mode {#timeout-overflow-mode}

クエリが実行される時間よりも長い場合の対処方法 ‘max\_execution\_time’: ‘throw’ または ‘break’. デフォルトでは、throw。

## min\_execution\_speed {#min-execution-speed}

毎秒行単位の最小実行速度。 すべてのデータブロックで ‘timeout\_before\_checking\_execution\_speed’ 有効期限が切れます。 実行速度が低い場合は、例外がスローされます。

## min\_execution\_speed\_bytes {#min-execution-speed-bytes}

秒あたりの最小実行バイト数。 すべてのデータブロックで ‘timeout\_before\_checking\_execution\_speed’ 有効期限が切れます。 実行速度が低い場合は、例外がスローされます。

## max\_execution\_speed {#max-execution-speed}

毎秒の実行行の最大数。 すべてのデータブロックで ‘timeout\_before\_checking\_execution\_speed’ 有効期限が切れます。 実行速度が高い場合は、実行速度が低下します。

## max\_execution\_speed\_bytes {#max-execution-speed-bytes}

毎秒の実行バイト数の最大値。 すべてのデータブロックで ‘timeout\_before\_checking\_execution\_speed’ 有効期限が切れます。 実行速度が高い場合は、実行速度が低下します。

## timeout\_before\_checking\_execution\_speed {#timeout-before-checking-execution-speed}

実行速度が遅すぎないことをチェックします ‘min\_execution\_speed’)、指定された時間が秒単位で経過した後。

## max\_columns\_to\_read {#max-columns-to-read}

単一のクエリ内のテーブルから読み取ることができる列の最大数。 クエリでより多くの列を読み取る必要がある場合は、例外がスローされます。

## max\_temporary\_columns {#max-temporary-columns}

定数列を含む、クエリを実行するときに同時にRAMに保持する必要がある一時列の最大数。 これよりも多くの一時列がある場合、例外がスローされます。

## max\_temporary\_non\_const\_columns {#max-temporary-non-const-columns}

同じことと ‘max\_temporary\_columns’ しかし、定数列を数えずに。
定数列は、クエリを実行するときにかなり頻繁に形成されますが、計算リソースはほぼゼロです。

## max\_subquery\_depth {#max-subquery-depth}

サブクエリの最大ネスト深さ。 サブクエリが深い場合は、例外がスローされます。 既定では100です。

## max\_pipeline\_depth {#max-pipeline-depth}

最大パイプライン深さ。 クエリ処理中に各データブロックが処理する変換の数に対応します。 単一サーバーの範囲内でカウントされます。 パイプラインの深さが大きい場合は、例外がスローされます。 既定では、1000です。

## max\_ast\_depth {#max-ast-depth}

クエリ構文ツリーの最大ネスト深さ。 超過すると、例外がスローされます。
現時点では、解析中にはチェックされず、クエリの解析後にのみチェックされます。 つまり、解析中に深すぎる構文ツリーを作成することができますが、クエリは失敗します。 既定では、1000です。

## max\_ast\_elements {#max-ast-elements}

クエリ構文ツリー内の要素の最大数。 超過すると、例外がスローされます。
前の設定と同じように、クエリを解析した後にのみチェックされます。 既定では、50,000です。

## max\_rows\_in\_set {#max-rows-in-set}

サブクエリから作成されたIN句内のデータ-セットの最大行数。

## max\_bytes\_in\_set {#max-bytes-in-set}

サブクエリから作成されたIN句のセットで使用される最大バイト数(非圧縮データ)。

## set\_overflow\_mode {#set-overflow-mode}

データ量がいずれかの制限を超えた場合の対処方法: ‘throw’ または ‘break’. デフォルトでは、throw。

## max\_rows\_in\_distinct {#max-rows-in-distinct}

DISTINCTを使用する場合の最大行数。

## max\_bytes\_in\_distinct {#max-bytes-in-distinct}

DISTINCTを使用するときにハッシュテーブルで使用される最大バイト数。

## distinct\_overflow\_mode {#distinct-overflow-mode}

データ量がいずれかの制限を超えた場合の対処方法: ‘throw’ または ‘break’. デフォルトでは、throw。

## max\_rows\_to\_transfer {#max-rows-to-transfer}

グローバルINを使用するときに、リモートサーバーに渡すか、一時テーブルに保存できる最大行数。

## max\_bytes\_to\_transfer {#max-bytes-to-transfer}

グローバルINを使用するときに、リモートサーバーに渡すか、一時テーブルに保存できる最大バイト数(非圧縮データ)。

## transfer\_overflow\_mode {#transfer-overflow-mode}

データ量がいずれかの制限を超えた場合の対処方法: ‘throw’ または ‘break’. デフォルトでは、throw。

## max\_rows\_in\_join {#settings-max_rows_in_join}

テーブルを結合するときに使用されるハッシュテーブル内の行数を制限します。

この設定は以下に適用されます [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) 操作および [参加](../../engines/table-engines/special/join.md) テーブルエンジン。

クエリに複数の結合が含まれている場合、ClickHouseはこの設定で中間結果をすべてチェックします。

ClickHouseは、制限に達したときにさまざまなアクションを続行できます。 使用する [join\_overflow\_mode](#settings-join_overflow_mode) アクションを選択する設定。

可能な値:

-   正の整数。
-   0 — Unlimited number of rows.

デフォルト値は0です。

## max\_bytes\_in\_join {#settings-max_bytes_in_join}

制限サイズをバイトのハッシュテーブルが参加す。

この設定は以下に適用されます [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) 操作および [結合テーブルエンジン](../../engines/table-engines/special/join.md).

クエリに結合が含まれている場合、ClickHouseは中間結果ごとにこの設定をチェックします。

ClickHouseは、制限に達したときにさまざまなアクションを続行できます。 使用 [join\_overflow\_mode](#settings-join_overflow_mode) アクションを選択する設定。

可能な値:

-   正の整数。
-   0 — Memory control is disabled.

デフォルト値は0です。

## join\_overflow\_mode {#settings-join_overflow_mode}

次のいずれかの結合制限に達したときにClickHouseが実行するアクションを定義します:

-   [max\_bytes\_in\_join](#settings-max_bytes_in_join)
-   [max\_rows\_in\_join](#settings-max_rows_in_join)

可能な値:

-   `THROW` — ClickHouse throws an exception and breaks operation.
-   `BREAK` — ClickHouse breaks operation and doesn't throw an exception.

デフォルト値: `THROW`.

**も参照。**

-   [JOIN句](../../sql-reference/statements/select/join.md#select-join)
-   [結合テーブルエンジン](../../engines/table-engines/special/join.md)

## max\_partitions\_per\_insert\_block {#max-partitions-per-insert-block}

単一挿入ブロック内のパーティションの最大数を制限します。

-   正の整数。
-   0 — Unlimited number of partitions.

デフォルト値は100です。

**詳細**

を挿入する際、データClickHouse計算パーティションの数に挿入されます。 パーティションの数が `max_partitions_per_insert_block`,ClickHouseは、次のテキストで例外をスローします:

> “Too many partitions for single INSERT block (more than” +toString(max\_parts)+ “). The limit is controlled by ‘max\_partitions\_per\_insert\_block’ setting. A large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc).”

[元の記事](https://clickhouse.tech/docs/en/operations/settings/query_complexity/) <!--hide-->

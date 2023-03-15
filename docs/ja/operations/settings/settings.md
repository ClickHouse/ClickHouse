---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# 設定 {#settings}

## distributed_product_mode {#distributed-product-mode}

の動作を変更します [分散サブクエリ](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

制限:

-   INおよびJOINサブクエリにのみ適用されます。
-   FROMセクションが複数のシャードを含む分散テーブルを使用する場合のみ。
-   サブクエリが複数のシャードを含む分散テーブルに関係する場合。
-   テーブル値には使用されません [リモート](../../sql-reference/table-functions/remote.md) 機能。

可能な値:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” 例外）。
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` クエリ `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable_optimize_predicate_expression {#enable-optimize-predicate-expression}

述語プッシュダウンをオンにする `SELECT` クエリ。

述語プッシュダウ

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

使用法

次のクエリを検討します:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

もし `enable_optimize_predicate_expression = 1` ClickHouseが適用されるため、これらのクエリの実行時間は等しくなります `WHERE` それを処理するときにサブクエリに。

もし `enable_optimize_predicate_expression = 0` 次に、第二のクエリの実行時間ははるかに長くなります。 `WHERE` 句は、サブクエリの終了後にすべてのデータに適用されます。

## フォールバック_to_stale_replicas_for_distributed_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

更新されたデータが使用できない場合は、クエリを古いレプリカに強制します。 見る [複製](../../engines/table-engines/mergetree-family/replication.md).

ClickHouseは、テーブルの古いレプリカから最も関連性の高いものを選択します。

実行時使用 `SELECT` 複製されたテーブルを指す分散テーブルから。

デフォルトでは、1(有効)です。

## force_index_by_date {#settings-force_index_by_date}

インデックスを日付で使用できない場合は、クエリの実行を無効にします。

MergeTreeファミリ内のテーブルで動作します。

もし `force_index_by_date=1`,ClickHouseは、クエリにデータ範囲を制限するために使用できる日付キー条件があるかどうかをチェックします。 適切な条件がない場合は、例外をスローします。 ただし、条件によって読み取るデータ量が減少するかどうかはチェックされません。 たとえば、次の条件 `Date != ' 2000-01-01 '` テーブル内のすべてのデータと一致する場合でも許容されます（つまり、クエリを実行するにはフルスキャンが必要です）。 MergeTreeテーブル内のデータ範囲の詳細については、以下を参照してください [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md).

## force_primary_key {#force-primary-key}

主キーによる索引付けが不可能な場合は、クエリの実行を無効にします。

MergeTreeファミリ内のテーブルで動作します。

もし `force_primary_key=1`,ClickHouseは、クエリにデータ範囲を制限するために使用できる主キー条件があるかどうかを確認します。 適切な条件がない場合は、例外をスローします。 ただし、条件によって読み取るデータ量が減少するかどうかはチェックされません。 MergeTreeテーブルのデータ範囲の詳細については、以下を参照してください [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md).

## format_schema {#format-schema}

このパラメーターは、次のようなスキーマ定義を必要とする形式を使用している場合に便利です [Cap'N Proto](https://capnproto.org/) または [プロトブフ](https://developers.google.com/protocol-buffers/). 値は形式によって異なります。

## fsync_metadata {#fsync-metadata}

有効または無効にします [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) 書くとき `.sql` ファイル 既定で有効になっています。

ことは、あってはならないことで無効にすることもできれば、サーバは、数百万の小さなテーブルが続々と生まれてくると破壊されました。

## enable_http_compression {#settings-enable_http_compression}

HTTP要求に対する応答のデータ圧縮を有効または無効にします。

詳細については、 [HTTP](../../interfaces/http.md).

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値は0です。

## http_zlib_compression_level {#settings-http_zlib_compression_level}

HTTP要求に対する応答のデータ圧縮のレベルを次の場合に設定します [enable_http_compression=1](#settings-enable_http_compression).

可能な値:1から9までの数値。

デフォルト値は3です。

## http_native_compression_disable_checksumming_on_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

クライア ClickHouseネイティブ圧縮フォーマットにのみ使用されます( `gzip` または `deflate`).

詳細については、 [HTTP](../../interfaces/http.md).

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値は0です。

## send_progress_in_http_headers {#settings-send_progress_in_http_headers}

有効または無効にします `X-ClickHouse-Progress` HTTP応答ヘッダー `clickhouse-server` 応答。

詳細については、 [HTTP](../../interfaces/http.md).

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値は0です。

## max_http_get_redirects {#setting-max_http_get_redirects}

HTTP GETリダイレクトホップの最大数を制限します。 [URL](../../engines/table-engines/special/url.md)-エンジンテーブル。 この設定は、両方のタイプのテーブルに適用されます。 [CREATE TABLE](../../sql-reference/statements/create.md#create-table-query) クエリとによって [url](../../sql-reference/table-functions/url.md) テーブル関数。

可能な値:

-   任意の正の整数ホップ数。
-   0 — No hops allowed.

デフォルト値は0です。

## input_format_allow_errors_num {#settings-input_format_allow_errors_num}

テキスト形式（CSV、TSVなど）から読み取るときに許容されるエラーの最大数を設定します。).

既定値は0です。

常にそれをペアにします `input_format_allow_errors_ratio`.

行の読み取り中にエラーが発生したが、エラーカウンタがそれより小さい場合 `input_format_allow_errors_num`,ClickHouseは行を無視し、次の行に移動します。

両方の場合 `input_format_allow_errors_num` と `input_format_allow_errors_ratio` を超えた場合、ClickHouseは例外をスローします。

## input_format_allow_errors_ratio {#settings-input_format_allow_errors_ratio}

テキスト形式（CSV、TSVなど）から読み取るときに許容されるエラーの最大割合を設定します。).
エラーの割合は、0から1の間の浮動小数点数として設定されます。

既定値は0です。

常にそれをペアにします `input_format_allow_errors_num`.

行の読み取り中にエラーが発生したが、エラーカウンタがそれより小さい場合 `input_format_allow_errors_ratio`,ClickHouseは行を無視し、次の行に移動します。

両方の場合 `input_format_allow_errors_num` と `input_format_allow_errors_ratio` を超えた場合、ClickHouseは例外をスローします。

## input_format_values_interpret_expressions {#settings-input_format_values_interpret_expressions}

を有効または無効にしのSQLのパーサの場合の高速ストリームのパーサで構文解析のデータです。 この設定は、 [値](../../interfaces/formats.md#data-format-values) データ挿入時の書式。 構文解析の詳細については、以下を参照してください [構文](../../sql-reference/syntax.md) セクション

可能な値:

-   0 — Disabled.

    この場合、提供しなければなりません形式のデータです。 を参照。 [形式](../../interfaces/formats.md) セクション

-   1 — Enabled.

    この場合、SQL式を値として使用できますが、データの挿入はこの方法ではるかに遅くなります。 書式設定されたデータのみを挿入すると、ClickHouseは設定値が0であるかのように動作します。

デフォルト値:1。

使用例

挿入 [DateTime](../../sql-reference/data-types/datetime.md) 異なる設定で値を入力します。

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

最後のクエリは次のものと同じです:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## input_format_values_deduce_templates_of_expressions {#settings-input_format_values_deduce_templates_of_expressions}

SQL式のテンプレート控除を有効または無効にします。 [値](../../interfaces/formats.md#data-format-values) 形式。 この解析と解釈表現 `Values` 連続する行の式が同じ構造を持つ場合、はるかに高速です。 ClickHouseは式のテンプレートを推論し、このテンプレートを使用して次の行を解析し、正常に解析された行のバッチで式を評価しようとします。

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

次のクエリの場合:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   もし `input_format_values_interpret_expressions=1` と `format_values_deduce_templates_of_expressions=0`,式は各行ごとに別々に解釈されます（これは多数の行では非常に遅いです）。
-   もし `input_format_values_interpret_expressions=0` と `format_values_deduce_templates_of_expressions=1`、第一、第二および第三の行の式は、テンプレートを使用して解析されます `lower(String)` そして一緒に解釈されると、forth行の式は別のテンプレートで解析されます (`upper(String)`).
-   もし `input_format_values_interpret_expressions=1` と `format_values_deduce_templates_of_expressions=1`、前の場合と同じですが、テンプレートを推論できない場合は、式を別々に解釈することもできます。

## input_format_values_accurate_types_of_literals {#settings-input-format-values-accurate-types-of-literals}

この設定は次の場合にのみ使用されます `input_format_values_deduce_templates_of_expressions = 1`. いくつかの列の式は同じ構造を持ちますが、異なる型の数値リテラルが含まれています。

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

可能な値:

-   0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` または `Int64` 代わりに `UInt64` のために `42`が、その原因となりオーバーフローおよび精度の問題です。

-   1 — Enabled.

    この場合、ClickHouseはリテラルの実際の型をチェックし、対応する型の式テンプレートを使用します。 場合によっては、 `Values`.

デフォルト値:1。

## input_format_defaults_for_omitted_fields {#session_settings-input_format_defaults_for_omitted_fields}

実行するとき `INSERT` 省略された入力列の値を、それぞれの列のデフォルト値に置き換えます。 このオプションは [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) と [TabSeparated](../../interfaces/formats.md#tabseparated) フォーマット。

!!! note "注"
    このオプショ 消費量で追加的に計算資源をサーバーを低減できる。

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

## input_format_tsv_empty_as_default {#settings-input-format-tsv-empty-as-default}

有効にすると、TSVの空の入力フィールドを既定値に置き換えます。 複雑な既定の式の場合 `input_format_defaults_for_omitted_fields` 有効にする必要があります。

既定では無効です。

## input_format_null_as_default {#settings-input-format-null-as-default}

を有効または無効にし用のデフォルト値が入力データを含む `NULL` しかし、notの対応する列のデータ型 `Nullable(T)` （テキスト入力形式の場合）。

## input_format_skip_unknown_fields {#settings-input-format-skip-unknown-fields}

追加データのスキップ挿入を有効または無効にします。

書き込みデータClickHouseが例外をスローした場合入力データを含むカラム特別な権限は必要ありません使用します。 スキップが有効な場合、ClickHouseは余分なデータを挿入せず、例外をスローしません。

対応形式:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値は0です。

## input_format_import_nested_json {#settings-input_format_import_nested_json}

を有効または無効にし、挿入のJSONデータをネストしたオブジェクト。

対応形式:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値は0です。

も参照。:

-   [入れ子構造の使用](../../interfaces/formats.md#jsoneachrow-nested) と `JSONEachRow` 形式。

## input_format_with_names_use_header {#settings-input-format-with-names-use-header}

データ挿入時の列順序の確認を有効または無効にします。

挿入のパフォーマンスを向上させるために、入力データの列の順序がターゲットテーブルと同じであることが確実な場合は、このチェックを無効にするこ

対応形式:

-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

## date_time_input_format {#settings-date_time_input_format}

日付と時刻のテキスト表現のパーサーを選択できます。

この設定は次の場合には適用されません [日付と時刻の関数](../../sql-reference/functions/date-time-functions.md).

可能な値:

-   `'best_effort'` — Enables extended parsing.

    ClickHouseは基本を解析できます `YYYY-MM-DD HH:MM:SS` 書式とすべて [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) 日付と時刻の形式。 例えば, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouseは基本のみを解析できます `YYYY-MM-DD HH:MM:SS` 形式。 例えば, `'2019-08-20 10:18:56'`.

デフォルト値: `'basic'`.

も参照。:

-   [DateTimeデータ型。](../../sql-reference/data-types/datetime.md)
-   [日付と時刻を操作するための関数。](../../sql-reference/functions/date-time-functions.md)

## join_default_strictness {#settings-join_default_strictness}

デフォルトの厳密さを [結合句](../../sql-reference/statements/select/join.md#select-join).

可能な値:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [デカルト積](https://en.wikipedia.org/wiki/Cartesian_product) 一致する行から。 これは正常です `JOIN` 標準SQLからの動作。
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` と `ALL` 同じです。
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` または `ANY` クエリで指定されていない場合、ClickHouseは例外をスローします。

デフォルト値: `ALL`.

## join_any_take_last_row {#settings-join_any_take_last_row}

結合操作の動作を `ANY` 厳密さ。

!!! warning "注意"
    この設定は `JOIN` との操作 [参加](../../engines/table-engines/special/join.md) エンジンテーブル。

可能な値:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

デフォルト値は0です。

も参照。:

-   [JOIN句](../../sql-reference/statements/select/join.md#select-join)
-   [結合テーブルエンジン](../../engines/table-engines/special/join.md)
-   [join_default_strictness](#settings-join_default_strictness)

## join_use_nulls {#join_use_nulls}

のタイプを設定します。 [JOIN](../../sql-reference/statements/select/join.md) 行動。 際融合のテーブル、空細胞が表示される場合があります。 ClickHouseは、この設定に基づいて異なる塗りつぶします。

可能な値:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` 標準SQLと同じように動作します。 対応するフィールドの型は次のように変換されます [Null可能](../../sql-reference/data-types/nullable.md#data_type-nullable) 空のセルは [NULL](../../sql-reference/syntax.md).

デフォルト値は0です。

## max_block_size {#setting-max_block_size}

ClickHouseでは、データはブロック（列部分のセット）によって処理されます。 単一ブロックの内部処理サイクルは十分に効率的ですが、各ブロックには顕著な支出があります。 その `max_block_size` 設定は、テーブルからロードするブロックのサイズ（行数）の推奨事項です。 ブロックサイズは小さすぎるので、各ブロックの支出はまだ目立ちますが、最初のブロックの後に完了したLIMITのクエリがすばやく処理されるよう 目標は、複数のスレッドで多数の列を抽出するときにメモリを消費するのを避け、少なくともいくつかのキャッシュの局所性を保持することです。

デフォルト値は65,536です。

ブロックのサイズ `max_block_size` ていないから読み込まれます。 場合ことは明らかであることなくデータを取得され、さらに小型のブロックを処理します。

## preferred_block_size_bytes {#preferred-block-size-bytes}

と同じ目的のために使用される `max_block_size` しかし、ブロック内の行数に適応させることによって、推奨されるブロックサイズをバイト単位で設定します。
ただし、ブロックサイズは次のようになります `max_block_size` 行。
既定では、1,000,000です。 MergeTreeエンジンから読み取るときにのみ動作します。

## merge_tree_min_rows_for_concurrent_read {#setting-merge-tree-min-rows-for-concurrent-read}

のファイルから読み込まれる行数が [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md) テーブル超過 `merge_tree_min_rows_for_concurrent_read` その後ClickHouseしようとして行な兼職の状況からの読み出しこのファイルに複数のスレッド）。

可能な値:

-   任意の正の整数。

デフォルト値は163840です。

## merge_tree_min_bytes_for_concurrent_read {#setting-merge-tree-min-bytes-for-concurrent-read}

のファイルから読み取るバイト数が [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md)-エンジン表超過 `merge_tree_min_bytes_for_concurrent_read` 次に、ClickHouseはこのファイルから複数のスレッドで同時に読み取ろうとします。

可能な値:

-   任意の正の整数。

デフォルト値は251658240です。

## merge_tree_min_rows_for_seek {#setting-merge-tree-min-rows-for-seek}

一つのファイルに読み込まれる二つのデータブロック間の距離が `merge_tree_min_rows_for_seek` その後、ClickHouseはファイルをシークせず、データを順番に読み込みます。

可能な値:

-   任意の正の整数。

デフォルト値は0です。

## merge_tree_min_bytes_for_seek {#setting-merge-tree-min-bytes-for-seek}

一つのファイルに読み込まれる二つのデータブロック間の距離が `merge_tree_min_bytes_for_seek` その後、ClickHouseは両方のブロックを含むファイルの範囲を順番に読み込むため、余分なシークを避けます。

可能な値:

-   任意の正の整数。

デフォルト値は0です。

## merge_tree_coarse_index_granularity {#setting-merge-tree-coarse-index-granularity}

する場合のデータClickHouseチェックのデータにファイルです。 必要なキーがある範囲にあることをClickHouseが検出すると、この範囲を次のように分割します `merge_tree_coarse_index_granularity` 必要なキーを再帰的に検索します。

可能な値:

-   任意の正の偶数の整数。

デフォルト値:8。

## merge_tree_max_rows_to_use_cache {#setting-merge-tree-max-rows-to-use-cache}

ClickHouseがより多くを読むべきであれば `merge_tree_max_rows_to_use_cache` あるクエリでは、非圧縮ブロックのキャッシュを使用しません。

のキャッシュされた、圧縮解除されたブロックの店舗データを抽出したためます。 ClickHouseこのキャッシュの高速化対応小の繰り返します。 この設定は、大量のデータを読み取るクエリによってキャッシュが破損するのを防ぎます。 その [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) サーバー設定は、非圧縮ブロックのキャッシュのサイズを定義します。

可能な値:

-   任意の正の整数。

Default value: 128 ✕ 8192.

## merge_tree_max_bytes_to_use_cache {#setting-merge-tree-max-bytes-to-use-cache}

ClickHouseがより多くを読むべきであれば `merge_tree_max_bytes_to_use_cache` 一つのクエリでは、非圧縮ブロックのキャッシュを使用しません。

のキャッシュされた、圧縮解除されたブロックの店舗データを抽出したためます。 ClickHouseこのキャッシュの高速化対応小の繰り返します。 この設定は、大量のデータを読み取るクエリによってキャッシュが破損するのを防ぎます。 その [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) サーバー設定は、非圧縮ブロックのキャッシュのサイズを定義します。

可能な値:

-   任意の正の整数。

既定値:2013265920。

## min_bytes_to_use_direct_io {#settings-min-bytes-to-use-direct-io}

ストレージディスクへの直接I/Oアクセスを使用するために必要な最小データ量。

ClickHouseこの設定からデータを読み込むときます。 読み取るすべてのデータの合計ストレージ容量が超過した場合 `min_bytes_to_use_direct_io` その後、ClickHouseはストレージディスクからデータを読み取ります。 `O_DIRECT` オプション

可能な値:

-   0 — Direct I/O is disabled.
-   正の整数。

デフォルト値は0です。

## log_queries {#settings-log-queries}

クエリログの設定。

この設定でClickHouseに送信されたクエリは、 [query_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log) サーバー構成パラメータ。

例:

``` text
log_queries=1
```

## log_queries_min_type {#settings-log-queries-min-type}

`query_log` ログに記録する最小タイプ。

可能な値:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

デフォルト値: `QUERY_START`.

どのエンティリーが行くかを制限するために使用できます `query_log` んだ興味深いだけ誤差を利用することができ `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## log_query_threads {#settings-log-query-threads}

クエリスレッドログの設定。

この設定でClickHouseによって実行されたクエリのスレッドは、 [query_thread_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) サーバー構成パラメータ。

例:

``` text
log_query_threads=1
```

## max_insert_block_size {#settings-max_insert_block_size}

テーブルに挿入するために形成するブロックのサイズ。
この設定は、サーバーがブロックを形成する場合にのみ適用されます。
たとえば、HTTPインターフェイスを介した挿入の場合、サーバーはデータ形式を解析し、指定されたサイズのブロックを形成します。
しかし、clickhouse-clientを使用すると、クライアントはデータ自体を解析し、 ‘max_insert_block_size’ サーバーでの設定は、挿入されたブロックのサイズには影響しません。
データはSELECT後に形成されるのと同じブロックを使用して挿入されるため、INSERT SELECTを使用するときにもこの設定には目的がありません。

既定値は1,048,576です。

デフォルトは、 `max_block_size`. この理由は、特定のテーブルエンジンが原因です (`*MergeTree`）挿入されたブロックごとにディスク上にデータ部分を形成します。 同様に, `*MergeTree` テーブルデータを並べ替え時の挿入やるのに十分な大きさのブロックサイズを選別データにアプリです。

## min_insert_block_size_rows {#min-insert-block-size-rows}

テーブルに挿入できるブロック内の最小行数を設定します。 `INSERT` クエリ。 小さめサイズのブロックをつぶし入ります。

可能な値:

-   正の整数。
-   0 — Squashing disabled.

デフォルト値は1048576です。

## min_insert_block_size_bytes {#min-insert-block-size-bytes}

テーブルに挿入できるブロック内の最小バイト数を設定します。 `INSERT` クエリ。 小さめサイズのブロックをつぶし入ります。

可能な値:

-   正の整数。
-   0 — Squashing disabled.

デフォルト値:268435456。

## max_replica_delay_for_distributed_queries {#settings-max_replica_delay_for_distributed_queries}

分散クエリの遅延レプリカを無効にします。 見る [複製](../../engines/table-engines/mergetree-family/replication.md).

時間を秒単位で設定します。 レプリカが設定値より遅れている場合、このレプリカは使用されません。

デフォルト値は300です。

実行時使用 `SELECT` 複製されたテーブルを指す分散テーブルから。

## max_threads {#settings-max_threads}

リモートサーバーからデータを取得するためのスレッドを除く、クエリ処理スレッドの最大数 ‘max_distributed_connections’ 変数）。

このパラメータに適用されるスレッドは、それらのスレッドが同じ段階での問合せ処理パイプライン。
たとえば、テーブルから読み込むときに、関数で式を評価することができる場合は、少なくともを使用してWHEREとGROUP BYの事前集計を並列に使用してフィル ‘max_threads’ その後、スレッドの数 ‘max_threads’ 使用されます。

デフォルト値:物理CPUコアの数。

サーバーで同時に実行されるSELECTクエリが通常より少ない場合は、このパラメーターを実際のプロセッサコア数よりわずかに小さい値に設定します。

制限のために迅速に完了するクエリの場合は、低い値を設定できます ‘max_threads’. たとえば、必要な数のエントリがすべてのブロックにあり、max_threads=8の場合、8つのブロックが取得されます。

小さいほど `max_threads` 値は、より少ないメモリが消費されます。

## max_insert_threads {#settings-max-insert-threads}

実行するスレッドの最大数 `INSERT SELECT` クエリ。

可能な値:

-   0 (or 1) — `INSERT SELECT` 並列実行はありません。
-   正の整数。 1より大きい。

デフォルト値は0です。

平行 `INSERT SELECT` のみ効果があります。 `SELECT` パーツは並列で実行されます。 [max_threads](#settings-max_threads) 設定。
値を大きくすると、メモリ使用量が増えます。

## max_compress_block_size {#max-compress-block-size}

テーブルに書き込むための圧縮前の非圧縮データのブロックの最大サイズ。 既定では、1,048,576(1MiB)です。 サイズを小さくすると、圧縮率が大幅に低下し、キャッシュの局所性のために圧縮と解凍の速度がわずかに増加し、メモリ消費が減少します。 通常、この設定を変更する理由はありません。

圧縮のためのブロック（バイトで構成されるメモリの塊）とクエリ処理のためのブロック（テーブルからの行のセット）を混同しないでください。

## min_compress_block_size {#min-compress-block-size}

のために [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md)"テーブル。 削減のため、遅延が処理クエリーのブロックの圧縮を書くとき、次のマークがそのサイズは少なくとも ‘min_compress_block_size’. 既定では、65,536です。

圧縮されていないデータが以下の場合、ブロックの実際のサイズ ‘max_compress_block_size’ は、この値以上であり、一つのマークのデータ量以上である。

例を見てみましょう。 仮定すると ‘index_granularity’ テーブル作成時に8192に設定されました。

UInt32型の列（値あたり4バイト）を書いています。 8192行を書き込むと、合計は32KBのデータになります。 Min_compress_block_size=65,536ので、圧縮されたブロックはすべてのマークに対して形成されます。

文字列タイプ（値あたり60バイトの平均サイズ）のURL列を作成しています。 8192行を書き込むと、平均は500KBのデータよりわずかに小さくなります。 これは65,536以上であるため、各マークに圧縮されたブロックが形成されます。 この場合、単一のマークの範囲でディスクからデータを読み取るとき、余分なデータは解凍されません。

通常、この設定を変更する理由はありません。

## max_query_size {#settings-max_query_size}

SQLパーサーを使用して解析するためにRAMに取り込むことができるクエリの最大部分。
INSERTクエリには、別のストリームパーサー(O(1)RAMを消費する)によって処理されるINSERTのデータも含まれていますが、この制限には含まれていません。

デフォルト値:256KiB。

## interactive_delay {#interactive-delay}

区間マイクロ秒単位で確認を行うための要求実行中止となり、送信を行います。

デフォルト値:100,000(キャンセルをチェックし、進行状況を秒単位で送信します)。

## connect_timeout,receive_timeout,send_timeout {#connect-timeout-receive-timeout-send-timeout}

クライアントとの通信に使用されるソケットの秒単位のタイムアウト。

デフォルト値:10,300,300。

## cancel_http_readonly_queries_on_client_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

デフォルト値:0

## poll_interval {#poll-interval}

指定された秒数の待機ループをロックします。

デフォルト値は10です。

## max_distributed_connections {#max-distributed-connections}

単一の分散テーブルへの単一のクエリの分散処理のためのリモートサーバーとの同時接続の最大数。 クラスター内のサーバー数以上の値を設定することをお勧めします。

デフォルト値:1024。

次のパラメーターは、分散テーブルを作成するとき(およびサーバーを起動するとき)にのみ使用されるため、実行時に変更する理由はありません。

## distributed_connections_pool_size {#distributed-connections-pool-size}

単一の分散テーブルに対するすべてのクエリの分散処理のためのリモートサーバーとの同時接続の最大数。 クラスター内のサーバー数以上の値を設定することをお勧めします。

デフォルト値:1024。

## connect_timeout_with_failover_ms {#connect-timeout-with-failover-ms}

分散テーブルエンジンのリモートサーバーに接続するためのタイムアウト時間(ミリ秒)。 ‘shard’ と ‘replica’ セクションは、クラスタ定義で使用されます。
失敗した場合は、さまざまなレプリカへの接続が試行されます。

デフォルト値は50です。

## connections_with_failover_max_tries {#connections-with-failover-max-tries}

分散テーブルエンジンの各レプリカとの接続試行の最大数。

デフォルト値は3です。

## 極端な {#extremes}

極端な値(クエリ結果の列の最小値と最大値)を数えるかどうか。 0または1を受け入れます。 既定では、0(無効)です。
詳細については “Extreme values”.

## use_uncompressed_cache {#setting-use_uncompressed_cache}

非圧縮ブロックのキャッシュを使用するかどうか。 0または1を受け入れます。 既定では、0(無効)です。
非圧縮キャッシュ(MergeTreeファミリ内のテーブルのみ)を使用すると、多数の短いクエリを処理する場合に、待ち時間を大幅に削減してスループットを向上させ この設定を有効にユーザーに送信頻繁に短います。 またに注意を払って下さい [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

少なくとも幾分大きな量のデータ（百万行以上）を読み取るクエリの場合、圧縮されていないキャッシュは自動的に無効になり、本当に小さなクエリの これは保つことができることを意味する ‘use_uncompressed_cache’ 設定は常に1に設定します。

## replace_running_query {#replace-running-query}

HTTPインターフェイスを使用する場合、 ‘query_id’ 変数は渡すことができます。 これは、クエリ識別子として機能する任意の文字列です。
同じユーザーからのクエリが同じ場合 ‘query_id’ この時点で既に存在しているので、動作は ‘replace_running_query’ パラメータ。

`0` (default) – Throw an exception (don't allow the query to run if a query with the same ‘query_id’ すでに実行されています）。

`1` – Cancel the old query and start running the new one.

Yandex.Metricaこのパラメータセットが1の実施のための提案のための分割ます。 次の文字を入力した後、古いクエリがまだ終了していない場合は、キャンセルする必要があります。

## stream_flush_interval_ms {#stream-flush-interval-ms}

作品のテーブルストリーミングの場合はタイムアウトした場合、またはスレッドを生成す [max_insert_block_size](#settings-max_insert_block_size) 行。

既定値は7500です。

値が小さいほど、データがテーブルにフラッシュされる頻度が高くなります。 値を小さく設定すると、パフォーマンスが低下します。

## load_balancing {#settings-load_balancing}

分散クエリ処理に使用されるレプリカ選択のアルゴリズムを指定します。

ClickHouse対応し、以下のようなアルゴリズムの選択のレプリカ:

-   [ランダム](#load_balancing-random) （デフォルトでは)
-   [最寄りのホスト名](#load_balancing-nearest_hostname)
-   [順番に](#load_balancing-in_order)
-   [最初またはランダム](#load_balancing-first_or_random)

### ランダム(デフォルト) {#load_balancing-random}

``` sql
load_balancing = random
```

エラーの数は、レプリカごとにカウントされます。 クエリは、エラーが最も少ないレプリカに送信されます。
レプリカのデータが異なる場合は、異なるデータも取得されます。

### 最寄りのホスト名 {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server's hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

例えば、example01-01-1やexample01-01-2.yandex.ru 一つの場所で異なるが、example01-01-1とexample01-02-2は二つの場所で異なる。
この方法は原始的に見えるかもしれませんが、ネットワークトポロジに関する外部データを必要とせず、Ipアドレスを比較することもありません。

したがって、同等のレプリカがある場合は、名前で最も近いレプリカが優先されます。
また、同じサーバーにクエリを送信するときに、障害がない場合、分散クエリも同じサーバーに移動すると仮定することもできます。 したがって、レプリカに異なるデータが配置されても、クエリはほとんど同じ結果を返します。

### 順番に {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

同じ数のエラーを持つレプリカは、構成で指定されている順序と同じ順序でアクセスされます。
この方法は、適切なレプリカを正確に把握している場合に適しています。

### 最初またはランダム {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

このアルゴリズムは、セット内の最初のレプリカを選択します。 クロスレプリケーショントポロジの設定では有効ですが、他の構成では役に立ちません。

その `first_or_random` アルゴリズムはの問題を解決します `in_order` アルゴリズム と `in_order` あるレプリカがダウンした場合、残りのレプリカは通常のトラフィック量を処理しますが、次のレプリカは二重負荷を受けます。 を使用する場合 `first_or_random` アルゴリズムでは、負荷はまだ利用可能なレプリカ間で均等に分散されます。

## prefer_localhost_replica {#settings-prefer-localhost-replica}

を有効/無効にしが好ましいのlocalhostレプリカ処理時に分布します。

可能な値:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [load_balancing](#settings-load_balancing) 設定。

デフォルト値:1。

!!! warning "警告"
    使用する場合は、この設定を無効にします [max_parallel_replicas](#settings-max_parallel_replicas).

## totals_mode {#totals-mode}

有するときの合計の計算方法、およびmax_rows_to_group_byおよびgroup_by_overflow_mode= ‘any’ 存在する。
セクションを参照 “WITH TOTALS modifier”.

## totals_auto_threshold {#totals-auto-threshold}

のしきい値 `totals_mode = 'auto'`.
セクションを参照 “WITH TOTALS modifier”.

## max_parallel_replicas {#settings-max_parallel_replicas}

クエリ実行時の各シャードのレプリカの最大数。
のための一貫性を異なる部分に同じデータを分割)、このオプションにしているときだけサンプリングキーを設定します。
レプリカラグは制御されません。

## output_format_json_quote_64bit_integers {#session_settings-output_format_json_quote_64bit_integers}

値がtrueの場合、json\*Int64およびUInt64形式（ほとんどのJavaScript実装との互換性のため）を使用するときに整数が引用符で表示されます。

## format_csv_delimiter {#settings-format_csv_delimiter}

CSVデータの区切り文字として解釈される文字。 デフォルトでは、区切り文字は `,`.

## input_format_csv_unquoted_null_literal_as_null {#settings-input_format_csv_unquoted_null_literal_as_null}

CSV入力形式の場合、引用符なしの解析を有効または無効にします `NULL` リテラルとして(のシノニム `\N`).

## output_format_csv_crlf_end_of_line {#settings-output-format-csv-crlf-end-of-line}

CSVでは、UNIXスタイル（LF）の代わりにDOS/Windowsスタイルの行区切り記号（CRLF）を使用します。

## output_format_tsv_crlf_end_of_line {#settings-output-format-tsv-crlf-end-of-line}

TSVでは、UNIXスタイル(LF)の代わりにDOC/Windowsスタイルの行区切り記号(CRLF)を使用します。

## insert_quorum {#settings-insert_quorum}

を決議の定足数を書き込みます.

-   もし `insert_quorum < 2` クォーラム書き込みは無効です。
-   もし `insert_quorum >= 2` クォーラム書き込みが有効になります。

デフォルト値は0です。

定足数書き込み

`INSERT` 承ClickHouse管理を正しく書き込みデータの `insert_quorum` の間のレプリカの `insert_quorum_timeout`. 何らかの理由で書き込みが成功したレプリカの数が `insert_quorum` を書くのは失敗したとClickHouseを削除するに挿入したブロックからすべてのレプリカがデータがすでに記されています。

クォーラム内のすべてのレプリカは一貫性があります。 `INSERT` クエリ。 その `INSERT` シーケンスは線形化されます。

から書き込まれたデータを読み取ると `insert_quorum` を使用することができます [select_sequential_consistency](#settings-select_sequential_consistency) オプション

ClickHouseは例外を生成します

-   クエリの時点で使用可能なレプリカの数が `insert_quorum`.
-   前のブロックがまだ挿入されていないときにデータを書き込もうとすると `insert_quorum` レプリカの。 この状況は、ユーザーが `INSERT` 前のものの前に `insert_quorum` 完了です。

も参照。:

-   [insert_quorum_timeout](#settings-insert_quorum_timeout)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## insert_quorum_timeout {#settings-insert_quorum_timeout}

書き込み数が定員タイムアウトを秒で指定します。 タイムアウトが経過し、まだ書き込みが行われていない場合、ClickHouseは例外を生成し、クライアントは同じまたは他のレプリカに同じブロックを書き込む

デフォルト値は60秒です。

も参照。:

-   [insert_quorum](#settings-insert_quorum)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## select_sequential_consistency {#settings-select_sequential_consistency}

の順次整合性を有効または無効にします `SELECT` クエリ:

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値は0です。

使用法

順次整合性が有効になっている場合、ClickHouseはクライアントが `SELECT` 以前のすべてのデータを含むレプリカのみを照会します `INSERT` 以下で実行されるクエリ `insert_quorum`. クライアントが部分レプリカを参照している場合、ClickHouseは例外を生成します。 SELECTクエリには、レプリカのクォーラムにまだ書き込まれていないデータは含まれません。

も参照。:

-   [insert_quorum](#settings-insert_quorum)
-   [insert_quorum_timeout](#settings-insert_quorum_timeout)

## insert_deduplicate {#settings-insert-deduplicate}

重複除外のブロックを有効または無効にします。 `INSERT` (複製された\*テーブルの場合)。

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

デフォルトでは、ブロックは `INSERT` ステートメントは重複排除されます [データ複製](../../engines/table-engines/mergetree-family/replication.md)).

## deduplicate_blocks_in_dependent_materialized_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

を有効または無効にし、重複排除圧縮をチェックを実現し意見を受け取るデータから複製\*ます。

可能な値:

      0 — Disabled.
      1 — Enabled.

デフォルト値は0です。

使用法

デフォルトでは、重複除外はマテリアライズドビューでは実行されませんが、ソーステーブルの上流で実行されます。
ソーステーブルの重複除外のために挿入されたブロックがスキップされた場合、添付されたマテリアライズドビューには挿入されません。 この動作は、マテリアライズドビューの集計後に挿入されたブロックが同じで、ソーステーブルへの異なる挿入から派生した場合に、高度に集計されたデータ
同時に、この動作 “breaks” `INSERT` べき等性。 もし `INSERT` メインテーブルに成功したと `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won't receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` この動作を変更できます。 再試行すると、マテリアライズドビューは繰り返し挿入を受け取り、重複除外チェックを単独で実行します,
ソーステーブルのチェック結果を無視し、最初の失敗のために失われた行を挿入します。

## max_network_bytes {#settings-max-network-bytes}

クエリの実行時にネットワーク経由で受信または送信されるデータ量(バイト単位)を制限します。 この設定は、個々のクエリごとに適用されます。

可能な値:

-   正の整数。
-   0 — Data volume control is disabled.

デフォルト値は0です。

## max_network_bandwidth {#settings-max-network-bandwidth}

ネットワーク上でのデータ交換の速度を毎秒バイト単位で制限します。 この設定はすべての照会に適用されます。

可能な値:

-   正の整数。
-   0 — Bandwidth control is disabled.

デフォルト値は0です。

## max_network_bandwidth_for_user {#settings-max-network-bandwidth-for-user}

ネットワーク上でのデータ交換の速度を毎秒バイト単位で制限します。 この設定は、単一ユーザーが同時に実行するすべてのクエリに適用されます。

可能な値:

-   正の整数。
-   0 — Control of the data speed is disabled.

デフォルト値は0です。

## max_network_bandwidth_for_all_users {#settings-max-network-bandwidth-for-all-users}

ネットワーク経由でデータが交換される速度を毎秒バイト単位で制限します。 この設定が適用されるのはすべての同時走行に関するお問い合わせます。

可能な値:

-   正の整数。
-   0 — Control of the data speed is disabled.

デフォルト値は0です。

## count_distinct_implementation {#settings-count_distinct_implementation}

これは、 `uniq*` を実行するために使用する必要があります。 [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference.md#agg_function-count) 建設。

可能な値:

-   [uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqhll12)
-   [uniqExact](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqexact)

デフォルト値: `uniqExact`.

## skip_unavailable_shards {#settings-skip_unavailable_shards}

を有効または無効にし静キの不可欠片.

ザ-シャーがある場合にはご利用できないそのすべてのレプリカのためご利用いただけません。 次の場合、レプリカは使用できません:

-   ClickHouseは何らかの理由でレプリカに接続できません。

    レプリカに接続すると、ClickHouseはいくつかの試行を実行します。 すべてこれらの試みが失敗し、レプリカとはできます。

-   レプリカはDNSで解決できません。

    レプリカのホスト名をDNS経由で解決できない場合は、次の状況を示すことがあります:

    -   レプリカのホストにDNSレコードがない。 これは、動的DNSを持つシステムで発生する可能性があります。, [Kubernetes](https://kubernetes.io) ここで、ノードはダウンタイム中に解決できず、これはエラーではありません。

    -   設定エラー。 ClickHouse設定ファイルが含まれて間違ったホスト名.

可能な値:

-   1 — skipping enabled.

    シャードが使用できない場合、ClickHouseは部分的なデータに基づいて結果を返し、ノードの可用性の問題は報告しません。

-   0 — skipping disabled.

    シャードが使用できない場合、ClickHouseは例外をスローします。

デフォルト値は0です。

## optimize_skip_unused_shards {#settings-optimize_skip_unused_shards}

PREWHERE/WHEREにシャーディングキー条件があるSELECTクエリの未使用のシャードのスキップを有効または無効にします(データがシャーディングキーによって配布される

デフォルト値:0

## force_optimize_skip_unused_shards {#settings-force_optimize_skip_unused_shards}

を有効または無効にしクエリの実行の場合 [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) 未使用のシャードを有効にしてスキップすることはできません。 スキップが不可能で、設定が有効になっている場合は例外がスローされます。

可能な値:

-   0-無効(スローしない)
-   1-除クエリの実行の場合のみ表はshardingキー
-   2-を無効にクエリの実行に関わらずshardingキー定義のテーブル

デフォルト値:0

## optimize_throw_if_noop {#setting-optimize_throw_if_noop}

例外のスローを有効または無効にします。 [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) クエリがマージを実行しませんでした。

既定では, `OPTIMIZE` 何もしなくても正常に戻ります。 この設定を使用すると、これらの状況を区別し、例外メッセージで理由を取得できます。

可能な値:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

デフォルト値は0です。

## ディストリビューター {#settings-distributed_replica_error_half_life}

-   タイプ:秒
-   デフォルト値:60秒

分散テーブルのエラーをゼロにする速度を制御します。 レプリカがしばらく使用できず、5つのエラーが蓄積され、distributed_replica_error_half_lifeが1秒に設定されている場合、レプリカは最後のエラーの3秒後に正常と見なされま

も参照。:

-   [分散テーブルエンジン](../../engines/table-engines/special/distributed.md)
-   [ディストリビューター](#settings-distributed_replica_error_cap)

## ディストリビューター {#settings-distributed_replica_error_cap}

-   型:unsigned int
-   デフォルト値:1000

各レプリカのエラ

も参照。:

-   [分散テーブルエンジン](../../engines/table-engines/special/distributed.md)
-   [ディストリビューター](#settings-distributed_replica_error_half_life)

## distributed_directory_monitor_sleep_time_ms {#distributed_directory_monitor_sleep_time_ms}

の基本区間 [分散](../../engines/table-engines/special/distributed.md) データを送信する表エンジン。 実際の間隔は、エラーが発生した場合に指数関数的に増加します。

可能な値:

-   ミリ秒の正の整数。

既定値は100ミリ秒です。

## distributed_directory_monitor_max_sleep_time_ms {#distributed_directory_monitor_max_sleep_time_ms}

の最大間隔 [分散](../../engines/table-engines/special/distributed.md) データを送信する表エンジン。 の区間の指数関数的成長を制限する。 [distributed_directory_monitor_sleep_time_ms](#distributed_directory_monitor_sleep_time_ms) 設定。

可能な値:

-   ミリ秒の正の整数。

デフォルト値:30000ミリ秒(30秒)。

## distributed_directory_monitor_batch_inserts {#distributed_directory_monitor_batch_inserts}

挿入されたデータのバッチ送信を有効または無効にします。

バッチ送信が有効になっている場合、 [分散](../../engines/table-engines/special/distributed.md) テーブルエンジンをお送り複数のファイルの挿入データを移動するようになっていますの代わりに送信します。 一括送信の改善にクラスターの性能をより活用してサーバやネットワーク資源です。

可能な値:

-   1 — Enabled.
-   0 — Disabled.

デフォルト値は0です。

## os_thread_priority {#setting-os-thread-priority}

優先度を設定します ([ニース](https://en.wikipedia.org/wiki/Nice_(Unix)))クエリを実行するスレッドの場合。 OSスケジューラは、使用可能な各CPUコアで実行する次のスレッドを選択する際に、この優先順位を考慮します。

!!! warning "警告"
    この設定を使用するには、 `CAP_SYS_NICE` 能力。 その `clickhouse-server` パッケ いくつかの仮想環境では、 `CAP_SYS_NICE` 能力。 この場合, `clickhouse-server` 開始時にそれに関するメッセージを表示します。

可能な値:

-   範囲の値を設定できます `[-20, 19]`.

値が低いほど優先度が高くなります。 低いの糸 `nice` 優先度の値は、高い値のスレッドよりも頻繁に実行されます。 長時間実行される非対話型クエリでは、短い対話型クエリが到着したときにリソースをすばやく放棄できるため、高い値が望ましいです。

デフォルト値は0です。

## query_profiler_real_time_period_ns {#query_profiler_real_time_period_ns}

の実クロックタイマーの期間を設定します。 [クエリプロファイラ](../../operations/optimizing-performance/sampling-query-profiler.md). リアルクロックタイマーカウント壁掛時計。

可能な値:

-   ナノ秒単位の正の整数です。

    推奨値:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   タイマーをオフにする場合は0。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

デフォルト値:1000000000ナノ秒(秒)。

も参照。:

-   システム表 [trace_log](../../operations/system-tables.md#system_tables-trace_log)

## query_profiler_cpu_time_period_ns {#query_profiler_cpu_time_period_ns}

のCPUクロックタイマーの期間を設定します。 [クエリプロファイラ](../../operations/optimizing-performance/sampling-query-profiler.md). このタイマーカウントのみのCPU時間。

可能な値:

-   ナノ秒の正の整数。

    推奨値:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   タイマーをオフにする場合は0。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

デフォルト値:1000000000ナノ秒。

も参照。:

-   システム表 [trace_log](../../operations/system-tables.md#system_tables-trace_log)

## allow_introspection_functions {#settings-allow_introspection_functions}

ディスエーブルの有効 [イントロスペクション関数](../../sql-reference/functions/introspection.md) クエリプロファイル用。

可能な値:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

デフォルト値は0です。

**も参照。**

-   [サンプリングクロファイラ](../optimizing-performance/sampling-query-profiler.md)
-   システム表 [trace_log](../../operations/system-tables.md#system_tables-trace_log)

## input_format_parallel_parsing {#input-format-parallel-parsing}

-   タイプ:bool
-   既定値:True

データ形式の順序保持並列解析を有効にします。 TSV、TKSV、CSVおよびJSONEachRow形式でのみサポートされます。

## min_chunk_bytes_for_parallel_parsing {#min-chunk-bytes-for-parallel-parsing}

-   型:unsigned int
-   デフォルト値:1MiB

各スレッドが並列に解析する最小チャンクサイズをバイト単位で表します。

## output_format_avro_codec {#settings-output_format_avro_codec}

出力Avroファイルに使用する圧縮コーデックを設定します。

タイプ:文字列

可能な値:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [スナッピー](https://google.github.io/snappy/)

デフォルト値: `snappy` （利用可能な場合）または `deflate`.

## output_format_avro_sync_interval {#settings-output_format_avro_sync_interval}

出力Avroファイルの同期マーカー間の最小データサイズ(バイト単位)を設定します。

型:unsigned int

使用可能な値:32(32バイト)-1073741824(1GiB)

デフォルト値:32768(32KiB)

## format_avro_schema_registry_url {#settings-format_avro_schema_registry_url}

使用するConfluentスキーマレジストリURLを設定します [アブロコンフルエント](../../interfaces/formats.md#data-format-avro-confluent) 形式

タイプ:URL

既定値:空

## background_pool_size {#background_pool_size}

セットのスレッド数を行う背景事業のテーブルエンジン（例えば、合併に [MergeTreeエンジン](../../engines/table-engines/mergetree-family/index.md) テーブル）。 この設定はClickHouseサーバーの起動時に適用され、ユーザーセッションでは変更できません。 この設定を調整することで、CPUとディスクの負荷を管理します。 小さなプールサイズを以下のCPUやディスクの資源が背景のプロセスの事前の遅れが影響をクエリす。

可能な値:

-   任意の正の整数。

デフォルト値は16です。

[元の記事](https://clickhouse.com/docs/en/operations/settings/settings/) <!-- hide -->

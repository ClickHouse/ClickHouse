---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 60
toc_title: "\u8A2D\u5B9A"
---

# 設定 {#settings}

## distributed\_product\_mode {#distributed-product-mode}

の動作を変更します。 [分散サブクエリ](../../sql-reference/statements/select.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

制限:

-   INおよびJOINサブクエリにのみ適用されます。
-   FROMセクションが複数のシャードを含む分散テーブルを使用する場合のみ。
-   サブクエリが複数のシャードを含む分散テーブルに関係する場合。
-   テーブル値には使用されません [リモート](../../sql-reference/table-functions/remote.md) 機能。

可能な値:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” 例外）。
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` クエリと `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable\_optimize\_predicate\_expression {#enable-optimize-predicate-expression}

述語プッシュダウンをオンにする `SELECT` クエリ。

プレディケートプッシュダウ場合を大幅に削減ネットワーク通信のために配布します。

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

使い方

次のクエリを検討します:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

もし `enable_optimize_predicate_expression = 1` ClickHouseが適用されるため、これらのクエリの実行時間は等しくなります `WHERE` それを処理するときにサブクエリに。

もし `enable_optimize_predicate_expression = 0` その後、第二のクエリの実行時間がはるかに長いです。 `WHERE` サブクエリが終了した後、すべてのデータに句が適用されます。

## fallback\_to\_stale\_replicas\_for\_distributed\_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

更新されたデータが利用できない場合、クエリを古いレプリカに強制的に適用します。 見る [複製](../../engines/table-engines/mergetree-family/replication.md).

ClickHouseは、テーブルの古いレプリカから最も関連性の高いものを選択します。

実行するときに使用 `SELECT` レプリケートされたテーブルを指す分散テーブルから

デフォルトでは、1(有効)。

## force\_index\_by\_date {#settings-force_index_by_date}

インデックスを日付で使用できない場合は、クエリの実行を無効にします。

MergeTreeファミリーのテーブルで動作します。

もし `force_index_by_date=1` ClickHouseは、データ範囲の制限に使用できる日付キー条件がクエリにあるかどうかをチェックします。 適切な条件がない場合は、例外がスローされます。 ただし、読み取るデータの量が条件によって減少するかどうかはチェックされません。 たとえば、条件 `Date != ' 2000-01-01 '` テーブル内のすべてのデータに一致する場合でも許容されます（つまり、クエリを実行するにはフルスキャンが必要です）。 MergeTreeテーブルのデータ範囲の詳細については、次を参照してください [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force\_primary\_key {#force-primary-key}

オクエリの実行が指数付けにより、主キーはできません。

MergeTreeファミリーのテーブルで動作します。

もし `force_primary_key=1` ClickHouseは、データ範囲の制限に使用できる主キー条件がクエリにあるかどうかを確認します。 適切な条件がない場合は、例外がスローされます。 ただし、読み取るデータの量が条件によって減少するかどうかはチェックされません。 MergeTreeテーブルのデータ範囲の詳細については、 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## format\_schema {#format-schema}

このパラメーターは、次のようなスキーマ定義を必要とする形式を使用する場合に便利です [Cap’n Proto](https://capnproto.org/) または [Protobuf](https://developers.google.com/protocol-buffers/). 値は形式によって異なります。

## fsync\_metadata {#fsync-metadata}

有効または無効 [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) 書くとき `.sql` ファイル 既定で有効になっています。

ことは、あってはならないことで無効にすることもできれば、サーバは、数百万の小さなテーブルが続々と生まれてくると破壊されました。

## enable\_http\_compression {#settings-enable_http_compression}

HTTP要求に対する応答のデータ圧縮を有効または無効にします。

詳細については、 [HTTPインタフェースの説明](../../interfaces/http.md).

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:0.

## http\_zlib\_compression\_level {#settings-http_zlib_compression_level}

HTTP要求に対する応答のデータ圧縮レベルを次の場合に設定します [enable\_http\_compression=1](#settings-enable_http_compression).

可能な値：1から9までの数字。

デフォルト値:3.

## http\_native\_compression\_disable\_checksumming\_on\_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

クライアントからのhttp postデータの解凍時にチェックサム検証を有効または無効にします。 にのみ使用clickhouseネイティブの圧縮フォーマット（使用されません `gzip` または `deflate`).

詳細については、 [HTTPインタフェースの説明](../../interfaces/http.md).

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:0.

## send\_progress\_in\_http\_headers {#settings-send_progress_in_http_headers}

有効または無効 `X-ClickHouse-Progress` HTTP応答ヘッダー `clickhouse-server` 応答。

詳細については、 [HTTPインタフェースの説明](../../interfaces/http.md).

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:0.

## max\_http\_get\_redirects {#setting-max_http_get_redirects}

HTTP GETリダイレクトホップの最大数を制限する [URL](../../engines/table-engines/special/url.md)-エンジンテーブル。 この設定は、両方のタイプのテーブルに適用されます。 [CREATE TABLE](../../query_language/create/#create-table-query) クエリとによって [url](../../sql-reference/table-functions/url.md) テーブル機能。

可能な値:

-   任意の正の整数のホップ数。
-   0 — No hops allowed.

デフォルト値:0.

## input\_format\_allow\_errors\_num {#settings-input_format_allow_errors_num}

テキスト形式（csv、tsvなど）から読み取るときに許容されるエラーの最大数を設定します。).

デフォルト値は0です。

常にペアそれと `input_format_allow_errors_ratio`.

行の読み取り中にエラーが発生したが、エラーカウンタがまだ小さい場合 `input_format_allow_errors_num`、ClickHouseは、行を無視して、次のいずれかに移動します。

両方の場合 `input_format_allow_errors_num` と `input_format_allow_errors_ratio` 超過すると、ClickHouseは例外をスローします。

## input\_format\_allow\_errors\_ratio {#settings-input_format_allow_errors_ratio}

テキスト形式（csv、tsvなど）から読み取るときに許可されるエラーの最大パーセントを設定します。).
エラーの割合は、0～1の間の浮動小数点数として設定されます。

デフォルト値は0です。

常にペアそれと `input_format_allow_errors_num`.

行の読み取り中にエラーが発生したが、エラーカウンタがまだ小さい場合 `input_format_allow_errors_ratio`、ClickHouseは、行を無視して、次のいずれかに移動します。

両方の場合 `input_format_allow_errors_num` と `input_format_allow_errors_ratio` 超過すると、ClickHouseは例外をスローします。

## input\_format\_values\_interpret\_expressions {#settings-input_format_values_interpret_expressions}

を有効または無効にしのsqlのパーサの場合の高速ストリームのパーサで構文解析のデータです。 この設定は、 [値](../../interfaces/formats.md#data-format-values) データ挿入時のフォーマット。 構文の解析の詳細については、以下を参照してください [構文](../../sql-reference/syntax.md) セクション。

可能な値:

-   0 — Disabled.

    この場合、提供しなければなりません形式のデータです。 を見る [形式](../../interfaces/formats.md) セクション。

-   1 — Enabled.

    この場合、sql式を値として使用できますが、データの挿入はこの方法ではるかに遅くなります。 書式設定されたデータのみを挿入する場合、clickhouseは設定値が0であるかのように動作します。

デフォルト値:1。

使用例

を挿入 [DateTime](../../sql-reference/data-types/datetime.md) 異なる設定で値を入力します。

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

最後のクエリは次のクエリと同じです:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## input\_format\_values\_deduce\_templates\_of\_expressions {#settings-input_format_values_deduce_templates_of_expressions}

Sql式のテンプレート控除を有効または無効にします。 [値](../../interfaces/formats.md#data-format-values) フォーマット。 これにより、式の解析と解釈が可能になります。 `Values` 連続する行の式が同じ構造を持つ場合、はるかに高速です。 ClickHouseは、式のテンプレートを推測し、このテンプレートを使用して次の行を解析し、正常に解析された行のバッチで式を評価しようとします。 次のクエリの場合:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   もし `input_format_values_interpret_expressions=1` と `format_values_deduce_templates_of_expressions=0` 式は行ごとに別々に解釈されます（これは、多数の行では非常に遅いです)
-   もし `input_format_values_interpret_expressions=0` と `format_values_deduce_templates_of_expressions=1` 最初、二番目、および三番目の行の式は、テンプレートを使用して解析されます `lower(String)` 一緒に解釈されると、式はforth行が別のテンプレートで解析されます (`upper(String)`)
-   もし `input_format_values_interpret_expressions=1` と `format_values_deduce_templates_of_expressions=1` -前の場合と同じですが、テンプレートを推論することができない場合は、式を別々に解釈することもできます。

既定で有効になっています。

## input\_format\_values\_accurate\_types\_of\_literals {#settings-input-format-values-accurate-types-of-literals}

この設定は次の場合にのみ使用されます `input_format_values_deduce_templates_of_expressions = 1`. いくつかの列の式は同じ構造を持ちますが、異なる型の数値リテラルを含んでいます

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

この設定を有効にすると、clickhouseは実際のリテラルの型をチェックし、対応する型の式テンプレートを使用します。 場合によっては、式の評価が大幅に遅くなることがあります。 `Values`.
When disabled, ClickHouse may use more general type for some literals (e.g. `Float64` または `Int64` 代わりに `UInt64` のために `42`が、その原因となりオーバーフローおよび精度の問題です。
既定で有効になっています。

## input\_format\_defaults\_for\_omitted\_fields {#session_settings-input_format_defaults_for_omitted_fields}

実行するとき `INSERT` クエリでは、省略された入力列の値をそれぞれの列の既定値に置き換えます。 このオプションは [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) と [タブ区切り](../../interfaces/formats.md#tabseparated) フォーマット。

!!! note "メモ"
    このオプショ それはサーバーの付加的な計算資源を消費し、性能を減らすことができる。

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

## input\_format\_tsv\_empty\_as\_default {#settings-input-format-tsv-empty-as-default}

有効にすると、tsvの空の入力フィールドを既定値に置き換えます。 複雑な既定の式の場合 `input_format_defaults_for_omitted_fields` 有効にする必要があります。

デフォルトでは無効です。

## input\_format\_null\_as\_default {#settings-input-format-null-as-default}

入力デー `NULL` しかし、対応する列のデータ型は `Nullable(T)` (テキスト入力形式の場合)。

## input\_format\_skip\_unknown\_fields {#settings-input-format-skip-unknown-fields}

追加データの挿入のスキップを有効または無効にします。

書き込みデータclickhouseが例外をスローした場合入力データを含むカラム特別な権限は必要ありません使用します。 スキップが有効になっている場合、clickhouseは余分なデータを挿入せず、例外もスローしません。

対応フォーマット:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [Csvwithnamesname](../../interfaces/formats.md#csvwithnames)
-   [Tabseparatedwithnamesname](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:0.

## input\_format\_import\_nested\_json {#settings-input_format_import_nested_json}

を有効または無効にし、挿入のjsonデータをネストしたオブジェクト。

対応フォーマット:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:0.

また見なさい:

-   [入れ子構造の使用法](../../interfaces/formats.md#jsoneachrow-nested) と `JSONEachRow` フォーマット。

## input\_format\_with\_names\_use\_header {#settings-input-format-with-names-use-header}

データ挿入時に列の順序の確認を有効または無効にします。

挿入パフォーマンスを向上させるには、入力データの列の順序がターゲットテーブルと同じであることが確実な場合は、このチェックを無効にすることを

対応フォーマット:

-   [Csvwithnamesname](../../interfaces/formats.md#csvwithnames)
-   [Tabseparatedwithnamesname](../../interfaces/formats.md#tabseparatedwithnames)

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

## date\_time\_input\_format {#settings-date_time_input_format}

日付と時刻のテキスト表現のパーサーを選択できます。

この設定は、以下には適用されません [日付と時刻の関数](../../sql-reference/functions/date-time-functions.md).

可能な値:

-   `'best_effort'` — Enables extended parsing.

    ClickHouseは基本を解析することができます `YYYY-MM-DD HH:MM:SS` 形式とすべて [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) 日付と時刻の形式。 例えば, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouseは基本のみを解析できます `YYYY-MM-DD HH:MM:SS` フォーマット。 例えば, `'2019-08-20 10:18:56'`.

デフォルト値: `'basic'`.

また見なさい:

-   [DateTimeデータ型。](../../sql-reference/data-types/datetime.md)
-   [日付と時刻を操作するための関数。](../../sql-reference/functions/date-time-functions.md)

## join\_default\_strictness {#settings-join_default_strictness}

デフォルトの厳密さを [結合句](../../sql-reference/statements/select.md#select-join).

可能な値:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [デカルト積](https://en.wikipedia.org/wiki/Cartesian_product) 一致する行から。 これは正常です `JOIN` 標準SQLからの動作。
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` と `ALL` 同じです。
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` または `ANY` クエリで指定されていない場合、ClickHouseは例外をスローします。

デフォルト値: `ALL`.

## join\_any\_take\_last\_row {#settings-join_any_take_last_row}

Join操作の動作を次のもので変更する `ANY` 厳密さ

!!! warning "注意"
    この設定は、 `JOIN` との操作 [参加](../../engines/table-engines/special/join.md) エンジンテーブル。

可能な値:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

デフォルト値:0.

また見なさい:

-   [JOIN句](../../sql-reference/statements/select.md#select-join)
-   [結合テーブルエンジン](../../engines/table-engines/special/join.md)
-   [join\_default\_strictness](#settings-join_default_strictness)

## join\_use\_nulls {#join_use_nulls}

のタイプを設定します。 [JOIN](../../sql-reference/statements/select.md) 行動。 際融合のテーブル、空細胞が表示される場合があります。 ClickHouseは、この設定に基づいて異なる塗りつぶします。

可能な値:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` 標準SQLと同じように動作します。 対応するフィールドの型は次のように変換されます [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable) 空のセルは [NULL](../../sql-reference/syntax.md).

デフォルト値:0.

## max\_block\_size {#setting-max_block_size}

ClickHouseでは、データはブロック（列部分のセット）によって処理されます。 単一のブロックの内部処理サイクルは十分に効率的ですが、各ブロックに顕著な支出があります。 その `max_block_size` 設定は、テーブルからロードするブロックのサイズ（行数）の推奨値です。 ブロックサイズが小さすぎないようにして、各ブロックの支出はまだ目立つが、最初のブロックが迅速に処理された後に完了する制限付きのクエ 目標は、複数のスレッドで多数の列を抽出するときに大量のメモリを消費しないようにし、少なくともいくつかのキャッシュの局所性を維持する

デフォルト値:65,536.

ブロックのサイズ `max_block_size` ていないから読み込まれます。 少ないデータを取得する必要があることが明らかであれば、小さいブロックが処理されます。

## preferred\_block\_size\_bytes {#preferred-block-size-bytes}

同じ目的のために使用される `max_block_size` しかし、ブロック内の行数に適応させることによって、推奨されるブロックサイズをバイト単位で設定します。
ただし、ブロックサイズは `max_block_size` 行。
デフォルト:1,000,000。 mergetreeエンジンから読み取る場合にのみ機能します。

## merge\_tree\_min\_rows\_for\_concurrent\_read {#setting-merge-tree-min-rows-for-concurrent-read}

Aのファイルから読み込まれる行の数 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルを超え `merge_tree_min_rows_for_concurrent_read` その後ClickHouseしようとして行な兼職の状況からの読み出しこのファイルに複数のスレッド）。

可能な値:

-   任意の正の整数。

デフォルト値:163840.

## merge\_tree\_min\_bytes\_for\_concurrent\_read {#setting-merge-tree-min-bytes-for-concurrent-read}

ファイルから読み込むバイト数 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-エンジンテーブル超え `merge_tree_min_bytes_for_concurrent_read` そのClickHouseを同時に読みこのファイルから複数のスレッド）。

可能な値:

-   任意の正の整数。

デフォルト値:251658240.

## merge\_tree\_min\_rows\_for\_seek {#setting-merge-tree-min-rows-for-seek}

ファイル内で読み込まれる二つのデータブロック間の距離がより小さい場合 `merge_tree_min_rows_for_seek` その後、ClickHouseはファイルをシークしませんが、データを順次読み取ります。

可能な値:

-   任意の正の整数。

デフォルト値:0.

## merge\_tree\_min\_bytes\_for\_seek {#setting-merge-tree-min-bytes-for-seek}

ファイル内で読み込まれる二つのデータブロック間の距離がより小さい場合 `merge_tree_min_bytes_for_seek` その後、ClickHouseは両方のブロックを含むファイルの範囲を順次読み取り、余分なシークを避けます。

可能な値:

-   任意の正の整数。

デフォルト値:0.

## merge\_tree\_coarse\_index\_granularitycomment {#setting-merge-tree-coarse-index-granularity}

する場合のデータclickhouseチェックのデータにファイルです。 まclickhouseが必要なキーの一部の範囲、とりわけこの範囲を `merge_tree_coarse_index_granularity` 必要なキーを再帰的に検索します。

可能な値:

-   任意の正の偶数の整数。

デフォルト値:8.

## merge\_tree\_max\_rows\_to\_use\_cache {#setting-merge-tree-max-rows-to-use-cache}

ClickHouseはより多くを読むべきであれば `merge_tree_max_rows_to_use_cache` あるクエリの行では、圧縮されていないブロックのキャッシュは使用されません。

のキャッシュされた、圧縮解除されたブロックの店舗データを抽出したためます。 clickhouseこのキャッシュの高速化対応小の繰り返します。 この設定は、大量のデータを読み取るクエリによってキャッシュが破棄されるのを防ぎます。 その [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) サーバー設定は、非圧縮ブロックのキャッシュのサイズを定義します。

可能な値:

-   任意の正の整数。

Default value: 128 ✕ 8192.

## merge\_tree\_max\_bytes\_to\_use\_cache {#setting-merge-tree-max-bytes-to-use-cache}

ClickHouseはより多くを読むべきであれば `merge_tree_max_bytes_to_use_cache` バイトあるクエリでは、圧縮されていないブロックのキャッシュは使用されません。

のキャッシュされた、圧縮解除されたブロックの店舗データを抽出したためます。 clickhouseこのキャッシュの高速化対応小の繰り返します。 この設定は、大量のデータを読み取るクエリによってキャッシュが破棄されるのを防ぎます。 その [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) サーバー設定は、非圧縮ブロックのキャッシュのサイズを定義します。

可能な値:

-   任意の正の整数。

デフォルト値:2013265920.

## min\_bytes\_to\_use\_direct\_io {#settings-min-bytes-to-use-direct-io}

記憶域ディスクへの直接i/oアクセスを使用するために必要な最小データ量。

ClickHouseこの設定からデータを読み込むときます。 読み取られるすべてのデータの合計ストレージボリュームが `min_bytes_to_use_direct_io` ディスクからデータを読み取ります。 `O_DIRECT` オプション。

可能な値:

-   0 — Direct I/O is disabled.
-   正の整数。

デフォルト値:0.

## log\_queries {#settings-log-queries}

クエリログの設定。

この設定でclickhouseに送信されたクエリは、次のルールに従ってログに記録されます。 [クエリーログ](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log) サーバー構成パラメータ。

例えば:

``` text
log_queries=1
```

## log\_query\_threads {#settings-log-query-threads}

クエリスレッドログの設定。

この設定でclickhouseによって実行されたクエリのスレッドは、以下のルールに従ってログに記録されます [query\_thread\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) サーバー構成パラメータ。

例えば:

``` text
log_query_threads=1
```

## max\_insert\_block\_size {#settings-max_insert_block_size}

テーブルに挿入するために形成するブロックのサイズ。
この設定は、サーバーがブロックを形成する場合にのみ適用されます。
たとえば、httpインターフェイスを介した挿入の場合、サーバーはデータ形式を解析し、指定されたサイズのブロックを形成します。
しかし、clickhouse-clientを使用すると、クライアントはデータ自体を解析し、 ‘max\_insert\_block\_size’ サーバー上の設定は、挿入されたブロックのサイズには影響しません。
この設定は、select後に形成されるのと同じブロックを使用してデータが挿入されるため、insert selectを使用する場合にも目的がありません。

デフォルト値:1,048,576.

デフォルトは、 `max_block_size`. この理由は、特定のテーブルエンジン (`*MergeTree`）挿入された各ブロックのディスク上にデータ部分を形成する。 同様に, `*MergeTree` テーブルデータを並べ替え時の挿入やるのに十分な大きさのブロックサイズを選別データにアプリです。

## max\_replica\_delay\_for\_distributed\_queries {#settings-max_replica_delay_for_distributed_queries}

分散クエリの遅延レプリカを無効にします。 見る [複製](../../engines/table-engines/mergetree-family/replication.md).

時間を秒単位で設定します。 レプリカが設定値よりも遅れている場合、このレプリカは使用されません。

デフォルト値:300.

実行するときに使用 `SELECT` レプリケートされたテーブルを指す分散テーブルから

## max\_threads {#settings-max_threads}

最大の問合せ処理のスレッドを除き、スレッドの取得のためのデータからリモートサーバーの ‘max\_distributed\_connections’ パラメータ）。

このパラメータに適用されるスレッドは、それらのスレッドが同じ段階での問合せ処理パイプライン。
たとえば、テーブルから読み取るときに、関数を使用して式を評価できる場合は、whereとfilterを使用し、少なくともusingを使用してgroup byを並列に事前集計しま ‘max\_threads’ その後、スレッドの数 ‘max\_threads’ 使用されます。

デフォルト値:物理cpuコアの数。

通常、一度にサーバーで実行されるselectクエリが少ない場合は、このパラメーターを実際のプロセッサコア数より少し小さい値に設定します。

制限があるためすぐに完了するクエリの場合は、以下を設定できます ‘max\_threads’. たとえば、必要な数のエントリがすべてのブロックにあり、max\_threads=8の場合、8つのブロックが取得されますが、読み込むだけで十分です。

小さい `max_threads` 値は、消費されるメモリ量が少ない。

## max\_insert\_threads {#settings-max-insert-threads}

実行するスレッドの最大数 `INSERT SELECT` クエリ。

可能な値:

-   0 (or 1) — `INSERT SELECT` 並列実行なし。
-   正の整数。 1より大きい。

デフォルト値:0.

並列 `INSERT SELECT` のみ有効です。 `SELECT` パートは並列に実行されます。 [max\_threads](#settings-max_threads) 設定。
値を大きくすると、メモリ使用量が増加します。

## max\_compress\_block\_size {#max-compress-block-size}

テーブルへの書き込み用に圧縮する前の非圧縮データのブロックの最大サイズ。 デフォルトでは、1,048,576(1mib)。 サイズが縮小されると、圧縮率が大幅に低下し、キャッシュの局所性のために圧縮および圧縮解凍速度がわずかに増加し、メモリ消費が減少する。 通常、この設定を変更する理由はありません。

圧縮のためのブロック（バイトからなるメモリの塊）とクエリ処理のためのブロック（テーブルからの行のセット）を混同しないでください。

## min\_compress\_block\_size {#min-compress-block-size}

のために [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)"テーブル。 削減のため、遅延が処理クエリーのブロックの圧縮を書くとき、次のマークがそのサイズは少なくとも ‘min\_compress\_block\_size’. デフォルトでは、65,536。

圧縮されていないデータが以下の場合、ブロックの実際のサイズ ‘max\_compress\_block\_size’、この値よりも小さく、一つのマークのためのデータの量よりも小さくありません。

例を見よう。 それを仮定する ‘index\_granularity’ テーブルの作成中に8192に設定されました。

私たちはuint32型の列（値あたり4バイト）を書いています。 8192行を書き込む場合、合計は32kbのデータになります。 min\_compress\_block\_size=65,536以降、圧縮ブロックは二つのマークごとに形成されます。

私たちは、文字列型（値あたり60バイトの平均サイズ）でurl列を書いています。 8192行を書き込む場合、平均はデータの500kbよりわずかに小さくなります。 これは65,536以上であるため、各マークに圧縮ブロックが形成されます。 この場合、ディスクからシングルマークの範囲でデータを読み取るとき、余分なデータは解凍されません。

通常、この設定を変更する理由はありません。

## max\_query\_size {#settings-max_query_size}

SQLパーサーで解析するためにRAMに取り込むことができるクエリの最大部分。
INSERTクエリには、この制限に含まれていない別のストリームパーサー（O(1)RAMを消費する）によって処理されるINSERTのデータも含まれています。

デフォルト値:256kib.

## interactive\_delay {#interactive-delay}

区間マイクロ秒単位で確認を行うための要求実行中止となり、送信を行います。

デフォルト値:100,000(キャンセルのチェックを行い、進捗を毎秒十回送信します)。

## connect\_timeout,receive\_timeout,send\_timeout {#connect-timeout-receive-timeout-send-timeout}

クライアントとの通信に使用されるソケットの秒単位のタイムアウト。

デフォルト値:10、300、300。

## cancel\_http\_readonly\_queries\_on\_client\_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

デフォルト値:0

## poll\_interval {#poll-interval}

指定された秒数の待機ループでロックします。

デフォルト値:10.

## max\_distributed\_connections {#max-distributed-connections}

単一の分散テーブルへの単一クエリの分散処理のためのリモートサーバーとの同時接続の最大数。 クラスター内のサーバー数以上の値を設定することをお勧めします。

デフォルト値:1024。

次のパラメータは、分散テーブルを作成するとき（およびサーバーを起動するとき）にのみ使用されるため、実行時に変更する必要はありません。

## distributed\_connections\_pool\_size {#distributed-connections-pool-size}

単一の分散テーブルへのすべてのクエリの分散処理のためのリモートサーバーとの同時接続の最大数。 クラスター内のサーバー数以上の値を設定することをお勧めします。

デフォルト値:1024。

## connect\_timeout\_with\_failover\_ms {#connect-timeout-with-failover-ms}

分散テーブルエンジンのリモートサーバーに接続するためのタイムアウト(ミリ秒)。 ‘shard’ と ‘replica’ セクションはクラスター定義で使用されます。
失敗した場合は、さまざまなレプリカへの接続を何度か試行します。

デフォルト値:50。

## connections\_with\_failover\_max\_tries {#connections-with-failover-max-tries}

分散テーブルエンジンの各レプリカでの接続試行の最大数。

デフォルト値:3.

## 極端な {#extremes}

極値(クエリ結果の列の最小値と最大値)をカウントするかどうか。 0または1を受け入れます。 デフォルトでは、0(無効)。
詳細については、以下を参照してください “Extreme values”.

## use\_uncompressed\_cache {#setting-use_uncompressed_cache}

非圧縮ブロックのキャッシュを使用するかどうか。 0または1を受け入れます。 デフォルトでは、0(無効)。
圧縮されていないキャッシュ(mergetreeファミリーのテーブルのみ)を使用すると、多数の短いクエリを処理するときに待ち時間が大幅に短縮され、スループット この設定を有効にユーザーに送信頻繁に短います。 また、に注意を払う [uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

少なくとも大量のデータ（百万行以上）を読み取るクエリの場合、圧縮されていないキャッシュは自動的に無効になり、本当に小さなクエリの容量を節 これは保つことができることを意味する ‘use\_uncompressed\_cache’ 常に1に設定します。

## replace\_running\_query {#replace-running-query}

HTTPインターフェイスを使用する場合、 ‘query\_id’ 変数は渡すことができます。 これは、クエリ識別子として機能する任意の文字列です。
同じユーザーからのクエリが同じ場合 ‘query\_id’ この時点で既に存在している場合、その動作は ‘replace\_running\_query’ パラメータ。

`0` (default) – Throw an exception (don’t allow the query to run if a query with the same ‘query\_id’ すでに実行されている）。

`1` – Cancel the old query and start running the new one.

Yandexの。Metricaこのパラメータセットが1の実施のための提案のための分割ます。 次の文字を入力した後、古いクエリがまだ完了していない場合は、キャンセルする必要があります。

## stream\_flush\_interval\_ms {#stream-flush-interval-ms}

作品のテーブルストリーミングの場合はタイムアウトした場合、またはスレッドを生成す [max\_insert\_block\_size](#settings-max_insert_block_size) 行。

デフォルト値は7500です。

値が小さいほど、データはテーブルにフラッシュされる頻度が高くなります。 値が低すぎると、パフォーマンスが低下します。

## load\_balancing {#settings-load_balancing}

分散クエリ処理に使用するレプリカ選択のアルゴリズムを指定します。

ClickHouse対応し、以下のようなアルゴリズムの選択のレプリカ:

-   [ランダム](#load_balancing-random) （デフォルトでは)
-   [最寄りのホスト名](#load_balancing-nearest_hostname)
-   [順番に](#load_balancing-in_order)
-   [最初またはランダム](#load_balancing-first_or_random)

### ランダム(デフォルト) {#load_balancing-random}

``` sql
load_balancing = random
```

エラーの数は、各レプリカに対して数えられます。 クエリは、エラーが最も少なくレプリカに送信され、エラーがいくつかある場合は、そのうちの誰にも送信されます。
レプリカに異なるデータがある場合は、異なるデータも取得します。

### 最寄りのホスト名 {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server’s hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

たとえば、example01-01-1とexample01-01-2.yandex.ru example01-01-1とexample01-02-2は二つの場所で異なりますが、一つの位置では異なります。
この方法はプリミティブに思えるかもしれませんが、ネットワークトポロジに関する外部データを必要とせず、ipv6アドレスでは複雑なipアドレスを

したがって、同等の複製がある場合は、名前によって最も近い複製が優先されます。
また、同じサーバーにクエリを送信するときに、障害がなければ、分散クエリも同じサーバーに移動すると想定することもできます。 したがって、レプリカに異なるデータが配置されていても、クエリはほとんど同じ結果を返します。

### 順番に {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

エラーの数が同じレプリカは、構成で指定されているのと同じ順序でアクセスされます。
この方法は適切なが分かっている場合は、正確にレプリカが好ましい。

### 最初またはランダム {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

このアルゴリズムは、セット内の最初のレプリカを選択します。 この効果のクロス-複製をトポロジーのセットアップ、ものなどが可能です。-

その `first_or_random` アルゴリズムはの問題を解決します `in_order` アルゴリズムだ と `in_order` あるレプリカがダウンした場合、次のレプリカは二重の負荷を受け、残りのレプリカは通常のトラフィック量を処理します。 を使用する場合 `first_or_random` アルゴリズムは、負荷が均等にまだ利用可能なレプリカ間で分散されています。

## prefer\_localhost\_replica {#settings-prefer-localhost-replica}

を有効/無効にしが好ましいのlocalhostレプリカ処理時に分布します。

可能な値:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [load\_balancing](#settings-load_balancing) 設定。

デフォルト値:1。

!!! warning "警告"
    使用する場合は、この設定を無効にします [max\_parallel\_replicas](#settings-max_parallel_replicas).

## totals\_mode {#totals-mode}

HAVINGが存在する場合の合計を計算する方法と、max\_rows\_to\_group\_byとgroup\_by\_overflow\_mode= ‘any’ 存在する。
セクションを見る “WITH TOTALS modifier”.

## totals\_auto\_threshold {#totals-auto-threshold}

のしきい値 `totals_mode = 'auto'`.
セクションを見る “WITH TOTALS modifier”.

## max\_parallel\_replicas {#settings-max_parallel_replicas}

クエリを実行するときの各シャードのレプリカの最大数。
一貫性（同じデータ分割の異なる部分を取得する）の場合、このオプションはサンプリングキーが設定されている場合にのみ機能します。
レプリカの遅延は制御されません。

## コンパイル {#compile}

を編集ます。 デフォルトでは、0(無効)。

コンパイルは、クエリ処理パイプラインの一部にのみ使用されます。
この部分のパイプラインのためのクエリを実行するアによる展開の短サイクルinlining集計機能。 複数の単純な集計関数を使用するクエリでは、パフォーマンスの最大向上(まれに、最大で四倍高速)が見られます。 通常、パフォーマンスの向上はわずかです。 まれに、クエリの実行が遅くなることがあります。

## min\_count\_to\_compile {#min-count-to-compile}

コンパイルを実行する前にコンパイルされたコードのチャンクを使用する回数。 デフォルトでは、3。
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
値が1以上の場合、コンパイルは別のスレッドで非同期に行われます。 結果は、現在実行中のクエリを含め、準備が整ったらすぐに使用されます。

コンパイルされたコードは、クエリで使用される集計関数とgroup by句のキーのタイプの組み合わせごとに必要です。
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don’t use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## output\_format\_json\_quote\_64bit\_integers {#session_settings-output_format_json_quote_64bit_integers}

値がtrueの場合、json\*int64およびuint64形式を使用するときに整数が引用符で囲まれます（ほとんどのjavascript実装との互換性のため）。

## format\_csv\_delimiter {#settings-format_csv_delimiter}

CSVデータの区切り文字として解釈される文字。 デフォルトでは、区切り文字は `,`.

## input\_format\_csv\_unquoted\_null\_literal\_as\_null {#settings-input_format_csv_unquoted_null_literal_as_null}

FOR CSV入力形式では、引用符なしの解析を有効または無効にします `NULL` リテラルとして(シノニム `\N`).

## output\_format\_csv\_crlf\_end\_of\_line {#settings-output-format-csv-crlf-end-of-line}

UNIXスタイル（LF）の代わりにCSVでDOS/Windowsスタイルの行区切り（CRLF）を使用します。

## output\_format\_tsv\_crlf\_end\_of\_line {#settings-output-format-tsv-crlf-end-of-line}

UNIXスタイル（LF）の代わりにTSVでDOC/Windowsスタイルの行区切り（CRLF）を使用します。

## insert\_quorum {#settings-insert_quorum}

を決議の定足数を書き込みます.

-   もし `insert_quorum < 2` クォーラム書き込みは無効です。
-   もし `insert_quorum >= 2` クォーラム書き込みが有効になります。

デフォルト値:0.

定足数書き込み

`INSERT` 承ClickHouse管理を正しく書き込みデータの `insert_quorum` の間のレプリカの `insert_quorum_timeout`. 何らかの理由で書き込みが成功したレプリカの数に達しない場合 `insert_quorum` を書くのは失敗したとClickHouseを削除するに挿入したブロックからすべてのレプリカがデータがすでに記されています。

クォーラム内のすべてのレプリカは一貫性があります。 `INSERT` クエリ。 その `INSERT` シーケンスは線形化されます。

から書き込まれたデータを読み取る場合 `insert_quorum`、を使用することができ [select\_sequential\_consistency](#settings-select_sequential_consistency) オプション。

ClickHouseは例外を生成します

-   クエリの時点で利用可能なレプリカの数がクエリの時点でのレプリカの数より少ない場合 `insert_quorum`.
-   前のブロックがまだ挿入されていないときにデータを書き込もうとします。 `insert_quorum` レプリカの。 こうした状況が発生した場合、ユーザーしようとしを行う `INSERT` 前のものの前に `insert_quorum` 完了です。

また見なさい:

-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## insert\_quorum\_timeout {#settings-insert_quorum_timeout}

書き込み数が定員タイムアウトを秒で指定します。 タイムアウトが経過し、まだ書き込みが行われていない場合、clickhouseは例外を生成し、クライアントは同じブロックまたは他のレプリカに同じブロック

デフォルト値:60秒。

また見なさい:

-   [insert\_quorum](#settings-insert_quorum)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## select\_sequential\_consistency {#settings-select_sequential_consistency}

シーケンシャル-コンシステ `SELECT` クエリ:

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:0.

使い方

シーケンシャル一貫性が有効になっている場合、clickhouseはクライアントが `SELECT` 以前のすべてのデータを含むレプリカに対してのみ照会する `INSERT` 以下で実行されるクエリ `insert_quorum`. クライアントが部分レプリカを参照している場合、ClickHouseは例外を生成します。 SELECTクエリには、レプリカのクォーラムにまだ書き込まれていないデータは含まれません。

また見なさい:

-   [insert\_quorum](#settings-insert_quorum)
-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)

## insert\_deduplicate {#settings-insert-deduplicate}

ブロックの重複排除を有効または無効にします。 `INSERT` (複製された\*テーブルの場合)。

可能な値:

-   0 — Disabled.
-   1 — Enabled.

デフォルト値:1。

デフォルトでは、レプリケートされたテーブルに `INSERT` ステートメントは重複排除されます(\[データレプリケーション\](../engines/table\_engines/mergetree\_family/replication.md)を参照)。

## 重複除外\_blocks\_in\_dependent\_materialized\_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

を有効または無効にし、重複排除圧縮をチェックを実現し意見を受け取るデータから複製\*ます。

可能な値:

      0 — Disabled.
      1 — Enabled.

デフォルト値:0.

使い方

デフォルトで、重複排除圧縮を行わないための顕在化が行われは上流のソース。
ソーステーブルの重複排除により挿入されたブロックがスキップされた場合、マテリアライズドビューには挿入されません。 この動作は、マテリアライズドビューに高度に集計されたデータを挿入できるようにするために存在します。
同時に、この動作 “breaks” `INSERT` 冪等性 もし `INSERT` メインテーブルに成功したと `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won’t receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` この動作を変更できます。 再試行の際、マテリアライズドビューは繰り返しインサートを受け取り、重複排除チェックを単独で実行します,
ソーステーブルのチェック結果を無視すると、最初の失敗のために失われた行が挿入されます。

## max\_network\_bytes {#settings-max-network-bytes}

クエリの実行時にネットワークを介して受信または送信されるデータ量をバイト単位で制限します。 この設定は、個々のクエリごとに適用されます。

可能な値:

-   正の整数。
-   0 — Data volume control is disabled.

デフォルト値:0.

## max\_network\_bandwidth {#settings-max-network-bandwidth}

ネットワーク上でのデータ交換の速度をバイト/秒で制限します。 この設定は、各クエリに適用されます。

可能な値:

-   正の整数。
-   0 — Bandwidth control is disabled.

デフォルト値:0.

## max\_network\_bandwidth\_for\_user {#settings-max-network-bandwidth-for-user}

ネットワーク上でのデータ交換の速度をバイト/秒で制限します。 この設定は、単一ユーザーが実行するすべての同時実行クエリに適用されます。

可能な値:

-   正の整数。
-   0 — Control of the data speed is disabled.

デフォルト値:0.

## max\_network\_bandwidth\_for\_all\_users {#settings-max-network-bandwidth-for-all-users}

ネットワーク上でデータが交換される速度をバイト/秒で制限します。 この設定が適用されるのはすべての同時走行に関するお問い合わせます。

可能な値:

-   正の整数。
-   0 — Control of the data speed is disabled.

デフォルト値:0.

## count\_distinct\_implementation {#settings-count_distinct_implementation}

どちらを指定するか `uniq*` 機能は実行するのに使用されるべきです [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference.md#agg_function-count) 建設。

可能な値:

-   [uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined64)
-   [unihll12](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqhll12)
-   [ユニキャック](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqexact)

デフォルト値: `uniqExact`.

## skip\_unavailable\_shards {#settings-skip_unavailable_shards}

を有効または無効にし静キの不可欠片.

ザ-シャーがある場合にはご利用できないそのすべてのレプリカのためご利用いただけません。 次の場合、レプリカは使用できません:

-   ClickHouseは何らかの理由でレプリカに接続できません。

    レプリカに接続するとき、clickhouseはいくつかの試行を実行します。 すべてこれらの試みが失敗し、レプリカとはできます。

-   レプリカはdnsで解決できません。

    レプリカのホスト名をdnsで解決できない場合は、次のような状況を示すことができます:

    -   レプリカのホストにdnsレコードがない。 たとえば、動的dnsを使用するシステムで発生する可能性があります, [Kubernetes](https://kubernetes.io) ダウンタイム中にノードが解けなくなる可能性があり、これはエラーではありません。

    -   構成エラー。 clickhouse設定ファイルが含まれて間違ったホスト名.

可能な値:

-   1 — skipping enabled.

    シャードが使用できない場合、clickhouseは部分的なデータに基づいて結果を返し、ノードの可用性の問題は報告しません。

-   0 — skipping disabled.

    シャードが使用できない場合、clickhouseは例外をスローします。

デフォルト値:0.

## optimize\_skip\_unused\_shards {#settings-optimize_skip_unused_shards}

PREWHERE/WHEREでシャーディングキー条件を持つSELECTクエリの未使用シャードのスキップを有効または無効にします(データがシャーディングキーによって分散されてい

デフォルト値:0

## force\_optimize\_skip\_unused\_shards {#settings-force_optimize_skip_unused_shards}

を有効または無効にしクエリの実行の場合 [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) 未使用のシャードを有効にしてスキップすることはできません。 スキップが不可能で、設定が有効になっている場合は例外がスローされます。

可能な値:

-   0-無効(スローしない)
-   1-テーブ
-   2-を無効にクエリの実行に関わらずshardingキー定義のテーブル

デフォルト値:0

## force\_optimize\_skip\_unused\_shards\_no\_nested {#settings-force_optimize_skip_unused_shards_no_nested}

リセット [`optimize_skip_unused_shards`](#settings-force_optimize_skip_unused_shards) 入れ子のため `Distributed` テーブル

可能な値:

-   1 — Enabled.
-   0 — Disabled.

デフォルト値:0.

## optimize\_throw\_if\_noop {#setting-optimize_throw_if_noop}

例外のスローを有効または無効にします。 [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) クエリはマージを実行しませんでした。

デフォルトでは, `OPTIMIZE` 何もしなかった場合でも正常に戻ります。 この設定では、これらの状況を区別し、例外メッセージの理由を取得できます。

可能な値:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

デフォルト値:0.

## distributed\_replica\_error\_half\_life {#settings-distributed_replica_error_half_life}

-   タイプ:秒
-   デフォルト値:60秒

分散テーブルのエラーをゼロにする速度を制御します。 レプリカがしばらく使用できず、5つのエラーが蓄積され、distributed\_replica\_error\_half\_lifeが1秒に設定されている場合、レプリカは最後のエラーから3秒後に通常の状態

また見なさい:

-   [分散テーブルエンジン](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap](#settings-distributed_replica_error_cap)

## distributed\_replica\_error\_cap {#settings-distributed_replica_error_cap}

-   タイプ:unsigned int
-   デフォルト値:1000

各レプリカのエラー数がこの値に制限されるため、単一のレプリカによるエラーの蓄積が妨げられます。

また見なさい:

-   [分散テーブルエンジン](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_half\_life](#settings-distributed_replica_error_half_life)

## distributed\_directory\_monitor\_sleep\_time\_ms {#distributed_directory_monitor_sleep_time_ms}

のための基礎間隔 [分散](../../engines/table-engines/special/distributed.md) データを送信する表エンジン。 実際の間隔は、エラーが発生した場合に指数関数的に増加します。

可能な値:

-   ミリ秒の正の整数の数。

デフォルト値:100ミリ秒。

## distributed\_directory\_monitor\_max\_sleep\_time\_ms {#distributed_directory_monitor_max_sleep_time_ms}

のための最大間隔 [分散](../../engines/table-engines/special/distributed.md) データを送信する表エンジン。 インターバルセットの指数関数的な成長を制限する [distributed\_directory\_monitor\_sleep\_time\_ms](#distributed_directory_monitor_sleep_time_ms) 設定。

可能な値:

-   ミリ秒の正の整数の数。

デフォルト値:30000ミリ秒(30秒)。

## distributed\_directory\_monitor\_batch\_inserts {#distributed_directory_monitor_batch_inserts}

挿入されたデータのバッチでの送信を有効/無効にします。

バッチ送信が有効になっている場合は、 [分散](../../engines/table-engines/special/distributed.md) テーブルエンジンをお送り複数のファイルの挿入データを移動するようになっていますの代わりに送信します。 一括送信の改善にクラスターの性能をより活用してサーバやネットワーク資源です。

可能な値:

-   1 — Enabled.
-   0 — Disabled.

デフォルト値:0.

## os\_thread\_priority {#setting-os-thread-priority}

優先度を設定します ([ニース](https://en.wikipedia.org/wiki/Nice_(Unix)))クエリを実行するスレッドの場合。 OSスケジューラは、使用可能な各CPUコアで実行する次のスレッドを選択するときにこの優先度を考慮します。

!!! warning "警告"
    この設定を使用するには、以下を設定する必要があります。 `CAP_SYS_NICE` 機能。 その `clickhouse-server` パッケージ設定します。 一部の仮想環境では、以下の設定を行うことができません。 `CAP_SYS_NICE` 機能。 この場合, `clickhouse-server` 開始時にそれについてのメッセージを表示します。

可能な値:

-   範囲内で値を設定できます `[-20, 19]`.

低値が高い優先されます。 低いの糸 `nice` 優先度の値は、高い値を持つスレッドよりも頻繁に実行されます。 長時間実行される非インタラクティブクエリでは、短いインタラクティブクエリが到着したときにリソースをすばやく放棄することができるため、

デフォルト値:0.

## query\_profiler\_real\_time\_period\_ns {#query_profiler_real_time_period_ns}

の実際のクロックタイマーの期間を設定します。 [クエリプロファイラ](../../operations/optimizing-performance/sampling-query-profiler.md). リアルクロックタイマーカウント壁時計の時間。

可能な値:

-   ナノ秒単位の正の整数。

    推奨値:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   タイマーを消すための0。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

デフォルト値:1000000000ナノ秒(秒)。

また見なさい:

-   システム表 [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## query\_profiler\_cpu\_time\_period\_ns {#query_profiler_cpu_time_period_ns}

のcpuクロックタイマーの期間を設定します。 [クエリプロファイラ](../../operations/optimizing-performance/sampling-query-profiler.md). このタ

可能な値:

-   ナノ秒の正の整数。

    推奨値:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   タイマーを消すための0。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

デフォルト値:1000000000ナノ秒。

また見なさい:

-   システム表 [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## allow\_introspection\_functions {#settings-allow_introspection_functions}

ディスエーブルの有効 [introspections関数](../../sql-reference/functions/introspection.md) のためのクエリープロファイリング.

可能な値:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

デフォルト値:0.

**また見なさい**

-   [クエリプ](../optimizing-performance/sampling-query-profiler.md)
-   システム表 [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## input\_format\_parallel\_parallel\_paralsing {#input-format-parallel-parsing}

-   タイプ:bool
-   デフォルト値:true

データ形式の並行解析を有効にします。 tsv、tksv、csvおよびjsoneachrow形式でのみサポートされています。

## min\_chunk\_bytes\_for\_parallel\_parall\_paral {#min-chunk-bytes-for-parallel-parsing}

-   タイプ:unsigned int
-   デフォルト値:1mib

各スレッドが並列に解析する最小チャンクサイズ(バイト単位)。

## output\_format\_avro\_codec {#settings-output_format_avro_codec}

出力avroファイルに使用する圧縮コーデックを設定します。

タイプ:文字列

可能な値:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [てきぱき](https://google.github.io/snappy/)

デフォルト値: `snappy` (利用可能な場合)または `deflate`.

## output\_format\_avro\_sync\_interval {#settings-output_format_avro_sync_interval}

出力avroファイルの同期マーカー間の最小データサイズ(バイト単位)を設定します。

タイプ:unsigned int

使用可能な値:32(32バイト)-1073741824(1gib)

デフォルト値:32768(32kib)

## format\_avro\_schema\_registry\_url {#settings-format_avro_schema_registry_url}

使用するスキーマレジストリurlを設定します [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) 書式

タイプ:url

デフォルト値:空

[元の記事](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->

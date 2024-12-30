---
slug: /ja/operations/settings/query-complexity
sidebar_position: 59
sidebar_label: クエリの複雑さの制限
---

# クエリの複雑さの制限

クエリの複雑さの制限は設定の一部です。これらはユーザーインターフェースからの安全な実行を提供するために使用されます。ほとんどの制限は`SELECT`にのみ適用されます。分散クエリ処理のために、制限は各サーバーごとに個別に適用されます。

ClickHouse はデータパーツの制限をチェックし、各行ごとではありません。つまり、制限の値をデータパーツのサイズで超えることができます。

「何かの最大量」の制限は 0 の値を取ることができ、これは「制限なし」を意味します。ほとんどの制限には、制限を超えた場合にどうするかを決定する「overflow_mode」設定もあります。それは 2 つの値のいずれかを取ることができます: `throw`または`break`。集計（group_by_overflow_mode）の制限には、`any`の値もあります。

`throw` – 例外を投げる（デフォルト）。

`break` – クエリの実行を停止し、部分的な結果を返す（ソースデータが尽きたかのように）。

`any（group_by_overflow_mode のみ）` – 集約のキーがセットに入ったキーに対してのみ継続し、新しいキーをセットに追加しない。

## max_memory_usage {#settings_max_memory_usage}

単一サーバーでクエリの実行に使用される最大RAM量。

デフォルト設定は無制限（`0`に設定）。

クラウドのデフォルト値：レプリカのRAM量に依存。

この設定は、利用可能なメモリの容量やマシンのメモリの合計容量を考慮しません。この制限は単一サーバー内の単一クエリに適用されます。`SHOW PROCESSLIST`を使用すると、各クエリの現在のメモリ消費を確認できます。さらに、各クエリのピークメモリ消費が追跡され、ログに書き込まれます。

特定の集計関数の状態のメモリ使用量は監視されません。

`min`、`max`、`any`、`anyLast`、`argMin`、`argMax`の集計関数の状態のメモリ使用量は完全には追跡されていません。

メモリ消費は`max_memory_usage_for_user`および[max_server_memory_usage](../../operations/server-configuration-parameters/settings.md#max_server_memory_usage)のパラメータによっても制限されます。

## max_memory_usage_for_user {#max-memory-usage-for-user}

単一サーバーでユーザーのクエリの実行に使用される最大RAM量。

デフォルト値は[Settings.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L288)で定義されています。デフォルトでは、量は制限されていません（`max_memory_usage_for_user = 0`）。

[max_memory_usage](#settings_max_memory_usage)の説明も参照してください。

たとえば、`clickhouse_read`という名前のユーザーに対して`max_memory_usage_for_user`を1000バイトに設定したい場合は、次のステートメントを使用します。

``` sql
ALTER USER clickhouse_read SETTINGS max_memory_usage_for_user = 1000;
```

それが機能したことを確認するには、クライアントからログアウトし、再度ログインしてから、`getSetting`関数を使用します：

```sql
SELECT getSetting('max_memory_usage_for_user');
```

## max_rows_to_read {#max-rows-to-read}

次の制限は各ブロックでチェックされる場合があります（各行ではありません）。つまり、制限を少し超えることができます。

クエリを実行する際にテーブルから読み取ることができる最大行数。

## max_bytes_to_read {#max-bytes-to-read}

クエリを実行する際にテーブルから読み取ることができる最大バイト数（非圧縮データ）。

## read_overflow_mode {#read-overflow-mode}

読み取りデータボリュームが制限を超えた場合にどうするか：`throw`または`break`。デフォルトは`trow`。

## max_rows_to_read_leaf {#max-rows-to-read-leaf}

次の制限は各ブロックでチェックされる場合があります（各行ではありません）。つまり、制限を少し超えることができます。

分散クエリを実行する際にリーフノード上のローカルテーブルから読み取ることができる最大行数です。分散クエリは各シャード（リーフ）に対する複数のサブクエリを発行することができます - この制限はリーフノードでの読み取り段階でのみチェックされ、ルートノードでの結果マージ段階では無視されます。たとえば、2つのシャードから成るクラスターがあり、各シャードに100行のテーブルがあるとします。それから分散クエリは両方のテーブルからすべてのデータを読み取ろうとしているので、`max_rows_to_read=150`を設定するとトータルで200行になるため失敗します。一方で`max_rows_to_read_leaf=150`を設定したクエリは成功します、なぜならリーフノードが最大で100行を読み取るからです。

## max_bytes_to_read_leaf {#max-bytes-to-read-leaf}

分散クエリを実行する際にリーフノード上のローカルテーブルから読み取ることができる最大バイト数（非圧縮データ）。分散クエリは各シャード（リーフ）に対する複数のサブクエリを発行することができます - この制限はリーフノードでの読み取り段階でのみチェックされ、ルートノードでの結果マージ段階では無視されます。たとえば、2つのシャードから成るクラスターがあり、各シャードに100バイトのデータテーブルがあるとします。それから分散クエリがすべてのデータを両方のテーブルから読み取ろうとしているので、`max_bytes_to_read=150`を設定するとトータルで200バイトになるため失敗します。一方で`max_bytes_to_read_leaf=150`を設定したクエリは成功します、なぜならリーフノードが最大で100バイトを読み取るからです。

## read_overflow_mode_leaf {#read-overflow-mode-leaf}

リーフ制限を超えるデータ量で読み取りが行われた場合にどのようにするか：`throw`または`break`です。デフォルトは`trow`です。

## max_rows_to_group_by {#settings-max-rows-to-group-by}

集計から受け取るユニークなキーの最大数。この設定は集計時のメモリ消費を制限することを可能にします。

## group_by_overflow_mode {#group-by-overflow-mode}

集計のユニークなキー数が制限を超えた場合にどうするか：`throw`、 `break`、または `any`。 デフォルトは `throw` です。 `any` 値を使用すると、GROUP BY の近似値を実行できます。この近似値の精度はデータの統計的性質に依存します。

## max_bytes_before_external_group_by {#settings-max_bytes_before_external_group_by}

`GROUP BY`句の外部メモリでの実行を有効または無効にします。 詳細は[外部メモリでのGROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory)を参照してください。

可能な値：

- 単一の[GROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-clause)操作で使用できる最大のRAMボリューム（バイト単位）。
- 0 — 外部メモリでの`GROUP BY`が無効。

デフォルト値：`0`.

クラウドのデフォルト値：レプリカあたりのメモリ量の半分。

## max_bytes_before_external_sort {#settings-max_bytes_before_external_sort}

`ORDER BY`句の外部メモリでの実行を有効または無効にします。 詳細は[ORDER BY の実装方法](../../sql-reference/statements/select/order-by.md#implementation-details)を参照してください。

- 単一の[ORDER BY](../../sql-reference/statements/select/order-by.md)操作で使用可能な最大RAMボリューム（バイト単位）。 推奨値は、利用可能なシステムメモリの半分です。
- 0 — 外部メモリでの`ORDER BY`が無効。

デフォルト値：0。

クラウドのデフォルト値：レプリカあたりのメモリ量の半分。

## max_rows_to_sort {#max-rows-to-sort}

ソート前の最大行数。これにより、ソート時のメモリ消費を制限することができます。

## max_bytes_to_sort {#max-bytes-to-sort}

ソート前の最大バイト数。

## sort_overflow_mode {#sort-overflow-mode}

ソートする前に受け取った行の数が制限を超えた場合にどうするか：`throw`または`break`。 デフォルトは`throw`です。

## max_result_rows {#setting-max_result_rows}

結果の行数の制限。サブクエリでもチェックされ、分散クエリの一部を実行する際のリモートサーバーでもチェックされます。値が`0`の場合は制限は適用されません。

デフォルト値：`0`.

クラウドのデフォルト値：`0`.

## max_result_bytes {#max-result-bytes}

結果のバイト数の制限。前の設定と同様。

## result_overflow_mode {#result-overflow-mode}

結果のボリュームがいずれかの制限を超えた場合にどうするか：`throw`または`break`。

`break`を使用することは、LIMITを使用することと似ています。`Break`はブロックレベルでのみ実行を中断します。これは、返される行数が[max_result_rows](#setting-max_result_rows)より多いことを意味し、[max_block_size](../../operations/settings/settings.md#setting-max_block_size)の倍数であり、[max_threads](../../operations/settings/settings.md#max_threads)に依存します。

デフォルト値：`throw`.

クラウドのデフォルト値：`throw`.

例：

``` sql
SET max_threads = 3, max_block_size = 3333;
SET max_result_rows = 3334, result_overflow_mode = 'break';

SELECT *
FROM numbers_mt(100000)
FORMAT Null;
```

結果：

``` text
6666 rows in set. ...
```

## max_execution_time {#max-execution-time}

クエリの最大実行時間（秒単位）。この時間、ソート段階のいずれか、または集計関数のマージや最終化時にはチェックされません。

`max_execution_time`パラメータは理解しづらい場合があります。これは現在のクエリ実行速度に対する補間に基づいて動作します（この動作は[timeout_before_checking_execution_speed](#timeout-before-checking-execution-speed)によって制御されます）。ClickHouseは、指定された`max_execution_time`を超えると予測される実行時間がある場合、クエリを中断します。デフォルトで、timeout_before_checking_execution_speedは10秒に設定されています。これは、クエリの実行開始から10秒後に、ClickHouseが総実行時間を見積もり始めることを意味します。たとえば、`max_execution_time`が3600秒（1時間）に設定されている場合、見積もり時間がこの3600秒の制限を超えると、ClickHouseはクエリを終了します。`timeout_before_checking_execution_speed`を0に設定すると、ClickHouseはクロック時間を`max_execution_time`の基準として使用します。

## timeout_overflow_mode {#timeout-overflow-mode}

クエリが`max_execution_time`を超えて実行された場合または見積もり実行時間が`max_estimated_execution_time`を超えている場合にどうするか：`throw`または`break`。デフォルトは`throw`。

## max_execution_time_leaf

`max_execution_time`と同様の意味ですが、分散またはリモートクエリのリーフノードにのみ適用されます。

たとえば、リーフノードの実行時間を`10s`に制限して、初期ノードには制限をかけない場合、入れ子のサブクエリ設定の中で`max_execution_time`を持つ代わりに：

``` sql
SELECT count() FROM cluster(cluster, view(SELECT * FROM t SETTINGS max_execution_time = 10));
```

クエリ設定として`max_execution_time_leaf`を使用できます：

``` sql
SELECT count() FROM cluster(cluster, view(SELECT * FROM t)) SETTINGS max_execution_time_leaf = 10;
```

## timeout_overflow_mode_leaf

リーフノードのクエリが`max_execution_time_leaf`より長く実行された場合にどうするか：`throw`または`break`。デフォルトは`throw`。

## min_execution_speed {#min-execution-speed}

秒あたりの行数での最小実行速度。‘timeout_before_checking_execution_speed’が期限切れになった時点で各データブロックでチェックされます。実行速度が低い場合、例外がスローされます。

## min_execution_speed_bytes {#min-execution-speed-bytes}

1秒あたりの最小実行バイト数。‘timeout_before_checking_execution_speed’が期限切れになった時点で各データブロックでチェックされます。実行速度が低い場合、例外がスローされます。

## max_execution_speed {#max-execution-speed}

1秒あたりの最大実行行数。‘timeout_before_checking_execution_speed’が期限切れになった時点で各データブロックでチェックされます。実行速度が高い場合、実行速度は低減されます。

## max_execution_speed_bytes {#max-execution-speed-bytes}

1秒あたりの最大実行バイト数。‘timeout_before_checking_execution_speed’が期限切れになった時点で各データブロックでチェックされます。実行速度が高い場合、実行速度は低減されます。

## timeout_before_checking_execution_speed {#timeout-before-checking-execution-speed}

指定された秒数が過ぎた後に、実行速度が遅すぎないこと（‘min_execution_speed’以上であること）をチェックします。

## max_estimated_execution_time {#max_estimated_execution_time}

秒単位のクエリ推定実行時間の最大値。‘timeout_before_checking_execution_speed’が期限切れになった時点で各データブロックでチェックされます。

## max_columns_to_read {#max-columns-to-read}

1つのクエリでテーブルから読み取れるカラムの最大数。クエリがより多くのカラムを読み取る必要がある場合、例外がスローされます。

## max_temporary_columns {#max-temporary-columns}

クエリ実行中に同時にRAMに保持される一時カラムの最大数で、定数カラムを含みます。一時カラムがこの数を超えた場合、例外がスローされます。

## max_temporary_non_const_columns {#max-temporary-non-const-columns}

‘max_temporary_columns’と同じですが、定数カラムをカウントしません。クエリ実行中、一時的に生成される定数カラムは非常に頻繁に形成されますが、計算資源はほとんど必要としません。

## max_subquery_depth {#max-subquery-depth}

サブクエリの最大ネスト深度。サブクエリがより深い場合、例外がスローされます。デフォルトで100です。

## max_pipeline_depth {#max-pipeline-depth}

最大パイプライン深度。クエリ処理中に各データブロックが通過する変換の数に対応します。単一サーバー内での制限内でカウントされます。パイプラインの深さが大きい場合、例外がスローされます。デフォルトで1000です。

## max_ast_depth {#max-ast-depth}

クエリの構文木の最大ネスト深度。超過すると、例外がスローされます。この時点では、解析中ではなく、クエリの解析後にのみチェックされます。つまり、解析中に深すぎる構文木を作成することができるが、クエリは失敗します。デフォルトで1000です。

## max_ast_elements {#max-ast-elements}

クエリの構文木の要素の最大数。超過すると、例外がスローされます。前の設定と同様に、クエリの解析後にのみチェックされます。デフォルトで50,000です。

## max_rows_in_set {#max-rows-in-set}

サブクエリから作成されたIN句のデータセットの最大行数。

## max_bytes_in_set {#max-bytes-in-set}

サブクエリから作成されたIN句で使用されるセットの最大バイト数（非圧縮データ）。

## set_overflow_mode {#set-overflow-mode}

データ量が制限を超えた場合にどうするか：`throw`または`break`。デフォルトで`trow`。

## max_rows_in_distinct {#max-rows-in-distinct}

DISTINCTを使用する際の異なる行数の最大数。

## max_bytes_in_distinct {#max-bytes-in-distinct}

DISTINCTを使用する際にハッシュテーブルで使用される最大バイト数。

## distinct_overflow_mode {#distinct-overflow-mode}

データ量が制限を超えた場合にどうするか：`throw`または`break`。デフォルトで`trow`。

## max_rows_to_transfer {#max-rows-to-transfer}

GLOBAL INを使用する際にリモートサーバーに渡されるか、一時テーブルに保存される最大行数。

## max_bytes_to_transfer {#max-bytes-to-transfer}

GLOBAL INを使用する際にリモートサーバーに渡されるか、一時テーブルに保存される最大バイト数（非圧縮データ）。

## transfer_overflow_mode {#transfer-overflow-mode}

データ量が制限を超えた場合にどうするか：'throw'または'break'。デフォルトで`throw`。

## max_rows_in_join {#settings-max_rows_in_join}

テーブル結合時に使用されるハッシュテーブルの行数を制限します。

この設定は[SELECT ... JOIN](../../sql-reference/statements/select/join.md#select-join)操作および[Join](../../engines/table-engines/special/join.md)テーブルエンジンに適用されます。

クエリに複数の結合が含まれる場合、ClickHouseはこの設定を各中間結果に対してチェックします。

制限に達した場合、ClickHouseは異なるアクションを実行することができます。アクションを選択するには[join_overflow_mode](#settings-join_overflow_mode)設定を使用します。

可能な値：

- 正の整数
- 0 — 無制限の行数

デフォルト値：0

## max_bytes_in_join {#settings-max_bytes_in_join}

テーブル結合時に使用されるハッシュテーブルのサイズをバイト単位で制限します。

この設定は[SELECT ... JOIN](../../sql-reference/statements/select/join.md#select-join)操作および[Joinテーブルエンジン](../../engines/table-engines/special/join.md)に適用されます。

クエリに結合が含まれている場合、ClickHouseはこの設定を各中間結果に対してチェックします。

制限に達した場合、ClickHouseは異なるアクションを実行することができます。アクションを選択するには[join_overflow_mode](#settings-join_overflow_mode)設定を使用します。

可能な値：

- 正の整数
- 0 — メモリ管理が無効

デフォルト値：0

## join_overflow_mode {#settings-join_overflow_mode}

次の結合制限のいずれかに達した場合にClickHouseがどのアクションを実行するかを定義します：

- [max_bytes_in_join](#settings-max_bytes_in_join)
- [max_rows_in_join](#settings-max_rows_in_join)

可能な値：

- `THROW` — ClickHouseが例外をスローし、操作を中断。
- `BREAK` — ClickHouseが操作を中断し、例外をスローしない。

デフォルト値：`THROW`。

**関連情報**

- [JOIN句](../../sql-reference/statements/select/join.md#select-join)
- [Joinテーブルエンジン](../../engines/table-engines/special/join.md)

## max_partitions_per_insert_block {#settings-max_partitions_per_insert_block}

単一の挿入ブロックにおける最大パーティション数を制限します。

- 正の整数
- 0 — パーティション数無制限。

デフォルト値：100

**詳細**

データを挿入すると、ClickHouseは挿入ブロックのパーティション数を計算します。もしパーティション数が`max_partitions_per_insert_block`より多い場合、`throw_on_max_partitions_per_insert_block`に基づいて、ClickHouseは警告をログするか、例外をスローします。例外のテキストは次のとおりです：

> “1つのINSERTブロックに対してパーティションが多すぎます（`partitions_count`パーティション、制限は” + toString(max_partitions) + “）。制限は‘max_partitions_per_insert_block’設定で制御されます。大量のパーティションは一般的な誤解であり、深刻なパフォーマンスの悪影響をもたらします、例えばサーバーの起動時間が遅くなったり、INSERTクエリとSELECTクエリが遅くなったりします。推奨されるテーブルのパーティションの合計数は1000〜10000以下です。SELECTクエリを高速化するためにパーティションが意図されていないことに注意してください（ORDER BYキーは範囲クエリを高速化するのに十分です）。パーティションはデータ操作（DROP PARTITIONなど）のために意図されています。”

## throw_on_max_partitions_per_insert_block {#settings-throw_on_max_partition_per_insert_block}

`max_partitions_per_insert_block`に達した場合の動作を制御します。

- `true`  - 挿入ブロックが`max_partitions_per_insert_block`に達した場合、例外を発生します。
- `false` - `max_partitions_per_insert_block`に達した場合に警告をログします。

デフォルト値：`true`

## max_temporary_data_on_disk_size_for_user {#settings_max_temporary_data_on_disk_size_for_user}

すべての同時実行されているユーザークエリのためにディスク上で消費される一時ファイルのデータの最大量（バイト単位）。ゼロは無制限を意味します。

デフォルト値：0。

## max_temporary_data_on_disk_size_for_query {#settings_max_temporary_data_on_disk_size_for_query}

すべての同時実行されているクエリのためにディスク上で消費される一時ファイルのデータの最大量（バイト単位）。ゼロは無制限を意味します。

デフォルト値：0。

## max_sessions_for_user {#max-sessions-per-user}

ClickHouseサーバーへの認証済みユーザーごとの同時セッションの最大数。

例：

``` xml
<profiles>
    <single_session_profile>
        <max_sessions_for_user>1</max_sessions_for_user>
    </single_session_profile>
    <two_sessions_profile>
        <max_sessions_for_user>2</max_sessions_for_user>
    </two_sessions_profile>
    <unlimited_sessions_profile>
        <max_sessions_for_user>0</max_sessions_for_user>
    </unlimited_sessions_profile>
</profiles>
<users>
     <!-- ユーザーAliceは、一度にClickHouseサーバーに接続できるのは1回のみです。 -->
    <Alice>
        <profile>single_session_user</profile>
    </Alice>
    <!-- ユーザーBobは、2つの同時セッションを使用できます。 -->
    <Bob>
        <profile>two_sessions_profile</profile>
    </Bob>
    <!-- ユーザーチャールズは無制限の同時セッションを使用できます。 -->
    <Charles>
       <profile>unlimited_sessions_profile</profile>
    </Charles>
</users>
```

デフォルト値：0（無制限の同時セッション数）。

## max_partitions_to_read {#max-partitions-to-read}

1つのクエリでアクセスできる最大パーティション数を制限します。

テーブルが作成されたときに指定された設定値は、クエリレベルの設定で上書きできます。

可能な値：

- 任意の正の整数

デフォルト値：-1（無制限）。

テーブルの設定でMergeTree設定[max_partitions_to_read](merge-tree-settings#max-partitions-to-read)を指定することもできます。

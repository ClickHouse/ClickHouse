---
description: 'クエリの複雑度を制限する設定。'
sidebar_label: 'クエリ複雑度の制限'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: 'クエリ複雑度の制限'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

[設定](/ja/operations/settings/overview)の一部として、ClickHouse では
クエリ複雑度に制限を設けることができます。これにより、
リソースを大量に消費する可能性のあるクエリを防ぎ、より安全で予測しやすい
実行を実現できます。特にユーザーインターフェイスを使用する場合に有効です。

ほとんどの制限は `SELECT` クエリにのみ適用され、分散
クエリ処理では、制限は各サーバーに個別に適用されます。

ClickHouse は通常、各行ごとに制限を確認するのではなく、データパーツが
完全に処理された後でのみ制限を確認します。そのため、パーツの処理中に
制限を超過する状況が発生することがあります。

<div id="overflow_mode_setting">
  ## `overflow_mode` の設定
</div>

ほとんどの制限には `overflow_mode` 設定もあり、制限を超えたときの動作を定義します。指定できる値は次の 2 つです。

* `throw`: 例外をスローします (デフォルト) 。
* `break`: クエリの実行を停止し、部分的な結果を返します。これは、
  ログソースデータが尽きたかのように動作します。

<div id="group_by_overflow_mode_settings">
  ## `group_by_overflow_mode` 設定
</div>

`group_by_overflow_mode` 設定には、
値 `any` もあります。

* `any` : セットに入ったキーについては集約を継続しますが、
  新しいキーはセットに追加しません。

<div id="relevant-settings">
  ## 設定一覧
</div>

以下の設定は、クエリ複雑度に関する制限を適用する際に使用されます。

:::note
「何らかの最大量」に対する制限には、値として `0` を指定できます。
これは「無制限」を意味します。
:::

| 設定                                                                                                                     | 概要                                                                                           |
| ---------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [`max_memory_usage`](/ja/operations/settings/settings#max_memory_usage)                                                   | 単一のサーバーでクエリを実行する際に使用できるRAMの最大量。                                                              |
| [`max_memory_usage_for_user`](/ja/operations/settings/settings#max_memory_usage_for_user)                                 | 単一のサーバーでユーザーのクエリを実行する際に使用できるRAMの最大量。                                                         |
| [`max_rows_to_read`](/ja/operations/settings/settings#max_rows_to_read)                                                   | クエリ実行時にテーブルから読み取ることができる行数の上限。                                                                |
| [`max_bytes_to_read`](/ja/operations/settings/settings#max_bytes_to_read)                                                 | クエリ実行時にテーブルから読み取ることができるバイト数 (非圧縮データ) の上限。                                                    |
| [`read_overflow_mode_leaf`](/ja/operations/settings/settings#read_overflow_mode_leaf)                                     | 読み取るデータ量がリーフ制限のいずれかを超えた場合の動作を設定します。                                                          |
| [`max_rows_to_read_leaf`](/ja/operations/settings/settings#max_rows_to_read_leaf)                                         | 分散クエリ実行時に、リーフノード上のローカルテーブルから読み取ることができる行数の上限。                                                 |
| [`max_bytes_to_read_leaf`](/ja/operations/settings/settings#max_bytes_to_read_leaf)                                       | 分散クエリ実行時に、リーフノード上のローカルテーブルから読み取ることができるバイト数 (非圧縮データ) の上限。                                     |
| [`read_overflow_mode_leaf`](/ja/docs/operations/settings/settings#read_overflow_mode_leaf)                                | 読み取るデータ量がリーフ制限のいずれかを超えた場合の動作を設定します。                                                          |
| [`max_rows_to_group_by`](/ja/operations/settings/settings#max_rows_to_group_by)                                           | 集約で受け取る一意なキー数の上限。                                                                            |
| [`group_by_overflow_mode`](/ja/operations/settings/settings#group_by_overflow_mode)                                       | 集約時の一意なキー数が制限を超えた場合の動作を設定します。                                                                |
| [`max_bytes_before_external_group_by`](/ja/operations/settings/settings#max_bytes_before_external_group_by)               | 外部メモリでの`GROUP BY`句の実行を有効または無効にします。                                                           |
| [`max_bytes_ratio_before_external_group_by`](/ja/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | `GROUP BY`に使用できる利用可能メモリの比率。これに達すると、集約に外部メモリが使用されます。                                          |
| [`max_bytes_before_external_sort`](/ja/operations/settings/settings#max_bytes_before_external_sort)                       | 外部メモリでの`ORDER BY`句の実行を有効または無効にします。                                                           |
| [`max_bytes_ratio_before_external_sort`](/ja/operations/settings/settings#max_bytes_ratio_before_external_sort)           | `ORDER BY`に使用できる利用可能メモリの比率。これに達すると、外部ソートが使用されます。                                             |
| [`max_rows_to_sort`](/ja/operations/settings/settings#max_rows_to_sort)                                                   | ソート前の行数の上限。ソート時のメモリ使用量を制限できます。                                                               |
| [`max_bytes_to_sort`](/ja/operations/settings/settings#max_rows_to_sort)                                                  | ソート前のバイト数の上限。                                                                                |
| [`sort_overflow_mode`](/ja/operations/settings/settings#sort_overflow_mode)                                               | ソート前に受け取った行数がいずれかの制限を超えた場合の動作を設定します。                                                         |
| [`max_result_rows`](/ja/operations/settings/settings#max_result_rows)                                                     | 結果の行数を制限します。                                                                                 |
| [`max_result_bytes`](/ja/operations/settings/settings#max_result_bytes)                                                   | 結果サイズをバイト単位 (非圧縮) で制限します。                                                                    |
| [`result_overflow_mode`](/ja/operations/settings/settings#result_overflow_mode)                                           | 結果量がいずれかの制限を超えた場合の動作を設定します。                                                                  |
| [`max_execution_time`](/ja/operations/settings/settings#max_execution_time)                                               | クエリ実行時間の上限 (秒) 。                                                                             |
| [`timeout_overflow_mode`](/ja/operations/settings/settings#timeout_overflow_mode)                                         | クエリの実行時間が`max_execution_time`を超えた場合、または推定実行時間が`max_estimated_execution_time`を超える場合の動作を設定します。 |
| [`max_execution_time_leaf`](/ja/operations/settings/settings#max_execution_time_leaf)                                     | 意味的には`max_execution_time`と同様ですが、分散クエリまたはリモートクエリのリーフノードにのみ適用されます。                             |
| [`timeout_overflow_mode_leaf`](/ja/operations/settings/settings#timeout_overflow_mode_leaf)                               | リーフノード上のクエリ実行時間が`max_execution_time_leaf`を超えた場合の動作を設定します。                                    |
| [`min_execution_speed`](/ja/operations/settings/settings#min_execution_speed)                                             | 1秒あたりの最小実行速度 (行数) 。                                                                          |
| [`min_execution_speed_bytes`](/ja/operations/settings/settings#min_execution_speed_bytes)                                 | 1秒あたりの実行バイト数の下限。                                                                             |
| [`max_execution_speed`](/ja/operations/settings/settings#max_execution_speed)                                             | 1秒あたりの実行行数の上限。                                                                               |
| [`max_execution_speed_bytes`](/ja/operations/settings/settings#max_execution_speed_bytes)                                 | 1秒あたりの実行バイト数の上限。                                                                             |
| [`timeout_before_checking_execution_speed`](/ja/operations/settings/settings#timeout_before_checking_execution_speed)     | 指定した秒数が経過した後、実行速度が遅すぎないこと (`min_execution_speed`以上であること) を確認します。                             |
| [`max_estimated_execution_time`](/ja/operations/settings/settings#max_estimated_execution_time)                           | クエリの推定実行時間の上限 (秒) 。                                                                          |
| [`max_columns_to_read`](/ja/operations/settings/settings#max_columns_to_read)                                             | 1つのクエリでテーブルから読み取れるカラムの最大数。                                                                   |
| [`max_temporary_columns`](/ja/operations/settings/settings#max_temporary_columns)                                         | クエリ実行時に、定数カラムを含めて同時にRAMに保持する必要がある一時カラムの最大数。                                                  |
| [`max_temporary_non_const_columns`](/ja/operations/settings/settings#max_temporary_non_const_columns)                     | クエリ実行時に、定数カラムを除いて同時にRAMに保持する必要がある一時カラムの最大数。                                                  |
| [`max_subquery_depth`](/ja/operations/settings/settings#max_subquery_depth)                                               | クエリに、指定した数を超えるネストされたサブクエリがある場合の動作を設定します。                                                     |
| [`max_ast_depth`](/ja/operations/settings/settings#max_ast_depth)                                                         | クエリの構文木の最大ネスト深度。                                                                             |
| [`max_ast_elements`](/ja/operations/settings/settings#max_ast_elements)                                                   | クエリの構文木に含まれる要素の最大数。                                                                          |
| [`max_rows_in_set`](/ja/operations/settings/settings#max_rows_in_set)                                                     | サブクエリから作成されるIN句のデータセット内の最大行数。                                                                |
| [`max_bytes_in_set`](/ja/operations/settings/settings#max_bytes_in_set)                                                   | サブクエリから作成されるIN句のセットで使用される最大バイト数 (非圧縮データ) 。                                                   |
| [`set_overflow_mode`](/ja/operations/settings/settings#max_bytes_in_set)                                                  | データ量がいずれかの制限を超えた場合の動作を設定します。                                                                 |
| [`max_rows_in_distinct`](/ja/operations/settings/settings#max_rows_in_distinct)                                           | DISTINCT使用時の異なる行の最大数。                                                                        |
| [`max_bytes_in_distinct`](/ja/operations/settings/settings#max_bytes_in_distinct)                                         | DISTINCT使用時にハッシュテーブルで使用される、メモリ内の状態の最大サイズ (非圧縮バイト数) 。                                         |
| [`distinct_overflow_mode`](/ja/operations/settings/settings#distinct_overflow_mode)                                       | データ量がいずれかの制限を超えた場合の動作を設定します。                                                                 |
| [`max_rows_to_transfer`](/ja/operations/settings/settings#max_rows_to_transfer)                                           | GLOBAL IN/JOIN セクションの実行時に、リモートサーバーに渡す、または一時テーブルに保存できる最大サイズ (行数) 。                            |
| [`max_bytes_to_transfer`](/ja/operations/settings/settings#max_bytes_to_transfer)                                         | GLOBAL IN/JOIN セクションの実行時に、リモートサーバーに渡す、または一時テーブルに保存できる最大バイト数 (非圧縮データ) 。                       |
| [`transfer_overflow_mode`](/ja/operations/settings/settings#transfer_overflow_mode)                                       | データ量がいずれかの制限を超えた場合の動作を設定します。                                                                 |
| [`max_rows_in_join`](/ja/operations/settings/settings#max_rows_in_join)                                                   | テーブル結合時に使用されるハッシュテーブル内の行数を制限します。                                                             |
| [`max_bytes_in_join`](/ja/operations/settings/settings#max_bytes_in_join)                                                 | テーブル結合時に使用されるハッシュテーブルの最大サイズ (バイト数) 。                                                         |
| [`join_overflow_mode`](/ja/operations/settings/settings#join_overflow_mode)                                               | 以下のJOIN制限のいずれかに達したときに、ClickHouseが実行する動作を定義します。                                               |
| [`max_partitions_per_insert_block`](/ja/operations/settings/settings#max_partitions_per_insert_block)                     | 1つの挿入blockに含められるパーティションの最大数を制限し、blockに含まれるパーティション数が多すぎる場合は例外をスローします。                         |
| [`throw_on_max_partitions_per_insert_block`](/ja/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | `max_partitions_per_insert_block` に達したときの動作を制御できます。                                          |
| [`max_temporary_data_on_disk_size_for_user`](/ja/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | 同時実行中のそのユーザーの全クエリについて、ディスク上の一時ファイルが消費できるデータ量の最大値 (バイト単位) 。                                   |
| [`max_temporary_data_on_disk_size_for_query`](/ja/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | 同時実行中のすべてのクエリについて、ディスク上の一時ファイルが消費できるデータ量の最大値 (バイト単位) 。                                       |
| [`max_sessions_for_user`](/ja/operations/settings/settings#max_sessions_for_user)                                         | 認証済みユーザーごとにClickHouseサーバーへ同時接続できるセッションの最大数。                                                  |
| [`max_partitions_to_read`](/ja/operations/settings/settings#max_partitions_to_read)                                       | 1つのクエリでアクセスできるパーティションの最大数を制限します。                                                             |

<div id="obsolete-settings">
  ## 廃止された設定
</div>

:::note
以下の設定は廃止されています。
:::

<div id="max-pipeline-depth">
  ### max_pipeline_depth
</div>

パイプラインの最大深度です。各データブロックがクエリ処理中に経由する
変換の数に対応します。この値は単一のサーバー内で
カウントされます。パイプライン深度がこれを超えると、
例外が発生します。
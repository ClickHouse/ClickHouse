---
title: システムログのパラメータ
---

以下の設定はサブタグで指定できます。

| 設定                                 | 説明                                                                                                                                  | デフォルト               | 注記                                                                                   |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------ |
| `database`                         | データベース名。                                                                                                                            |                     |                                                                                      |
| `table`                            | システムテーブル名。                                                                                                                          |                     |                                                                                      |
| `engine`                           | システムテーブルの [MergeTree エンジン定義](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。 |                     | `partition_by` または `order_by` が定義されている場合は使用できません。指定しない場合は、デフォルトで `MergeTree` が選択されます |
| `partition_by`                     | システムテーブルの[カスタムパーティションキー](../../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。                               |                     | システムテーブルに `engine` を指定する場合、`partition_by` パラメータは &#39;engine&#39; 内に直接指定する必要があります    |
| `ttl`                              | テーブルの[有効期限 (TTL)](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) を指定します。                             |                     | システムテーブルに `engine` を指定する場合、`ttl` パラメータは &#39;engine&#39; 内に直接指定する必要があります             |
| `order_by`                         | システムテーブルの[カスタムソートキー](../../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。            |                     | システムテーブルに `engine` を指定する場合、`order_by` パラメータは &#39;engine&#39; 内に直接指定する必要があります        |
| `storage_policy`                   | テーブルに使用するストレージポリシー名 (任意) 。                                                                                                          |                     | システムテーブルに `engine` を指定する場合、`storage_policy` パラメータは &#39;engine&#39; 内に直接指定する必要があります  |
| `settings`                         | MergeTree の動作を制御する[追加パラメータ](../../../engines/table-engines/mergetree-family/mergetree.md/#settings) (任意) 。                          |                     | システムテーブルに `engine` を指定する場合、`settings` パラメータは &#39;engine&#39; 内に直接指定する必要があります        |
| `flush_interval_milliseconds`      | メモリ上のバッファからテーブルへデータをフラッシュする間隔。                                                                                                      | `7500`              |                                                                                      |
| `max_size_rows`                    | ログの最大行数。未フラッシュのログ量が max&#95;size に達すると、ログはディスクにダンプされます。                                                                             | `1048576`           |                                                                                      |
| `reserved_size_rows`               | ログ用に事前確保されるメモリの行数。                                                                                                                  | `8192`              |                                                                                      |
| `buffer_size_rows_flush_threshold` | 行数のしきい値。このしきい値に達すると、ログをディスクへフラッシュする処理がバックグラウンドで開始されます。                                                                              | `max_size_rows / 2` |                                                                                      |
| `flush_on_crash`                   | クラッシュ時にログをディスクへダンプするかどうかを設定します。                                                                                                     | `false`             |                                                                                      |

さらに、以下のサーバーレベル設定により、すべてのシステムログテーブルのデフォルトのフラッシュポリシーを制御できます。

```xml
<default_system_log_flush_policy>
    <skip_alias_columns>true</skip_alias_columns>
</default_system_log_flush_policy>
```

| 設定                   | 説明                                                                           | デフォルト   |
| -------------------- | ---------------------------------------------------------------------------- | ------- |
| `skip_alias_columns` | `true` の場合、ALIAS カラムは system logテーブルのスキーマから除外されます。S3 をバックエンドとするシステムログでは必須です。 | `false` |
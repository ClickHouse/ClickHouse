---
slug: /ja/operations/system-tables/azure_queue_settings
---
# azure_queue_settings

[AzureQueue](../../engines/table-engines/integrations/azure-queue.md)テーブルの設定に関する情報を含みます。
`24.10` サーバーバージョンから利用可能です。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — テーブル名.
- `table` ([String](../../sql-reference/data-types/string.md)) — データベース名.
- `name` ([String](../../sql-reference/data-types/string.md)) — 設定名.
- `value` ([String](../../sql-reference/data-types/string.md)) — 設定値.
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 設定が明示的にコンフィグで定義されたか、変更されたか.
- `description` ([String](../../sql-reference/data-types/string.md)) — 設定の説明.
- `alterable` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `ALTER TABLE ... MODIFY SETTING`によって設定を変更できるかどうかを示します。
    - `0` — 現在のユーザーが設定を変更できます。
    - `1` — 現在のユーザーは設定を変更できません。
- `type` ([String](../../sql-reference/data-types/string.md)) — 設定タイプ（実装固有の文字列値）。

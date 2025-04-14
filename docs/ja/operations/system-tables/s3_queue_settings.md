---
slug: /ja/operations/system-tables/s3_queue_settings
---
# s3_queue_settings

[S3Queue](../../engines/table-engines/integrations/s3queue.md)テーブルの設定情報を含んでいます。`24.10`サーバーバージョンから利用可能です。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — テーブル名。
- `table` ([String](../../sql-reference/data-types/string.md)) — データベース名。
- `name` ([String](../../sql-reference/data-types/string.md)) — 設定名。
- `value` ([String](../../sql-reference/data-types/string.md)) — 設定値。
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 設定が設定ファイルで明示的に定義されたか、または明示的に変更されたかどうか。
- `description` ([String](../../sql-reference/data-types/string.md)) — 設定の説明。
- `alterable` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `ALTER TABLE ... MODIFY SETTING`を使用して設定を変更できるかどうかを示します。
  - `0` — 現在のユーザーが設定を変更できる。
  - `1` — 現在のユーザーが設定を変更できない。
- `type` ([String](../../sql-reference/data-types/string.md)) — 設定のタイプ（実装固有の文字列値）。

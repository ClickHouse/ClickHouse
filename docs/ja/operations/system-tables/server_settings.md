---
slug: /ja/operations/system-tables/server_settings
---
# server_settings

このテーブルには、`config.xml`で指定されたサーバーのグローバル設定に関する情報が含まれています。現在、このテーブルは`config.xml`の第一階層の設定のみを表示しており、ネストされた設定（例: [logger](../../operations/server-configuration-parameters/settings.md#logger)）には対応していません。

カラム:

- `name` ([String](../../sql-reference/data-types/string.md)) — サーバー設定の名前。
- `value` ([String](../../sql-reference/data-types/string.md)) — サーバー設定の値。
- `default` ([String](../../sql-reference/data-types/string.md)) — サーバー設定のデフォルト値。
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 設定が`config.xml`で指定されたかどうかを示します。
- `description` ([String](../../sql-reference/data-types/string.md)) — サーバー設定の短い説明。
- `type` ([String](../../sql-reference/data-types/string.md)) — サーバー設定の値のタイプ。
- `changeable_without_restart` ([Enum8](../../sql-reference/data-types/enum.md)) — サーバーの実行中に設定を変更できるかどうかを示します。値:
    - `'No' `
    - `'IncreaseOnly'`
    - `'DecreaseOnly'`
    - `'Yes'`
- `is_obsolete` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) - 設定が廃止されているかどうかを示します。

**例**

以下の例は、`thread_pool`を名前に含むサーバー設定に関する情報を取得する方法を示しています。

``` sql
SELECT *
FROM system.server_settings
WHERE name LIKE '%thread_pool%'
```

``` text
┌─name──────────────────────────────────────────┬─value─┬─default─┬─changed─┬─description─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─type───┬─changeable_without_restart─┬─is_obsolete─┐
│ max_thread_pool_size                          │ 10000 │ 10000   │       0 │ クエリ実行とバックグラウンド操作のためにOSから割り当てられるスレッドの最大数。                                                                                         │ UInt64 │                         No │           0 │
│ max_thread_pool_free_size                     │ 1000  │ 1000    │       0 │ 一度割り当てられるとグローバルスレッドプールに常に残り、タスクの不足時にアイドル状態でいるスレッドの最大数。                                                   │ UInt64 │                         No │           0 │
│ thread_pool_queue_size                        │ 10000 │ 10000   │       0 │ 実行待ちのタスクをキューに置くことができる最大数。                                                                                                               │ UInt64 │                         No │           0 │
│ max_io_thread_pool_size                       │ 100   │ 100     │       0 │ IO操作に使用されるスレッドの最大数。                                                                                                                             │ UInt64 │                         No │           0 │
│ max_io_thread_pool_free_size                  │ 0     │ 0       │       0 │ IOスレッドプールの最大フリーサイズ。                                                                                                                             │ UInt64 │                         No │           0 │
│ io_thread_pool_queue_size                     │ 10000 │ 10000   │       0 │ IOスレッドプールのキューサイズ。                                                                                                                                 │ UInt64 │                         No │           0 │
│ max_active_parts_loading_thread_pool_size     │ 64    │ 64      │       0 │ 起動時にアクティブなデータパーツを読み込むスレッドの数。                                                                                                          │ UInt64 │                         No │           0 │
│ max_outdated_parts_loading_thread_pool_size   │ 32    │ 32      │       0 │ 起動時に非アクティブなデータパーツを読み込むスレッドの数。                                                                                                        │ UInt64 │                         No │           0 │
│ max_unexpected_parts_loading_thread_pool_size │ 32    │ 32      │       0 │ 起動時に予期しないデータパーツを読み込むスレッドの数。                                                                                                           │ UInt64 │                         No │           0 │
│ max_parts_cleaning_thread_pool_size           │ 128   │ 128     │       0 │ 非アクティブなデータパーツを同時に削除するためのスレッド数。                                                                                                     │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_size               │ 1000  │ 1000    │       0 │ BACKUPクエリのIO操作に使用されるスレッドの最大数。                                                                                                               │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_free_size          │ 0     │ 0       │       0 │ バックアップIOスレッドプールの最大フリーサイズ。                                                                                                                 │ UInt64 │                         No │           0 │
│ backups_io_thread_pool_queue_size             │ 0     │ 0       │       0 │ バックアップIOスレッドプールのキューサイズ。                                                                                                                     │ UInt64 │                         No │           0 │
└───────────────────────────────────────────────┴───────┴─────────┴─────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴────────────────────────────┴─────────────┘

```

`WHERE changed`を使用することは、たとえば、構成ファイル内の設定が正しく読み込まれているか、使用されているかを確認したい場合に有用です。

<!-- -->

``` sql
SELECT * FROM system.server_settings WHERE changed AND name='max_thread_pool_size'
```

**関連項目**

- [設定](../../operations/system-tables/settings.md)
- [構成ファイル](../../operations/configuration-files.md)
- [サーバー設定](../../operations/server-configuration-parameters/settings.md)

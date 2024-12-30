---
slug: /ja/operations/system-tables/settings
---
# settings

現在のユーザーのセッション設定に関する情報を含んでいます。

カラム:

- `name` ([String](../../sql-reference/data-types/string.md)) — 設定名。
- `value` ([String](../../sql-reference/data-types/string.md)) — 設定値。
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 設定がデフォルト値から変更されているかどうかを示します。
- `description` ([String](../../sql-reference/data-types/string.md)) — 設定の簡単な説明。
- `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — [制約](../../operations/settings/constraints-on-settings.md#constraints-on-settings)を通じて設定された最小値があればその値。最小値がない場合は[NULL](../../sql-reference/syntax.md#null-literal)が含まれます。
- `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — [制約](../../operations/settings/constraints-on-settings.md#constraints-on-settings)を通じて設定された最大値があればその値。最大値がない場合は[NULL](../../sql-reference/syntax.md#null-literal)が含まれます。
- `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 現在のユーザーが設定を変更できるかどうかを示します:
    - `0` — 現在のユーザーは設定を変更できます。
    - `1` — 現在のユーザーは設定を変更できません。
- `default` ([String](../../sql-reference/data-types/string.md)) — 設定のデフォルト値。
- `is_obsolete` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) - 設定が廃止されているかどうかを示します。
- `tier` ([Enum8](../../sql-reference/data-types/enum.md)) — この機能のサポートレベル。ClickHouseの機能は階層に組織化されており、開発の現在の状況と使用時の期待に応じて変化します。値:
    - `'Production'` — 機能は安定しており、安全に使用でき、他の**本番用**機能と問題なく相互作用します。
    - `'Beta'` — 機能は安定しており安全です。他の機能と一緒に使用する際の結果は未知であり、正確性は保証されません。テストや報告が歓迎されます。
    - `'Experimental'` — 機能は開発中です。開発者やClickHouseの愛好者のみが対象です。機能が動作するかどうかは不明であり、いつでも削除される可能性があります。
    - `'Obsolete'` — もはやサポートされていません。すでに削除されているか、将来のリリースで削除される予定です。

**例**

次の例は、名前に`min_i`を含む設定に関する情報を取得する方法を示しています。

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_insert_block_size_%'
FORMAT Vertical
```

``` text
Row 1:
──────
name:        min_insert_block_size_rows
value:       1048449
changed:     0
description: テーブルに`INSERT`クエリによって挿入できるブロック内の最小行数を設定します。小さなサイズのブロックは大きなものに圧縮されます。

可能な値:

- 正の整数。
- 0 — 圧縮無効。
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     1048449
alias_for:   
is_obsolete: 0
tier:        Production

Row 2:
──────
name:        min_insert_block_size_bytes
value:       268402944
changed:     0
description: テーブルに`INSERT`クエリによって挿入できるブロック内の最小バイト数を設定します。小さなサイズのブロックは大きなものに圧縮されます。

可能な値:

- 正の整数。
- 0 — 圧縮無効。
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     268402944
alias_for:   
is_obsolete: 0
tier:        Production

Row 3:
──────
name:        min_insert_block_size_rows_for_materialized_views
value:       0
changed:     0
description: テーブルに`INSERT`クエリによって挿入できるブロック内の最小行数を設定します。小さなサイズのブロックは大きなものに圧縮されます。この設定は[materialized view](../../sql-reference/statements/create/view.md)に挿入されるブロックにのみ適用されます。この設定を調整することで、materialized viewへのプッシュ中のブロック圧縮を制御し、過剰なメモリ使用を避けることができます。

可能な値:

- 任意の正の整数。
- 0 — 圧縮無効。

**関連項目**

- [min_insert_block_size_rows](#min-insert-block-size-rows)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:   
is_obsolete: 0
tier:        Production

Row 4:
──────
name:        min_insert_block_size_bytes_for_materialized_views
value:       0
changed:     0
description: テーブルに`INSERT`クエリによって挿入できるブロック内の最小バイト数を設定します。小さなサイズのブロックは大きなものに圧縮されます。この設定は[materialized view](../../sql-reference/statements/create/view.md)に挿入されるブロックにのみ適用されます。この設定を調整することで、materialized viewへのプッシュ中のブロック圧縮を制御し、過剰なメモリ使用を避けることができます。

可能な値:

- 任意の正の整数。
- 0 — 圧縮無効。

**関連項目**

- [min_insert_block_size_bytes](#min-insert-block-size-bytes)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:   
is_obsolete: 0
tier:        Production
 ```

`WHERE changed`の使用は、例えば以下の内容を確認したいときに有効です。

- 設定ファイル内の設定が正しく読み込まれて使用されているかどうか。
- 現在のセッションで変更された設定。

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**関連項目**

- [Settings](../../operations/settings/index.md#session-settings-intro)
- [クエリの権限](../../operations/settings/permissions-for-queries.md#settings_readonly)
- [設定に対する制約](../../operations/settings/constraints-on-settings.md)
- [SHOW SETTINGS](../../sql-reference/statements/show.md#show-settings) ステートメント

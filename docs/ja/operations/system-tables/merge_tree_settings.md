---
slug: /ja/operations/system-tables/merge_tree_settings
title: merge_tree_settings
---

`MergeTree` テーブルの設定に関する情報を含みます。

カラム:

- `name` ([String](../../sql-reference/data-types/string.md)) — 設定名。
- `value` ([String](../../sql-reference/data-types/string.md)) — 設定値。
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 設定が設定ファイルで明示的に定義されたか、明示的に変更されたかどうか。
- `description` ([String](../../sql-reference/data-types/string.md)) — 設定の説明。
- `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — [制約](../../operations/settings/constraints-on-settings.md#constraints-on-settings)により設定される最小値がある場合、その最小値。最小値がない場合、[NULL](../../sql-reference/syntax.md#null-literal)を含む。
- `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — [制約](../../operations/settings/constraints-on-settings.md#constraints-on-settings)により設定される最大値がある場合、その最大値。最大値がない場合、[NULL](../../sql-reference/syntax.md#null-literal)を含む。
- `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 現在のユーザーが設定を変更できるかどうかを示す:
    - `0` — 現在のユーザーは設定を変更できる。
    - `1` — 現在のユーザーは設定を変更できない。
- `type` ([String](../../sql-reference/data-types/string.md)) — 設定タイプ（実装に特有の文字列値）。
- `is_obsolete` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) - 設定が廃止されているかどうかを示す。
- `tier` ([Enum8](../../sql-reference/data-types/enum.md)) — この機能のサポートレベル。ClickHouseの機能は、開発の現状と利用時の期待に応じた異なる段階に整理される。値:
    - `'Production'` — 機能は安定しており、安全に使用できる。**プロダクション**の他の機能と相互作用する際に問題はない。
    - `'Beta'` — 機能は安定しており、安全である。他の機能と一緒に使用した場合の結果は未知であり、正確性は保証されていない。テストと報告が歓迎される。
    - `'Experimental'` — 機能は開発中である。開発者とClickHouse愛好者のためにのみ意図されている。機能は動作するかもしれず、されないかもしれず、いつでも削除される可能性がある。
    - `'Obsolete'` — もはやサポートされない。すでに削除されているか、将来のリリースで削除される予定である。

**例**
```sql
SELECT * FROM system.merge_tree_settings LIMIT 4 FORMAT Vertical;
```

```response
Row 1:
──────
name:        min_compress_block_size
value:       0
changed:     0
description: グラニュールが書き込まれるとき、保留中の非圧縮データのサイズが指定された閾値以上またはそれ以上の場合、バッファ内のデータを圧縮します。この設定が設定されていない場合、対応するグローバル設定が使用されます。
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

Row 2:
──────
name:        max_compress_block_size
value:       0
changed:     0
description: バッファ内の保留中の非圧縮データが指定された閾値以上のサイズである場合、それを圧縮します。データブロックは現在のグラニュールが終了していなくても圧縮されます。この設定が設定されていない場合、対応するグローバル設定が使用されます。
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

Row 3:
──────
name:        index_granularity
value:       8192
changed:     0
description: 1つの主キー値に対応する行数。
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

Row 4:
──────
name:        max_digestion_size_per_segment
value:       268435456
changed:     0
description: GINインデックスを構築するためにセグメントごとに消化する最大バイト数。
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

4 rows in set. Elapsed: 0.009 sec.
```

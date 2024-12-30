---
slug: /ja/sql-reference/aggregate-functions/reference/grouparraysample
sidebar_position: 145
---

# groupArraySample

サンプル引数値の配列を作成します。生成される配列のサイズは `max_size` 要素に制限されています。引数値はランダムに選択され、配列に追加されます。

**構文**

``` sql
groupArraySample(max_size[, seed])(x)
```

**引数**

- `max_size` — 生成される配列の最大サイズ。 [UInt64](../../data-types/int-uint.md)。
- `seed` — 乱数生成器のシード。オプション。[UInt64](../../data-types/int-uint.md)。デフォルト値: `123456`。
- `x` — 引数（カラム名または式）。

**返される値**

- ランダムに選ばれた `x` 引数の配列。

型: [Array](../../data-types/array.md)。

**例**

テーブル `colors` を考えます:

``` text
┌─id─┬─color──┐
│  1 │ red    │
│  2 │ blue   │
│  3 │ green  │
│  4 │ white  │
│  5 │ orange │
└────┴────────┘
```

カラム名を引数とするクエリ:

``` sql
SELECT groupArraySample(3)(color) as newcolors FROM colors;
```

結果:

```text
┌─newcolors──────────────────┐
│ ['white','blue','green']   │
└────────────────────────────┘
```

異なるシードを指定したカラム名を引数とするクエリ:

``` sql
SELECT groupArraySample(3, 987654321)(color) as newcolors FROM colors;
```

結果:

```text
┌─newcolors──────────────────┐
│ ['red','orange','green']   │
└────────────────────────────┘
```

式を引数とするクエリ:

``` sql
SELECT groupArraySample(3)(concat('light-', color)) as newcolors FROM colors;
```

結果:

```text
┌─newcolors───────────────────────────────────┐
│ ['light-blue','light-orange','light-green'] │
└─────────────────────────────────────────────┘
```

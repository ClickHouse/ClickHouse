---
alias: []
description: 'PNG画像出力フォーマットのドキュメント'
input_format: false
keywords: ['PNG']
output_format: true
slug: /interfaces/formats/PNG
title: 'PNG'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✗  | ✔  | ✗     |

<div id="description">
  ## 説明
</div>

クエリの結果を PNG イメージとしてレンダリングします。組み込みの可視化ツールとして便利です。

出力イメージのサイズは、設定
[`output_format_image_width`](/ja/operations/settings/formats#output_format_image_width) と
[`output_format_image_height`](/ja/operations/settings/formats#output_format_image_height)
で固定されます
(どちらのデフォルト値も 1024) 。結果でカバーされないピクセルは黒で塗りつぶされます
(`RGB` およびグレースケールモードの場合) 。`RGBA` モードでは透明な黒になります。

カラーモードは、結果のカラム名と型に基づいて自動的に決定されます。

| Columns            | Mode                                            |
| ------------------ | ----------------------------------------------- |
| `r`, `g`, `b`      | 8-bit RGB                                       |
| `r`, `g`, `b`, `a` | 8-bit RGBA                                      |
| 整数型の `v`           | 8-bit グレースケール                                   |
| `Float*` 型の `v`    | 8-bit グレースケール (`[0, 1]` の値 → `[0, 255]`)        |
| `Bool` 型の `v`      | Binary (8-bit グレースケールとしてレンダリング: `0` または `255`)  |

カラム名は大文字・小文字を区別せずに照合されます。カラーモードを一意に
判定できない場合 (例: 不明なカラム名、`v` と `r`/`g`/`b`/`a` の混在、または `r`/`g`/`b` のいずれかが欠けている場合) 、
クエリは例外をスローします。

ピクセルチャネルについては、整数値は `[0, 255]` にクランプされ、浮動小数点値
は `[0, 1]` にクランプされた後、`[0, 255]` にスケーリングされます。

イメージ内での各レコードの位置は、次の 2 つのモードのいずれかで決まります。

* **暗黙的** (デフォルト。`x` と `y` のどちらも存在しない場合) 。各レコードは
  1 つのピクセルに対応し、ピクセルは走査線順、つまり左から右、上から下へと埋められます。
* **明示的** (`x` と `y` のカラムが存在し、両方とも整数型の場合) 。
  `x` カラムと `y` カラムでピクセル座標を指定します。座標がイメージの範囲外にあるレコードは
  何も通知されずに無視されます。同じ座標を持つレコードが複数ある場合は、
  最後のレコードが優先されます (画家のアルゴリズム) 。

<div id="example-usage">
  ## 使用例
</div>

<div id="implicit-rgb">
  ### 暗黙的な座標 (1行につき1ピクセル) 、RGB
</div>

```sql
SELECT
    toUInt8(x * 25) AS r,
    toUInt8(y * 25) AS g,
    toUInt8((x + y) * 12) AS b
FROM
(
    SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100)
)
INTO OUTFILE 'gradient.png'
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10;
```

<div id="explicit-grayscale">
  ### 明示的な座標とグレースケール
</div>

```sql
SELECT
    toInt32(x) AS x,
    toInt32(y) AS y,
    toUInt8(intensity) AS v
FROM points
INTO OUTFILE 'points.png'
FORMAT PNG
SETTINGS output_format_image_width = 512, output_format_image_height = 512;
```

<div id="terminal-mode">
  ## ターミナルで画像を表示する
</div>

デフォルトでは、`PNG` フォーマットは画像の生バイト列を書き出します。設定
[`output_format_image_terminal_mode`](/ja/operations/settings/formats#output_format_image_terminal_mode)
を使うと、代わりにインライン画像プロトコルを使用して、画像をターミナルに直接描画できます。

| 値              | 動作                                                                                           |
| -------------- | -------------------------------------------------------------------------------------------- |
| &#96;&#96; (空) | 画像の生バイト列を書き出します (デフォルト) 。                                                                    |
| `iterm`        | iTerm2 のインライン画像プロトコルを使用します。                                                                  |
| `kitty`        | Kitty graphics プロトコルを使用します。                                                                  |
| `sixel`        | Sixel プロトコルを使用します。画像は固定の 6×6×6 パレットに減色され、アルファチャネルがある場合は黒い背景に合成されます。                          |
| `auto`         | 出力先がターミナルの場合は、その対応機能を検出して `iterm`、`kitty`、または `sixel` を使用します (この順) 。それ以外の場合は画像の生バイト列を書き出します。 |

```sql
SELECT toUInt8(x * 25) AS r, toUInt8(y * 25) AS g, toUInt8((x + y) * 12) AS b
FROM (SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100))
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10, output_format_image_terminal_mode = 'auto';
```

<div id="format-settings">
  ## フォーマット設定
</div>

| Setting                             | Description                   | Default         |
| ----------------------------------- | ----------------------------- | --------------- |
| `output_format_image_width`         | 出力イメージの幅 (ピクセル単位) 。           | `1024`          |
| `output_format_image_height`        | 出力イメージの高さ (ピクセル単位) 。          | `1024`          |
| `output_format_image_terminal_mode` | インラインのターミナルイメージプロトコル (上記参照) 。 | &#96;&#96; (空)  |
---
alias: []
description: 'Documentation for the PNG image output format'
input_format: false
keywords: ['PNG']
output_format: true
slug: /interfaces/formats/PNG
title: 'PNG'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |   ✗   |

## Description {#description}

Renders the result of a query as a PNG image. This is useful as a built-in visualization tool.

The size of the output image is fixed by the settings
[`output_format_image_width`](/operations/settings/formats#output_format_image_width) and
[`output_format_image_height`](/operations/settings/formats#output_format_image_height)
(both default to 1024). Pixels that are not covered by the result are filled with black
(in `RGB` and grayscale modes) or with transparent black (in `RGBA` mode).

The color mode is determined automatically from the column names and types of the result:

| Columns                | Mode                                              |
|------------------------|---------------------------------------------------|
| `r`, `g`, `b`          | 8-bit RGB                                         |
| `r`, `g`, `b`, `a`     | 8-bit RGBA                                        |
| `v` of integer type    | 8-bit grayscale                                   |
| `v` of `Float*` type   | 8-bit grayscale (values in `[0, 1]` → `[0, 255]`) |
| `v` of `Bool` type     | Binary (rendered as 8-bit grayscale: `0` or `255`)|

Column names are matched case-insensitively. If the color mode cannot be unambiguously
determined (e.g. unknown column names, mixed `v` with `r`/`g`/`b`/`a`, or one of `r`/`g`/`b` missing),
the query throws an exception.

For pixel channels, integer values are clamped to `[0, 255]` and floating-point values
are clamped to `[0, 1]` and then scaled to `[0, 255]`.

The position of each record in the image is determined by one of two modes:

- **Implicit** (the default — when neither `x` nor `y` is present). Each record corresponds
  to a single pixel; pixels are filled in scanline order: left to right, top to bottom.
- **Explicit** (when `x` and `y` columns are present, both of integer types).
  The `x` and `y` columns give the pixel coordinates. Records with coordinates outside
  the image are silently ignored. In case of multiple records with the same coordinates,
  the last one wins (painter's algorithm).

## Example usage {#example-usage}

### Implicit coordinates (row-per-pixel), RGB {#implicit-rgb}

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

### Explicit coordinates, grayscale {#explicit-grayscale}

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

## Displaying images in the terminal {#terminal-mode}

By default, the `PNG` format writes the raw image bytes. The setting
[`output_format_image_terminal_mode`](/operations/settings/formats#output_format_image_terminal_mode)
makes the format render the image directly to the terminal using an inline image protocol instead:

| Value           | Behaviour                                                                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|
| `` (empty)      | Write the raw image bytes (the default).                                                                |
| `iterm`         | Use the iTerm2 inline image protocol.                                                                   |
| `kitty`         | Use the Kitty graphics protocol.                                                                        |
| `sixel`         | Use the Sixel protocol. The image is reduced to a fixed 6×6×6 palette and the alpha channel, if any, is composited over a black background. |
| `auto`          | If the output is a terminal, detect its capabilities and use `iterm`, `kitty`, or `sixel` (in this order); otherwise write the raw image bytes. |

```sql
SELECT toUInt8(x * 25) AS r, toUInt8(y * 25) AS g, toUInt8((x + y) * 12) AS b
FROM (SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100))
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10, output_format_image_terminal_mode = 'auto';
```

## Format settings {#format-settings}

| Setting                              | Description                                  | Default    |
|--------------------------------------|----------------------------------------------|------------|
| `output_format_image_width`          | Width of the output image in pixels.         | `1024`     |
| `output_format_image_height`         | Height of the output image in pixels.        | `1024`     |
| `output_format_image_terminal_mode`  | Inline terminal image protocol (see above).  | `` (empty) |

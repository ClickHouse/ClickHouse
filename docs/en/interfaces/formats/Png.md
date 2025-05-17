---
alias: []
description: 'Documentation for the PNG image format'
input_format: false
keywords: ['Png']
output_format: true
slug: /interfaces/formats/Png
title: 'Png'
---


| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |   ✗   |

	
## Description {#description}

Export query results directly as PNG images for visualization purposes. This format converts numeric data into pixel values, supporting `RGBA`, `RGB`, `grayscale`, and `binary` pixel formats with `8` or `16-bit` color depth and compression level control. You can control pixel coordinates explicitly or map each row to a pixel automatically

## Example Usage {#example-usage}
- `Implicit (row-per-pixel) coordinates`:
```sql
SELECT 
    r, g, b, a 
FROM table
INTO OUTFILE 'output.png' 
FORMAT PNG
SETTINGS 
    output_png_image_pixel_format = 'RGBA', 
    output_png_image_max_width=1512,
    output_png_image_max_height=1502,
```

- `Explicit coordinates`:
```sql
SELECT 
    x, y, r, g, b, a
FROM table
INTO OUTFILE 'output.png'
FORMAT PNG
SETTINGS 
    output_png_image_pixel_format = 'RGBA', 
    output_png_image_max_width=1512,
    output_png_image_max_height=1502,
    output_png_image_input_mode = 'EXPLICIT_COORDINATES'
```

## Format Settings {#format-settings}
When working with `PNG` format you can control settings:

- `output_png_image_pixel_format`: Color channel: `RGBA`, `RGB` (default), `Grayscale` or `Binary` (case-insensitive)

- `output_png_image_max_width`: Maximum image width in pixels (default: `4096`). The resulting image will have a width up to this value

- `output_png_image_max_height`: Maximum image height in pixels (default: `4096`). The resulting image will have a width up to this value

- `output_png_image_bit_depth`: Bit depth per channel (requires matching input data range) (default: `8`)

- `output_png_image_compression_level`: Image compression level. Possible range: `[-1, 9]`, where `0` is `no compression` and `9` is the `fastest`. Default: `-1` (approximately is at level `6`)

- `output_png_image_pixel_input_mode`: Pixel input format. Possible values: `SCANLINE` (default) - Row-per-pixel approach where only pixel data is provided in scanline order (left to right, top to bottom) and `EXPLICIT_COORDINATES` — Each pixel is specified with explicit coordinates (x, y)

### Requirements

* Input values must align with bit depth range (would be clamped)

* Supports only numeric types (`UInt*`, `Int*`, `Float*`) for pixels and only integers for coordinates

* Supports `LowCardinality`, `Nullable` and `Const` columns
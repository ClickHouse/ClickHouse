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

Export query results directly as PNG images for visualization purposes. This format converts numeric data into pixel values, supporting `RGBA`, `RGB`, `grayscale`, and `binary` pixel formats with `8` or `16-bit` color depth and compression level control

## Example Usage {#example-usage}

```sql
SELECT 
    r, g, b, a 
FROM file('pixel_cloud_data.csv', CSV)
INTO OUTFILE 'output.png' 
FORMAT PNG
```

## Format Settings {#format-settings}
When working with `PNG` format you can control settings:

- `output_png_image_pixel_format`: Color channel: `RGBA`, `RGB` (default), `Grayscale` or `Binary` (case-insensitive)

- `output_png_image_max_width`: Maximum image width in pixels (default: `4096`)

- `output_png_image_max_height`: Maximum image height in pixels (default: `4096`) 

- `output_png_image_bit_depth`: Bit depth per channel (requires matching input data range) (default: `8`)

- `output_png_image_compression_level`: Image compression level. Possible range: `[-1, 9]`, where `0` is `no compression` and `9` is the `fastest`. Default: `-1` (approximately is at level `6`)


### Requirements

* Column count must match pixel format channels (e.g. 4 columns for `RGBA`)

* Input values must align with bit depth range (clamped if out-of-range)

* Supports only numeric types (`UInt*`, `Int*`, `Float*`)

* Supports `LowCardinality`, `Nullable` and `Const` columns
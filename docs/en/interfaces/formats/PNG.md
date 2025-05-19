---
alias: []
description: 'Documentation for the PNG image output format'
input_format: false
keywords: ['PNG']
output_format: true
title: 'PNG'
---


| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |   ✗   |

	
## Description 

Export query results directly as PNG images for visualization purposes. This format converts numeric data into pixel values, supporting `RGBA`, `RGB`, `Grayscale`, and `Binary` pixel formats with `8` or `16-bit` color depth and compression level control. You can control pixel coordinates explicitly or map each row to a pixel automatically (implicit).

**Important:** Input tables must have columns named according to the selected format:


| Pixels | Required Columns          | 
|--------------|---------------------------|
| RGBA         | `r`, `g`, `b`, `a`        | 
| RGB          | `r`, `g`, `b`             |                              
| Grayscale    | `v`                       | 
| Binary       | `v`                       |


| Coordinates | Required Columns          |
|-------------|---------------------------| 
|Explicit     | `x`, `y` plus above       | 



## Example Usage 
- `Implicit (row-per-pixel) coordinates` (default):
```sql
SELECT 
    toUInt8(col1) as r, 
    toUInt8(col2) as g,
    toUInt8(col3) as b, 
    toUInt8(col3) as a 
FROM table
INTO OUTFILE 'output.png'
FORMAT PNG
SETTINGS 
    output_format_png_pixel_output_format='RGBA', 
    output_format_png_pixel_coordinates_format='IMPLICIT'
    output_format_png_max_width=1512,
    output_format_png_max_height=1512,
    output_format_png_bit_depth=8
```

- `Explicit coordinates`:
```sql
SELECT 
    toInt32(col1) as x,
    toInt32(col2) as y,
    toUInt8(col3) as v
FROM table
INTO OUTFILE 'output.png'
FORMAT PNG
SETTINGS 
    output_format_png_pixel_output_format='Grayscale', 
    output_format_png_pixel_coordinates_format='EXPLICIT'
    output_format_png_max_width=14014,
    output_format_png_max_height=6659
```

## Format Settings 
When working with `PNG` format you can control settings:

- `output_format_png_pixel_format`: Color channel: `RGBA`, `RGB` (default), `Grayscale` or `Binary`

- `output_format_png_max_width`: Maximum image width in pixels (default: `4096`). The resulting image will have a width up to this value

- `output_format_png_max_height`: Maximum image height in pixels (default: `4096`). The resulting image will have a width up to this value

- `output_format_png_bit_depth`: Bit depth per channel (requires matching input data range) (default: `8`)

- `output_format_png_compression_level`: Image compression level. Possible range: `[-1, 9]`, where `0` is `no compression` and `9` is the `fastest`. Default: `-1` (approximately is at level `6`)

- `output_format_png_coordinates_format`: Coordinates format. Possible values: `IMPLICIT` (default) - Row-per-pixel approach where only columns with pixel data as row-per-pixel order (left to right, top to bottom) and `EXPLICIT` — Each pixel in your query is specified with explicit coordinates (x, y)

### Requirements

* Input values must align with bit depth range (would be clamped if not)

* Supports only numeric types (`UInt*`, `Int*`, `Float*` and `Bool`) for pixels and only integers for coordinates

* Supports `LowCardinality`, `Nullable` and `Const` columns

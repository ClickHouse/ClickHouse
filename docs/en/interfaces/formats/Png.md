description 	keywords 	slug 	title
Documentation for the PNG image format
	
PNG, image export, visualization
	
/interfaces/formats/PNG


| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |   ✗   |

	
## Description {#description}

Export query results directly as PNG images for visualization purposes. This format converts numeric data into pixel values, supporting RGBA, RGB, Grayscale, and Binary pixel formats with 8-bit or 16-bit color depth. Designed for outputting heatmaps, charts, or processed image data.

Example Usage {#example-usage}

```sql
SELECT 
    r, g, b, a 
FROM file('pixel_data.csv', CSV)
INTO OUTFILE 'output.png' 
FORMAT PNG
SETTINGS 
    output_png_image_pixel_format = 'RGBA',
    output_png_image_max_width = 1200,
    output_png_image_max_height = 1200,
    output_png_image_bit_depth = 16

Format Settings {#format-settings}
Setting	Description	Values
output_png_image_pixel_format	Defines color channels in output image	RGBA, RGB, Grayscale, Binary
output_png_image_max_width	Maximum image width in pixels	Any positive integer
output_png_image_max_height	Maximum image height in pixels	Any positive integer
output_png_image_bit_depth	Bit depth per channel (requires matching input data range)	8 (0-255), 16 (0-65535)

Requirements:

    Column count must match pixel format channels (e.g., 4 columns for RGBA).

    Input values must align with bit depth range (clamped if out-of-range).

    Supports only numeric types (UInt*, Int*, Float*).
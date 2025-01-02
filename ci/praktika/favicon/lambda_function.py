import base64
import random
import struct
import zlib


def create_favicon():
    # Image dimensions
    width = 32
    height = 32

    # Initialize a transparent background image (RGBA: 4 bytes per pixel)
    image_data = bytearray(
        [0, 0, 0, 0] * width * height
    )  # Set alpha to 0 for transparency

    # Draw 4 vertical lines with color #FAFF68 (RGB: 250, 255, 104)
    line_color = [250, 255, 104, 255]  # RGBA for #FAFF68 with full opacity
    line_width = 4
    space_width = 3
    x_start = space_width
    line_number = 4

    line_height = height - space_width

    for i in range(line_number):
        # Randomly pick a starting y position for each line
        y_start = random.randint(0, height - 1)

        # Draw the line with random shift along Y-axis
        for y in range(line_height):
            y_pos = (y + y_start) % height
            for x in range(line_width):
                pixel_index = (y_pos * width + x_start + x) * 4
                image_data[pixel_index : pixel_index + 4] = line_color

        x_start += line_width + space_width

    # Convert the RGBA image to PNG format
    png_data = create_png(width, height, image_data)

    # Convert PNG to ICO format
    ico_data = create_ico(png_data)

    return ico_data


def create_png(width, height, image_data):
    def write_chunk(chunk_type, data):
        chunk_len = struct.pack(">I", len(data))
        chunk_crc = struct.pack(">I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
        return chunk_len + chunk_type + data + chunk_crc

    png_signature = b"\x89PNG\r\n\x1a\n"
    ihdr_chunk = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    idat_data = zlib.compress(
        b"".join(
            b"\x00" + image_data[y * width * 4 : (y + 1) * width * 4]
            for y in range(height)
        ),
        9,
    )
    idat_chunk = write_chunk(b"IDAT", idat_data)
    iend_chunk = write_chunk(b"IEND", b"")

    return png_signature + write_chunk(b"IHDR", ihdr_chunk) + idat_chunk + iend_chunk


def create_ico(png_data):
    # ICO header: reserved (2 bytes), type (2 bytes), image count (2 bytes)
    ico_header = struct.pack("<HHH", 0, 1, 1)
    # ICO entry: width, height, color count, reserved, color planes, bits per pixel, size, offset
    ico_entry = struct.pack("<BBBBHHII", 32, 32, 0, 0, 1, 32, len(png_data), 22)
    return ico_header + ico_entry + png_data


def save_favicon_to_disk(ico_data, file_path="favicon.ico"):
    with open(file_path, "wb") as f:
        f.write(ico_data)
    print(f"Favicon saved to {file_path}")


def lambda_handler(event, context):
    # Generate the favicon
    favicon_data = create_favicon()

    # Return the favicon as a binary response
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "image/x-icon",
            "Content-Disposition": 'inline; filename="favicon.ico"',
        },
        "body": base64.b64encode(favicon_data).decode("utf-8"),
        "isBase64Encoded": True,
    }


# Optional: Call the function directly to generate and save favicon locally (if running outside Lambda)
if __name__ == "__main__":
    favicon_data = create_favicon()
    save_favicon_to_disk(favicon_data)

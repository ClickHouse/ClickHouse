#!/usr/bin/env python3
import argparse
import random
import struct
import zlib
from pathlib import Path


BASE_WIDTH = 32
BASE_HEIGHT = 32
LINE_WIDTH = 4
SPACE_WIDTH = 3
LINE_NUMBER = 4
LINE_COLOR = (0, 0, 0, 255)
TRANSPARENT = (0, 0, 0, 0)


def png_chunk(kind, data):
    return (
        struct.pack(">I", len(data))
        + kind
        + data
        + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
    )


def write_png(path, width, height, pixels):
    rows = []
    for y in range(height):
        row = bytearray([0])
        for x in range(width):
            row.extend(pixels[y][x])
        rows.append(bytes(row))

    raw = b"".join(rows)
    png = b"".join(
        [
            b"\x89PNG\r\n\x1a\n",
            png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)),
            png_chunk(b"IDAT", zlib.compress(raw, 9)),
            png_chunk(b"IEND", b""),
        ]
    )
    path.write_bytes(png)


def create_base_icon(rng):
    pixels = [[TRANSPARENT for _ in range(BASE_WIDTH)] for _ in range(BASE_HEIGHT)]

    x_start = SPACE_WIDTH
    for _ in range(LINE_NUMBER):
        y_start = rng.randrange(BASE_HEIGHT)

        for y in range(BASE_HEIGHT - SPACE_WIDTH):
            y_pos = (y + y_start) % BASE_HEIGHT
            for x in range(x_start, x_start + LINE_WIDTH):
                pixels[y_pos][x] = LINE_COLOR

        x_start += LINE_WIDTH + SPACE_WIDTH

    return pixels


def scale_with_padding(base_pixels, output_size, padding):
    inner_size = round(output_size * (1 - padding * 2))
    offset = (output_size - inner_size) // 2
    pixels = [[TRANSPARENT for _ in range(output_size)] for _ in range(output_size)]

    for y in range(inner_size):
        source_y = (y * BASE_HEIGHT) // inner_size
        target_y = offset + y
        for x in range(inner_size):
            source_x = (x * BASE_WIDTH) // inner_size
            target_x = offset + x
            pixels[target_y][target_x] = base_pixels[source_y][source_x]

    return pixels


def main():
    default_output = Path(__file__).with_name("favicon-128-variant.png")
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=default_output)
    parser.add_argument("--size", type=int, default=128)
    parser.add_argument("--padding", type=float, default=0.15)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    base_pixels = create_base_icon(rng)
    pixels = scale_with_padding(base_pixels, args.size, args.padding)
    write_png(args.output, args.size, args.size, pixels)


if __name__ == "__main__":
    main()

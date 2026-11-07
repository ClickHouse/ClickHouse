#!/usr/bin/env python3
"""Express the pinned HDFS Yasm source in NASM syntax without changing code."""

from pathlib import Path
import re
import sys


def convert(source):
    # NASM requires explicit RIP-relative addressing and CRC32 operand sizes.
    # Keep Yasm's sign-extended immediate encoding instead of NASM's shorter
    # zero-extending move, preserving instruction offsets and the jump table.
    replacements = [
        (r'\[([^\]]+) wrt rip\]', r'[rel \1]', 2),
        (r'(crc32\s+(?:crc_init|crc1|crc2),\s*)(\[)', r'\1qword \2', 9),
        (r'(mov\s+rax, )(2731|128)', r'\1strict dword \2', 2),
    ]
    for pattern, replacement, expected in replacements:
        source, count = re.subn(pattern, replacement, source)
        if count != expected:
            raise ValueError(f'HDFS assembly changed: expected {expected} matches, found {count}')
    return source


if __name__ == '__main__':
    Path(sys.argv[2]).write_text(convert(Path(sys.argv[1]).read_text()))

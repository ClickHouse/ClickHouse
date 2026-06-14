#!/usr/bin/env python3
"""
This script reads the vendored Jieba dictionary, processes it, and serializes it into
a binary file suitable for use with a Double-Array Trie (darts-clone), then zstd-compresses it.

The input file `jieba.dict.utf8` is a snapshot of
https://raw.githubusercontent.com/yanyiwu/cppjieba/refs/heads/master/dict/jieba.dict.utf8
vendored alongside this script so the build is reproducible without network access
and independent of the (archived) upstream cppjieba repository.

Processing steps:
1. Read the dictionary lines from the vendored file.
2. Parse each line into a word and its optional weight (default 1.0).
3. Keep only BMP characters (codepoints <= 0xFFFF) to ensure each UTF-16 code unit is 2 bytes.
4. Encode each word into UTF-16-LE and replace any null bytes (0x00) with 0xF0 to avoid
   conflicts in darts-clone, which does not allow internal null bytes.
5. Normalize weights using log(weight / total_weight).
6. Sort the words lexicographically.
7. Build the Double-Array Trie using the UTF-16 encoded words, their lengths in bytes, and
   integer values (0..n-1).
8. Write a single little-endian binary file and zstd-compress it. ClickHouse only targets
   little-endian platforms, so a separate big-endian dictionary is not produced.

Binary file layout (uncompressed `dict_le.dat`):

+--------+----------------+-------------------------------------------+
| Offset | Size (bytes)   | Description                               |
+--------+----------------+-------------------------------------------+
| 0x00   | 8              | min_weight (float64)                       |
| 0x08   | 8              | num_elems (uint64, number of words)       |
| 0x10   | 8              | dat_size (uint64, bytes of trie array)    |
| 0x18   | 8 * num_elems  | weights array (float64)                   |
| ...    | 4 * dat_size   | trie data array (uint32 per element)      |
+--------+----------------+-------------------------------------------+

Notes:
- Null bytes in UTF-16 words are replaced to avoid trie conflicts.
- Values array corresponds to 0..n-1 indices of words.
- The runtime side must apply the same 0x00 -> 0xF0 byte replacement when
  encoding lookup keys; see `decodeUTF8Rune` in `jieba_common.h`.
"""

import numpy as np
import struct
import math
import os
import sys
import zstandard
from dartsclone import DoubleArray

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(SCRIPT_DIR, "jieba.dict.utf8")
with open(INPUT_PATH, "r", encoding="utf-8") as f:
    lines = f.read().splitlines()

keys_bytes = []
lengths = []
weights = []


for idx, line in enumerate(lines):
    parts = line.strip().split()
    if not parts:
        continue
    word = parts[0]
    weight = float(parts[1]) if len(parts) > 1 else 1.0

    if not all(ord(c) <= 0xFFFF for c in word):
        continue

    utf16_bytes = word.encode("utf-16-le").replace(b"\x00", b"\xF0")
    keys_bytes.append(utf16_bytes)
    lengths.append(len(utf16_bytes))
    weights.append(weight)

total_weight = sum(w for w in weights)
weights = [math.log(w / total_weight) for w in weights]

sorted_indices = sorted(range(len(keys_bytes)), key=lambda i: keys_bytes[i])
keys_bytes = [keys_bytes[i] for i in sorted_indices]
lengths = [lengths[i] for i in sorted_indices]
weights = np.array([weights[i] for i in sorted_indices], dtype=np.float64)
values = list(range(len(weights)))  # 0..n-1

for i, kb in enumerate(keys_bytes[:5]):
    orig_bytes = kb.replace(b"\xF0", b"\x00")
    word = orig_bytes.decode("utf-16-le")
    print(f"Word {i}: {word}, weight: {weights[i]}")

da = DoubleArray()
da.build(keys_bytes, lengths=lengths, values=values)
arr = np.frombuffer(da.array(), dtype=np.uint32)

header = struct.pack(
    "<dQQ",
    np.min(weights),
    len(weights),
    da.size(),
)
payload = header + weights.tobytes() + arr.tobytes()

with open(os.path.join(SCRIPT_DIR, "dict_le.dat"), "wb") as f:
    f.write(payload)

# Use the highest zstd level so the compressed dict (~3.5 MiB) stays under the 5 MiB
# in-tree size limit enforced by `ci/jobs/scripts/check_style/various_checks.sh`.
compressor = zstandard.ZstdCompressor(level=22)
with open(os.path.join(SCRIPT_DIR, "dict_le.dat.zst"), "wb") as f:
    f.write(compressor.compress(payload))

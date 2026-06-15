#!/usr/bin/env python3
"""
This script downloads the Jieba dictionary from a pinned cppjieba commit,
verifies its SHA-256 checksum, processes it, and serializes it into a binary
file suitable for use with a Double-Array Trie (darts-clone), then
zstd-compresses it.

The source URL points to a specific commit (not a branch ref), and the
response is verified against a hard-coded SHA-256 before use, so regeneration
is fully reproducible and resistant to upstream tampering even though the
cppjieba repository is archived.

Processing steps:
1. Download the dictionary lines from the pinned cppjieba commit and verify
   the SHA-256 checksum.
2. Parse each line into a word and its optional weight (default 1.0).
3. Keep only BMP characters (codepoints <= 0xFFFF) so each rune fits in `uint16_t`
   (matching the runtime `Rune` type in `jieba_common.h`).
4. Encode each rune (`uint16_t` codepoint) as 3 bytes in big-endian, with the
   high bit set in every byte (see `BYTES_PER_RUNE` in `jieba_dict.h`):

       byte0 = ((rune >> 12) & 0x0F) | 0x80   # bits 12..15 -> 0x80..0x8F
       byte1 = ((rune >>  6) & 0x3F) | 0x80   # bits  6..11 -> 0x80..0xBF
       byte2 = ( rune        & 0x3F) | 0x80   # bits  0.. 5 -> 0x80..0xBF

   This encoding is injective (no two distinct runes map to the same 3-byte
   sequence), contains no `0x00` bytes (`darts-clone` cannot store `\\0` inside
   keys), is endian-independent, and is lexicographically order-preserving so
   `Darts.build` accepts the sorted keys.
5. Drop dictionary entries containing pure-ASCII codepoints (`< 0x80`). The
   runtime never looks up ASCII tokens in the trie — the HMM segmenter handles
   English/digit runs separately. Mixed entries (e.g. `B超`) are kept; their
   ASCII parts use the same 3-byte encoding and never appear at lookup time
   because the runtime tokenizes mixed inputs in two separate passes.
6. Normalize weights using log(weight / total_weight).
7. Sort the words lexicographically by their encoded byte form.
8. Build the Double-Array Trie using the encoded keys, their lengths in bytes,
   and integer values (0..n-1).
9. Write a single little-endian binary file and zstd-compress it. ClickHouse
   only targets little-endian platforms, so a separate big-endian dictionary
   is not produced.

Binary file layout (uncompressed `dict_le.dat`):

+--------+----------------+-------------------------------------------+
| Offset | Size (bytes)   | Description                               |
+--------+----------------+-------------------------------------------+
| 0x00   | 8              | min_weight (float64)                      |
| 0x08   | 8              | num_elems (uint64, number of words)       |
| 0x10   | 8              | dat_size (uint64, bytes of trie array)    |
| 0x18   | 8 * num_elems  | weights array (float64)                   |
| ...    | 4 * dat_size   | trie data array (uint32 per element)      |
+--------+----------------+-------------------------------------------+
"""

import hashlib
import math
import os
import struct
import sys
import urllib.request

import numpy as np
import zstandard
from dartsclone import DoubleArray

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Pin to a specific cppjieba commit so regeneration is reproducible and the
# downloaded bytes are auditable. The cppjieba repository is archived, so this
# commit will not change underneath us; the checksum below is the additional
# defense against the upstream repository being tampered with.
CPPJIEBA_COMMIT = "eed6bfe483105d1db4bfbebaf796f60c173d6e84"
DICT_URL = f"https://raw.githubusercontent.com/yanyiwu/cppjieba/{CPPJIEBA_COMMIT}/dict/jieba.dict.utf8"
DICT_SHA256 = "6f7d4350e8861ef4139b2e3a6fad05430c19ae71f4b8378190edecac8aae2e6a"


def fetch_verified(url, expected_sha256):
    with urllib.request.urlopen(url) as f:
        data = f.read()
    actual_sha256 = hashlib.sha256(data).hexdigest()
    if actual_sha256 != expected_sha256:
        raise RuntimeError(
            f"SHA-256 mismatch for {url}: expected {expected_sha256}, got {actual_sha256}"
        )
    return data


lines = fetch_verified(DICT_URL, DICT_SHA256).decode("utf-8").splitlines()


def encode_rune(cp):
    """Encode a single Unicode codepoint (uint16) as the 3-byte trie-key form.

    See the module docstring for the format. Mirrored by `encodeRuneIntoBuffer`
    in `jieba_dict.h` — the two encodings MUST stay byte-identical.
    """
    assert 0 <= cp <= 0xFFFF
    return bytes(
        [
            ((cp >> 12) & 0x0F) | 0x80,
            ((cp >>  6) & 0x3F) | 0x80,
            ( cp        & 0x3F) | 0x80,
        ]
    )


def encode_word(word):
    parts = []
    for ch in word:
        parts.append(encode_rune(ord(ch)))
    return b"".join(parts)


keys_bytes = []
lengths = []
weights = []
skipped_non_bmp = 0
skipped_ascii_only = 0

for line in lines:
    parts = line.strip().split()
    if not parts:
        continue
    word = parts[0]
    weight = float(parts[1]) if len(parts) > 1 else 1.0

    if not all(ord(c) <= 0xFFFF for c in word):
        skipped_non_bmp += 1
        continue

    if all(ord(c) < 0x80 for c in word):
        # Pure-ASCII entries are never looked up in the trie at runtime —
        # the HMM segmenter handles English/digit runs separately. Dropping
        # them keeps the trie smaller and avoids spurious matches.
        skipped_ascii_only += 1
        continue

    encoded = encode_word(word)
    keys_bytes.append(encoded)
    lengths.append(len(encoded))
    weights.append(weight)

print(f"Loaded {len(weights)} entries", file=sys.stderr)
print(f"Skipped {skipped_non_bmp} non-BMP entries", file=sys.stderr)
print(f"Skipped {skipped_ascii_only} ASCII-only entries", file=sys.stderr)

total_weight = sum(w for w in weights)
weights = [math.log(w / total_weight) for w in weights]

# Sanity check: the 3-byte encoding must be injective. If it isn't, the trie
# build would silently drop entries and tokenization would return inconsistent
# segmentation. Fail loud here instead.
unique_keys = set(keys_bytes)
if len(unique_keys) != len(keys_bytes):
    raise RuntimeError(
        f"Rune key encoding is not injective: "
        f"{len(keys_bytes)} entries collapsed into {len(unique_keys)} distinct keys"
    )

sorted_indices = sorted(range(len(keys_bytes)), key=lambda i: keys_bytes[i])
keys_bytes = [keys_bytes[i] for i in sorted_indices]
lengths = [lengths[i] for i in sorted_indices]
weights = np.array([weights[i] for i in sorted_indices], dtype=np.float64)
values = list(range(len(weights)))  # 0..n-1

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

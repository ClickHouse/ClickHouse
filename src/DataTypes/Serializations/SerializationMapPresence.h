#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Packed presence block for one granule of a per-key Map.
///
/// Layout:
///   [kind array]  2 bits per Tracked key, in manifest order
///                 0 = all absent, 1 = all present, 2 = mixed (bitmap follows)
///   [bitmap area] only for kind == 2, ceil(rows / 8) bytes each, in manifest order
class MapKeyPresenceBlock
{
public:
    static constexpr UInt8 KIND_ABSENT = 0;
    static constexpr UInt8 KIND_PRESENT = 1;
    static constexpr UInt8 KIND_MIXED = 2;

    /// `presence[key][row]` is 0 or 1. Keys that are AlwaysPresent must not be included.
    static void serialize(
        WriteBuffer & ostr,
        size_t rows,
        const std::vector<std::vector<UInt8>> & presence);

    /// Reads the kind of key `key_index` without touching the bitmap area when kind is 0 or 1.
    /// `bitmap_bytes_read` is incremented only when a mixed-key bitmap is consumed.
    static UInt8 deserializeKind(
        ReadBuffer & istr,
        size_t key_count,
        size_t key_index);

    /// Reads presence of one key for `rows` rows. Kind 0/1 fill a constant and do not parse bitmaps.
    static void deserializeKey(
        ReadBuffer & istr,
        size_t rows,
        size_t key_count,
        size_t key_index,
        std::vector<UInt8> & out,
        size_t * bitmap_bytes_read = nullptr);

    /// Reads presence of every Tracked key. The block is decompressed once.
    static void deserializeAll(
        ReadBuffer & istr,
        size_t rows,
        size_t key_count,
        std::vector<std::vector<UInt8>> & out,
        size_t * bitmap_bytes_read = nullptr);

    static size_t kindArrayBytes(size_t key_count);
    static size_t bitmapBytes(size_t rows);
};

}

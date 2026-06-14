#pragma once

#include <cstdint>
#include <span>
#include <vector>
#include <darts.h>
#include <jieba_common.h>

namespace Jieba
{

struct DAGNode
{
    std::vector<std::pair<size_t, double>> nexts;
    double max_weight = -3.14e+100;
    int max_next = -1;
};

using DAG = std::vector<DAGNode>;

/// On-disk header layout. Sizes match the format produced by `generate_dict.py`
/// (8 bytes each), so we use fixed-width types here regardless of the platform's `size_t`.
struct DartsHeader
{
    double min_weight = 0;
    uint64_t num_elems = 0;
    uint64_t da_size = 0;
};

/// Number of bytes used to encode a single `Rune` in a `darts-clone` trie key.
///
/// `darts-clone` cannot store `0x00` bytes inside keys, so we encode each
/// `uint16_t` codepoint as 3 bytes in big-endian, with the high bit set in
/// every byte:
///
///     byte0 = ((rune >> 12) & 0x0F) | 0x80   // bits 12..15 -> 0x80..0x8F
///     byte1 = ((rune >>  6) & 0x3F) | 0x80   // bits  6..11 -> 0x80..0xBF
///     byte2 = ( rune        & 0x3F) | 0x80   // bits  0.. 5 -> 0x80..0xBF
///
/// This encoding is:
///   * injective (no two distinct `Rune` values map to the same 3-byte sequence),
///   * free of `0x00` bytes (every byte is in `0x80..0xFF`),
///   * endian-independent (bytes are emitted in an explicit order rather than
///     reinterpreting `uint16_t` memory),
///   * lexicographically order-preserving (sorting by `Rune` value matches
///     sorting by encoded byte sequence — required for `Darts::build`).
///
/// The same encoding is produced by `generate_dict.py` when building the trie
/// keys. Runtime callers use `encodeRuneKey` to materialise the lookup key
/// before calling `Darts::DoubleArray::exactMatchSearch` / `commonPrefixSearch`.
constexpr size_t BYTES_PER_RUNE = 3;

/// See the comment on `BYTES_PER_RUNE` for the encoding scheme.
inline void encodeRuneIntoBuffer(Rune rune, char * out)
{
    out[0] = static_cast<char>(((rune >> 12) & 0x0F) | 0x80);
    out[1] = static_cast<char>(((rune >>  6) & 0x3F) | 0x80);
    out[2] = static_cast<char>(( rune        & 0x3F) | 0x80);
}

/// Encode a sequence of runes into the contiguous byte form expected by the
/// `darts-clone` trie. The returned buffer is exactly `runes.size() * BYTES_PER_RUNE`
/// bytes long.
inline std::vector<char> encodeRuneKey(std::span<const Rune> runes)
{
    std::vector<char> key(runes.size() * BYTES_PER_RUNE);
    for (size_t i = 0; i < runes.size(); ++i)
        encodeRuneIntoBuffer(runes[i], key.data() + i * BYTES_PER_RUNE);
    return key;
}

class DartsDict
{
public:
    DartsDict();

    /// `elems` and the internal `da` array point into `storage`, so copying or moving
    /// the object would leave those borrowed pointers dangling at the old buffer.
    /// The dictionary is only ever held as a single long-lived instance, so forbid both.
    DartsDict(const DartsDict &) = delete;
    DartsDict & operator=(const DartsDict &) = delete;
    DartsDict(DartsDict &&) = delete;
    DartsDict & operator=(DartsDict &&) = delete;

    double find(std::span<const Rune> key) const;
    DAG buildDAG(std::span<const Rune> runes) const;

private:
    /// Decompressed dictionary buffer. Held as uint64_t so that the underlying
    /// storage is guaranteed to be 8-byte aligned, which matches the alignment
    /// requirements of the embedded `double` weights array.
    std::vector<uint64_t> storage;

    ::Darts::DoubleArray da;
    const double * elems = nullptr;
    double min_weight = 0;
    uint64_t num_elems = 0;
};

}

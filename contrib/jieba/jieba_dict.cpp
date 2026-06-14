#include <jieba_dict.h>

#include <array>
#include <cstring>
#include <stdexcept>

#include <zstd.h>

/// The dictionary file on disk is little-endian (as produced by `generate_dict.py`):
///   - the `DartsHeader` fields are written as little-endian via Python `struct.pack('<dQQ', ...)`;
///   - the `double` weights array is `np.float64` little-endian;
///   - the `darts-clone` trie array is `np.uint32` little-endian.
///
/// We `reinterpret_cast` the decompressed buffer onto those types directly, so the
/// host must also be little-endian. ClickHouse builds jieba only on little-endian
/// targets (see the `ENABLE_JIEBA` / `ARCH_S390X` guard in `contrib/CMakeLists.txt`);
/// this `static_assert` is a belt-and-suspenders check in case someone manually
/// enables jieba on a big-endian platform.
///
/// The trie *key* encoding (`encodeRuneKey` in `jieba_dict.h`) is independent of
/// host endianness — bytes are emitted in an explicit big-endian order with the
/// high bit set in every byte.
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "Jieba dictionary file is little-endian only");

constexpr unsigned char resource_jieba_dict_zst[] =
{
    #embed "dict_le.dat.zst"
};

namespace Jieba
{

double DartsDict::find(std::span<const Rune> key) const
{
    /// `find` is called in a hot O(N²) loop from `QuerySegment::cut` with very
    /// short keys (1-3 runes), so use a stack buffer instead of heap-allocating
    /// the encoded form on every call. The hard cap matches `MAX_WORD_LENGTH`
    /// in `buildDAG` — longer keys are not part of jieba's standard search.
    static constexpr size_t MAX_RUNES_FIND = 32;
    if (key.size() > MAX_RUNES_FIND)
        return 0;

    std::array<char, MAX_RUNES_FIND * BYTES_PER_RUNE> encoded;
    for (size_t i = 0; i < key.size(); ++i)
        encodeRuneIntoBuffer(key[i], encoded.data() + i * BYTES_PER_RUNE);

    int res = da.exactMatchSearch<int>(encoded.data(), key.size() * BYTES_PER_RUNE);
    if (res < 0 || res >= static_cast<int>(num_elems))
        return 0;
    return elems[res];
}

DAG DartsDict::buildDAG(std::span<const Rune> runes) const
{
    size_t size = runes.size();
    DAG dag(size);

    /// Encode the entire input once and reuse contiguous suffixes for every starting
    /// position. The 3-byte encoding is endian-independent and contains no `0x00`
    /// bytes (see `BYTES_PER_RUNE` in `jieba_dict.h`).
    auto encoded = encodeRuneKey(runes);

    for (size_t i = 0; i < size; ++i)
    {
        static constexpr size_t MAX_RESULTS = 128;
        static constexpr size_t MAX_WORD_LENGTH = 32;

        ::Darts::DoubleArray::result_pair_type results[MAX_RESULTS] = {};
        size_t max_bytes = std::min(MAX_WORD_LENGTH, size - i) * BYTES_PER_RUNE;
        size_t num = da.commonPrefixSearch(
            encoded.data() + i * BYTES_PER_RUNE, results, MAX_RESULTS, max_bytes);

        dag[i].nexts.emplace_back(i + 1, min_weight); /// Single rune is always a word
        for (size_t j = 0; j < num; ++j)
        {
            auto & match = results[j];
            if (match.value < 0 || match.value >= static_cast<int>(num_elems))
                continue;

            size_t char_num = match.length / BYTES_PER_RUNE;
            if (char_num == 1)
                dag[i].nexts[0].second = elems[match.value];
            else
                dag[i].nexts.emplace_back(i + char_num, elems[match.value]);
        }
    }
    return dag;
}

DartsDict::DartsDict()
{
    /// Decompress the embedded zstd-compressed dictionary into an 8-byte aligned buffer.
    unsigned long long uncompressed_size
        = ZSTD_getFrameContentSize(resource_jieba_dict_zst, sizeof(resource_jieba_dict_zst));
    if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR || uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN)
        throw std::runtime_error("Jieba dictionary: failed to read decompressed size");

    /// Round up to a multiple of sizeof(uint64_t) so the std::vector<uint64_t> can hold the data.
    size_t storage_words = (static_cast<size_t>(uncompressed_size) + sizeof(uint64_t) - 1) / sizeof(uint64_t);
    storage.resize(storage_words);

    size_t actual = ZSTD_decompress(
        storage.data(), storage.size() * sizeof(uint64_t),
        resource_jieba_dict_zst, sizeof(resource_jieba_dict_zst));
    if (ZSTD_isError(actual) || actual != uncompressed_size)
        throw std::runtime_error("Jieba dictionary: zstd decompression failed");

    const auto * bytes = reinterpret_cast<const unsigned char *>(storage.data());

    /// Read the header via memcpy to avoid relying on the layout of `DartsHeader`
    /// matching the on-disk layout under aggressive optimization or unusual ABIs.
    DartsHeader header;
    std::memcpy(&header, bytes, sizeof(DartsHeader));

    min_weight = header.min_weight;
    num_elems = header.num_elems;

    /// `bytes` is 8-byte aligned because `storage.data()` is uint64_t-aligned, and
    /// the layout written by `generate_dict.py` keeps the weights array at offset
    /// 24 (multiple of 8) and the trie array at offset 24 + 8 * num_elems (multiple of 8).
    elems = reinterpret_cast<const double *>(bytes + sizeof(DartsHeader));
    const char * da_ptr = reinterpret_cast<const char *>(bytes + sizeof(DartsHeader) + sizeof(double) * num_elems);
    da.set_array(da_ptr, header.da_size);
}

}

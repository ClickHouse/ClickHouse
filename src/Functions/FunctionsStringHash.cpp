#include <Functions/FunctionsStringHash.h>

#include <Functions/ExtractString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/PODArray.h>

#include <Core/Defines.h>

#include <functional>
#include <tuple>
#include <vector>
#include <base/unaligned.h>

#include <city.h>

#if (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#include <vec_crc32.h>
#endif

#if defined(__s390x__) && __BYTE_ORDER__==__ORDER_BIG_ENDIAN__
#include <crc32-s390x.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct BytesRef
{
    const UInt8 * data;
    size_t size;
};

struct Hash
{
    static UInt64 crc32u64(UInt64 crc [[maybe_unused]], UInt64 val [[maybe_unused]])
    {
#ifdef __SSE4_2__
        return _mm_crc32_u64(crc, val);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32cd(static_cast<UInt32>(crc), val);
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return crc32_ppc(crc, reinterpret_cast<const unsigned char *>(&val), sizeof(val));
#elif defined(__s390x__) && __BYTE_ORDER__==__ORDER_BIG_ENDIAN__
        return crc32c_le(static_cast<UInt32>(crc), reinterpret_cast<unsigned char *>(&val), sizeof(val));
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "String hash is not implemented without sse4.2 support");
#endif
    }

    static UInt64 crc32u32(UInt32 crc [[maybe_unused]], UInt32 val [[maybe_unused]])
    {
#ifdef __SSE4_2__
        return _mm_crc32_u32(crc, val);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32cw(crc, val);
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return crc32_ppc(crc, reinterpret_cast<const unsigned char *>(&val), sizeof(val));
#elif defined(__s390x__) && __BYTE_ORDER__==__ORDER_BIG_ENDIAN__
        return crc32c_le(static_cast<UInt32>(crc), reinterpret_cast<unsigned char *>(&val), sizeof(val));
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "String hash is not implemented without sse4.2 support");
#endif
    }

    static UInt64 crc32u16(UInt32 crc [[maybe_unused]], UInt16 val [[maybe_unused]])
    {
#ifdef __SSE4_2__
        return _mm_crc32_u16(crc, val);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32ch(crc, val);
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return crc32_ppc(crc, reinterpret_cast<const unsigned char *>(&val), sizeof(val));
#elif defined(__s390x__) && __BYTE_ORDER__==__ORDER_BIG_ENDIAN__
        return crc32c_le(static_cast<UInt32>(crc), reinterpret_cast<unsigned char *>(&val), sizeof(val));
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "String hash is not implemented without sse4.2 support");
#endif
    }

    static UInt64 crc32u8(UInt32 crc [[maybe_unused]], UInt8 val [[maybe_unused]])
    {
#ifdef __SSE4_2__
        return _mm_crc32_u8(crc, val);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32cb(crc, val);
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return crc32_ppc(crc, reinterpret_cast<const unsigned char *>(&val), sizeof(val));
#elif defined(__s390x__) && __BYTE_ORDER__==__ORDER_BIG_ENDIAN__
        return crc32c_le(static_cast<UInt32>(crc), reinterpret_cast<unsigned char *>(&val), sizeof(val));
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "String hash is not implemented without sse4.2 support");
#endif
    }

    template <bool CaseInsensitive>
    static ALWAYS_INLINE inline UInt64 shingleHash(UInt64 crc, const UInt8 * start, size_t size)
    {
        if (size & 1)
        {
            UInt8 x = *start;

            if constexpr (CaseInsensitive)
                x |= 0x20u; /// see toLowerIfAlphaASCII from StringUtils.h

            crc = crc32u8(static_cast<UInt32>(crc), x);
            --size;
            ++start;
        }

        if (size & 2)
        {
            UInt16 x = unalignedLoad<UInt16>(start);

            if constexpr (CaseInsensitive)
                x |= 0x2020u;

            crc = crc32u16(static_cast<UInt32>(crc), x);
            size -= 2;
            start += 2;
        }

        if (size & 4)
        {
            UInt32 x = unalignedLoad<UInt32>(start);

            if constexpr (CaseInsensitive)
                x |= 0x20202020u;

            crc = crc32u32(static_cast<UInt32>(crc), x);
            size -= 4;
            start += 4;
        }

        while (size)
        {
            UInt64 x = unalignedLoad<UInt64>(start);

            if constexpr (CaseInsensitive)
                x |= 0x2020202020202020u;

            crc = crc32u64(crc, x);
            size -= 8;
            start += 8;
        }

        return crc;
    }

    template <bool CaseInsensitive>
    static ALWAYS_INLINE inline UInt64 shingleHash(const std::vector<BytesRef> & shingle, size_t offset = 0)
    {
        UInt64 crc = -1ULL;

        for (size_t i = offset; i < shingle.size(); ++i)
            crc = shingleHash<CaseInsensitive>(crc, shingle[i].data, shingle[i].size);

        for (size_t i = 0; i < offset; ++i)
            crc = shingleHash<CaseInsensitive>(crc, shingle[i].data, shingle[i].size);

        return crc;
    }
};

// SimHash String -> UInt64
// UTF8: means ASCII or UTF8, these two parameters CodePoint and UTF8 can only be (UInt8, false) or (UInt32, true)
// Ngram: means ngram(true) or words shingles(false)
// CaseInsensitive: means should we consider about letter case or not
template <bool UTF8, bool Ngram, bool CaseInsensitive>
struct SimHashImpl
{
    static constexpr size_t min_word_size = 4;

    /// Update fingerprint according to hash_value bits.
    static ALWAYS_INLINE inline void updateFingerVector(Int64 * finger_vec, UInt64 hash_value)
    {
        for (size_t i = 0; i < 64; ++i)
            finger_vec[i] += (hash_value & (1ULL << i)) ? 1 : -1;
    }

    /// Return a 64 bit value according to finger_vec.
    static ALWAYS_INLINE inline UInt64 getSimHash(const Int64 * finger_vec)
    {
        UInt64 res = 0;

        for (size_t i = 0; i < 64; ++i)
            if (finger_vec[i] > 0)
                res |= (1ULL << i);

        return res;
    }

    // SimHash ngram calculate function: String -> UInt64
    // this function extracting ngram from input string, and maintain a 64-dimensions vector
    // for each ngram, calculate a 64 bit hash value, and update the vector according the hash value
    // finally return a 64 bit value(UInt64), i'th bit is 1 means vector[i] > 0, otherwise, vector[i] < 0

    static ALWAYS_INLINE inline UInt64 ngramHashASCII(const UInt8 * data, size_t size, size_t shingle_size)
    {
        if (size < shingle_size)
            return Hash::shingleHash<CaseInsensitive>(-1ULL, data, size);

        Int64 finger_vec[64] = {};
        const UInt8 * end = data + size;

        for (const UInt8 * pos = data; pos + shingle_size <= end; ++pos)
        {
            UInt64 hash_value = Hash::shingleHash<CaseInsensitive>(-1ULL, pos, shingle_size);
            updateFingerVector(finger_vec, hash_value);
        }

        return getSimHash(finger_vec);
    }

    static ALWAYS_INLINE inline UInt64 ngramHashUTF8(const UInt8 * data, size_t size, size_t shingle_size)
    {
        const UInt8 * start = data;
        const UInt8 * end = data + size;

        const UInt8 * word_start = start;
        const UInt8 * word_end = start;

        for (size_t i = 0; i < shingle_size; ++i)
        {
            if (word_end >= end)
                return Hash::shingleHash<CaseInsensitive>(-1ULL, data, size);

            ExtractStringImpl::readOneUTF8Code(word_end, end);
        }

        Int64 finger_vec[64] = {};

        while (word_end < end)
        {
            ExtractStringImpl::readOneUTF8Code(word_start, word_end);
            ExtractStringImpl::readOneUTF8Code(word_end, end);

            size_t length = word_end - word_start;
            UInt64 hash_value = Hash::shingleHash<CaseInsensitive>(-1ULL, word_start, length);
            updateFingerVector(finger_vec, hash_value);
        }

        return getSimHash(finger_vec);
    }

    // SimHash word shingle calculate function: String -> UInt64
    // this function extracting n word shingle from input string, and maintain a 64-dimensions vector as well
    // for each word shingle, calculate a 64 bit hash value, and update the vector according the hash value
    // finally return a 64 bit value(UInt64), i'th bit is 1 means vector[i] > 0, otherwise, vector[i] < 0
    //
    // word shingle hash value calculate:
    // 1. at the first, extracts N word shingles and calculate N hash values, store into an array, use this N hash values
    // to calculate the first word shingle hash value
    // 2. next, we extract one word each time, and calculate a new hash value of the new word,then use the latest N hash
    // values to calculate the next word shingle hash value

    static ALWAYS_INLINE inline UInt64 wordShingleHash(const UInt8 * data, size_t size, size_t shingle_size)
    {
        const UInt8 * start = data;
        const UInt8 * end = data + size;

        // A 64 bit vector initialized to zero.
        Int64 finger_vec[64] = {};
        // An array to store N words.
        std::vector<BytesRef> words;
        words.reserve(shingle_size);

        // get first word shingle
        while (start < end && words.size() < shingle_size)
        {
            const UInt8 * word_start = ExtractStringImpl::readOneWord(start, end);
            size_t length = start - word_start;

            if (length >= min_word_size)
                words.emplace_back(BytesRef{word_start, length});
        }

        if (words.empty())
            return 0;

        UInt64 hash_value = Hash::shingleHash<CaseInsensitive>(words);
        updateFingerVector(finger_vec, hash_value);

        size_t offset = 0;
        while (start < end)
        {
            const UInt8 * word_start = ExtractStringImpl::readOneWord(start, end);
            size_t length = start - word_start;

            if (length < min_word_size)
                continue;

            // we need to store the new word hash value to the oldest location.
            // for example, N = 5, array |a0|a1|a2|a3|a4|, now, a0 is the oldest location,
            // so we need to store new word hash into location of a0, then this array become
            // |a5|a1|a2|a3|a4|, next time, a1 become the oldest location, we need to store new
            // word hash value into location of a1, then array become |a5|a6|a2|a3|a4|
            words[offset] = BytesRef{word_start, length};
            ++offset;
            if (offset >= shingle_size)
                offset = 0;

            // according to the word hash storation way, in order to not lose the word shingle's
            // sequence information, when calculation word shingle hash value, we need provide the offset
            // information, which is the offset of the first word's hash value of the word shingle
            hash_value = Hash::shingleHash<CaseInsensitive>(words, offset);
            updateFingerVector(finger_vec, hash_value);
        }

        return getSimHash(finger_vec);
    }

    static void apply(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, size_t shingle_size, PaddedPODArray<UInt64> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * one_data = &data[offsets[i - 1]];
            const size_t data_size = offsets[i] - offsets[i - 1];

            if constexpr (Ngram)
            {
                if constexpr (!UTF8)
                    res[i] = ngramHashASCII(one_data, data_size, shingle_size);
                else
                    res[i] = ngramHashUTF8(one_data, data_size, shingle_size);
            }
            else
            {
                res[i] = wordShingleHash(one_data, data_size, shingle_size);
            }
        }
    }
};

// MinHash: String -> Tuple(UInt64, UInt64)
// for each string, we extract ngram or word shingle,
// for each ngram or word shingle, calculate a hash value,
// then we take the K minimum hash values to calculate a hashsum,
// and take the K maximum hash values to calculate another hashsum,
// return this two hashsum: Tuple(hashsum1, hashsum2)
//
// UTF8: means ASCII or UTF8, these two parameters CodePoint and UTF8 can only be (UInt8, false) or (UInt32, true)
// Ngram: means ngram(true) or words shingles(false)
// CaseInsensitive: means should we consider about letter case or not
template <bool UTF8, bool Ngram, bool CaseInsensitive>
struct MinHashImpl
{
    static constexpr size_t min_word_size = 4;

    template <typename Comp>
    struct Heap
    {
        void update(UInt64 hash, BytesRef ref, size_t limit)
        {
            if (values.contains(hash))
                return;

            values[hash] = ref;

            if (values.size() > limit)
                values.erase(values.begin());
        }

        UInt64 getHash()
        {
            if (values.empty())
                return 0;

            UInt64 res = 0;
            for (auto it = values.begin(); it != values.end(); ++it)
                res = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(res, it->first));

            return res;
        }

        void fill(ColumnTuple & strings)
        {
            auto it = values.begin();
            for (size_t i = 0; i < strings.tupleSize(); ++i)
            {
                auto & col_string = static_cast<ColumnString &>(strings.getColumn(i));
                if (it != values.end())
                {
                    col_string.insertData(reinterpret_cast<const char *>(it->second.data), it->second.size);
                    ++it;
                }
                else
                    col_string.insertDefault();
            }
        }

        std::map<UInt64, BytesRef, Comp> values;
    };

    using MaxHeap = Heap<std::less<>>;
    using MinHeap = Heap<std::greater<>>;

    static ALWAYS_INLINE inline void ngramHashASCII(
        MinHeap & min_heap,
        MaxHeap & max_heap,
        const UInt8 * data,
        size_t size,
        size_t shingle_size,
        size_t heap_size)
    {
        if (size < shingle_size)
        {
            UInt64 hash_value = Hash::shingleHash<CaseInsensitive>(-1ULL, data, size);
            min_heap.update(hash_value, BytesRef{data, size}, heap_size);
            max_heap.update(hash_value, BytesRef{data, size}, heap_size);
            return;
        }

        const UInt8 * end = data + size;

        for (const UInt8 * pos = data; pos + shingle_size <= end; ++pos)
        {
            UInt64 hash_value = Hash::shingleHash<CaseInsensitive>(-1ULL, pos, shingle_size);

            // insert the new hash value into array used to store K minimum value
            // and K maximum value
            min_heap.update(hash_value, BytesRef{pos, shingle_size}, heap_size);
            max_heap.update(hash_value, BytesRef{pos, shingle_size}, heap_size);
        }
    }

    static ALWAYS_INLINE inline void ngramHashUTF8(
        MinHeap & min_heap,
        MaxHeap & max_heap,
        const UInt8 * data,
        size_t size,
        size_t shingle_size,
        size_t heap_size)
    {
        const UInt8 * start = data;
        const UInt8 * end = data + size;

        const UInt8 * word_start = start;
        const UInt8 * word_end = start;

        for (size_t i = 0; i < shingle_size; ++i)
        {
            if (word_end >= end)
            {
                auto hash_value = Hash::shingleHash<CaseInsensitive>(-1ULL, data, size);
                min_heap.update(hash_value, BytesRef{data, size}, heap_size);
                max_heap.update(hash_value, BytesRef{data, size}, heap_size);
                return;
            }

            ExtractStringImpl::readOneUTF8Code(word_end, end);
        }

        while (word_end < end)
        {
            ExtractStringImpl::readOneUTF8Code(word_start, word_end);
            ExtractStringImpl::readOneUTF8Code(word_end, end);

            size_t length = word_end - word_start;
            UInt64 hash_value = Hash::shingleHash<CaseInsensitive>(-1ULL, word_start, length);

            min_heap.update(hash_value, BytesRef{word_start, length}, heap_size);
            max_heap.update(hash_value, BytesRef{word_start, length}, heap_size);
        }
    }

    // MinHash word shingle hash value calculate function: String ->Tuple(UInt64, UInt64)
    // for each word shingle, we calculate a hash value, but in fact, we just maintain the
    // K minimum and K maximum hash value
    static ALWAYS_INLINE inline void wordShingleHash(
        MinHeap & min_heap,
        MaxHeap & max_heap,
        const UInt8 * data,
        size_t size,
        size_t shingle_size,
        size_t heap_size)
    {
        const UInt8 * start = data;
        const UInt8 * end = data + size;

        // An array to store N words.
        std::vector<BytesRef> words;
        words.reserve(shingle_size);

        // get first word shingle
        while (start < end && words.size() < shingle_size)
        {
            const UInt8 * word_start = ExtractStringImpl::readOneWord(start, end);
            size_t length = start - word_start;

            if (length >= min_word_size)
                words.emplace_back(BytesRef{word_start, length});
        }

        if (words.empty())
            return;

        UInt64 hash_value = Hash::shingleHash<CaseInsensitive>(words);
        {
            const UInt8 * shingle_start = words.front().data;
            const UInt8 * shingle_end = words.back().data + words.back().size;
            BytesRef ref{shingle_start, static_cast<size_t>(shingle_end - shingle_start)};
            min_heap.update(hash_value, ref, heap_size);
            max_heap.update(hash_value, ref, heap_size);
        }

        size_t offset = 0;
        while (start < end)
        {
            const UInt8 * word_start = ExtractStringImpl::readOneWord(start, end);

            size_t length = start - word_start;

            if (length < min_word_size)
                continue;

            words[offset] = BytesRef{word_start, length};
            const UInt8 * shingle_end = words[offset].data + length;

            ++offset;
            if (offset >= shingle_size)
                offset = 0;

            const UInt8 * shingle_start = words[offset].data;

            hash_value = Hash::shingleHash<CaseInsensitive>(words, offset);
            BytesRef ref{shingle_start, static_cast<size_t>(shingle_end - shingle_start)};
            min_heap.update(hash_value, ref, heap_size);
            max_heap.update(hash_value, ref, heap_size);
        }
    }

    static void apply(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        size_t shingle_size,
        size_t heap_size,
        PaddedPODArray<UInt64> * res1,
        PaddedPODArray<UInt64> * res2,
        ColumnTuple * res1_strings,
        ColumnTuple * res2_strings,
        size_t input_rows_count)
    {
        MinHeap min_heap;
        MaxHeap max_heap;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * one_data = &data[offsets[i - 1]];
            const size_t data_size = offsets[i] - offsets[i - 1];

            min_heap.values.clear();
            max_heap.values.clear();

            if constexpr (Ngram)
            {
                if constexpr (!UTF8)
                    ngramHashASCII(min_heap, max_heap, one_data, data_size, shingle_size, heap_size);
                else
                    ngramHashUTF8(min_heap, max_heap, one_data, data_size, shingle_size, heap_size);
            }
            else
            {
                wordShingleHash(min_heap, max_heap, one_data, data_size, shingle_size, heap_size);
            }

            if (res1)
                (*res1)[i] = min_heap.getHash();
            if (res2)
                (*res2)[i] = max_heap.getHash();

            if (res1_strings)
                min_heap.fill(*res1_strings);
            if (res2_strings)
                max_heap.fill(*res2_strings);
        }
    }
};

struct NameNgramSimHash
{
    static constexpr auto name = "ngramSimHash";
};

struct NameNgramSimHashCaseInsensitive
{
    static constexpr auto name = "ngramSimHashCaseInsensitive";
};

struct NameNgramSimHashUTF8
{
    static constexpr auto name = "ngramSimHashUTF8";
};

struct NameNgramSimHashCaseInsensitiveUTF8
{
    static constexpr auto name = "ngramSimHashCaseInsensitiveUTF8";
};

struct NameWordShingleSimHash
{
    static constexpr auto name = "wordShingleSimHash";
};

struct NameWordShingleSimHashCaseInsensitive
{
    static constexpr auto name = "wordShingleSimHashCaseInsensitive";
};

struct NameWordShingleSimHashUTF8
{
    static constexpr auto name = "wordShingleSimHashUTF8";
};

struct NameWordShingleSimHashCaseInsensitiveUTF8
{
    static constexpr auto name = "wordShingleSimHashCaseInsensitiveUTF8";
};

struct NameNgramMinHash
{
    static constexpr auto name = "ngramMinHash";
};

struct NameNgramMinHashCaseInsensitive
{
    static constexpr auto name = "ngramMinHashCaseInsensitive";
};

struct NameNgramMinHashUTF8
{
    static constexpr auto name = "ngramMinHashUTF8";
};

struct NameNgramMinHashCaseInsensitiveUTF8
{
    static constexpr auto name = "ngramMinHashCaseInsensitiveUTF8";
};

struct NameWordShingleMinHash
{
    static constexpr auto name = "wordShingleMinHash";
};

struct NameWordShingleMinHashCaseInsensitive
{
    static constexpr auto name = "wordShingleMinHashCaseInsensitive";
};

struct NameWordShingleMinHashUTF8
{
    static constexpr auto name = "wordShingleMinHashUTF8";
};

struct NameWordShingleMinHashCaseInsensitiveUTF8
{
    static constexpr auto name = "wordShingleMinHashCaseInsensitiveUTF8";
};

struct NameNgramMinHashArg
{
    static constexpr auto name = "ngramMinHashArg";
};

struct NameNgramMinHashArgCaseInsensitive
{
    static constexpr auto name = "ngramMinHashArgCaseInsensitive";
};

struct NameNgramMinHashArgUTF8
{
    static constexpr auto name = "ngramMinHashArgUTF8";
};

struct NameNgramMinHashArgCaseInsensitiveUTF8
{
    static constexpr auto name = "ngramMinHashArgCaseInsensitiveUTF8";
};

struct NameWordShingleMinHashArg
{
    static constexpr auto name = "wordShingleMinHashArg";
};

struct NameWordShingleMinHashArgCaseInsensitive
{
    static constexpr auto name = "wordShingleMinHashArgCaseInsensitive";
};

struct NameWordShingleMinHashArgUTF8
{
    static constexpr auto name = "wordShingleMinHashArgUTF8";
};

struct NameWordShingleMinHashArgCaseInsensitiveUTF8
{
    static constexpr auto name = "wordShingleMinHashArgCaseInsensitiveUTF8";
};

// SimHash
using FunctionNgramSimHash = FunctionsStringHash<SimHashImpl<false, true, false>, NameNgramSimHash, true>;

using FunctionNgramSimHashCaseInsensitive
    = FunctionsStringHash<SimHashImpl<false, true, true>, NameNgramSimHashCaseInsensitive, true>;

using FunctionNgramSimHashUTF8 = FunctionsStringHash<SimHashImpl<true, true, false>, NameNgramSimHashUTF8, true>;

using FunctionNgramSimHashCaseInsensitiveUTF8
    = FunctionsStringHash<SimHashImpl<true, true, true>, NameNgramSimHashCaseInsensitiveUTF8, true>;

using FunctionWordShingleSimHash = FunctionsStringHash<SimHashImpl<false, false, false>, NameWordShingleSimHash, true>;

using FunctionWordShingleSimHashCaseInsensitive
    = FunctionsStringHash<SimHashImpl<false, false, true>, NameWordShingleSimHashCaseInsensitive, true>;

using FunctionWordShingleSimHashUTF8 = FunctionsStringHash<SimHashImpl<true, false, false>, NameWordShingleSimHashUTF8, true>;

using FunctionWordShingleSimHashCaseInsensitiveUTF8
    = FunctionsStringHash<SimHashImpl<true, false, true>, NameWordShingleSimHashCaseInsensitiveUTF8, true>;

// MinHash
using FunctionNgramMinHash = FunctionsStringHash<MinHashImpl<false, true, false>, NameNgramMinHash, false>;

using FunctionNgramMinHashCaseInsensitive
    = FunctionsStringHash<MinHashImpl<false, true, true>, NameNgramMinHashCaseInsensitive, false>;

using FunctionNgramMinHashUTF8 = FunctionsStringHash<MinHashImpl<true, true, false>, NameNgramMinHashUTF8, false>;

using FunctionNgramMinHashCaseInsensitiveUTF8
    = FunctionsStringHash<MinHashImpl<true, true, true>, NameNgramMinHashCaseInsensitiveUTF8, false>;

using FunctionWordShingleMinHash = FunctionsStringHash<MinHashImpl<false, false, false>, NameWordShingleMinHash, false>;

using FunctionWordShingleMinHashCaseInsensitive
    = FunctionsStringHash<MinHashImpl<false, false, true>, NameWordShingleMinHashCaseInsensitive, false>;

using FunctionWordShingleMinHashUTF8
    = FunctionsStringHash<MinHashImpl<true, false, false>, NameWordShingleMinHashUTF8, false>;

using FunctionWordShingleMinHashCaseInsensitiveUTF8
    = FunctionsStringHash<MinHashImpl<true, false, true>, NameWordShingleMinHashCaseInsensitiveUTF8, false>;

// MinHasArg

using FunctionNgramMinHashArg = FunctionsStringHash<MinHashImpl<false, true, false>, NameNgramMinHashArg, false, true>;

using FunctionNgramMinHashArgCaseInsensitive
= FunctionsStringHash<MinHashImpl<false, true, true>, NameNgramMinHashArgCaseInsensitive, false, true>;

using FunctionNgramMinHashArgUTF8 = FunctionsStringHash<MinHashImpl<true, true, false>, NameNgramMinHashArgUTF8, false, true>;

using FunctionNgramMinHashArgCaseInsensitiveUTF8
= FunctionsStringHash<MinHashImpl<true, true, true>, NameNgramMinHashArgCaseInsensitiveUTF8, false, true>;

using FunctionWordShingleMinHashArg = FunctionsStringHash<MinHashImpl<false, false, false>, NameWordShingleMinHashArg, false, true>;

using FunctionWordShingleMinHashArgCaseInsensitive
= FunctionsStringHash<MinHashImpl<false, false, true>, NameWordShingleMinHashArgCaseInsensitive, false, true>;

using FunctionWordShingleMinHashArgUTF8
= FunctionsStringHash<MinHashImpl<true, false, false>, NameWordShingleMinHashArgUTF8, false, true>;

using FunctionWordShingleMinHashArgCaseInsensitiveUTF8
= FunctionsStringHash<MinHashImpl<true, false, true>, NameWordShingleMinHashArgCaseInsensitiveUTF8, false, true>;

REGISTER_FUNCTION(StringHash)
{
    FunctionDocumentation::Description ngramSimHash_description = R"(
Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`.

Can be used for detection of semi-duplicate strings with [`bitHammingDistance`](../functions/bit-functions.md/#bitHammingDistance).
The smaller the [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax ngramSimHash_syntax = "ngramSimHash(string[, ngramsize])";
    FunctionDocumentation::Arguments ngramSimHash_arguments = {
        {"string", "String for which to compute the case sensitive `simhash`.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is`3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramSimHash_returned_value = {"Returns the computed hash of the input string.", {"UInt64"}};
    FunctionDocumentation::Examples ngramSimHash_examples = {
        {
            "Usage example",
            "SELECT ngramSimHash('ClickHouse') AS Hash;",
            R"(
┌───────Hash─┐
│ 1627567969 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramSimHash_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramSimHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramSimHash_documentation = {ngramSimHash_description, ngramSimHash_syntax, ngramSimHash_arguments, ngramSimHash_returned_value, ngramSimHash_examples, ngramSimHash_introduced_in, ngramSimHash_category};
    factory.registerFunction<FunctionNgramSimHash>(ngramSimHash_documentation);

    FunctionDocumentation::Description ngramSimHashCaseInsensitive_description = R"(
Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`.
It is case insensitive.

Can be used for detection of semi-duplicate strings with [`bitHammingDistance`](/sql-reference/functions/bit-functions#bitHammingDistance).
The smaller the [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax ngramSimHashCaseInsensitive_syntax = "ngramSimHashCaseInsensitive(string[, ngramsize])";
    FunctionDocumentation::Arguments ngramSimHashCaseInsensitive_arguments = {
        {"string", "String for which to compute the case insensitive `simhash`.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramSimHashCaseInsensitive_returned_value = {"Hash value. [UInt64](../data-types/int-uint.md).", {"UInt64"}};
    FunctionDocumentation::Examples ngramSimHashCaseInsensitive_examples = {
        {
            "Usage example",
            "SELECT ngramSimHashCaseInsensitive('ClickHouse') AS Hash;",
            R"(
┌──────Hash─┐
│ 562180645 │
└───────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramSimHashCaseInsensitive_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramSimHashCaseInsensitive_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramSimHashCaseInsensitive_documentation = {ngramSimHashCaseInsensitive_description, ngramSimHashCaseInsensitive_syntax, ngramSimHashCaseInsensitive_arguments, ngramSimHashCaseInsensitive_returned_value, ngramSimHashCaseInsensitive_examples, ngramSimHashCaseInsensitive_introduced_in, ngramSimHashCaseInsensitive_category};
    factory.registerFunction<FunctionNgramSimHashCaseInsensitive>(ngramSimHashCaseInsensitive_documentation);

    FunctionDocumentation::Description ngramSimHashUTF8_description = R"(
Splits a UTF-8 encoded string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`.
It is case sensitive.

Can be used for detection of semi-duplicate strings with [`bitHammingDistance`](../functions/bit-functions.md/#bitHammingDistance).
The smaller the [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax ngramSimHashUTF8_syntax = "ngramSimHashUTF8(string[, ngramsize])";
    FunctionDocumentation::Arguments ngramSimHashUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramSimHashUTF8_returned_value = {"Returns the computed hash value.", {"UInt64"}};
    FunctionDocumentation::Examples ngramSimHashUTF8_examples = {
        {
            "Usage example",
            "SELECT ngramSimHashUTF8('ClickHouse') AS Hash;",
            R"(
┌───────Hash─┐
│ 1628157797 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramSimHashUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramSimHashUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramSimHashUTF8_documentation = {ngramSimHashUTF8_description, ngramSimHashUTF8_syntax, ngramSimHashUTF8_arguments, ngramSimHashUTF8_returned_value, ngramSimHashUTF8_examples, ngramSimHashUTF8_introduced_in, ngramSimHashUTF8_category};
    factory.registerFunction<FunctionNgramSimHashUTF8>(ngramSimHashUTF8_documentation);

    FunctionDocumentation::Description ngramSimHashCaseInsensitiveUTF8_description = R"(
Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`.
It is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bitHammingDistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax ngramSimHashCaseInsensitiveUTF8_syntax = "ngramSimHashCaseInsensitiveUTF8(string[, ngramsize])";
    FunctionDocumentation::Arguments ngramSimHashCaseInsensitiveUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramSimHashCaseInsensitiveUTF8_returned_value = {"Returns the computed hash value.", {"UInt64"}};
    FunctionDocumentation::Examples ngramSimHashCaseInsensitiveUTF8_examples = {
        {
            "Usage example",
            "SELECT ngramSimHashCaseInsensitiveUTF8('ClickHouse') AS Hash;",
            R"(
┌───────Hash─┐
│ 1636742693 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramSimHashCaseInsensitiveUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramSimHashCaseInsensitiveUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramSimHashCaseInsensitiveUTF8_documentation = {ngramSimHashCaseInsensitiveUTF8_description, ngramSimHashCaseInsensitiveUTF8_syntax, ngramSimHashCaseInsensitiveUTF8_arguments, ngramSimHashCaseInsensitiveUTF8_returned_value, ngramSimHashCaseInsensitiveUTF8_examples, ngramSimHashCaseInsensitiveUTF8_introduced_in, ngramSimHashCaseInsensitiveUTF8_category};
    factory.registerFunction<FunctionNgramSimHashCaseInsensitiveUTF8>(ngramSimHashCaseInsensitiveUTF8_documentation);

    FunctionDocumentation::Description wordShingleSimHash_description = R"(
Splits a ASCII string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`.
Is is case sensitive.

Can be used for detection of semi-duplicate strings with [`bitHammingDistance`](../functions/bit-functions.md/#bitHammingDistance).
The smaller the [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleSimHash_syntax = "wordShingleSimHash(string[, shinglesize])";
    FunctionDocumentation::Arguments wordShingleSimHash_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleSimHash_returned_value = {"Returns the computed hash value.", {"UInt64"}};
    FunctionDocumentation::Examples wordShingleSimHash_examples = {
        {
            "Usage example",
            "SELECT wordShingleSimHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;",
            R"(
┌───────Hash─┐
│ 2328277067 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleSimHash_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleSimHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleSimHash_documentation = {wordShingleSimHash_description, wordShingleSimHash_syntax, wordShingleSimHash_arguments, wordShingleSimHash_returned_value, wordShingleSimHash_examples, wordShingleSimHash_introduced_in, wordShingleSimHash_category};
    factory.registerFunction<FunctionWordShingleSimHash>(wordShingleSimHash_documentation);

    FunctionDocumentation::Description wordShingleSimHashCaseInsensitive_description = R"(
Splits a ASCII string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`.
It is case insensitive.

Can be used for detection of semi-duplicate strings with [`bitHammingDistance`](../functions/bit-functions.md/#bitHammingDistance).
The smaller the [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleSimHashCaseInsensitive_syntax = "wordShingleSimHashCaseInsensitive(string[, shinglesize])";
    FunctionDocumentation::Arguments wordShingleSimHashCaseInsensitive_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleSimHashCaseInsensitive_returned_value = {"Returns the computed hash value.", {"UInt64"}};
    FunctionDocumentation::Examples wordShingleSimHashCaseInsensitive_examples = {
        {
            "Usage example",
            "SELECT wordShingleSimHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;",
            R"(
┌───────Hash─┐
│ 2194812424 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleSimHashCaseInsensitive_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleSimHashCaseInsensitive_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleSimHashCaseInsensitive_documentation = {wordShingleSimHashCaseInsensitive_description, wordShingleSimHashCaseInsensitive_syntax, wordShingleSimHashCaseInsensitive_arguments, wordShingleSimHashCaseInsensitive_returned_value, wordShingleSimHashCaseInsensitive_examples, wordShingleSimHashCaseInsensitive_introduced_in, wordShingleSimHashCaseInsensitive_category};
    factory.registerFunction<FunctionWordShingleSimHashCaseInsensitive>(wordShingleSimHashCaseInsensitive_documentation);

    FunctionDocumentation::Description wordShingleSimHashUTF8_description = R"(
Splits a UTF-8 string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`.
It is case sensitive.

Can be used for detection of semi-duplicate strings with [`bitHammingDistance`](../functions/bit-functions.md/#bitHammingDistance).
The smaller the [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleSimHashUTF8_syntax = "wordShingleSimHashUTF8(string[, shinglesize])";
    FunctionDocumentation::Arguments wordShingleSimHashUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleSimHashUTF8_returned_value = {"Returns the computed hash value.", {"UInt64"}};
    FunctionDocumentation::Examples wordShingleSimHashUTF8_examples = {
        {
            "Usage example",
            "SELECT wordShingleSimHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;",
            R"(
┌───────Hash─┐
│ 2328277067 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleSimHashUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleSimHashUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleSimHashUTF8_documentation = {wordShingleSimHashUTF8_description, wordShingleSimHashUTF8_syntax, wordShingleSimHashUTF8_arguments, wordShingleSimHashUTF8_returned_value, wordShingleSimHashUTF8_examples, wordShingleSimHashUTF8_introduced_in, wordShingleSimHashUTF8_category};
    factory.registerFunction<FunctionWordShingleSimHashUTF8>(wordShingleSimHashUTF8_documentation);

    FunctionDocumentation::Description wordShingleSimHashCaseInsensitiveUTF8_description = R"(
Splits a UTF-8 encoded string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`.
It is case insensitive.

Can be used for detection of semi-duplicate strings with [`bitHammingDistance`](../functions/bit-functions.md/#bitHammingDistance).
The smaller the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleSimHashCaseInsensitiveUTF8_syntax = "wordShingleSimHashCaseInsensitiveUTF8(string[, shinglesize])";
    FunctionDocumentation::Arguments wordShingleSimHashCaseInsensitiveUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleSimHashCaseInsensitiveUTF8_returned_value = {"Returns the computed hash value.", {"UInt64"}};
    FunctionDocumentation::Examples wordShingleSimHashCaseInsensitiveUTF8_examples = {
        {
            "Usage example",
            "SELECT wordShingleSimHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;",
            R"(
┌───────Hash─┐
│ 2194812424 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleSimHashCaseInsensitiveUTF8_introduced_in = {1, 1};
    FunctionDocumentation::Category wordShingleSimHashCaseInsensitiveUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleSimHashCaseInsensitiveUTF8_documentation = {wordShingleSimHashCaseInsensitiveUTF8_description, wordShingleSimHashCaseInsensitiveUTF8_syntax, wordShingleSimHashCaseInsensitiveUTF8_arguments, wordShingleSimHashCaseInsensitiveUTF8_returned_value, wordShingleSimHashCaseInsensitiveUTF8_examples, wordShingleSimHashCaseInsensitiveUTF8_introduced_in, wordShingleSimHashCaseInsensitiveUTF8_category};
    factory.registerFunction<FunctionWordShingleSimHashCaseInsensitiveUTF8>(wordShingleSimHashCaseInsensitiveUTF8_documentation);

    FunctionDocumentation::Description ngramMinHash_description = R"(
Splits a ASCII string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram and returns a tuple with these hashes.
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case sensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax ngramMinHash_syntax = "ngramMinHash(string[, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHash_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramMinHash_returned_value = {"Returns a tuple with two hashes — the minimum and the maximum.", {"Tuple"}};
    FunctionDocumentation::Examples ngramMinHash_examples = {
        {
            "Usage example",
            "SELECT ngramMinHash('ClickHouse') AS Tuple;",
            R"(
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,9054248444481805918) │
└────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHash_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHash_documentation = {ngramMinHash_description, ngramMinHash_syntax, ngramMinHash_arguments, ngramMinHash_returned_value, ngramMinHash_examples, ngramMinHash_introduced_in, ngramMinHash_category};
    factory.registerFunction<FunctionNgramMinHash>(ngramMinHash_documentation);

    FunctionDocumentation::Description ngramMinHashCaseInsensitive_description = R"(
Splits a ASCII string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram and returns a tuple with these hashes
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case insensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax ngramMinHashCaseInsensitive_syntax = "ngramMinHashCaseInsensitive(string[, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHashCaseInsensitive_arguments = {
        {"string", "String. [String](../data-types/string.md)."},
        {"ngramsize", "The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md)."},
        {"hashnum", "The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md)."}
    };
    FunctionDocumentation::ReturnedValue ngramMinHashCaseInsensitive_returned_value = {"Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).", {"Tuple"}};
    FunctionDocumentation::Examples ngramMinHashCaseInsensitive_examples = {
        {
            "Usage example",
            "SELECT ngramMinHashCaseInsensitive('ClickHouse') AS Tuple;",
            R"(
┌─Tuple──────────────────────────────────────┐
│ (2106263556442004574,13203602793651726206) │
└────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHashCaseInsensitive_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHashCaseInsensitive_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHashCaseInsensitive_documentation = {ngramMinHashCaseInsensitive_description, ngramMinHashCaseInsensitive_syntax, ngramMinHashCaseInsensitive_arguments, ngramMinHashCaseInsensitive_returned_value, ngramMinHashCaseInsensitive_examples, ngramMinHashCaseInsensitive_introduced_in, ngramMinHashCaseInsensitive_category};
    factory.registerFunction<FunctionNgramMinHashCaseInsensitive>(ngramMinHashCaseInsensitive_documentation);

    FunctionDocumentation::Description ngramMinHashUTF8_description = R"(
Splits a UTF-8 string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram and returns a tuple with these hashes.
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case sensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax ngramMinHashUTF8_syntax = "ngramMinHashUTF8(string[, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHashUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramMinHashUTF8_returned_value = {"Returns a tuple with two hashes — the minimum and the maximum.", {"Tuple"}};
    FunctionDocumentation::Examples ngramMinHashUTF8_examples = {
        {
            "Usage example",
            "SELECT ngramMinHashUTF8('ClickHouse') AS Tuple;",
            R"(
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,6742163577938632877) │
└────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHashUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHashUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHashUTF8_documentation = {ngramMinHashUTF8_description, ngramMinHashUTF8_syntax, ngramMinHashUTF8_arguments, ngramMinHashUTF8_returned_value, ngramMinHashUTF8_examples, ngramMinHashUTF8_introduced_in, ngramMinHashUTF8_category};
    factory.registerFunction<FunctionNgramMinHashUTF8>(ngramMinHashUTF8_documentation);

    FunctionDocumentation::Description ngramMinHashCaseInsensitiveUTF8_description = R"(
Splits a UTF-8 string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram and returns a tuple with these hashes..
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case insensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax ngramMinHashCaseInsensitiveUTF8_syntax = "ngramMinHashCaseInsensitiveUTF8(string [, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHashCaseInsensitiveUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramMinHashCaseInsensitiveUTF8_returned_value = {"Returns a tuple with two hashes — the minimum and the maximum.", {"Tuple"}};
    FunctionDocumentation::Examples ngramMinHashCaseInsensitiveUTF8_examples = {
        {
            "Usage example",
            "SELECT ngramMinHashCaseInsensitiveUTF8('ClickHouse') AS Tuple;",
            R"(
┌─Tuple───────────────────────────────────────┐
│ (12493625717655877135,13203602793651726206) │
└─────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHashCaseInsensitiveUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHashCaseInsensitiveUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHashCaseInsensitiveUTF8_documentation = {ngramMinHashCaseInsensitiveUTF8_description, ngramMinHashCaseInsensitiveUTF8_syntax, ngramMinHashCaseInsensitiveUTF8_arguments, ngramMinHashCaseInsensitiveUTF8_returned_value, ngramMinHashCaseInsensitiveUTF8_examples, ngramMinHashCaseInsensitiveUTF8_introduced_in, ngramMinHashCaseInsensitiveUTF8_category};
    factory.registerFunction<FunctionNgramMinHashCaseInsensitiveUTF8>(ngramMinHashCaseInsensitiveUTF8_documentation);

    FunctionDocumentation::Description wordShingleMinHash_description = R"(
Splits a ASCII string into parts (shingles) of `shinglesize` words, calculates hash values for each word shingle and returns a tuple with these hashes.
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case sensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleMinHash_syntax = "wordShingleMinHash(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHash_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHash_returned_value = {"Returns a tuple with two hashes — the minimum and the maximum.", {"Tuple(UInt64, UInt64)"}};
    FunctionDocumentation::Examples wordShingleMinHash_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;",
            R"(
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHash_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleMinHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHash_documentation = {wordShingleMinHash_description, wordShingleMinHash_syntax, wordShingleMinHash_arguments, wordShingleMinHash_returned_value, wordShingleMinHash_examples, wordShingleMinHash_introduced_in, wordShingleMinHash_category};
    factory.registerFunction<FunctionWordShingleMinHash>(wordShingleMinHash_documentation);

    FunctionDocumentation::Description wordShingleMinHashCaseInsensitive_description = R"(
Splits a ASCII string into parts (shingles) of `shinglesize` words, calculates hash values for each word shingle and returns a tuple with these hashes.
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case insensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleMinHashCaseInsensitive_syntax = "wordShingleMinHashCaseInsensitive(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHashCaseInsensitive_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHashCaseInsensitive_returned_value = {"Returns a tuple with two hashes — the minimum and the maximum.", {"Tuple(UInt64, UInt64)"}};
    FunctionDocumentation::Examples wordShingleMinHashCaseInsensitive_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHashCaseInsensitive_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleMinHashCaseInsensitive_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHashCaseInsensitive_documentation = {wordShingleMinHashCaseInsensitive_description, wordShingleMinHashCaseInsensitive_syntax, wordShingleMinHashCaseInsensitive_arguments, wordShingleMinHashCaseInsensitive_returned_value, wordShingleMinHashCaseInsensitive_examples, wordShingleMinHashCaseInsensitive_introduced_in, wordShingleMinHashCaseInsensitive_category};
    factory.registerFunction<FunctionWordShingleMinHashCaseInsensitive>(wordShingleMinHashCaseInsensitive_documentation);

    FunctionDocumentation::Description wordShingleMinHashUTF8_description = R"(
Splits a UTF-8 string into parts (shingles) of `shinglesize` words, calculates hash values for each word shingle and returns a tuple with these hashes.
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case sensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleMinHashUTF8_syntax = "wordShingleMinHashUTF8(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHashUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHashUTF8_returned_value = {"Returns a tuple with two hashes — the minimum and the maximum.", {"Tuple(UInt64, UInt64)"}};
    FunctionDocumentation::Examples wordShingleMinHashUTF8_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;",
            R"(
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHashUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleMinHashUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHashUTF8_documentation = {wordShingleMinHashUTF8_description, wordShingleMinHashUTF8_syntax, wordShingleMinHashUTF8_arguments, wordShingleMinHashUTF8_returned_value, wordShingleMinHashUTF8_examples, wordShingleMinHashUTF8_introduced_in, wordShingleMinHashUTF8_category};
    factory.registerFunction<FunctionWordShingleMinHashUTF8>(wordShingleMinHashUTF8_documentation);

    FunctionDocumentation::Description wordShingleMinHashCaseInsensitiveUTF8_description = R"(
Splits a UTF-8 string into parts (shingles) of `shinglesize` words, calculates hash values for each word shingle and returns a tuple with these hashes.
Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash.
It is case insensitive.

Can be used to detect semi-duplicate strings with [`tupleHammingDistance`](../functions/tuple-functions.md#tupleHammingDistance).
For two strings, if the returned hashes are the same for both strings, then those strings are the same.
)";
    FunctionDocumentation::Syntax wordShingleMinHashCaseInsensitiveUTF8_syntax = "wordShingleMinHashCaseInsensitiveUTF8(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHashCaseInsensitiveUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHashCaseInsensitiveUTF8_returned_value = {"Returns a tuple with two hashes — the minimum and the maximum.", {"Tuple(UInt64, UInt64)"}};
    FunctionDocumentation::Examples wordShingleMinHashCaseInsensitiveUTF8_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHashCaseInsensitiveUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleMinHashCaseInsensitiveUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHashCaseInsensitiveUTF8_documentation = {wordShingleMinHashCaseInsensitiveUTF8_description, wordShingleMinHashCaseInsensitiveUTF8_syntax, wordShingleMinHashCaseInsensitiveUTF8_arguments, wordShingleMinHashCaseInsensitiveUTF8_returned_value, wordShingleMinHashCaseInsensitiveUTF8_examples, wordShingleMinHashCaseInsensitiveUTF8_introduced_in, wordShingleMinHashCaseInsensitiveUTF8_category};
    factory.registerFunction<FunctionWordShingleMinHashCaseInsensitiveUTF8>(wordShingleMinHashCaseInsensitiveUTF8_documentation);

    FunctionDocumentation::Description ngramMinHashArg_description = R"(
Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [`ngramMinHash`](#ngramMinHash) function with the same input.
It is case sensitive.
)";
    FunctionDocumentation::Syntax ngramMinHashArg_syntax = "ngramMinHashArg(string[, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHashArg_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramMinHashArg_returned_value = {"Returns a tuple with two tuples with `hashnum` n-grams each.", {"Tuple(String)"}};
    FunctionDocumentation::Examples ngramMinHashArg_examples = {
        {
            "Usage example",
            "SELECT ngramMinHashArg('ClickHouse') AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('Hou','lic','ick','ous','ckH','Cli')) │
└───────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHashArg_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHashArg_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHashArg_documentation = {ngramMinHashArg_description, ngramMinHashArg_syntax, ngramMinHashArg_arguments, ngramMinHashArg_returned_value, ngramMinHashArg_examples, ngramMinHashArg_introduced_in, ngramMinHashArg_category};
    factory.registerFunction<FunctionNgramMinHashArg>(ngramMinHashArg_documentation);

    FunctionDocumentation::Description ngramMinHashArgCaseInsensitive_description = R"(
Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [`ngramMinHashCaseInsensitive`](#ngramMinHashCaseInsensitive) function with the same input.
It is case insensitive.
)";
    FunctionDocumentation::Syntax ngramMinHashArgCaseInsensitive_syntax = "ngramMinHashArgCaseInsensitive(string[, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHashArgCaseInsensitive_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramMinHashArgCaseInsensitive_returned_value = {"Returns a tuple with two tuples with `hashnum` n-grams each.", {"Tuple(Tuple(String))"}};
    FunctionDocumentation::Examples ngramMinHashArgCaseInsensitive_examples = {
        {
            "Usage example",
            "SELECT ngramMinHashArgCaseInsensitive('ClickHouse') AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','kHo','use','Cli'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHashArgCaseInsensitive_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHashArgCaseInsensitive_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHashArgCaseInsensitive_documentation = {ngramMinHashArgCaseInsensitive_description, ngramMinHashArgCaseInsensitive_syntax, ngramMinHashArgCaseInsensitive_arguments, ngramMinHashArgCaseInsensitive_returned_value, ngramMinHashArgCaseInsensitive_examples, ngramMinHashArgCaseInsensitive_introduced_in, ngramMinHashArgCaseInsensitive_category};
    factory.registerFunction<FunctionNgramMinHashArgCaseInsensitive>(ngramMinHashArgCaseInsensitive_documentation);

    FunctionDocumentation::Description ngramMinHashArgUTF8_description = R"(
Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the `ngramMinHashUTF8` function with the same input.
It is case sensitive.
)";
    FunctionDocumentation::Syntax ngramMinHashArgUTF8_syntax = "ngramMinHashArgUTF8(string[, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHashArgUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramMinHashArgUTF8_returned_value = {"Returns a tuple with two tuples with `hashnum` n-grams each.", {"Tuple(Tuple(String))"}};
    FunctionDocumentation::Examples ngramMinHashArgUTF8_examples = {
        {
            "Usage example",
            "SELECT ngramMinHashArgUTF8('ClickHouse') AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('kHo','Hou','lic','ick','ous','ckH')) │
└───────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHashArgUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHashArgUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHashArgUTF8_documentation = {ngramMinHashArgUTF8_description, ngramMinHashArgUTF8_syntax, ngramMinHashArgUTF8_arguments, ngramMinHashArgUTF8_returned_value, ngramMinHashArgUTF8_examples, ngramMinHashArgUTF8_introduced_in, ngramMinHashArgUTF8_category};
    factory.registerFunction<FunctionNgramMinHashArgUTF8>(ngramMinHashArgUTF8_documentation);

    FunctionDocumentation::Description ngramMinHashArgCaseInsensitiveUTF8_description = R"(
Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the ngramMinHashCaseInsensitiveUTF8 function with the same input.
It is case insensitive.
)";
    FunctionDocumentation::Syntax ngramMinHashArgCaseInsensitiveUTF8_syntax = "ngramMinHashArgCaseInsensitiveUTF8(string[, ngramsize, hashnum])";
    FunctionDocumentation::Arguments ngramMinHashArgCaseInsensitiveUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"ngramsize", "Optional. The size of an n-gram, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue ngramMinHashArgCaseInsensitiveUTF8_returned_value = {"Returns a tuple with two tuples with `hashnum` n-grams each.", {"Tuple(Tuple(String))"}};
    FunctionDocumentation::Examples ngramMinHashArgCaseInsensitiveUTF8_examples = {
        {
            "Usage example",
            "SELECT ngramMinHashArgCaseInsensitiveUTF8('ClickHouse') AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ckH','ous','ick','lic','kHo','use'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn ngramMinHashArgCaseInsensitiveUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category ngramMinHashArgCaseInsensitiveUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation ngramMinHashArgCaseInsensitiveUTF8_documentation = {ngramMinHashArgCaseInsensitiveUTF8_description, ngramMinHashArgCaseInsensitiveUTF8_syntax, ngramMinHashArgCaseInsensitiveUTF8_arguments, ngramMinHashArgCaseInsensitiveUTF8_returned_value, ngramMinHashArgCaseInsensitiveUTF8_examples, ngramMinHashArgCaseInsensitiveUTF8_introduced_in, ngramMinHashArgCaseInsensitiveUTF8_category};
    factory.registerFunction<FunctionNgramMinHashArgCaseInsensitiveUTF8>(ngramMinHashArgCaseInsensitiveUTF8_documentation);

    FunctionDocumentation::Description wordShingleMinHashArg_description = R"(
Splits a ASCII string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the wordShingleMinHash function with the same input.
It is case sensitive.
)";
    FunctionDocumentation::Syntax wordShingleMinHashArg_syntax = "wordShingleMinHashArg(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHashArg_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHashArg_returned_value = {"Returns a tuple with two tuples with `hashnum` word shingles each.", {"Tuple(Tuple(String))"}};
    FunctionDocumentation::Examples wordShingleMinHashArg_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHashArg('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHashArg_introduced_in = {1, 1};
    FunctionDocumentation::Category wordShingleMinHashArg_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHashArg_documentation = {wordShingleMinHashArg_description, wordShingleMinHashArg_syntax, wordShingleMinHashArg_arguments, wordShingleMinHashArg_returned_value, wordShingleMinHashArg_examples, wordShingleMinHashArg_introduced_in, wordShingleMinHashArg_category};
    factory.registerFunction<FunctionWordShingleMinHashArg>(wordShingleMinHashArg_documentation);

    FunctionDocumentation::Description wordShingleMinHashArgCaseInsensitive_description = R"(
Splits a ASCII string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [`wordShingleMinHashCaseInsensitive`](#wordShingleMinHashCaseInsensitive) function with the same input.
It is case insensitive.
)";
    FunctionDocumentation::Syntax wordShingleMinHashArgCaseInsensitive_syntax = "wordShingleMinHashArgCaseInsensitive(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHashArgCaseInsensitive_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHashArgCaseInsensitive_returned_value = {"Returns a tuple with two tuples with `hashnum` word shingles each.", {"Tuple(Tuple(String))"}};
    FunctionDocumentation::Examples wordShingleMinHashArgCaseInsensitive_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHashArgCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;",
            R"(
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHashArgCaseInsensitive_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleMinHashArgCaseInsensitive_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHashArgCaseInsensitive_documentation = {wordShingleMinHashArgCaseInsensitive_description, wordShingleMinHashArgCaseInsensitive_syntax, wordShingleMinHashArgCaseInsensitive_arguments, wordShingleMinHashArgCaseInsensitive_returned_value, wordShingleMinHashArgCaseInsensitive_examples, wordShingleMinHashArgCaseInsensitive_introduced_in, wordShingleMinHashArgCaseInsensitive_category};
    factory.registerFunction<FunctionWordShingleMinHashArgCaseInsensitive>(wordShingleMinHashArgCaseInsensitive_documentation);

    FunctionDocumentation::Description wordShingleMinHashArgUTF8_description = R"(
Splits a UTF-8 string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [`wordShingleMinHashUTF8`](#wordShingleMinHashUTF8) function with the same input.
It is case sensitive.
)";
    FunctionDocumentation::Syntax wordShingleMinHashArgUTF8_syntax = "wordShingleMinHashArgUTF8(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHashArgUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHashArgUTF8_returned_value = {"Returns a tuple with two tuples with `hashnum` word shingles each.", {"Tuple(Tuple(String))"}};
    FunctionDocumentation::Examples wordShingleMinHashArgUTF8_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHashArgUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;",
            R"(
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHashArgUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleMinHashArgUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHashArgUTF8_documentation = {wordShingleMinHashArgUTF8_description, wordShingleMinHashArgUTF8_syntax, wordShingleMinHashArgUTF8_arguments, wordShingleMinHashArgUTF8_returned_value, wordShingleMinHashArgUTF8_examples, wordShingleMinHashArgUTF8_introduced_in, wordShingleMinHashArgUTF8_category};
    factory.registerFunction<FunctionWordShingleMinHashArgUTF8>(wordShingleMinHashArgUTF8_documentation);

    FunctionDocumentation::Description wordShingleMinHashArgCaseInsensitiveUTF8_description = R"(
Splits a UTF-8 string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [`wordShingleMinHashCaseInsensitiveUTF8`](#wordShingleMinHashCaseInsensitiveUTF8) function with the same input.
It is case insensitive.
)";
    FunctionDocumentation::Syntax wordShingleMinHashArgCaseInsensitiveUTF8_syntax = "wordShingleMinHashArgCaseInsensitiveUTF8(string[, shinglesize, hashnum])";
    FunctionDocumentation::Arguments wordShingleMinHashArgCaseInsensitiveUTF8_arguments = {
        {"string", "String for which to compute the hash.", {"String"}},
        {"shinglesize", "Optional. The size of a word shingle, any number from `1` to `25`. The default value is `3`.", {"UInt8"}},
        {"hashnum", "Optional. The number of minimum and maximum hashes used to calculate the result, any number from `1` to `25`. The default value is `6`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue wordShingleMinHashArgCaseInsensitiveUTF8_returned_value = {"Returns a tuple with two tuples with `hashnum` word shingles each.", {"Tuple(Tuple(String))"}};
    FunctionDocumentation::Examples wordShingleMinHashArgCaseInsensitiveUTF8_examples = {
        {
            "Usage example",
            "SELECT wordShingleMinHashArgCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;",
            R"(
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn wordShingleMinHashArgCaseInsensitiveUTF8_introduced_in = {21, 1};
    FunctionDocumentation::Category wordShingleMinHashArgCaseInsensitiveUTF8_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wordShingleMinHashArgCaseInsensitiveUTF8_documentation = {wordShingleMinHashArgCaseInsensitiveUTF8_description, wordShingleMinHashArgCaseInsensitiveUTF8_syntax, wordShingleMinHashArgCaseInsensitiveUTF8_arguments, wordShingleMinHashArgCaseInsensitiveUTF8_returned_value, wordShingleMinHashArgCaseInsensitiveUTF8_examples, wordShingleMinHashArgCaseInsensitiveUTF8_introduced_in, wordShingleMinHashArgCaseInsensitiveUTF8_category};
    factory.registerFunction<FunctionWordShingleMinHashArgCaseInsensitiveUTF8>(wordShingleMinHashArgCaseInsensitiveUTF8_documentation);
}
}

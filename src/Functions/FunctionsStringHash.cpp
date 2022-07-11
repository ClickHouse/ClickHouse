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
        return __crc32cd(crc, val);
#else
        throw Exception("String hash is not implemented without sse4.2 support", ErrorCodes::NOT_IMPLEMENTED);
#endif
    }

    static UInt64 crc32u32(UInt64 crc [[maybe_unused]], UInt32 val [[maybe_unused]])
    {
#ifdef __SSE4_2__
        return _mm_crc32_u32(crc, val);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32cw(crc, val);
#else
        throw Exception("String hash is not implemented without sse4.2 support", ErrorCodes::NOT_IMPLEMENTED);
#endif
    }

    static UInt64 crc32u16(UInt64 crc [[maybe_unused]], UInt16 val [[maybe_unused]])
    {
#ifdef __SSE4_2__
        return _mm_crc32_u16(crc, val);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32ch(crc, val);
#else
        throw Exception("String hash is not implemented without sse4.2 support", ErrorCodes::NOT_IMPLEMENTED);
#endif
    }

    static UInt64 crc32u8(UInt64 crc [[maybe_unused]], UInt8 val [[maybe_unused]])
    {
#ifdef __SSE4_2__
        return _mm_crc32_u8(crc, val);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32cb(crc, val);
#else
        throw Exception("String hash is not implemented without sse4.2 support", ErrorCodes::NOT_IMPLEMENTED);
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

            crc = crc32u8(crc, x);
            --size;
            ++start;
        }

        if (size & 2)
        {
            UInt16 x = unalignedLoad<UInt16>(start);

            if constexpr (CaseInsensitive)
                x |= 0x2020u;

            crc = crc32u16(crc, x);
            size -= 2;
            start += 2;
        }

        if (size & 4)
        {
            UInt32 x = unalignedLoad<UInt32>(start);

            if constexpr (CaseInsensitive)
                x |= 0x20202020u;

            crc = crc32u32(crc, x);
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
            // for example, N = 5, array |a0|a1|a2|a3|a4|, now , a0 is the oldest location,
            // so we need to store new word hash into location of a0, then ,this array become
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

    static void apply(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, size_t shingle_size, PaddedPODArray<UInt64> & res)
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * one_data = &data[offsets[i - 1]];
            const size_t data_size = offsets[i] - offsets[i - 1] - 1;

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
        ColumnTuple * res2_strings)
    {
        MinHeap min_heap;
        MaxHeap max_heap;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * one_data = &data[offsets[i - 1]];
            const size_t data_size = offsets[i] - offsets[i - 1] - 1;

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

void registerFunctionsStringHash(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNgramSimHash>();
    factory.registerFunction<FunctionNgramSimHashCaseInsensitive>();
    factory.registerFunction<FunctionNgramSimHashUTF8>();
    factory.registerFunction<FunctionNgramSimHashCaseInsensitiveUTF8>();
    factory.registerFunction<FunctionWordShingleSimHash>();
    factory.registerFunction<FunctionWordShingleSimHashCaseInsensitive>();
    factory.registerFunction<FunctionWordShingleSimHashUTF8>();
    factory.registerFunction<FunctionWordShingleSimHashCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionNgramMinHash>();
    factory.registerFunction<FunctionNgramMinHashCaseInsensitive>();
    factory.registerFunction<FunctionNgramMinHashUTF8>();
    factory.registerFunction<FunctionNgramMinHashCaseInsensitiveUTF8>();
    factory.registerFunction<FunctionWordShingleMinHash>();
    factory.registerFunction<FunctionWordShingleMinHashCaseInsensitive>();
    factory.registerFunction<FunctionWordShingleMinHashUTF8>();
    factory.registerFunction<FunctionWordShingleMinHashCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionNgramMinHashArg>();
    factory.registerFunction<FunctionNgramMinHashArgCaseInsensitive>();
    factory.registerFunction<FunctionNgramMinHashArgUTF8>();
    factory.registerFunction<FunctionNgramMinHashArgCaseInsensitiveUTF8>();
    factory.registerFunction<FunctionWordShingleMinHashArg>();
    factory.registerFunction<FunctionWordShingleMinHashArgCaseInsensitive>();
    factory.registerFunction<FunctionWordShingleMinHashArgUTF8>();
    factory.registerFunction<FunctionWordShingleMinHashArgCaseInsensitiveUTF8>();
}
}


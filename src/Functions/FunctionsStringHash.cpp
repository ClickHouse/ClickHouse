#include <Functions/FunctionsStringHash.h>

#include <Functions/ExtractString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/PODArray.h>

#include <Core/Defines.h>

#include <bitset>
#include <functional>
#include <memory>
#include <tuple>
#include <vector>
#include <common/unaligned.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

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

    static ALWAYS_INLINE inline UInt64 ngramASCIIHash(const UInt8 * code_points)
    {
        return crc32u64(-1ULL, unalignedLoad<UInt32>(code_points));
    }

    static ALWAYS_INLINE inline UInt64 ngramUTF8Hash(const UInt32 * code_points)
    {
        UInt64 crc = -1ULL;
        crc = crc32u64(crc, code_points[0]);
        crc = crc32u64(crc, code_points[1]);
        crc = crc32u64(crc, code_points[2]);
        return crc;
    }

    static ALWAYS_INLINE inline UInt64 wordShinglesHash(const UInt64 * hashes, size_t size, size_t offset)
    {
        UInt64 crc1 = -1ULL;
        UInt64 crc2 = -1ULL;

        for (size_t i = offset; i < size; i += 2)
            crc1 = crc32u64(crc1, hashes[i]);
        for (size_t i = offset + 1; i < size; i += 2)
            crc2 = crc32u64(crc2, hashes[i]);

        if ((size - offset) & 1)
        {
            for (size_t i = 0; i < offset; i += 2)
                crc2 = crc32u64(crc2, hashes[i]);
            for (size_t i = 1; i < offset; i += 2)
                crc1 = crc32u64(crc1, hashes[i]);
        }
        else
        {
            for (size_t i = 0; i < offset; i += 2)
                crc1 = crc32u64(crc1, hashes[i]);
            for (size_t i = 1; i < offset; i += 2)
                crc2 = crc32u64(crc2, hashes[i]);
        }

        return crc1 | (crc2 << 32u);
    }

    static ALWAYS_INLINE inline UInt64 hashSum(const UInt8 * hashes [[maybe_unused]], size_t K [[maybe_unused]])
    {
        UInt64 crc1 = -1ULL;
        UInt64 crc2 = -1ULL;

        for (size_t i = 0; i < K; i += 2)
            crc1 = crc32u8(crc1, hashes[i]);
        for (size_t i = 1; i < K; i += 2)
            crc2 = crc32u8(crc2, hashes[i]);

        return crc1 | (crc2 << 32u);
    }

    static ALWAYS_INLINE inline UInt64 hashSum(const UInt32 * hashes [[maybe_unused]], size_t K [[maybe_unused]])
    {
        UInt64 crc1 = -1ULL;
        UInt64 crc2 = -1ULL;

        for (size_t i = 0; i < K; i += 2)
            crc1 = crc32u32(crc1, hashes[i]);
        for (size_t i = 1; i < K; i += 2)
            crc2 = crc32u32(crc2, hashes[i]);

        return crc1 | (crc2 << 32u);
    }

    static ALWAYS_INLINE inline UInt64 hashSum(const UInt64 * hashes, size_t K)
    {
        UInt64 crc1 = -1ULL;
        UInt64 crc2 = -1ULL;

        for (size_t i = 0; i < K; i += 2)
            crc1 = crc32u64(crc1, hashes[i]);
        for (size_t i = 1; i < K; i += 2)
            crc2 = crc32u64(crc2, hashes[i]);

        return crc1 | (crc2 << 32u);
    }
};

// SimHash String -> UInt64
// N: the length of ngram or words shingles
// CodePoint: UInt8(ASCII) or UInt32(UTF8)
// UTF8: means ASCII or UTF8, these two parameters CodePoint and UTF8 can only be (UInt8, false) or (UInt32, true)
// Ngram: means ngram(true) or words shingles(false)
// CaseInsensitive: means should we consider about letter case or not
template <size_t N, typename CodePoint, bool UTF8, bool Ngram, bool CaseInsensitive>
struct SimHashImpl
{
    using StrOp = ExtractStringImpl<N, CaseInsensitive>;
    static constexpr size_t simultaneously_codepoints_num = StrOp::buffer_size;

    // SimHash ngram calculate function: String ->UInt64
    // this function extracting ngram from input string, and maintain a 64-dimensions vector
    // for each ngram, calculate a 64 bit hash value, and update the vector according the hash value
    // finally return a 64 bit value(UInt64), i'th bit is 1 means vector[i] > 0, otherwise, vector[i] < 0
    static ALWAYS_INLINE inline UInt64 ngramCalculateHashValue(
        const char * data,
        size_t size,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt64 (*hash_functor)(const CodePoint *))
    {
        const char * start = data;
        const char * end = data + size;
        // fingerprint vector, all dimensions initialized to zero at the first
        Int64 finger_vec[64] = {};
        CodePoint cp[simultaneously_codepoints_num] = {};

        size_t found = read_code_points(cp, start, end);
        size_t iter = N - 1;

        do
        {
            for (; iter + N <= found; ++iter)
            {
                // for each ngram, we can calculate an 64 bit hash
                // then update finger_vec according to this hash value
                // if the i'th bit is 1, finger_vec[i] plus 1, otherwise minus 1
                UInt64 hash_value = hash_functor(cp + iter);
                std::bitset<64> bits(hash_value);
                for (size_t i = 0; i < 64; ++i)
                {
                    finger_vec[i] += ((bits.test(i)) ? 1 : -1);
                }
            }
            iter = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        // finally, we return a 64 bit value according to finger_vec
        // if finger_vec[i] > 0, the i'th bit of the value is 1, otherwise 0
        std::bitset<64> res_bit(0u);
        for (size_t i = 0; i < 64; ++i)
        {
            if (finger_vec[i] > 0)
                res_bit.set(i);
        }
        return res_bit.to_ullong();
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
    static ALWAYS_INLINE inline UInt64 wordShinglesCalculateHashValue(
        const char * data,
        size_t size,
        size_t (*read_one_word)(PaddedPODArray<CodePoint> &, const char *&, const char *),
        UInt64 (*hash_functor)(const UInt64 *, size_t, size_t))
    {
        const char * start = data;
        const char * end = data + size;

        // Also, a 64 bit vector initialized to zero
        Int64 finger_vec[64] = {};
        // a array to store N word hash values
        UInt64 nword_hashes[N] = {};
        // word buffer to store one word
        PaddedPODArray<CodePoint> word_buf;
        // get first word shingle
        for (size_t i = 0; i < N && start < end; ++i)
        {
            read_one_word(word_buf, start, end);
            if (!word_buf.empty())
            {
                // for each word, calculate a hash value and stored into the array
                nword_hashes[i++] = Hash::hashSum(word_buf.data(), word_buf.size());
            }
        }

        // calculate the first word shingle hash value
        UInt64 hash_value = hash_functor(nword_hashes, N, 0);
        std::bitset<64> first_bits(hash_value);
        for (size_t i = 0; i < 64; ++i)
        {
            finger_vec[i] += ((first_bits.test(i)) ? 1 : -1);
        }

        size_t offset = 0;
        while (start < end && read_one_word(word_buf, start, end))
        {
            // we need to store the new word hash value to the oldest location.
            // for example, N = 5, array |a0|a1|a2|a3|a4|, now , a0 is the oldest location,
            // so we need to store new word hash into location of a0, then ,this array become
            // |a5|a1|a2|a3|a4|, next time, a1 become the oldest location, we need to store new
            // word hash value into location of a1, then array become |a5|a6|a2|a3|a4|
            nword_hashes[offset] = Hash::hashSum(word_buf.data(), word_buf.size());
            offset = (offset + 1) % N;
            // according to the word hash storation way, in order to not lose the word shingle's
            // sequence information, when calculation word shingle hash value, we need provide the offset
            // information, which is the offset of the first word's hash value of the word shingle
            hash_value = hash_functor(nword_hashes, N, offset);
            std::bitset<64> bits(hash_value);
            for (size_t i = 0; i < 64; ++i)
            {
                finger_vec[i] += ((bits.test(i)) ? 1 : -1);
            }
        }

        std::bitset<64> res_bit(0u);
        for (size_t i = 0; i < 64; ++i)
        {
            if (finger_vec[i] > 0)
                res_bit.set(i);
        }
        return res_bit.to_ullong();
    }

    static void apply(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * one_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            const size_t data_size = offsets[i] - offsets[i - 1] - 1;

            if constexpr (Ngram)
            {
                if constexpr (!UTF8)
                    res[i] = ngramCalculateHashValue(one_data, data_size, StrOp::readASCIICodePoints, Hash::ngramASCIIHash);
                else
                    res[i] = ngramCalculateHashValue(one_data, data_size, StrOp::readUTF8CodePoints, Hash::ngramUTF8Hash);
            }
            else
            {
                if constexpr (!UTF8)
                    res[i] = wordShinglesCalculateHashValue(one_data, data_size, StrOp::readOneASCIIWord, Hash::wordShinglesHash);
                else
                    res[i] = wordShinglesCalculateHashValue(one_data, data_size, StrOp::readOneUTF8Word, Hash::wordShinglesHash);
            }
        }
    }
};

template <typename F, size_t K, size_t v>
class FixedHeap
{
public:
    FixedHeap() = delete;

    explicit FixedHeap(F f_) : f(f_), data_t(std::make_shared<std::vector<UInt64>>(K, v))
    {
        std::make_heap(data_t->begin(), data_t->end(), f);
    }

    void insertAndReplace(UInt64 new_v)
    {
        data_t->push_back(new_v);
        std::push_heap(data_t->begin(), data_t->end(), f);
        std::pop_heap(data_t->begin(), data_t->end(), f);
        data_t->pop_back();
    }

    const UInt64 * data() { return data_t->data(); }

private:
    F f;
    std::shared_ptr<std::vector<UInt64>> data_t;
};


// MinHash: String -> Tuple(UInt64, UInt64)
// for each string, we extract ngram or word shingle,
// for each ngram or word shingle, calculate a hash value,
// then we take the K minimum hash values to calculate a hashsum,
// and take the K maximum hash values to calculate another hashsum,
// return this two hashsum: Tuple(hashsum1, hashsum2)
//
// N: the length of ngram or words shingles
// K: the number of minimum hashes and maximum hashes that we keep
// CodePoint: UInt8(ASCII) or UInt32(UTF8)
// UTF8: means ASCII or UTF8, these two parameters CodePoint and UTF8 can only be (UInt8, false) or (UInt32, true)
// Ngram: means ngram(true) or words shingles(false)
// CaseInsensitive: means should we consider about letter case or not
template <size_t N, size_t K, typename CodePoint, bool UTF8, bool Ngram, bool CaseInsensitive>
struct MinHashImpl
{
    using Less = std::less<size_t>;
    using Greater = std::greater<size_t>;
    using MaxHeap = FixedHeap<std::less<size_t>, K, -1ULL>;
    using MinHeap = FixedHeap<std::greater<size_t>, K, 0>;
    using StrOp = ExtractStringImpl<N, CaseInsensitive>;
    static constexpr size_t simultaneously_codepoints_num = StrOp::buffer_size;

    // MinHash ngram calculate function, String -> Tuple(UInt64, UInt64)
    // we extract ngram from input string, and calculate a hash value for each ngram
    // then we take the K minimum hash values to calculate a hashsum,
    // and take the K maximum hash values to calculate another hashsum,
    // return this two hashsum: Tuple(hashsum1, hashsum2)
    static ALWAYS_INLINE inline std::tuple<UInt64, UInt64> ngramCalculateHashValue(
        const char * data,
        size_t size,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt64 (*hash_functor)(const CodePoint *))
    {
        const char * start = data;
        const char * end = data + size;
        // we just maintain the K minimu and K maximum hash values
        MaxHeap k_minimum_hashes(Less{});
        MinHeap k_maximum_hashes(Greater{});
        CodePoint cp[simultaneously_codepoints_num] = {};

        size_t found = read_code_points(cp, start, end);
        size_t iter = N - 1;

        do
        {
            for (; iter + N <= found; ++iter)
            {
                auto new_hash = hash_functor(cp + iter);
                // insert the new hash value into array used to store K minimum value
                // and K maximum value
                k_minimum_hashes.insertAndReplace(new_hash);
                k_maximum_hashes.insertAndReplace(new_hash);
            }
            iter = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        // calculate hashsum of the K minimum hash values and K maximum hash values
        UInt64 res1 = Hash::hashSum(k_minimum_hashes.data(), K);
        UInt64 res2 = Hash::hashSum(k_maximum_hashes.data(), K);
        return std::make_tuple(res1, res2);
    }

    // MinHash word shingle hash value calculate function: String ->Tuple(UInt64, UInt64)
    // for each word shingle, we calculate a hash value, but in fact, we just maintain the
    // K minimum and K maximum hash value
    static ALWAYS_INLINE inline std::tuple<UInt64, UInt64> wordShinglesCalculateHashValue(
        const char * data,
        size_t size,
        size_t (*read_one_word)(PaddedPODArray<CodePoint> &, const char *&, const char *),
        UInt64 (*hash_functor)(const UInt64 *, size_t, size_t))
    {
        const char * start = data;
        const char * end = start + size;
        // also we just store the K minimu and K maximum hash values
        MaxHeap k_minimum_hashes(Less{});
        MinHeap k_maximum_hashes(Greater{});
        // array to store n word hashes
        UInt64 nword_hashes[N] = {};
        // word buffer to store one word
        PaddedPODArray<CodePoint> word_buf;
        // how word shingle hash value calculation and word hash storation is same as we
        // have descripted in SimHash wordShinglesCalculateHashValue function
        for (size_t i = 0; i < N && start < end; ++i)
        {
            read_one_word(word_buf, start, end);
            if (!word_buf.empty())
            {
                nword_hashes[i++] = Hash::hashSum(word_buf.data(), word_buf.size());
            }
        }

        auto new_hash = hash_functor(nword_hashes, N, 0);
        k_minimum_hashes.insertAndReplace(new_hash);
        k_maximum_hashes.insertAndReplace(new_hash);

        size_t offset = 0;
        while (start < end && read_one_word(word_buf, start, end))
        {
            nword_hashes[offset] = Hash::hashSum(word_buf.data(), word_buf.size());
            offset = (offset + 1) % N;
            new_hash = hash_functor(nword_hashes, N, offset);
            k_minimum_hashes.insertAndReplace(new_hash);
            k_maximum_hashes.insertAndReplace(new_hash);
        }

        // calculate hashsum
        UInt64 res1 = Hash::hashSum(k_minimum_hashes.data(), K);
        UInt64 res2 = Hash::hashSum(k_maximum_hashes.data(), K);
        return std::make_tuple(res1, res2);
    }

    static void apply(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<UInt64> & res1,
        PaddedPODArray<UInt64> & res2)
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * one_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            const size_t data_size = offsets[i] - offsets[i - 1] - 1;

            if constexpr (Ngram)
            {
                if constexpr (!UTF8)
                    std::tie(res1[i], res2[i]) = ngramCalculateHashValue(one_data, data_size, StrOp::readASCIICodePoints, Hash::ngramASCIIHash);
                else
                    std::tie(res1[i], res2[i]) = ngramCalculateHashValue(one_data, data_size, StrOp::readUTF8CodePoints, Hash::ngramUTF8Hash);
            }
            else
            {
                if constexpr (!UTF8)
                    std::tie(res1[i], res2[i]) = wordShinglesCalculateHashValue(one_data, data_size, StrOp::readOneASCIIWord, Hash::wordShinglesHash);
                else
                    std::tie(res1[i], res2[i]) = wordShinglesCalculateHashValue(one_data, data_size, StrOp::readOneUTF8Word, Hash::wordShinglesHash);
            }
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

// SimHash
using FunctionNgramSimHash = FunctionsStringHash<SimHashImpl<4, UInt8, false, true, false>, NameNgramSimHash, true>;

using FunctionNgramSimHashCaseInsensitive
    = FunctionsStringHash<SimHashImpl<4, UInt8, false, true, true>, NameNgramSimHashCaseInsensitive, true>;

using FunctionNgramSimHashUTF8 = FunctionsStringHash<SimHashImpl<3, UInt32, true, true, false>, NameNgramSimHashUTF8, true>;

using FunctionNgramSimHashCaseInsensitiveUTF8
    = FunctionsStringHash<SimHashImpl<3, UInt32, true, true, true>, NameNgramSimHashCaseInsensitiveUTF8, true>;

using FunctionWordShingleSimHash = FunctionsStringHash<SimHashImpl<3, UInt8, false, false, false>, NameWordShingleSimHash, true>;

using FunctionWordShingleSimHashCaseInsensitive
    = FunctionsStringHash<SimHashImpl<3, UInt8, false, false, true>, NameWordShingleSimHashCaseInsensitive, true>;

using FunctionWordShingleSimHashUTF8 = FunctionsStringHash<SimHashImpl<3, UInt32, true, false, false>, NameWordShingleSimHashUTF8, true>;

using FunctionWordShingleSimHashCaseInsensitiveUTF8
    = FunctionsStringHash<SimHashImpl<3, UInt32, true, false, true>, NameWordShingleSimHashCaseInsensitiveUTF8, true>;

// MinHash
using FunctionNgramMinHash = FunctionsStringHash<MinHashImpl<4, 6, UInt8, false, true, false>, NameNgramMinHash, false>;

using FunctionNgramMinHashCaseInsensitive
    = FunctionsStringHash<MinHashImpl<4, 6, UInt8, false, true, true>, NameNgramMinHashCaseInsensitive, false>;

using FunctionNgramMinHashUTF8 = FunctionsStringHash<MinHashImpl<4, 6, UInt32, true, true, false>, NameNgramMinHashUTF8, false>;

using FunctionNgramMinHashCaseInsensitiveUTF8
    = FunctionsStringHash<MinHashImpl<4, 6, UInt32, true, true, true>, NameNgramMinHashCaseInsensitiveUTF8, false>;

using FunctionWordShingleMinHash = FunctionsStringHash<MinHashImpl<3, 6, UInt8, false, false, false>, NameWordShingleMinHash, false>;

using FunctionWordShingleMinHashCaseInsensitive
    = FunctionsStringHash<MinHashImpl<3, 6, UInt8, false, false, true>, NameWordShingleMinHashCaseInsensitive, false>;

using FunctionWordShingleMinHashUTF8
    = FunctionsStringHash<MinHashImpl<3, 6, UInt32, true, false, false>, NameWordShingleMinHashUTF8, false>;

using FunctionWordShingleMinHashCaseInsensitiveUTF8
    = FunctionsStringHash<MinHashImpl<3, 6, UInt32, true, false, true>, NameWordShingleMinHashCaseInsensitiveUTF8, false>;

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
}
}


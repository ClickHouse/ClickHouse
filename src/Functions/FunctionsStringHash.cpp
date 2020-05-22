#include <Functions/FunctionsStringHash.h>

#include <Functions/ExtractString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>

#include <Core/Defines.h>

#include <bitset>
#include <tuple>
#include <common/unaligned.h>

namespace DB
{
struct Hash
{
    static ALWAYS_INLINE inline UInt64 ngramASCIIHash(const UInt8 * code_points)
    {
        return intHashCRC32(unalignedLoad<UInt32>(code_points));
    }

    static ALWAYS_INLINE inline UInt64 ngramUTF8Hash(const UInt32 * code_points)
    {
        UInt64 combined = (static_cast<UInt64>(code_points[0]) << 32) | code_points[1];
#ifdef __SSE4_2__
        return _mm_crc32_u64(code_points[2], combined);
#else
        return (intHashCRC32(combined) ^ intHashCRC32(code_points[2]));
#endif
    }

    static ALWAYS_INLINE inline UInt64 wordShinglesHash(const UInt64 * hashes, size_t size, size_t offset)
    {
        UInt64 crc = -1ULL;
#ifdef __SSE4_2__
        for (size_t i = offset; i < size; ++i)
            crc = _mm_crc32_u64(crc, hashes[i]);
        for (size_t i = 0; i < offset; ++i)
            crc = _mm_crc32_u64(crc, hashes[i]);
#else
        for (size_t i = offset; i < size; ++i)
            crc = intHashCRC32(crc) ^ intHashCRC32(hashes[i]);
        for (size_t i = 0; i < offset; ++i)
            crc = intHashCRC32(crc) ^ intHashCRC32(hashes[i]);
#endif
        return crc;
    }

    template <typename CodePoint>
    static ALWAYS_INLINE inline UInt64 hashSum(const CodePoint * hashes, size_t K)
    {
        UInt64 crc = -1ULL;
#ifdef __SSE4_2__
        for (size_t i = 0; i < K; ++i)
            crc = _mm_crc32_u64(crc, hashes[i]);
#else
        for (size_t i = 0; i < K; ++i)
            crc = intHashCRC32(crc) ^ intHashCRC32(hashes[i]);
#endif
        return crc;
    }
};

// Simhash String -> UInt64
template <size_t N, typename CodePoint, bool UTF8, bool Ngram, bool CaseInsensitive>
struct SimhashImpl
{
    using ResultType = UInt64;
    using StrOp = ExtractStringImpl<N, CaseInsensitive>;
    // we made an assumption that the size of one word cann't exceed 128, which may not true
    // if some word's size exceed 128, it would be cut up to several word
    static constexpr size_t max_word_size = 1u << 7;
    static constexpr size_t max_string_size = 1u << 15;
    static constexpr size_t simultaneously_codepoints_num = StrOp::default_padding + N - 1;

    // Simhash ngram calculate function: String ->UInt64
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

    // Simhash word shingle calculate funtion: String -> UInt64
    // this function extracting n word shingle from input string, and maintain a 64-dimensions vector as well
    // for each word shingle, calculate a 64 bit hash value, and update the vector according the hash value
    // finally return a 64 bit value(UInt64), i'th bit is 1 means vector[i] > 0, otherwise, vector[i] < 0
    //
    // word shingle hash value calculate:
    // 1. at the first, extracts N word shingles and calculate N hash values, store into an array, use this N hash values
    // to calculate the first word shingle hash value
    // 2. next, we extrac one word each time, and calculate a new hash value of the new word,then use the latest N hash
    // values to caculate the next word shingle hash value
    static ALWAYS_INLINE inline UInt64 wordShinglesCalculateHashValue(
        const char * data,
        size_t size,
        size_t (*read_one_word)(CodePoint *, const char *&, const char *, size_t),
        UInt64 (*hash_functor)(const UInt64 *, size_t, size_t))
    {
        const char * start = data;
        const char * end = data + size;

        // Also, a 64 bit vector initialized to zero
        Int64 finger_vec[64] = {};
        // a array to store N word hash values
        UInt64 nword_hashes[N] = {};
        // word buffer to store one word
        CodePoint word_buf[max_word_size] = {};
        size_t word_size;
        // get first word shingle
        for (size_t i = 0; i < N && start < end; ++i)
        {
            word_size = read_one_word(word_buf, start, end, max_word_size);
            if (word_size)
            {
                // for each word, calculate a hash value and stored into the array
                nword_hashes[i++] = Hash::hashSum(word_buf, word_size);
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
        while (start < end && (word_size = read_one_word(word_buf, start, end, max_word_size)))
        {
            // we need to store the new word hash value to the oldest location.
            // for example, N = 5, array |a0|a1|a2|a3|a4|, now , a0 is the oldest location,
            // so we need to store new word hash into location of a0, then ,this array become
            // |a5|a1|a2|a3|a4|, next time, a1 become the oldest location, we need to store new
            // word hash value into locaion of a1, then array become |a5|a6|a2|a3|a4|
            nword_hashes[offset] = Hash::hashSum(word_buf, word_size);
            offset = (offset + 1) % N;
            // according to the word hash storation way, in order to not lose the word shingle's
            // sequence information, when calculation word shingle hash value, we need provide the offset
            // inforation, which is the offset of the first word's hash value of the word shingle
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

    template <typename CalcFunc, typename... Args>
    static ALWAYS_INLINE inline auto dispatch(CalcFunc calc_func, Args &&... args)
    {
        if constexpr (Ngram)
        {
            if constexpr (!UTF8)
                return calc_func(std::forward<Args>(args)..., StrOp::readASCIICodePointsNoPadding, Hash::ngramASCIIHash);
            else
                return calc_func(std::forward<Args>(args)..., StrOp::readUTF8CodePoints, Hash::ngramUTF8Hash);
        }
        else
        {
            if constexpr (!UTF8)
                return calc_func(std::forward<Args>(args)..., StrOp::readOneASCIIWord, Hash::wordShinglesHash);
            else
                return calc_func(std::forward<Args>(args)..., StrOp::readOneUTF8Word, Hash::wordShinglesHash);
        }
    }

    // constant string
    static inline void constant(const String data, UInt64 & res)
    {
        if constexpr (Ngram)
            res = dispatch(ngramCalculateHashValue, data.data(), data.size());
        else
            res = dispatch(wordShinglesCalculateHashValue, data.data(), data.size());
    }

    // non-constant string
    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * one_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            const size_t data_size = offsets[i] - offsets[i - 1] - 1;
            if (data_size <= max_string_size)
            {
                if constexpr (Ngram)
                    res[i] = dispatch(ngramCalculateHashValue, one_data, data_size);
                else
                    res[i] = dispatch(wordShinglesCalculateHashValue, one_data, data_size);
            }
        }
    }
};

// Minhash: String -> Tuple(UInt64, UInt64)
// for each string, we extract ngram or word shingle,
// for each ngram or word shingle, calculate a hash value,
// then we take the K minimum hash values to calculate a hashsum,
// and take the K maximum hash values to calculate another hashsum,
// return this two hashsum: Tuple(hashsum1, hashsum2)
template <size_t N, size_t K, typename CodePoint, bool UTF8, bool Ngram, bool CaseInsensitive>
struct MinhashImpl
{
    using ResultType = UInt64;
    using StrOp = ExtractStringImpl<N, CaseInsensitive>;
    static constexpr size_t max_word_size = 1u << 7;
    static constexpr size_t max_string_size = 1u << 15;
    static constexpr size_t simultaneously_codepoints_num = StrOp::default_padding + N - 1;

    // insert a new value into K minimum hash array if this value
    // is smaller than the greatest value in the array
    static ALWAYS_INLINE inline void insertMinValue(UInt64 * hashes, UInt64 v)
    {
        size_t i = 0;
        for (; i < K && hashes[i] <= v; ++i)
            ;
        if (i == K)
            return;
        for (size_t j = K - 2; j >= i; --j)
            hashes[j + 1] = hashes[j];
        hashes[i] = v;
    }

    // insert a new value into K maximum hash array if this value
    // is greater than the smallest value in the array
    static ALWAYS_INLINE inline void insertMaxValue(UInt64 * hashes, UInt64 v)
    {
        int i = K - 1;
        for (; i >= 0 && hashes[i] >= v; --i)
            ;
        if (i < 0)
            return;
        for (int j = 1; j <= i; ++j)
            hashes[j - 1] = hashes[j];
        hashes[i] = v;
    }

    // Minhash ngram calculate function, String -> Tuple(UInt64, UInt64)
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
        UInt64 k_minimum[K] = {};
        UInt64 k_maxinum[K] = {};
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
                insertMinValue(k_minimum, new_hash);
                insertMaxValue(k_maxinum, new_hash);
            }
            iter = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        // calculate hashsum of the K minimum hash values and K maximum hash values
        UInt64 res1 = Hash::hashSum(k_maxinum, K);
        UInt64 res2 = Hash::hashSum(k_maxinum, K);
        return std::make_tuple(res1, res2);
    }

    // Minhash word shingle hash value calculate function: String ->Tuple(UInt64, UInt64)
    // for each word shingle, we calculate a hash value, but in fact, we just maintain the
    // K minimum and K maximum hash value
    static ALWAYS_INLINE inline std::tuple<UInt64, UInt64> wordShinglesCalculateHashValue(
        const char * data,
        size_t size,
        size_t (*read_one_word)(CodePoint *, const char *&, const char *, size_t),
        UInt64 (*hash_functor)(const UInt64 *, size_t, size_t))
    {
        const char * start = data;
        const char * end = start + size;
        // also we just store the K minimu and K maximum hash values
        UInt64 k_minimum[K] = {};
        UInt64 k_maxinum[K] = {};
        // array to store n word hashes
        UInt64 nword_hashes[N] = {};
        // word buffer to store one word
        CodePoint word_buf[max_word_size] = {};
        size_t word_size;
        // how word shingle hash value calculation and word hash storation is same as we
        // have descripted in Simhash wordShinglesCalculateHashValue function
        for (size_t i = 0; i < N && start < end; ++i)
        {
            word_size = read_one_word(word_buf, start, end, max_word_size);
            if (word_size)
            {
                nword_hashes[i++] = Hash::hashSum(word_buf, word_size);
            }
        }

        auto new_hash = hash_functor(nword_hashes, N, 0);
        insertMinValue(k_minimum, new_hash);
        insertMaxValue(k_maxinum, new_hash);

        size_t offset = 0;
        while (start < end && (word_size = read_one_word(word_buf, start, end, max_word_size)))
        {
            nword_hashes[offset] = Hash::hashSum(word_buf, word_size);
            offset = (offset + 1) % N;
            new_hash = hash_functor(nword_hashes, N, offset);
            insertMinValue(k_minimum, new_hash);
            insertMaxValue(k_maxinum, new_hash);
        }

        // calculate hashsum
        UInt64 res1 = Hash::hashSum(k_minimum, K);
        UInt64 res2 = Hash::hashSum(k_maxinum, K);
        return std::make_tuple(res1, res2);
    }

    template <typename CalcFunc, typename... Args>
    static ALWAYS_INLINE inline auto dispatch(CalcFunc calc_func, Args &&... args)
    {
        if constexpr (Ngram)
        {
            if constexpr (!UTF8)
                return calc_func(std::forward<Args>(args)..., StrOp::readASCIICodePointsNoPadding, Hash::ngramASCIIHash);
            else
                return calc_func(std::forward<Args>(args)..., StrOp::readUTF8CodePoints, Hash::ngramUTF8Hash);
        }
        else
        {
            if constexpr (!UTF8)
                return calc_func(std::forward<Args>(args)..., StrOp::readOneASCIIWord, Hash::wordShinglesHash);
            else
                return calc_func(std::forward<Args>(args)..., StrOp::readOneUTF8Word, Hash::wordShinglesHash);
        }
    }

    // constant string
    static void constant(const String data, UInt64 & res1, UInt64 & res2)
    {
        if constexpr (Ngram)
            std::tie(res1, res2) = dispatch(ngramCalculateHashValue, data.data(), data.size());
        else
            std::tie(res1, res2) = dispatch(wordShinglesCalculateHashValue, data.data(), data.size());
    }

    // non-constant string
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<UInt64> & res1,
        PaddedPODArray<UInt64> & res2)
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * one_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            const size_t data_size = offsets[i] - offsets[i - 1] - 1;
            if (data_size <= max_string_size)
            {
                if constexpr (Ngram)
                    std::tie(res1[i], res2[i]) = dispatch(ngramCalculateHashValue, one_data, data_size);
                else
                    std::tie(res1[i], res2[i]) = dispatch(wordShinglesCalculateHashValue, one_data, data_size);
            }
        }
    }
};

struct NameNgramSimhash
{
    static constexpr auto name = "ngramSimhash";
};

struct NameNgramSimhashCaseInsensitive
{
    static constexpr auto name = "ngramSimhashCaseInsensitive";
};

struct NameNgramSimhashUTF8
{
    static constexpr auto name = "ngramSimhashUTF8";
};

struct NameNgramSimhashCaseInsensitiveUTF8
{
    static constexpr auto name = "ngramSimhashCaseInsensitiveUTF8";
};

struct NameWordShingleSimhash
{
    static constexpr auto name = "wordShingleSimhash";
};

struct NameWordShingleSimhashCaseInsensitive
{
    static constexpr auto name = "wordShingleSimhashCaseInsensitive";
};

struct NameWordShingleSimhashUTF8
{
    static constexpr auto name = "wordShingleSimhashUTF8";
};

struct NameWordShingleSimhashCaseInsensitiveUTF8
{
    static constexpr auto name = "wordShingleSimhashCaseInsensitiveUTF8";
};

struct NameNgramMinhash
{
    static constexpr auto name = "ngramMinhash";
};

struct NameNgramMinhashCaseInsensitive
{
    static constexpr auto name = "ngramMinhashCaseInsensitive";
};

struct NameNgramMinhashUTF8
{
    static constexpr auto name = "ngramMinhashUTF8";
};

struct NameNgramMinhashCaseInsensitiveUTF8
{
    static constexpr auto name = "ngramMinhashCaseInsensitiveUTF8";
};

struct NameWordShingleMinhash
{
    static constexpr auto name = "wordShingleMinhash";
};

struct NameWordShingleMinhashCaseInsensitive
{
    static constexpr auto name = "wordShingleMinhashCaseInsensitive";
};

struct NameWordShingleMinhashUTF8
{
    static constexpr auto name = "wordShingleMinhashUTF8";
};

struct NameWordShingleMinhashCaseInsensitiveUTF8
{
    static constexpr auto name = "wordShingleMinhashCaseInsensitiveUTF8";
};

// Simhash
using FunctionNgramSimhash = FunctionsStringHash<SimhashImpl<4, UInt8, false, true, false>, NameNgramSimhash, true>;

using FunctionNgramSimhashCaseInsensitive
    = FunctionsStringHash<SimhashImpl<4, UInt8, false, true, true>, NameNgramSimhashCaseInsensitive, true>;

using FunctionNgramSimhashUTF8 = FunctionsStringHash<SimhashImpl<3, UInt32, true, true, false>, NameNgramSimhashUTF8, true>;

using FunctionNgramSimhashCaseInsensitiveUTF8
    = FunctionsStringHash<SimhashImpl<3, UInt32, true, true, true>, NameNgramSimhashCaseInsensitiveUTF8, true>;

using FunctionWordShingleSimhash = FunctionsStringHash<SimhashImpl<3, UInt8, false, false, false>, NameWordShingleSimhash, true>;

using FunctionWordShingleSimhashCaseInsensitive
    = FunctionsStringHash<SimhashImpl<3, UInt8, false, false, true>, NameWordShingleSimhashCaseInsensitive, true>;

using FunctionWordShingleSimhashUTF8 = FunctionsStringHash<SimhashImpl<3, UInt32, true, false, false>, NameWordShingleSimhashUTF8, true>;

using FunctionWordShingleSimhashCaseInsensitiveUTF8
    = FunctionsStringHash<SimhashImpl<3, UInt32, true, false, true>, NameWordShingleSimhashCaseInsensitiveUTF8, true>;

// Minhash
using FunctionNgramMinhash = FunctionsStringHash<MinhashImpl<4, 6, UInt8, false, true, false>, NameNgramMinhash, false>;

using FunctionNgramMinhashCaseInsensitive
    = FunctionsStringHash<MinhashImpl<4, 6, UInt8, false, true, true>, NameNgramMinhashCaseInsensitive, false>;

using FunctionNgramMinhashUTF8 = FunctionsStringHash<MinhashImpl<4, 6, UInt32, true, true, false>, NameNgramMinhashUTF8, false>;

using FunctionNgramMinhashCaseInsensitiveUTF8
    = FunctionsStringHash<MinhashImpl<4, 6, UInt32, true, true, true>, NameNgramMinhashCaseInsensitiveUTF8, false>;

using FunctionWordShingleMinhash = FunctionsStringHash<MinhashImpl<3, 6, UInt8, false, false, false>, NameWordShingleMinhash, false>;

using FunctionWordShingleMinhashCaseInsensitive
    = FunctionsStringHash<MinhashImpl<3, 6, UInt8, false, false, true>, NameWordShingleMinhashCaseInsensitive, false>;

using FunctionWordShingleMinhashUTF8
    = FunctionsStringHash<MinhashImpl<3, 6, UInt32, true, false, false>, NameWordShingleMinhashUTF8, false>;

using FunctionWordShingleMinhashCaseInsensitiveUTF8
    = FunctionsStringHash<MinhashImpl<3, 6, UInt32, true, false, true>, NameWordShingleMinhashCaseInsensitiveUTF8, false>;

void registerFunctionsStringHash(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNgramSimhash>();
    factory.registerFunction<FunctionNgramSimhashCaseInsensitive>();
    factory.registerFunction<FunctionNgramSimhashUTF8>();
    factory.registerFunction<FunctionNgramSimhashCaseInsensitiveUTF8>();
    factory.registerFunction<FunctionWordShingleSimhash>();
    factory.registerFunction<FunctionWordShingleSimhashCaseInsensitive>();
    factory.registerFunction<FunctionWordShingleSimhashUTF8>();
    factory.registerFunction<FunctionWordShingleSimhashCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionNgramMinhash>();
    factory.registerFunction<FunctionNgramMinhashCaseInsensitive>();
    factory.registerFunction<FunctionNgramMinhashUTF8>();
    factory.registerFunction<FunctionNgramMinhashCaseInsensitiveUTF8>();
    factory.registerFunction<FunctionWordShingleMinhash>();
    factory.registerFunction<FunctionWordShingleMinhashCaseInsensitive>();
    factory.registerFunction<FunctionWordShingleMinhashUTF8>();
    factory.registerFunction<FunctionWordShingleMinhashCaseInsensitiveUTF8>();
}
}


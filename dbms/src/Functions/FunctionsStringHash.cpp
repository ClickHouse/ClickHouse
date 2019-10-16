#include <Functions/FunctionsStringHash.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>

#include <Core/Defines.h>

#include <common/unaligned.h>

#include <algorithm>
#include <bitset>
#include <climits>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_SMALL_STRING_SIZE;
}
template <size_t N, bool CaseInsensitive>
struct ExactStringImpl
{
    static constexpr size_t default_padding = 16;

    static ALWAYS_INLINE inline bool isAlphaNum(const UInt8 c)
    {
        return (c >= 48 && c <= 57) || (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
    }

    template <size_t Offset, typename Container, size_t... I>
    static ALWAYS_INLINE inline void unrollLowering(Container & cont, const std::index_sequence<I...> &)
    {
        ((cont[Offset + I] = std::tolower(cont[Offset + I])), ...);
    }

    static ALWAYS_INLINE inline void loweringString(UInt8 * s, size_t size) {}


    static ALWAYS_INLINE size_t readASCIICodePoints(UInt8 * code_points, const char *& pos, const char * end)
    {
        constexpr size_t padding_offset = default_padding - N + 1;
        memcpy(code_points, code_points + padding_offset, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(UInt8));

        memcpy(code_points + (N - 1), pos, padding_offset * sizeof(UInt8));

        if constexpr (CaseInsensitive)
        {
        }
        pos += padding_offset;
        if (pos > end)
            return default_padding - (pos - end);
        return default_padding;
    }

    static ALWAYS_INLINE inline size_t readOneASCIIWord(UInt8 * word, const char *& pos, const char * end, const size_t & max_word_size)
    {
        //jump seperators
        while (pos < end && !isAlphaNum(*pos))
            ++pos;

        // word start from here
        const char * word_start = pos;
        while (pos < end && isAlphaNum(*pos))
            ++pos;

        size_t word_size = (static_cast<size_t>(pos - word_start) <= max_word_size) ? pos - word_start : max_word_size;

        memcpy(word, word_start, word_size);
        if (CaseInsensitive)
        {
            std::transform(word, word + word_size, word, [](UInt8 c) { return tolower(c); });
        }
        return word_size;
    }

    static ALWAYS_INLINE inline size_t readUTF8CodePoints(UInt32 * code_points, const char *& pos, const char * end)
    {
        memcpy(code_points, code_points + default_padding - N + 1, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(UInt32));

        size_t num = N - 1;
        while (num < default_padding && pos < end)
        {
            size_t length = UTF8::seqLength(*pos);

            if (pos + length > end)
                length = end - pos;
            UInt32 res;
            switch (length)
            {
                case 1:
                    res = 0;
                    memcpy(&res, pos, 1);
                    break;
                case 2:
                    res = 0;
                    memcpy(&res, pos, 2);
                    break;
                case 3:
                    res = 0;
                    memcpy(&res, pos, 3);
                    break;
                default:
                    memcpy(&res, pos, 4);
            }

            if constexpr (CaseInsensitive)
            {
                switch (length)
                {
                    case 4:
                        res &= ~(1u << (5 + 3 * CHAR_BIT));
                        [[fallthrough]];
                    case 3:
                        res &= ~(1u << (5 + 2 * CHAR_BIT));
                        [[fallthrough]];
                    case 2:
                        res &= ~(1u);
                        res &= ~(1u << (5 + CHAR_BIT));
                        [[fallthrough]];
                    default:
                        res &= ~(1u << 5);
                }
            }

            pos += length;
            code_points[num++] = res;
        }
        return num;
    }

    static ALWAYS_INLINE inline bool isUTF8Sep(const UInt8 c) { return c < 128 && !isAlphaNum(c); }

    static ALWAYS_INLINE inline size_t readOneUTF8Word(UInt32 * word, const char *& pos, const char * end, const size_t & max_word_size)
    {
        // jump UTF8 seperator
        while (pos < end && isUTF8Sep(*pos))
            ++pos;
        //UTF8 word's character number
        size_t num = 0;
        while (pos < end && num < max_word_size && !isUTF8Sep(*pos))
        {
            size_t length = UTF8::seqLength(*pos);
            if (pos + length > end)
                length = end - pos;
            UInt32 res;
            switch (length)
            {
                case 1:
                    res = 0;
                    memcpy(&res, pos, 1);
                    break;
                case 2:
                    res = 0;
                    memcpy(&res, pos, 2);
                    break;
                case 3:
                    res = 0;
                    memcpy(&res, pos, 3);
                    break;
                default:
                    memcpy(&res, pos, 4);
            }

            if constexpr (CaseInsensitive)
            {
                switch (length)
                {
                    case 4:
                        res &= ~(1u << (5 + 3 * CHAR_BIT));
                        [[fallthrough]];
                    case 3:
                        res &= ~(1u << (5 + 2 * CHAR_BIT));
                        [[fallthrough]];
                    case 2:
                        res &= ~(1u);
                        res &= ~(1u << (5 + CHAR_BIT));
                        [[fallthrough]];
                    default:
                        res &= ~(1u << 5);
                }
            }
            pos += length;
            word[num++] = res;
        }
        return num;
    }
};

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

    template <typename CodePoint>
    static ALWAYS_INLINE inline UInt64 wordShinglesHash(const std::deque<std::vector<CodePoint>> & word_shingles)
    {
        size_t words_size = word_shingles.size();
        UInt64 res = 0;
        size_t i = 0;
        for (; i + 1 < words_size; i += 2)
        {
            res |= hashSum(word_shingles[i].data(), word_shingles[i].size());
            res &= hashSum(word_shingles[i + 1].data(), word_shingles[i + 1].size());
        }
        if (i < words_size)
            res |= hashSum(word_shingles[i].data(), word_shingles[i].size());
        return res;
    }

    template <typename CodePoint>
    static ALWAYS_INLINE inline UInt64 hashSum(const CodePoint * hashes, size_t K)
    {
        if (K == 1)
            return intHashCRC32(hashes[0]);
        UInt64 even = 0;
        UInt64 odd = 0;
        size_t i = 0;
        for (; i + 1 < K; i += 2)
        {
            even |= intHashCRC32(hashes[i]);
            odd |= intHashCRC32(hashes[i + 1]);
        }
        if (i < K)
            even |= intHashCRC32(hashes[K - 1]);
#ifdef __SSE4_2__
        return _mm_crc32_u64(even, odd);
#else
        return (intHashCRC32(even) ^ intHashCRC32(odd));
#endif
    }
};


template <typename ExactString, typename CodePoint, size_t N, bool UTF8, bool Ngram>
struct SimhashImpl
{
    using ResultType = UInt64;
    using WordShingles = std::deque<std::vector<CodePoint>>;
    static constexpr size_t max_word_size = 1u << 7;
    static constexpr size_t max_string_size = 1u << 15;
    static constexpr size_t simultaneously_codepoints_num = ExactString::default_padding + N - 1;

    static ALWAYS_INLINE inline UInt64 ngramCalculateHashValue(
        const char * data,
        const size_t size,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt64 (*hash_functor)(const CodePoint *))
    {
        const char * start = data;
        const char * end = data + size;
        // fingerprint vector, all dimensions initialized to zero as first
        Int64 finger_vec[64] = {};
        CodePoint cp[simultaneously_codepoints_num] = {};

        size_t found = read_code_points(cp, start, end);
        size_t iter = N - 1;

        do
        {
            for (; iter + N <= found; ++iter)
            {
                UInt64 hash_value = hash_functor(cp + iter);
                std::bitset<64> bits(hash_value);
                for (size_t i = 0; i < 64; ++i)
                {
                    finger_vec[i] += ((bits.test(i)) ? 1 : -1);
                }
            }
            iter = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        std::bitset<64> res_bit(0u);
        for (size_t i = 0; i < 64; ++i)
        {
            if (finger_vec[i] > 0)
                res_bit.set(i);
        }
        return res_bit.to_ullong();
    }

    static ALWAYS_INLINE inline UInt64 wordShinglesCalculateHashValue(
        const char * data,
        const size_t size,
        size_t (*read_one_word)(CodePoint *, const char *&, const char *, const size_t &),
        UInt64 (*hash_functor)(const WordShingles & word_shingles_))
    {
        const char * start = data;
        const char * end = data + size;

        Int64 finger_vec[64] = {};
        WordShingles word_shingles;
        // word buffer to store one word
        CodePoint word_buf[max_word_size] = {};
        size_t word_size;
        //get first word shingle
        for (size_t i = 0; i < N && start < end; ++i)
        {
            word_size = read_one_word(word_buf, start, end, max_word_size);
            if (word_size)
            {
                std::vector<CodePoint> word(word_buf, word_buf + word_size);
                word_shingles.push_back(word);
            }
        }
        if (word_shingles.size() < N)
        {
            throw Exception("String size is too small to get a " + std::to_string(N) + " words shingle", ErrorCodes::TOO_SMALL_STRING_SIZE);
        }

        UInt64 hash_value = hash_functor(word_shingles);
        std::bitset<64> bits_(hash_value);
        for (size_t i = 0; i < 64; ++i)
        {
            finger_vec[i] += ((bits_.test(i)) ? 1 : -1);
        }

        while (start < end && (word_size = read_one_word(word_buf, start, end, max_word_size)))
        {
            word_shingles.pop_front();
            std::vector<CodePoint> word(word_buf, word_buf + word_size);
            word_shingles.push_back(word);
            hash_value = hash_functor(word_shingles);
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
                return calc_func(std::forward<Args>(args)..., ExactString::readASCIICodePoints, Hash::ngramASCIIHash);
            else
                return calc_func(std::forward<Args>(args)..., ExactString::readUTF8CodePoints, Hash::ngramUTF8Hash);
        }
        else
        {
            if constexpr (!UTF8)
                return calc_func(std::forward<Args>(args)..., ExactString::readOneASCIIWord, Hash::wordShinglesHash);
            else
                return calc_func(std::forward<Args>(args)..., ExactString::readOneUTF8Word, Hash::wordShinglesHash);
        }
    }

    static inline void constant(const String data, UInt64 & res)
    {
        if constexpr (Ngram)
            res = dispatch(ngramCalculateHashValue, data.data(), data.size());
        else
            res = dispatch(wordShinglesCalculateHashValue, data.data(), data.size());
    }

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * one_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            const size_t data_size = offsets[i] - offsets[i - 1];
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

template <typename ExactString, typename CodePoint, size_t N, size_t K, bool UTF8, bool Ngram>
struct MinhashImpl
{
    using ResultType = UInt64;
    using WordShingles = std::deque<std::vector<CodePoint>>;
    static constexpr size_t max_word_size = 1u << 7;
    static constexpr size_t max_string_size = 1u << 15;
    static constexpr size_t simultaneously_codepoints_num = ExactString::default_padding + N - 1;
    using HashValues = UInt64[max_string_size];

    static ALWAYS_INLINE inline std::tuple<UInt64, UInt64> ngramCalculateHashValue(
        const char * data,
        const size_t size,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt64 (*hash_functor)(const CodePoint *))
    {
        const char * start = data;
        const char * end = data + size;
        HashValues hash_values = {};
        CodePoint cp[simultaneously_codepoints_num] = {};

        size_t found = read_code_points(cp, start, end);
        size_t iter = N - 1;
        size_t num = 0;

        do
        {
            for (; iter + N <= found; ++iter)
            {
                hash_values[num++] = hash_functor(cp + iter);
            }
            iter = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));
        std::sort(hash_values, hash_values + num);
        UInt64 res1 = Hash::hashSum(hash_values, K);
        UInt64 res2 = Hash::hashSum(hash_values + num - K, K);
        return std::make_tuple(res1, res2);
    }

    static ALWAYS_INLINE inline std::tuple<UInt64, UInt64> wordShinglesCalculateHashValue(
        const char * data,
        const size_t size,
        size_t (*read_one_word)(CodePoint *, const char *&, const char *, const size_t &),
        UInt64 (*hash_functor)(const WordShingles & word_shingles))
    {
        const char * start = data;
        const char * end = start + size;
        HashValues hash_values = {};
        WordShingles word_shingles;
        // word buffer to store one word
        CodePoint word_buf[max_word_size] = {};
        size_t word_size;
        //word shingles number
        size_t num = 0;
        //get first word shingle
        for (size_t i = 0; i < N && start < end; ++i)
        {
            word_size = read_one_word(word_buf, start, end, max_word_size);
            if (word_size)
            {
                std::vector<CodePoint> word(word_buf, word_buf + word_size);
                word_shingles.push_back(word);
            }
        }
        if (word_shingles.size() < N)
        {
            throw Exception("String size is too small to get a " + std::to_string(N) + " words shingle", ErrorCodes::TOO_SMALL_STRING_SIZE);
        }

        hash_values[num++] = hash_functor(word_shingles);

        while (start < end && (word_size = read_one_word(word_buf, start, end, max_word_size)))
        {
            word_shingles.pop_front();
            std::vector<CodePoint> word(word_buf, word_buf + word_size);
            word_shingles.push_back(word);
            hash_values[num++] = hash_functor(word_shingles);
        }

        std::sort(hash_values, hash_values + num);
        UInt64 res1 = Hash::hashSum(hash_values, K);
        UInt64 res2 = Hash::hashSum(hash_values + num - K, K);
        return std::make_tuple(res1, res2);
    }

    template <typename CalcFunc, typename... Args>
    static ALWAYS_INLINE inline auto dispatch(CalcFunc calc_func, Args &&... args)
    {
        if constexpr (Ngram)
        {
            if constexpr (!UTF8)
                return calc_func(std::forward<Args>(args)..., ExactString::readASCIICodePoints, Hash::ngramASCIIHash);
            else
                return calc_func(std::forward<Args>(args)..., ExactString::readUTF8CodePoints, Hash::ngramUTF8Hash);
        }
        else
        {
            if constexpr (!UTF8)
                return calc_func(std::forward<Args>(args)..., ExactString::readOneASCIIWord, Hash::wordShinglesHash);
            else
                return calc_func(std::forward<Args>(args)..., ExactString::readOneUTF8Word, Hash::wordShinglesHash);
        }
    }
    static void constant(const String data, UInt64 & res1, UInt64 & res2)
    {
        if constexpr (Ngram)
            std::tie(res1, res2) = dispatch(ngramCalculateHashValue, data.data(), data.size());
        else
            std::tie(res1, res2) = dispatch(wordShinglesCalculateHashValue, data.data(), data.size());
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<UInt64> & res1,
        PaddedPODArray<UInt64> & res2)
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * one_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            const size_t data_size = offsets[i] - offsets[i - 1];
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

//Simhash
using FunctionNgramSimhash = FunctionsStringHash<SimhashImpl<ExactStringImpl<4, false>, UInt8, 4, false, true>, NameNgramSimhash, true>;

using FunctionNgramSimhashCaseInsensitive
    = FunctionsStringHash<SimhashImpl<ExactStringImpl<4, true>, UInt8, 4, false, true>, NameNgramSimhashCaseInsensitive, true>;

using FunctionNgramSimhashUTF8
    = FunctionsStringHash<SimhashImpl<ExactStringImpl<3, false>, UInt32, 3, true, true>, NameNgramSimhashUTF8, true>;

using FunctionNgramSimhashCaseInsensitiveUTF8
    = FunctionsStringHash<SimhashImpl<ExactStringImpl<3, true>, UInt32, 3, true, true>, NameNgramSimhashCaseInsensitiveUTF8, true>;

using FunctionWordShingleSimhash
    = FunctionsStringHash<SimhashImpl<ExactStringImpl<3, false>, UInt8, 3, false, false>, NameWordShingleSimhash, true>;

using FunctionWordShingleSimhashCaseInsensitive
    = FunctionsStringHash<SimhashImpl<ExactStringImpl<3, true>, UInt8, 3, false, false>, NameWordShingleSimhashCaseInsensitive, true>;

using FunctionWordShingleSimhashUTF8
    = FunctionsStringHash<SimhashImpl<ExactStringImpl<3, false>, UInt32, 3, true, false>, NameWordShingleSimhashUTF8, true>;

using FunctionWordShingleSimhashCaseInsensitiveUTF8
    = FunctionsStringHash<SimhashImpl<ExactStringImpl<3, true>, UInt32, 3, true, false>, NameWordShingleSimhashCaseInsensitiveUTF8, true>;

//Minhash
using FunctionNgramMinhash = FunctionsStringHash<MinhashImpl<ExactStringImpl<4, false>, UInt8, 4, 6, false, true>, NameNgramMinhash, false>;

using FunctionNgramMinhashCaseInsensitive
    = FunctionsStringHash<MinhashImpl<ExactStringImpl<4, true>, UInt8, 4, 6, false, true>, NameNgramMinhashCaseInsensitive, false>;

using FunctionNgramMinhashUTF8
    = FunctionsStringHash<MinhashImpl<ExactStringImpl<3, false>, UInt32, 3, 6, true, true>, NameNgramMinhashUTF8, false>;

using FunctionNgramMinhashCaseInsensitiveUTF8
    = FunctionsStringHash<MinhashImpl<ExactStringImpl<3, true>, UInt32, 3, 6, true, true>, NameNgramMinhashCaseInsensitiveUTF8, false>;

using FunctionWordShingleMinhash
    = FunctionsStringHash<MinhashImpl<ExactStringImpl<3, false>, UInt8, 3, 6, false, false>, NameWordShingleMinhash, false>;

using FunctionWordShingleMinhashCaseInsensitive
    = FunctionsStringHash<MinhashImpl<ExactStringImpl<3, true>, UInt8, 3, 6, false, false>, NameWordShingleMinhashCaseInsensitive, false>;

using FunctionWordShingleMinhashUTF8
    = FunctionsStringHash<MinhashImpl<ExactStringImpl<3, false>, UInt32, 3, 6, true, false>, NameWordShingleMinhashUTF8, false>;

using FunctionWordShingleMinhashCaseInsensitiveUTF8 = FunctionsStringHash<
    MinhashImpl<ExactStringImpl<3, true>, UInt32, 3, 6, true, false>,
    NameWordShingleMinhashCaseInsensitiveUTF8,
    false>;

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


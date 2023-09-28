#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSimilarity.h>
#include <Common/PODArray.h>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Op>
struct FunctionStringDistanceImpl
{
    using ResultType = typename Op::ResultType;

    static void constantConstant(const std::string & haystack, const std::string & needle, ResultType & res)
    {
        res = Op::process(haystack.data(), haystack.size(), needle.data(), needle.size());
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<ResultType> & res)
    {
        size_t size = res.size();
        const char * haystack = reinterpret_cast<const char *>(haystack_data.data());
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = Op::process(
                haystack + haystack_offsets[i - 1],
                haystack_offsets[i] - haystack_offsets[i - 1] - 1,
                needle + needle_offsets[i - 1],
                needle_offsets[i] - needle_offsets[i - 1] - 1);
        }
    }

    static void constantVector(
        const std::string & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<ResultType> & res)
    {
        const char * haystack_data = haystack.data();
        size_t haystack_size = haystack.size();
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        size_t size = res.size();
        for (size_t i = 0; i < size; ++i)
        {
            res[i]
                = Op::process(haystack_data, haystack_size, needle + needle_offsets[i - 1], needle_offsets[i] - needle_offsets[i - 1] - 1);
        }
    }

    static void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        PaddedPODArray<ResultType> & res)
    {
        constantVector(needle, data, offsets, res);
    }

};

struct ByteHammingDistanceImpl
{
    using ResultType = UInt64;
    static ResultType inline process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        UInt64 res = 0;
        const char * haystack_end = haystack + haystack_size;
        const char * needle_end = needle + needle_size;

#ifdef __SSE4_2__
        static constexpr auto mode = _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_NEGATIVE_POLARITY;

        const char * haystack_end16 = haystack + haystack_size / 16 * 16;
        const char * needle_end16 = needle + needle_size / 16 * 16;

        for (; haystack < haystack_end16 && needle < needle_end16; haystack += 16, needle += 16)
        {
            __m128i s1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
            __m128i s2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(needle));
            auto result_mask = _mm_cmpestrm(s1, 16, s2, 16, mode);
            const __m128i mask_hi = _mm_unpackhi_epi64(result_mask, result_mask);
            res += _mm_popcnt_u64(_mm_cvtsi128_si64(result_mask)) + _mm_popcnt_u64(_mm_cvtsi128_si64(mask_hi));
        }
#endif
        for (; haystack != haystack_end && needle != needle_end; ++haystack, ++needle)
            res += *haystack != *needle;

        res = res + (haystack_end - haystack) + (needle_end - needle);
        return res;
    }
};

struct ByteJaccardIndexImpl
{
    using ResultType = Float64;
    static ResultType inline process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        if (haystack_size == 0 || needle_size == 0)
            return 0;

        std::unordered_set<char> haystack_set(haystack, haystack + haystack_size);
        std::unordered_set<char> needle_set(needle, needle + needle_size);

        size_t intersect = 0;
        for (auto elem : haystack_set)
        {
            intersect += needle_set.contains(elem);
        }
        return static_cast<Float64>(intersect) / (haystack_set.size() + needle_set.size() - intersect);
    }
};

struct ByteEditDistanceImpl
{
    using ResultType = UInt64;
    static constexpr size_t max_string_size = 1u << 16;

    static ResultType inline process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        if (haystack_size == 0 || needle_size == 0)
            return haystack_size + needle_size;

        /// Safety threshold against DoS, since we use two array to calculate the distance.
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function byteEditDistance. "
                "Should be at most {}",
                max_string_size);

        PaddedPODArray<ResultType> distances0(haystack_size + 1, 0);
        PaddedPODArray<ResultType> distances1(haystack_size + 1, 0);

        ResultType substitution = 0;
        ResultType insertion = 0;
        ResultType deletion = 0;

        for (size_t i = 0; i <= haystack_size; ++i)
            distances0[i] = i;

        for (size_t pos_needle = 0; pos_needle < needle_size; ++pos_needle)
        {
            distances1[0] = pos_needle + 1;

            for (size_t pos_haystack = 0; pos_haystack < haystack_size; pos_haystack++)
            {
                deletion = distances0[pos_haystack + 1] + 1;
                insertion = distances1[pos_haystack] + 1;
                substitution = distances0[pos_haystack];

                if (*(needle + pos_needle) != *(haystack + pos_haystack))
                    substitution += 1;

                distances1[pos_haystack + 1] = std::min(deletion, std::min(substitution, insertion));
            }
            distances0.swap(distances1);
        }

        return distances0[haystack_size];
    }
};

struct NameByteHammingDistance
{
    static constexpr auto name = "byteHammingDistance";
};

struct NameByteJaccardIndex
{
    static constexpr auto name = "byteJaccardIndex";
};

struct NameByteEditDistance
{
    static constexpr auto name = "byteEditDistance";
};

using FunctionByteHammingDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteHammingDistanceImpl>, NameByteHammingDistance>;

using FunctionByteJaccardIndex = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaccardIndexImpl>, NameByteJaccardIndex>;

using FunctionByteEditDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceImpl>, NameByteEditDistance>;

REGISTER_FUNCTION(StringHammingDistance)
{
    factory.registerFunction<FunctionByteHammingDistance>(
        FunctionDocumentation{.description = R"(Calculates the hamming distance between two bytes strings.)"});
    factory.registerAlias("mismatches", NameByteHammingDistance::name);

    factory.registerFunction<FunctionByteJaccardIndex>(
        FunctionDocumentation{.description = R"(Calculates the jaccard similarity index between two bytes strings.)"});

    factory.registerFunction<FunctionByteEditDistance>(
        FunctionDocumentation{.description = R"(Calculates the edit distance between two bytes strings.)"});
    factory.registerAlias("byteLevenshteinDistance", NameByteEditDistance::name);
}
}

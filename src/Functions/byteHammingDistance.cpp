#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSimilarity.h>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace DB
{
struct ByteHammingDistanceImpl
{
    using ResultType = UInt64;

    static void constantConstant(const std::string & haystack, const std::string & needle, UInt64 & res)
    {
        res = process(haystack.data(), haystack.size(), needle.data(), needle.size());
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<UInt64> & res)
    {
        size_t size = res.size();
        const char * haystack = reinterpret_cast<const char *>(haystack_data.data());
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = process(
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
        PaddedPODArray<UInt64> & res)
    {
        const char * haystack_data = haystack.data();
        size_t haystack_size = haystack.size();
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        size_t size = res.size();
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = process(haystack_data, haystack_size, needle + needle_offsets[i - 1], needle_offsets[i] - needle_offsets[i - 1] - 1);
        }
    }

    static void vectorConstant(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets, const std::string & needle, PaddedPODArray<UInt64> & res)
    {
        constantVector(needle, data, offsets, res);
    }

private:
    static UInt64 inline process(const char * haystack, size_t haystack_size, const char * needle, size_t needle_size)
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

struct NameByteHammingDistance
{
    static constexpr auto name = "byteHammingDistance";
};

using FunctionByteHammingDistance = FunctionsStringSimilarity<ByteHammingDistanceImpl, NameByteHammingDistance>;

REGISTER_FUNCTION(StringHammingDistance)
{
    factory.registerFunction<FunctionByteHammingDistance>(
        FunctionDocumentation{.description = R"(Calculates the hamming distance between two bytes strings.)"});
}
}

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSimilarity.h>
#include <Common/PODArray.h>
#include <Common/UTF8Helpers.h>
#include <Common/iota.h>

#include <algorithm>
#include <numeric>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Op>
struct FunctionStringDistanceImpl
{
    using ResultType = typename Op::ResultType;

    static void constantConstant(const String & haystack, const String & needle, ResultType & res)
    {
        res = Op::process(haystack.data(), haystack.size(), needle.data(), needle.size());
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<ResultType> & res,
        size_t input_rows_count)
    {
        const char * haystack = reinterpret_cast<const char *>(haystack_data.data());
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = Op::process(
                haystack + haystack_offsets[i - 1],
                haystack_offsets[i] - haystack_offsets[i - 1] - 1,
                needle + needle_offsets[i - 1],
                needle_offsets[i] - needle_offsets[i - 1] - 1);
        }
    }

    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<ResultType> & res,
        size_t input_rows_count)
    {
        const char * haystack_data = haystack.data();
        size_t haystack_size = haystack.size();
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = Op::process(haystack_data, haystack_size,
                needle + needle_offsets[i - 1], needle_offsets[i] - needle_offsets[i - 1] - 1);
        }
    }

    static void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const String & needle,
        PaddedPODArray<ResultType> & res,
        size_t input_rows_count)
    {
        constantVector(needle, data, offsets, res, input_rows_count);
    }

};

struct ByteHammingDistanceImpl
{
    using ResultType = UInt64;
    static ResultType process(
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

void parseUTF8String(const char * __restrict data, size_t size, std::function<void(UInt32)> utf8_consumer, std::function<void(unsigned char)> ascii_consumer = nullptr)
{
    const char * end = data + size;
    while (data < end)
    {
        size_t len = UTF8::seqLength(*data);
        if (len == 1)
        {
            if (ascii_consumer)
                ascii_consumer(static_cast<unsigned char>(*data));
            else
                utf8_consumer(static_cast<UInt32>(*data));
            ++data;
        }
        else
        {
            auto code_point = UTF8::convertUTF8ToCodePoint(data, end - data);
            if (code_point.has_value())
            {
                utf8_consumer(code_point.value());
                data += len;
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal UTF-8 sequence, while processing '{}'", StringRef(data, end - data));
            }
        }
    }
}

template <bool is_utf8>
struct ByteJaccardIndexImpl
{
    using ResultType = Float64;
    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        if (haystack_size == 0 || needle_size == 0)
            return 0;

        const char * haystack_end = haystack + haystack_size;
        const char * needle_end = needle + needle_size;

        /// For byte strings use plain array as a set
        constexpr size_t max_size = std::numeric_limits<unsigned char>::max() + 1;
        std::array<UInt8, max_size> haystack_set;
        std::array<UInt8, max_size> needle_set;

        /// For UTF-8 strings we also use sets of code points greater than max_size
        std::set<UInt32> haystack_utf8_set;
        std::set<UInt32> needle_utf8_set;

        haystack_set.fill(0);
        needle_set.fill(0);

        if constexpr (is_utf8)
        {
            parseUTF8String(
                haystack,
                haystack_size,
                [&](UInt32 data) { haystack_utf8_set.insert(data); },
                [&](unsigned char data) { haystack_set[data] = 1; });
            parseUTF8String(
                needle, needle_size, [&](UInt32 data) { needle_utf8_set.insert(data); }, [&](unsigned char data) { needle_set[data] = 1; });
        }
        else
        {
            while (haystack < haystack_end)
            {
                haystack_set[static_cast<unsigned char>(*haystack)] = 1;
                ++haystack;
            }
            while (needle < needle_end)
            {
                needle_set[static_cast<unsigned char>(*needle)] = 1;
                ++needle;
            }
        }

        UInt8 intersection = 0;
        UInt8 union_size = 0;

        if constexpr (is_utf8)
        {
            auto lit = haystack_utf8_set.begin();
            auto rit = needle_utf8_set.begin();
            while (lit != haystack_utf8_set.end() && rit != needle_utf8_set.end())
            {
                if (*lit == *rit)
                {
                    ++intersection;
                    ++lit;
                    ++rit;
                }
                else if (*lit < *rit)
                    ++lit;
                else
                    ++rit;
            }
            union_size = haystack_utf8_set.size() + needle_utf8_set.size() - intersection;
        }

        for (size_t i = 0; i < max_size; ++i)
        {
            intersection += haystack_set[i] & needle_set[i];
            union_size += haystack_set[i] | needle_set[i];
        }

        return static_cast<ResultType>(intersection) / static_cast<ResultType>(union_size);
    }
};

static constexpr size_t max_string_size = 1u << 16;

template<bool is_utf8>
struct ByteEditDistanceImpl
{
    using ResultType = UInt64;

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        if (haystack_size == 0 || needle_size == 0)
            return haystack_size + needle_size;

        /// Safety threshold against DoS, since we use two arrays to calculate the distance.
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function editDistance, should be at most {}", max_string_size);

        PaddedPODArray<UInt32> haystack_utf8;
        PaddedPODArray<UInt32> needle_utf8;
        if constexpr (is_utf8)
        {
            parseUTF8String(haystack, haystack_size, [&](UInt32 data) { haystack_utf8.push_back(data); });
            parseUTF8String(needle, needle_size, [&](UInt32 data) { needle_utf8.push_back(data); });
            haystack_size = haystack_utf8.size();
            needle_size = needle_utf8.size();
        }

        PaddedPODArray<ResultType> distances0(haystack_size + 1, 0);
        PaddedPODArray<ResultType> distances1(haystack_size + 1, 0);

        ResultType substitution = 0;
        ResultType insertion = 0;
        ResultType deletion = 0;

        iota(distances0.data(), haystack_size + 1, ResultType(0));

        for (size_t pos_needle = 0; pos_needle < needle_size; ++pos_needle)
        {
            distances1[0] = pos_needle + 1;

            for (size_t pos_haystack = 0; pos_haystack < haystack_size; pos_haystack++)
            {
                deletion = distances0[pos_haystack + 1] + 1;
                insertion = distances1[pos_haystack] + 1;
                substitution = distances0[pos_haystack];

                if constexpr (is_utf8)
                {
                    if (needle_utf8[pos_needle] != haystack_utf8[pos_haystack])
                        substitution += 1;
                }
                else
                {
                    if (*(needle + pos_needle) != *(haystack + pos_haystack))
                        substitution += 1;
                }
                distances1[pos_haystack + 1] = std::min({deletion, substitution, insertion});
            }
            distances0.swap(distances1);
        }

        return distances0[haystack_size];
    }
};

struct ByteDamerauLevenshteinDistanceImpl
{
    using ResultType = UInt64;

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        /// Safety threshold against DoS
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function damerauLevenshteinDistance, should be at most {}", max_string_size);

        /// Shortcuts:

        if (haystack_size == 0)
            return needle_size;

        if (needle_size == 0)
            return haystack_size;

        if (haystack_size == needle_size && memcmp(haystack, needle, haystack_size) == 0)
            return 0;

        /// Implements the algorithm for optimal string alignment distance from
        /// https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Optimal_string_alignment_distance

        /// Dynamically allocate memory for the 2D array
        /// Allocating a 2D array, for convenience starts is an array of pointers to the start of the rows.
        std::vector<int> d((needle_size + 1) * (haystack_size + 1));
        std::vector<int *> starts(haystack_size + 1);

        /// Setting the pointers in starts to the beginning of (needle_size + 1)-long intervals.
        /// Also initialize the row values based on the mentioned algorithm.
        for (size_t i = 0; i <= haystack_size; ++i)
        {
            starts[i] = d.data() + (needle_size + 1) * i;
            starts[i][0] = static_cast<int>(i);
        }

        for (size_t j = 0; j <= needle_size; ++j)
        {
            starts[0][j] = static_cast<int>(j);
        }

        for (size_t i = 1; i <= haystack_size; ++i)
        {
            for (size_t j = 1; j <= needle_size; ++j)
            {
                int cost = (haystack[i - 1] == needle[j - 1]) ? 0 : 1;
                starts[i][j] = std::min(
                    {starts[i - 1][j] + 1, /// deletion
                     starts[i][j - 1] + 1, /// insertion
                     starts[i - 1][j - 1] + cost} /// substitution
                );
                if (i > 1 && j > 1 && haystack[i - 1] == needle[j - 2] && haystack[i - 2] == needle[j - 1])
                    starts[i][j] = std::min(starts[i][j], starts[i - 2][j - 2] + 1); /// transposition
            }
        }

        return starts[haystack_size][needle_size];
    }
};

struct ByteJaroSimilarityImpl
{
    using ResultType = Float64;

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        /// Safety threshold against DoS
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function jaroSimilarity, should be at most {}", max_string_size);

        /// Shortcuts:

        if (haystack_size == 0)
            return needle_size;

        if (needle_size == 0)
            return haystack_size;

        if (haystack_size == needle_size && memcmp(haystack, needle, haystack_size) == 0)
            return 1.0;

        const int s1len = static_cast<int>(haystack_size);
        const int s2len = static_cast<int>(needle_size);

        /// Window size to search for matches in the other string
        const int max_range = std::max(0, std::max(s1len, s2len) / 2 - 1);
        std::vector<int> s1_matching(s1len, -1);
        std::vector<int> s2_matching(s2len, -1);

        /// Calculate matching characters
        size_t matching_characters = 0;
        for (int i = 0; i < s1len; i++)
        {
            /// Matching window
            const int min_index = std::max(i - max_range, 0);
            const int max_index = std::min(i + max_range + 1, s2len);
            for (int j = min_index; j < max_index; j++)
            {
                if (s2_matching[j] == -1 && haystack[i] == needle[j])
                {
                    s1_matching[i] = i;
                    s2_matching[j] = j;
                    matching_characters++;
                    break;
                }
            }
        }

        if (matching_characters == 0)
            return 0.0;

        /// Transpositions (one-way only)
        double transpositions = 0.0;
        for (size_t i = 0, s1i = 0, s2i = 0; i < matching_characters; i++)
        {
            while (s1_matching[s1i] == -1)
                s1i++;
            while (s2_matching[s2i] == -1)
                s2i++;
            if (haystack[s1i] != needle[s2i])
                transpositions += 0.5;
            s1i++;
            s2i++;
        }

        double m = static_cast<double>(matching_characters);
        double jaro_similarity = 1.0 / 3.0  * (m / static_cast<double>(s1len)
                                            + m / static_cast<double>(s2len)
                                            + (m - transpositions) / m);
        return jaro_similarity;
    }
};

struct ByteJaroWinklerSimilarityImpl
{
    using ResultType = Float64;

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        static constexpr int max_prefix_length = 4;
        static constexpr double scaling_factor =  0.1;
        static constexpr double boost_threshold = 0.7;

        /// Safety threshold against DoS
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function jaroWinklerSimilarity, should be at most {}", max_string_size);

        const int s1len = static_cast<int>(haystack_size);
        const int s2len = static_cast<int>(needle_size);

        ResultType jaro_winkler_similarity = ByteJaroSimilarityImpl::process(haystack, haystack_size, needle, needle_size);

        if (jaro_winkler_similarity > boost_threshold)
        {
            const int common_length = std::min({max_prefix_length, s1len, s2len});
            int common_prefix = 0;
            while (common_prefix < common_length && haystack[common_prefix] == needle[common_prefix])
                common_prefix++;

            jaro_winkler_similarity += common_prefix * scaling_factor * (1.0 - jaro_winkler_similarity);
        }
        return jaro_winkler_similarity;
    }
};

struct NameByteHammingDistance
{
    static constexpr auto name = "byteHammingDistance";
};
using FunctionByteHammingDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteHammingDistanceImpl>, NameByteHammingDistance>;

struct NameEditDistance
{
    static constexpr auto name = "editDistance";
};
using FunctionEditDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceImpl<false>>, NameEditDistance>;
struct NameEditDistanceUTF8
{
    static constexpr auto name = "editDistanceUTF8";
};
using FunctionEditDistanceUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceImpl<true>>, NameEditDistanceUTF8>;

struct NameDamerauLevenshteinDistance
{
    static constexpr auto name = "damerauLevenshteinDistance";
};
using FunctionDamerauLevenshteinDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteDamerauLevenshteinDistanceImpl>, NameDamerauLevenshteinDistance>;

struct NameJaccardIndex
{
    static constexpr auto name = "stringJaccardIndex";
};
using FunctionStringJaccardIndex = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaccardIndexImpl<false>>, NameJaccardIndex>;

struct NameJaccardIndexUTF8
{
    static constexpr auto name = "stringJaccardIndexUTF8";
};
using FunctionStringJaccardIndexUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaccardIndexImpl<true>>, NameJaccardIndexUTF8>;

struct NameJaroSimilarity
{
    static constexpr auto name = "jaroSimilarity";
};
using FunctionJaroSimilarity = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaroSimilarityImpl>, NameJaroSimilarity>;

struct NameJaroWinklerSimilarity
{
    static constexpr auto name = "jaroWinklerSimilarity";
};
using FunctionJaroWinklerSimilarity = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaroWinklerSimilarityImpl>, NameJaroWinklerSimilarity>;

REGISTER_FUNCTION(StringDistance)
{
    factory.registerFunction<FunctionByteHammingDistance>(
        FunctionDocumentation{.description = R"(Calculates Hamming distance between two byte-strings.)"});
    factory.registerAlias("mismatches", NameByteHammingDistance::name);

    factory.registerFunction<FunctionEditDistance>(
        FunctionDocumentation{.description = R"(Calculates the edit distance between two byte-strings.)"});
    factory.registerAlias("levenshteinDistance", NameEditDistance::name);

    factory.registerFunction<FunctionEditDistanceUTF8>(
        FunctionDocumentation{.description = R"(Calculates the edit distance between two UTF8 strings.)"});
    factory.registerAlias("levenshteinDistanceUTF8", NameEditDistanceUTF8::name);

    factory.registerFunction<FunctionDamerauLevenshteinDistance>(
        FunctionDocumentation{.description = R"(Calculates the Damerau-Levenshtein distance two between two byte-string.)"});

    factory.registerFunction<FunctionStringJaccardIndex>(
        FunctionDocumentation{.description = R"(Calculates the Jaccard similarity index between two byte strings.)"});
    factory.registerFunction<FunctionStringJaccardIndexUTF8>(
        FunctionDocumentation{.description = R"(Calculates the Jaccard similarity index between two UTF8 strings.)"});

    factory.registerFunction<FunctionJaroSimilarity>(
        FunctionDocumentation{.description = R"(Calculates the Jaro similarity between two byte-string.)"});

    factory.registerFunction<FunctionJaroWinklerSimilarity>(
        FunctionDocumentation{.description = R"(Calculates the Jaro-Winkler similarity between two byte-string.)"});
}
}

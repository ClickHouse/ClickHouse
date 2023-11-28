#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSimilarity.h>
#include <Common/PODArray.h>
#include <Common/UTF8Helpers.h>
#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif
#include <numeric>
#include <signal.h>

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
            res[i] = Op::process(haystack_data, haystack_size,
                needle + needle_offsets[i - 1], needle_offsets[i] - needle_offsets[i - 1] - 1);
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

template <bool is_utf8>
struct ByteJaccardIndexImpl
{
    using ResultType = Float64;
    static ResultType inline process(
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

        while (haystack < haystack_end)
        {
            size_t len = 1;
            if constexpr (is_utf8)
                len = UTF8::seqLength(*haystack);

            if (len == 1)
            {
                haystack_set[static_cast<unsigned char>(*haystack)] = 1;
                ++haystack;
            }
            else
            {
                auto code_point = UTF8::convertUTF8ToCodePoint(haystack, haystack_end - haystack);
                if (code_point.has_value())
                {
                    haystack_utf8_set.insert(code_point.value());
                    haystack += len;
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal UTF-8 sequence, while processing '{}'", StringRef(haystack, haystack_end - haystack));
                }
            }
        }

        while (needle < needle_end)
        {

            size_t len = 1;
            if constexpr (is_utf8)
                len = UTF8::seqLength(*needle);

            if (len == 1)
            {
                needle_set[static_cast<unsigned char>(*needle)] = 1;
                ++needle;
            }
            else
            {
                auto code_point = UTF8::convertUTF8ToCodePoint(needle, needle_end - needle);
                if (code_point.has_value())
                {
                    needle_utf8_set.insert(code_point.value());
                    needle += len;
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal UTF-8 sequence, while processing '{}'", StringRef(needle, needle_end - needle));
                }
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
                "The string size is too big for function editDistance, "
                "should be at most {}", max_string_size);

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

struct ByteDamerauLevenshteinDistanceImpl
{
    using ResultType = UInt64;
    static constexpr size_t max_string_size = 1u << 16;

    static ResultType inline process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        /// Safety threshold against DoS, since we use two array to calculate the distance.
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function editDistance, "
                "should be at most {}", max_string_size);

        // short cut cases:
  	// - null strings
  	// - zero length strings
  	// - identical length and value strings
  	if (haystack == nullptr || needle == nullptr ) return -1;
  	if (haystack_size == 0) return needle_size;
  	if (needle_size == 0) return haystack_size;
  	if (haystack_size == needle_size && memcmp(haystack, needle, haystack_size) == 0) return 0;

  	int i;
  	int j;
  	int l_cost;
  	int ptr_array_length = static_cast<int>(sizeof(int*) * (haystack_size + 1));
  	int int_array_length = static_cast<int>(sizeof(int) * (needle_size + 1) * (haystack_size + 1));
	int s1len = static_cast<int>(haystack_size);
	int s2len = static_cast<int>(needle_size);
	// Dynamically allocate memory for the 2D array
  	// Allocating a 2D array (with d being an array of pointers to the start of the rows)
  	int** d = reinterpret_cast<int**>(new int*[ptr_array_length]);
  	int* rows = reinterpret_cast<int*>(new int[int_array_length]);
  	// Setting the pointers in the pointer-array to the start of (s2len + 1) length
  	// intervals and initializing its values based on the mentioned algorithm.
  	
	for (i = 0; i <= s1len; ++i) {
    		d[i] = rows + (s2len + 1) * i;
    		d[i][0] = i;
 	}
  	std::iota(d[0], d[0] + s2len + 1, 0);

  	for (i = 1; i <= s1len; ++i) {
    		for (j = 1; j <= s2len; ++j) {
      			if (haystack[i - 1] == needle[j - 1]) {
        			l_cost = 0;
      			} else {
        			l_cost = 1;
      			}
      			d[i][j] = std::min(d[i - 1][j - 1] + l_cost, // substitution
                        	 std::min(d[i][j - 1] + 1, // insertion
                                 d[i - 1][j] + 1) // deletion
      			);
      			if (i > 1 && j > 1 && haystack[i - 1] == needle[j - 2]
          			&& haystack[i - 2] == needle[j - 1]) {
        				d[i][j] = std::min(d[i][j], d[i - 2][j - 2] + l_cost); // transposition
      			}
    		}
  	}
	
  	ResultType result = d[s1len][s2len];
	// Deallocate memory for each row
	delete[] rows;
	delete[] d;
  	return result;
    }
};

// Based on https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
// Implements Jaro similarity
struct ByteJaroSimilarityImpl {

    using ResultType = Float64;
    static constexpr size_t max_string_size = 1u << 16;

    static ResultType inline process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        int s1len = static_cast<int>(haystack_size);
        int s2len = static_cast<int>(needle_size);


        /// Safety threshold against DoS, since we use two array to calculate the distance.
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function editDistance, "
                "should be at most {}", max_string_size);

        // short cut cases:
        // - null strings
        // - zero length strings
        // - identical length and value strings
        if (haystack == nullptr || needle == nullptr ) return -1;
        if (haystack_size == 0) return needle_size;
        if (needle_size == 0) return haystack_size;
        if (haystack_size == needle_size && memcmp(haystack, needle, haystack_size) == 0) return ResultType(1.0);

        // the window size to search for matches in the other string
        int max_range = std::max(0, std::max(s1len, s2len) / 2 - 1);
        int* s1_matching = reinterpret_cast<int*>(new int[sizeof(int) * (s1len)]);
        int* s2_matching = reinterpret_cast<int*>(new int[sizeof(int) * (s2len)]);

        std::fill_n(s1_matching, s1len, -1);
        std::fill_n(s2_matching, s2len, -1);

        // calculate matching characters
        int matching_characters = 0;
        for (int i = 0; i < s1len; i++) {
            // matching window
            int min_index = std::max(i - max_range, 0);
            int max_index = std::min(i + max_range + 1, s2len);
            if (min_index >= max_index) break;

            for (int j = min_index; j < max_index; j++) {
              if (s2_matching[j] == -1 && haystack[i] == needle[j]) {
                s1_matching[i] = i;
                s2_matching[j] = j;
                matching_characters++;
                break;
              }
            }
        }

        if (matching_characters == 0) {
            delete [] s1_matching;
            delete [] s2_matching;
            return ResultType(0.0);
        }

        // transpositions (one-way only)
        double transpositions = 0.0;
        for (int i = 0, s1i = 0, s2i = 0; i < matching_characters; i++) {
        while (s1_matching[s1i] == -1) {
          s1i++;
        }
        while (s2_matching[s2i] == -1) {
          s2i++;
        }
        if (haystack[s1i] != needle[s2i]) transpositions += 0.5;
            s1i++;
            s2i++;
        }
        double m = static_cast<double>(matching_characters);
        double jaro_similarity = 1.0 / 3.0  * ( m / static_cast<double>(s1len)
                                            + m / static_cast<double>(s2len)
                                            + (m - transpositions) / m );
        delete [] s1_matching;
        delete [] s2_matching;
        return ResultType(jaro_similarity);
    }
};

// Based on https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
// Implements Jaro similarity
struct ByteJaroWinklerSimilarityImpl {

    using ResultType = Float64;
    static constexpr size_t max_string_size = 1u << 16;

    static ResultType inline process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        constexpr int MAX_PREFIX_LENGTH = 4;
        double scaling_factor =  0.10;
        double boost_threshold = 0.7;

        int s1len = static_cast<int>(haystack_size);
        int s2len = static_cast<int>(needle_size);


        /// Safety threshold against DoS, since we use two array to calculate the distance.
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function editDistance, "
                "should be at most {}", max_string_size);

        ResultType jaro_similarity = ByteJaroSimilarityImpl::process(haystack, haystack_size, needle, needle_size);
        if (jaro_similarity == -1.0) return ResultType(-1.0);

        ResultType jaro_winkler_similarity = jaro_similarity;
        if (jaro_similarity > boost_threshold) {
        	int common_length = std::min(MAX_PREFIX_LENGTH, std::min(s1len, s2len));
        	int common_prefix = 0;
       		while (common_prefix < common_length &&
              		haystack[common_prefix] == needle[common_prefix]) {
          		common_prefix++;
        	}
		
        	jaro_winkler_similarity += common_prefix * scaling_factor * (1.0 - jaro_similarity);
        }
        return ResultType(jaro_winkler_similarity);
    }
};

struct NameDamerauLevenshteinDistance
{
    static constexpr auto name = "damerauLevenshteinDistance";
};
using FunctionDamerauLevenshteinDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteDamerauLevenshteinDistanceImpl>, NameDamerauLevenshteinDistance>;

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

struct NameByteHammingDistance
{
    static constexpr auto name = "byteHammingDistance";
};
using FunctionByteHammingDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteHammingDistanceImpl>, NameByteHammingDistance>;

struct NameEditDistance
{
    static constexpr auto name = "editDistance";
};
using FunctionEditDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceImpl>, NameEditDistance>;

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

REGISTER_FUNCTION(StringDistance)
{
    factory.registerFunction<FunctionByteHammingDistance>(
        FunctionDocumentation{.description = R"(Calculates Hamming distance between two byte-strings.)"});
    factory.registerAlias("mismatches", NameByteHammingDistance::name);

    factory.registerFunction<FunctionEditDistance>(
        FunctionDocumentation{.description = R"(Calculates the edit distance between two byte-strings.)"});
    factory.registerAlias("levenshteinDistance", NameEditDistance::name);
    
    factory.registerFunction<FunctionDamerauLevenshteinDistance>(
	FunctionDocumentation{.description = R"(Calculates the damerau levenshtein distance two between two byte-string.)"});
    factory.registerAlias("damerauLevenshteinDistance", NameDamerauLevenshteinDistance::name);

    factory.registerFunction<FunctionJaroSimilarity>(
    FunctionDocumentation{.description = R"(Calculates the jaro similarity two between two byte-string.)"});
    factory.registerAlias("jaroSimilarity", NameJaroSimilarity::name);

    factory.registerFunction<FunctionJaroWinklerSimilarity>(
    FunctionDocumentation{.description = R"(Calculates the jaro winkler similarity two between two byte-string.)"});
    factory.registerAlias("jaroWinklerSimilarity", NameJaroWinklerSimilarity::name);

    factory.registerFunction<FunctionStringJaccardIndex>(
        FunctionDocumentation{.description = R"(Calculates the [Jaccard similarity index](https://en.wikipedia.org/wiki/Jaccard_index) between two byte strings.)"});
    factory.registerFunction<FunctionStringJaccardIndexUTF8>(
        FunctionDocumentation{.description = R"(Calculates the [Jaccard similarity index](https://en.wikipedia.org/wiki/Jaccard_index) between two UTF8 strings.)"});
}
}

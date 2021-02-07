#include <Functions/FunctionsTextClassification.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>

#include <Core/Defines.h>

#include <common/unaligned.h>

#include <algorithm>
#include <climits>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <utility>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace DB
{
template <size_t N>
struct TextClassificationImpl
{

    using ResultType = Float32;
    using CodePoint = UInt8;
    /// map_size for ngram count.
    static constexpr size_t map_size = 1u << 16;

    /// If the data size is bigger than this, behaviour is unspecified for this function.
    static constexpr size_t max_string_size = 1u << 15;

    /// Default padding to read safely.
    static constexpr size_t default_padding = 16;

    /// Max codepoints to store at once. 16 is for batching usage and PODArray has this padding.
    static constexpr size_t simultaneously_codepoints_num = default_padding + N - 1;

    /** map_size of this fits mostly in L2 cache all the time.
      * Actually use UInt16 as addings and subtractions do not UB overflow. But think of it as a signed
      * integer array.
      */
    using NgramCount = UInt16;

    static ALWAYS_INLINE size_t readCodePoints(CodePoint * code_points, const char *& pos, const char * end)
    {
        constexpr size_t padding_offset = default_padding - N + 1;
        memcpy(code_points, code_points + padding_offset, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(CodePoint));
        memcpy(code_points + (N - 1), pos, default_padding * sizeof(CodePoint));
        pos += padding_offset;
        if (pos > end)
            return default_padding - (pos - end);
        return default_padding;
    }

    static ALWAYS_INLINE inline size_t calculateStats(
        const char * data,
        const size_t size,
        NgramCount * ngram_stats,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        NgramCount * ngram_storage)
    {
        const char * start = data;
        const char * end = data + size;
        CodePoint cp[simultaneously_codepoints_num] = {};
        /// read_code_points returns the position of cp where it stopped reading codepoints.
        size_t found = read_code_points(cp, start, end);
        /// We need to start for the first time here, because first N - 1 codepoints mean nothing.
        size_t i = N - 1;
        size_t len = 0;
        do
        {
            for (; i + N <= found; ++i)
            {
                UInt16 hash = 0;
                for (size_t j = 0; j < N; ++j) {
                    hash <<= 8;
                    hash += *(cp + i + j);
                }
                if (ngram_stats[hash] == 0) {
                    ngram_storage[len] = hash;
                    ++len;
                }
                ++ngram_stats[hash];
            }
            i = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        return len;
    }
    
    
    static void constant(std::string data, Float32 & res)
    {
        std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}}; // frequency of N-grams 
        std::unique_ptr<NgramCount[]> ngram_storage{new NgramCount[map_size]{}}; // list of N-grams
        res = calculateStats(data.data(), data.size(), common_stats.get(), readCodePoints, ngram_storage.get()); // count of N-grams
        if (size_t i = 0; i < len; ++i) {
            // (ngram_storage.get()[0], common_stats.get()[ngram_storage.get()[0]]) - pair (N-gram, frequency)
            
        }

    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<Float32> & res)
    {
        const size_t offsets_size = offsets.size();
        size_t prev_offset = 0;

        for (size_t i = 0; i < offsets_size; ++i)
        {
            const char * haystack = reinterpret_cast<const char *>(&data[prev_offset]);
            std::string s1 = haystack;
            res[i] = s1.size();
            prev_offset = offsets[i];
        }
    }

};


struct NameBiGramcount
{
    static constexpr auto name = "biGramcount";
};
/*
struct NameTriGramcount
{
    static constexpr auto name = "triGramcount";
};
*/

using FunctionBiGramcount = FunctionsTextClassification<TextClassificationImpl<2>, NameBiGramcount>;
//using FunctionTriGramcount = FunctionsTextClassification<TextClassificationImpl<3>, NameTriGramcount>;

void registerFunctionsTextClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBiGramcount>();
//    factory.registerFunction<FunctionTriGramcount>();
}

}


//
// Created by sergey on 04.02.2021.
//

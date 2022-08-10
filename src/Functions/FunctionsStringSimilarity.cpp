#include <Functions/FunctionsStringSimilarity.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>

#include <Core/Defines.h>

#include <base/unaligned.h>

#include <algorithm>
#include <climits>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#    include <arm_acle.h>
#endif

namespace DB
{
/** Distance function implementation.
  * We calculate all the n-grams from left string and count by the index of
  * 16 bits hash of them in the map.
  * Then calculate all the n-grams from the right string and calculate
  * the n-gram distance on the flight by adding and subtracting from the hashmap.
  * Then return the map into the condition of which it was after the left string
  * calculation. If the right string size is big (more than 2**15 bytes),
  * the strings are not similar at all and we return 1.
  */
template <size_t N, class CodePoint, bool UTF8, bool case_insensitive, bool symmetric>
struct NgramDistanceImpl
{
    using ResultType = Float32;

    /// map_size for ngram difference.
    static constexpr size_t map_size = 1u << 16;

    /// If the haystack size is bigger than this, behaviour is unspecified for this function.
    static constexpr size_t max_string_size = 1u << 15;

    /// Default padding to read safely.
    static constexpr size_t default_padding = 32;

    /// Max codepoints to store at once. 16 is for batching usage and PODArray has this padding.
    static constexpr size_t simultaneously_codepoints_num = default_padding + N - 1;

    /** map_size of this fits mostly in L2 cache all the time.
      * Actually use UInt16 as addings and subtractions do not UB overflow. But think of it as a signed
      * integer array.
      */
    using NgramCount = UInt16;

    static ALWAYS_INLINE UInt16 calculateASCIIHash(const CodePoint * code_points)
    {
        return intHashCRC32(unalignedLoad<UInt32>(code_points)) & 0xFFFFu;
    }

    static ALWAYS_INLINE UInt16 calculateUTF8Hash(const CodePoint * code_points)
    {
        UInt64 combined = (static_cast<UInt64>(code_points[0]) << 32) | code_points[1];
#ifdef __SSE4_2__
        return _mm_crc32_u64(code_points[2], combined) & 0xFFFFu;
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32cd(code_points[2], combined) & 0xFFFFu;
#else
        return (intHashCRC32(combined) ^ intHashCRC32(code_points[2])) & 0xFFFFu;
#endif
    }

    template <size_t Offset, class Container, size_t... I>
    static ALWAYS_INLINE inline void unrollLowering(Container & cont, const std::index_sequence<I...> &)
    {
        ((cont[Offset + I] = std::tolower(cont[Offset + I])), ...);
    }

    static ALWAYS_INLINE size_t readASCIICodePoints(CodePoint * code_points, const char *& pos, const char * end)
    {
        /// Offset before which we copy some data.
        constexpr size_t padding_offset = default_padding - N + 1;
        /// We have an array like this for ASCII (N == 4, other cases are similar)
        /// |a0|a1|a2|a3|a4|a5|a6|a7|a8|a9|a10|a11|a12|a13|a14|a15|a16|a17|a18|
        /// And we copy                                ^^^^^^^^^^^^^^^ these bytes to the start
        /// Actually it is enough to copy 3 bytes, but memcpy for 4 bytes translates into 1 instruction
        memcpy(code_points, code_points + padding_offset, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(CodePoint));
        /// Now we have an array
        /// |a13|a14|a15|a16|a4|a5|a6|a7|a8|a9|a10|a11|a12|a13|a14|a15|a16|a17|a18|
        ///              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        /// Doing unaligned read of 16 bytes and copy them like above
        /// 16 is also chosen to do two `movups`.
        /// Such copying allow us to have 3 codepoints from the previous read to produce the 4-grams with them.
        memcpy(code_points + (N - 1), pos, default_padding * sizeof(CodePoint));

        if constexpr (case_insensitive)
        {
            /// We really need template lambdas with C++20 to do it inline
            unrollLowering<N - 1>(code_points, std::make_index_sequence<padding_offset>());
        }
        pos += padding_offset;
        if (pos > end)
            return default_padding - (pos - end);
        return default_padding;
    }

    static ALWAYS_INLINE size_t readUTF8CodePoints(CodePoint * code_points, const char *& pos, const char * end)
    {
        /// The same copying as described in the function above.
        memcpy(code_points, code_points + default_padding - N + 1, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(CodePoint));

        size_t num = N - 1;
        while (num < default_padding && pos < end)
        {
            size_t length = UTF8::seqLength(*pos);

            if (pos + length > end)
                length = end - pos;

            CodePoint res;
            /// This is faster than just memcpy because of compiler optimizations with moving bytes.
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

            /// This is not a really true case insensitive utf8. We zero the 5-th bit of every byte.
            /// And first bit of first byte if there are two bytes.
            /// For ASCII it works https://catonmat.net/ascii-case-conversion-trick. For most cyrillic letters also does.
            /// For others, we don't care now. Lowering UTF is not a cheap operation.
            if constexpr (case_insensitive)
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

    template <bool save_ngrams>
    static ALWAYS_INLINE inline size_t calculateNeedleStats(
        const char * data,
        const size_t size,
        NgramCount * ngram_stats,
        [[maybe_unused]] NgramCount * ngram_storage,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt16 (*hash_functor)(const CodePoint *))
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
                ++len;
                UInt16 hash = hash_functor(cp + i);
                if constexpr (save_ngrams)
                    *ngram_storage++ = hash;
                ++ngram_stats[hash];
            }
            i = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        return len;
    }

    template <bool reuse_stats>
    static ALWAYS_INLINE inline UInt64 calculateHaystackStatsAndMetric(
        const char * data,
        const size_t size,
        NgramCount * ngram_stats,
        size_t & distance,
        [[maybe_unused]] UInt16 * ngram_storage,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt16 (*hash_functor)(const CodePoint *))
    {
        size_t ngram_cnt = 0;
        const char * start = data;
        const char * end = data + size;
        CodePoint cp[simultaneously_codepoints_num] = {};

        /// read_code_points returns the position of cp where it stopped reading codepoints.
        size_t found = read_code_points(cp, start, end);
        /// We need to start for the first time here, because first N - 1 codepoints mean nothing.
        size_t iter = N - 1;

        do
        {
            for (; iter + N <= found; ++iter)
            {
                UInt16 hash = hash_functor(cp + iter);
                /// For symmetric version we should add when we can't subtract to get symmetric difference.
                if (static_cast<Int16>(ngram_stats[hash]) > 0)
                    --distance;
                else if constexpr (symmetric)
                    ++distance;
                if constexpr (reuse_stats)
                    ngram_storage[ngram_cnt] = hash;
                ++ngram_cnt;
                --ngram_stats[hash];
            }
            iter = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        /// Return the state of hash map to its initial.
        if constexpr (reuse_stats)
        {
            for (size_t i = 0; i < ngram_cnt; ++i)
                ++ngram_stats[ngram_storage[i]];
        }
        return ngram_cnt;
    }

    template <class Callback, class... Args>
    static inline auto dispatchSearcher(Callback callback, Args &&... args)
    {
        if constexpr (!UTF8)
            return callback(std::forward<Args>(args)..., readASCIICodePoints, calculateASCIIHash);
        else
            return callback(std::forward<Args>(args)..., readUTF8CodePoints, calculateUTF8Hash);
    }

    static void constantConstant(std::string data, std::string needle, Float32 & res)
    {
        std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}};

        /// We use unsafe versions of getting ngrams, so I decided to use padded strings.
        const size_t needle_size = needle.size();
        const size_t data_size = data.size();
        needle.resize(needle_size + default_padding);
        data.resize(data_size + default_padding);

        size_t second_size = dispatchSearcher(calculateNeedleStats<false>, needle.data(), needle_size, common_stats.get(), nullptr);
        size_t distance = second_size;
        if (data_size <= max_string_size)
        {
            size_t first_size = dispatchSearcher(calculateHaystackStatsAndMetric<false>, data.data(), data_size, common_stats.get(), distance, nullptr);
            /// For !symmetric version we should not use first_size.
            if constexpr (symmetric)
                res = distance * 1.f / std::max(first_size + second_size, static_cast<size_t>(1));
            else
                res = 1.f - distance * 1.f / std::max(second_size, static_cast<size_t>(1));
        }
        else
        {
            if constexpr (symmetric)
                res = 1.f;
            else
                res = 0.f;
        }
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<Float32> & res)
    {
        const size_t haystack_offsets_size = haystack_offsets.size();
        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;

        std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}};

        /// The main motivation is to not allocate more on stack because we have already allocated a lot (128Kb).
        /// And we can reuse these storages in one thread because we care only about what was written to first places.
        std::unique_ptr<UInt16[]> needle_ngram_storage(new UInt16[max_string_size]);
        std::unique_ptr<UInt16[]> haystack_ngram_storage(new UInt16[max_string_size]);

        for (size_t i = 0; i < haystack_offsets_size; ++i)
        {
            const char * haystack = reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]);
            const size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;
            const char * needle = reinterpret_cast<const char *>(&needle_data[prev_needle_offset]);
            const size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;

            if (needle_size <= max_string_size && haystack_size <= max_string_size)
            {
                /// Get needle stats.
                const size_t needle_stats_size = dispatchSearcher(
                    calculateNeedleStats<true>,
                    needle,
                    needle_size,
                    common_stats.get(),
                    needle_ngram_storage.get());

                size_t distance = needle_stats_size;

                /// Combine with haystack stats, return to initial needle stats.
                const size_t haystack_stats_size = dispatchSearcher(
                    calculateHaystackStatsAndMetric<true>,
                    haystack,
                    haystack_size,
                    common_stats.get(),
                    distance,
                    haystack_ngram_storage.get());

                /// Return to zero array stats.
                for (size_t j = 0; j < needle_stats_size; ++j)
                    --common_stats[needle_ngram_storage[j]];

                /// For now, common stats is a zero array.


                /// For !symmetric version we should not use haystack_stats_size.
                if constexpr (symmetric)
                    res[i] = distance * 1.f / std::max(haystack_stats_size + needle_stats_size, static_cast<size_t>(1));
                else
                    res[i] = 1.f - distance * 1.f / std::max(needle_stats_size, static_cast<size_t>(1));
            }
            else
            {
                /// Strings are too big, we are assuming they are not the same. This is done because of limiting number
                /// of bigrams added and not allocating too much memory.
                if constexpr (symmetric)
                    res[i] = 1.f;
                else
                    res[i] = 0.f;
            }

            prev_needle_offset = needle_offsets[i];
            prev_haystack_offset = haystack_offsets[i];
        }
    }

    static void constantVector(
        std::string haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<Float32> & res)
    {
        /// For symmetric version it is better to use vector_constant
        if constexpr (symmetric)
        {
            vectorConstant(needle_data, needle_offsets, std::move(haystack), res);
        }
        else
        {
            const size_t haystack_size = haystack.size();
            haystack.resize(haystack_size + default_padding);

            /// For logic explanation see vector_vector function.
            const size_t needle_offsets_size = needle_offsets.size();
            size_t prev_offset = 0;

            std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}};

            std::unique_ptr<UInt16[]> needle_ngram_storage(new UInt16[max_string_size]);
            std::unique_ptr<UInt16[]> haystack_ngram_storage(new UInt16[max_string_size]);

            for (size_t i = 0; i < needle_offsets_size; ++i)
            {
                const char * needle = reinterpret_cast<const char *>(&needle_data[prev_offset]);
                const size_t needle_size = needle_offsets[i] - prev_offset - 1;

                if (needle_size <= max_string_size && haystack_size <= max_string_size)
                {
                    const size_t needle_stats_size = dispatchSearcher(
                        calculateNeedleStats<true>,
                        needle,
                        needle_size,
                        common_stats.get(),
                        needle_ngram_storage.get());

                    size_t distance = needle_stats_size;

                    dispatchSearcher(
                        calculateHaystackStatsAndMetric<true>,
                        haystack.data(),
                        haystack_size,
                        common_stats.get(),
                        distance,
                        haystack_ngram_storage.get());

                    for (size_t j = 0; j < needle_stats_size; ++j)
                        --common_stats[needle_ngram_storage[j]];

                    res[i] = 1.f - distance * 1.f / std::max(needle_stats_size, static_cast<size_t>(1));
                }
                else
                {
                    res[i] = 0.f;
                }

                prev_offset = needle_offsets[i];
            }

        }
    }

    static void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        std::string needle,
        PaddedPODArray<Float32> & res)
    {
        /// zeroing our map
        std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}};

        /// We can reuse these storages in one thread because we care only about what was written to first places.
        std::unique_ptr<UInt16[]> ngram_storage(new NgramCount[max_string_size]);

        /// We use unsafe versions of getting ngrams, so I decided to use padded_data even in needle case.
        const size_t needle_size = needle.size();
        needle.resize(needle_size + default_padding);

        const size_t needle_stats_size = dispatchSearcher(calculateNeedleStats<false>, needle.data(), needle_size, common_stats.get(), nullptr);

        size_t distance = needle_stats_size;
        size_t prev_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * haystack = &data[prev_offset];
            const size_t haystack_size = offsets[i] - prev_offset - 1;
            if (haystack_size <= max_string_size)
            {
                size_t haystack_stats_size = dispatchSearcher(
                    calculateHaystackStatsAndMetric<true>,
                    reinterpret_cast<const char *>(haystack),
                    haystack_size, common_stats.get(),
                    distance,
                    ngram_storage.get());
                /// For !symmetric version we should not use haystack_stats_size.
                if constexpr (symmetric)
                    res[i] = distance * 1.f / std::max(haystack_stats_size + needle_stats_size, static_cast<size_t>(1));
                else
                    res[i] = 1.f - distance * 1.f / std::max(needle_stats_size, static_cast<size_t>(1));
            }
            else
            {
                /// if the strings are too big, we say they are completely not the same
                if constexpr (symmetric)
                    res[i] = 1.f;
                else
                    res[i] = 0.f;
            }
            distance = needle_stats_size;
            prev_offset = offsets[i];
        }
    }
};


struct NameNgramDistance
{
    static constexpr auto name = "ngramDistance";
};
struct NameNgramDistanceCaseInsensitive
{
    static constexpr auto name = "ngramDistanceCaseInsensitive";
};

struct NameNgramDistanceUTF8
{
    static constexpr auto name = "ngramDistanceUTF8";
};

struct NameNgramDistanceUTF8CaseInsensitive
{
    static constexpr auto name = "ngramDistanceCaseInsensitiveUTF8";
};

struct NameNgramSearch
{
    static constexpr auto name = "ngramSearch";
};
struct NameNgramSearchCaseInsensitive
{
    static constexpr auto name = "ngramSearchCaseInsensitive";
};
struct NameNgramSearchUTF8
{
    static constexpr auto name = "ngramSearchUTF8";
};

struct NameNgramSearchUTF8CaseInsensitive
{
    static constexpr auto name = "ngramSearchCaseInsensitiveUTF8";
};

using FunctionNgramDistance = FunctionsStringSimilarity<NgramDistanceImpl<4, UInt8, false, false, true>, NameNgramDistance>;
using FunctionNgramDistanceCaseInsensitive = FunctionsStringSimilarity<NgramDistanceImpl<4, UInt8, false, true, true>, NameNgramDistanceCaseInsensitive>;
using FunctionNgramDistanceUTF8 = FunctionsStringSimilarity<NgramDistanceImpl<3, UInt32, true, false, true>, NameNgramDistanceUTF8>;
using FunctionNgramDistanceCaseInsensitiveUTF8 = FunctionsStringSimilarity<NgramDistanceImpl<3, UInt32, true, true, true>, NameNgramDistanceUTF8CaseInsensitive>;

using FunctionNgramSearch = FunctionsStringSimilarity<NgramDistanceImpl<4, UInt8, false, false, false>, NameNgramSearch>;
using FunctionNgramSearchCaseInsensitive = FunctionsStringSimilarity<NgramDistanceImpl<4, UInt8, false, true, false>, NameNgramSearchCaseInsensitive>;
using FunctionNgramSearchUTF8 = FunctionsStringSimilarity<NgramDistanceImpl<3, UInt32, true, false, false>, NameNgramSearchUTF8>;
using FunctionNgramSearchCaseInsensitiveUTF8 = FunctionsStringSimilarity<NgramDistanceImpl<3, UInt32, true, true, false>, NameNgramSearchUTF8CaseInsensitive>;


REGISTER_FUNCTION(StringSimilarity)
{
    factory.registerFunction<FunctionNgramDistance>();
    factory.registerFunction<FunctionNgramDistanceCaseInsensitive>();
    factory.registerFunction<FunctionNgramDistanceUTF8>();
    factory.registerFunction<FunctionNgramDistanceCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionNgramSearch>();
    factory.registerFunction<FunctionNgramSearchCaseInsensitive>();
    factory.registerFunction<FunctionNgramSearchUTF8>();
    factory.registerFunction<FunctionNgramSearchCaseInsensitiveUTF8>();
}

}

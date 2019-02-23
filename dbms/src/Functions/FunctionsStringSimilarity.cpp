#include <Functions/FunctionsStringSimilarity.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>

#include <Core/Defines.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

namespace DB
{
/** Distance function implementation.
  * We calculate all the trigrams from left string and count by the index of
  * 16 bits hash of them in the map.
  * Then calculate all the trigrams from the right string and calculate
  * the trigram distance on the flight by adding and subtracting from the hashmap.
  * Then return the map into the condition of which it was after the left string
  * calculation. If the right string size is big (more than 2**15 bytes),
  * the strings are not similar at all and we return 1.
  */
struct TrigramDistanceImpl
{
    using ResultType = Float32;
    using CodePoint = UInt32;

    /// map_size for trigram difference
    static constexpr size_t map_size = 1u << 16;

    /// If the haystack size is bigger than this, behaviour is unspecified for this function
    static constexpr size_t max_string_size = 1u << 15;

    /** This fits mostly in L2 cache all the time.
      * Actually use UInt16 as addings and subtractions do not UB overflow. But think of it as a signed
      * integer array.
      */
    using TrigramStats = UInt16[map_size];

    static ALWAYS_INLINE UInt16 trigramHash(CodePoint one, CodePoint two, CodePoint three)
    {
        UInt64 combined = (static_cast<UInt64>(one) << 32) | two;
#ifdef __SSE4_2__
        return _mm_crc32_u64(three, combined) & 0xFFFFu;
#else
        return (intHashCRC32(combined) ^ intHashCRC32(three)) & 0xFFFFu;
#endif
    }

    static ALWAYS_INLINE CodePoint readCodePoint(const char *& pos, const char * end) noexcept
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

        pos += length;
        return res;
    }

    static inline size_t calculateNeedleStats(const char * data, const size_t size, TrigramStats & trigram_stats) noexcept
    {
        size_t len = 0;
        const char * start = data;
        const char * end = data + size;
        CodePoint cp1 = 0;
        CodePoint cp2 = 0;
        CodePoint cp3 = 0;

        while (start != end)
        {
            cp1 = cp2;
            cp2 = cp3;
            cp3 = readCodePoint(start, end);
            ++len;
            if (len < 3)
                continue;
            ++trigram_stats[trigramHash(cp1, cp2, cp3)];
        }
        return std::max(static_cast<Int64>(0), static_cast<Int64>(len) - 2);
    }

    static inline UInt64 calculateHaystackStatsAndMetric(const char * data, const size_t size, TrigramStats & trigram_stats, size_t & distance)
    {
        size_t len = 0;
        size_t trigram_cnt = 0;
        const char * start = data;
        const char * end = data + size;
        CodePoint cp1 = 0;
        CodePoint cp2 = 0;
        CodePoint cp3 = 0;

        /// allocation tricks, most strings are relatively small
        static constexpr size_t small_buffer_size = 256;
        std::unique_ptr<UInt16[]> big_buffer;
        UInt16 small_buffer[small_buffer_size];
        UInt16 * trigram_storage = small_buffer;

        if (size > small_buffer_size)
        {
            trigram_storage = new UInt16[size];
            big_buffer.reset(trigram_storage);
        }

        while (start != end)
        {
            cp1 = cp2;
            cp2 = cp3;
            cp3 = readCodePoint(start, end);
            ++len;
            if (len < 3)
                continue;

            UInt16 hash = trigramHash(cp1, cp2, cp3);

            if (static_cast<Int16>(trigram_stats[hash]) > 0)
                --distance;
            else
                ++distance;

            trigram_storage[trigram_cnt++] = hash;
            --trigram_stats[hash];
        }

        /// Return the state of hash map to its initial.
        for (size_t i = 0; i < trigram_cnt; ++i)
            ++trigram_stats[trigram_storage[i]];

        return trigram_cnt;
    }

    static void constant_constant(const std::string & data, const std::string & needle, Float32 & res)
    {
        TrigramStats common_stats;
        memset(common_stats, 0, sizeof(common_stats));
        size_t second_size = calculateNeedleStats(needle.data(), needle.size(), common_stats);
        size_t distance = second_size;
        if (data.size() <= max_string_size)
        {
            size_t first_size = calculateHaystackStatsAndMetric(data.data(), data.size(), common_stats, distance);
            res = distance * 1.f / std::max(first_size + second_size, size_t(1));
        }
        else
        {
            res = 1.f;
        }
    }

    static void vector_constant(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets, const std::string & needle, PaddedPODArray<Float32> & res)
    {
        TrigramStats common_stats;
        memset(common_stats, 0, sizeof(common_stats));
        const size_t needle_stats_size = calculateNeedleStats(needle.data(), needle.size(), common_stats);
        size_t distance = needle_stats_size;
        size_t prev_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * haystack = &data[prev_offset];
            const size_t haystack_size = offsets[i] - prev_offset - 1;
            if (haystack_size <= max_string_size)
            {
                size_t haystack_stats_size
                    = calculateHaystackStatsAndMetric(reinterpret_cast<const char *>(haystack), haystack_size, common_stats, distance);
                res[i] = distance * 1.f / std::max(haystack_stats_size + needle_stats_size, size_t(1));
            }
            else
            {
                res[i] = 1.f;
            }
            distance = needle_stats_size;
            prev_offset = offsets[i];
        }
    }
};


struct TrigramDistanceName
{
    static constexpr auto name = "trigramDistance";
};

using FunctionTrigramsDistance = FunctionsStringSimilarity<TrigramDistanceImpl, TrigramDistanceName>;

void registerFunctionsStringSimilarity(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTrigramsDistance>();
}

}

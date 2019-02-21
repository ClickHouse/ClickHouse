#include <Functions/FunctionsStringSimilarity.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>

namespace DB
{

struct TrigramDistanceImpl
{
    using ResultType = Float32;
    using CodePoint = UInt32;

    using TrigramMap = ClearableHashMap<UInt64, UInt64, TrivialHash>;

    static inline CodePoint readCodePoint(const char *& pos, const char * end) noexcept
    {
        size_t length = UTF8::seqLength(*pos);

        if (pos + length > end)
            length = end - pos;

        if (length > sizeof(CodePoint))
            length = sizeof(CodePoint);

        CodePoint res = 0;
        memcpy(&res, pos, length);
        pos += length;
        return res;
    }

    static inline size_t calculateStats(const char * data, const size_t size, TrigramMap & ans)
    {
        ans.clear();
        size_t len = 0;
        size_t trigramCnt = 0;
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
            ++trigramCnt;
            ++ans[intHashCRC32(intHashCRC32(cp1) ^ cp2) ^ cp3];
        }
        return trigramCnt;
    }

    static inline UInt64 calculateMetric(const TrigramMap & lhs, const TrigramMap & rhs)
    {
        UInt64 res = 0;

        for (const auto & [trigram, count] : lhs)
        {
            if (auto it = rhs.find(trigram); it != rhs.end())
                res += std::abs(static_cast<Int64>(count) - static_cast<Int64>(it->second));
            else
                res += count;
        }

        for (const auto & [trigram, count] : rhs)
            if (!lhs.has(trigram))
                res += count;

        return res;
    }

    static void constant_constant(const std::string & data, const std::string & needle, Float32 & res)
    {
        TrigramMap haystack_stats;
        TrigramMap needle_stats;
        size_t first_size = calculateStats(data.data(), data.size(), haystack_stats);
        size_t second_size = calculateStats(needle.data(), needle.size(), needle_stats);
        res = calculateMetric(needle_stats, haystack_stats) * 1.0 / std::max(first_size + second_size, size_t(1));
    }

    static void vector_constant(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, const std::string & needle, PaddedPODArray<Float32> & res)
    {
        TrigramMap needle_stats;
        TrigramMap haystack_stats;
        const size_t needle_stats_size = calculateStats(needle.data(), needle.size(), needle_stats);
        size_t prev_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const auto * haystack = &data[prev_offset];
            const size_t haystack_size = offsets[i] - prev_offset - 1;
            size_t haystack_stats_size = calculateStats(reinterpret_cast<const char *>(haystack), haystack_size, haystack_stats);
            res[i] = calculateMetric(haystack_stats, needle_stats) * 1.0 / std::max(haystack_stats_size + needle_stats_size, size_t(1));
            prev_offset = offsets[i];
        }
    }
};


struct TrigramDistanceName
{
    static constexpr auto name = "trigramDistance";
};

using FunctionTrigramDistance = FunctionsStringSimilarity<TrigramDistanceImpl, TrigramDistanceName>;

void registerFunctionsStringSimilarity(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTrigramDistance>();
}

}

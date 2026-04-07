#pragma once

#include <algorithm>
#include <vector>

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

struct HighlightImpl
{
    static constexpr size_t DEFAULT_MAX_MATCHES_PER_ROW = 10000;
    struct Interval
    {
        size_t begin;
        size_t end;
    };

    /// Sort and merge overlapping/adjacent intervals in-place.
    /// Uses <= for merge condition so that adjacent intervals like [0,5)+[5,10) merge into [0,10).
    static void mergeIntervals(std::vector<Interval> & intervals)
    {
        if (intervals.size() <= 1)
            return;

        std::sort(intervals.begin(), intervals.end(), [](const Interval & a, const Interval & b)
        {
            return a.begin < b.begin || (a.begin == b.begin && a.end > b.end);
        });

        size_t write = 0;
        for (size_t read = 1; read < intervals.size(); ++read)
        {
            if (intervals[read].begin <= intervals[write].end)
                intervals[write].end = std::max(intervals[write].end, intervals[read].end);
            else
                intervals[++write] = intervals[read];
        }
        intervals.resize(write + 1);
    }

    struct NeedleSearcher
    {
        VolnitskyCaseInsensitive searcher;
        size_t needle_size;
    };

    static void execute(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<std::string_view> & needles,
        const String & open_tag,
        const String & close_tag,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        UInt64 max_matches_per_row = DEFAULT_MAX_MATCHES_PER_ROW)
    {
        /// Pre-allocate output buffers — conservative estimate to avoid over-allocation
        /// with many needles: at most one tag pair per row on average.
        const size_t tag_overhead = open_tag.size() + close_tag.size();
        res_data.reserve(haystack_data.size() + input_rows_count * tag_overhead);
        res_offsets.resize(input_rows_count);

        /// Build searcher instances once outside the row loop, paired with needle sizes.
        /// We use VolnitskyCaseInsensitive with haystack_size_hint=0, which means
        /// each search() call decides internally whether to use the hash table
        /// or fall back to ASCIICaseInsensitiveStringSearcher for short haystacks.
        std::vector<NeedleSearcher> searchers;
        searchers.reserve(needles.size());
        for (const auto & needle : needles)
            if (!needle.empty())
                searchers.push_back({VolnitskyCaseInsensitive(needle.data(), needle.size(), 0), needle.size()});

        /// Reusable intervals buffer across rows
        std::vector<Interval> intervals;
        intervals.reserve(64);

        ColumnString::Offset res_offset = 0;
        ColumnString::Offset prev_haystack_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t cur_size = haystack_offsets[i] - prev_haystack_offset;

            if (cur_size > 0)
            {
                const UInt8 * cur_data = &haystack_data[prev_haystack_offset];

                /// Phase 1: find all matches
                intervals.clear();
                findAllMatches(cur_data, cur_size, searchers, intervals, max_matches_per_row);

                if (intervals.empty())
                {
                    /// No matches — copy as-is
                    append(res_data, res_offset, cur_data, cur_size);
                }
                else
                {
                    /// Phase 2: merge overlapping intervals
                    mergeIntervals(intervals);

                    /// Phase 3: build output with tags
                    buildOutput(cur_data, cur_size, intervals, open_tag, close_tag, res_data, res_offset);
                }
            }

            res_offsets[i] = res_offset;
            prev_haystack_offset = haystack_offsets[i];
        }
    }

private:
    /// Phase 1: For each needle, find all occurrence positions in the haystack.
    static void findAllMatches(
        const UInt8 * haystack,
        size_t haystack_size,
        const std::vector<NeedleSearcher> & searchers,
        std::vector<Interval> & intervals,
        UInt64 max_matches_per_row)
    {
        const UInt8 * haystack_end = haystack + haystack_size;

        for (const auto & [searcher, needle_size] : searchers)
        {
            const UInt8 * pos = haystack;
            while (pos < haystack_end)
            {
                const UInt8 * match = searcher.search(pos, haystack_end - pos);
                if (match == haystack_end)
                    break;

                const size_t offset = match - haystack;
                intervals.push_back({offset, offset + needle_size});
                pos = match + 1;

                if (intervals.size() > max_matches_per_row)
                    throw Exception(
                        ErrorCodes::LIMIT_EXCEEDED,
                        "Too many highlight matches per row: {}, max: {}. "
                        "You can increase this limit with the `highlight_max_matches_per_row` setting",
                        intervals.size(), max_matches_per_row);
            }
        }
    }

    /// Phase 3: Build the output string by interleaving non-matched text with tagged matched text.
    static void buildOutput(
        const UInt8 * haystack,
        size_t haystack_size,
        const std::vector<Interval> & intervals,
        const String & open_tag,
        const String & close_tag,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset)
    {
        size_t cursor = 0;
        for (const auto & interval : intervals)
        {
            /// Copy non-matched text before this interval
            if (interval.begin > cursor)
                append(res_data, res_offset, haystack + cursor, interval.begin - cursor);

            /// Insert open tag
            if (!open_tag.empty())
                append(res_data, res_offset, reinterpret_cast<const UInt8 *>(open_tag.data()), open_tag.size());

            /// Copy matched text (preserving original case)
            append(res_data, res_offset, haystack + interval.begin, interval.end - interval.begin);

            /// Insert close tag
            if (!close_tag.empty())
                append(res_data, res_offset, reinterpret_cast<const UInt8 *>(close_tag.data()), close_tag.size());

            cursor = interval.end;
        }

        /// Copy remaining text after the last interval
        if (cursor < haystack_size)
            append(res_data, res_offset, haystack + cursor, haystack_size - cursor);
    }

    static inline void append(
        ColumnString::Chars & data,
        ColumnString::Offset & offset,
        const void * src,
        size_t size)
    {
        data.resize(data.size() + size);
        memcpy(&data[offset], src, size);
        offset += size;
    }
};

}

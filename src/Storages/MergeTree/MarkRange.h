#pragma once

#include <cstddef>
#include <deque>
#include <Processors/Chunk.h>
#include <fmt/format.h>
#include <base/types.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;


/** A pair of marks that defines the range of rows in a part. Specifically,
 * the range has the form [begin * index_granularity, end * index_granularity).
 */
struct MarkRange
{
    size_t begin;
    size_t end;

    MarkRange() = default;
    MarkRange(size_t begin_, size_t end_);

    size_t getNumberOfMarks() const;

    bool operator==(const MarkRange & rhs) const;
    bool operator<(const MarkRange & rhs) const;
};

struct MarkRanges : public std::deque<MarkRange>
{
    using std::deque<MarkRange>::deque; /// NOLINT(modernize-type-traits)

    size_t getNumberOfMarks() const;
    bool isOneRangeForWholePart(size_t num_marks_in_part) const;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);
};

/** Get max range.end from ranges.
 */
size_t getLastMark(const MarkRanges & ranges);

std::string toString(const MarkRanges & ranges);

void assertSortedAndNonIntersecting(const MarkRanges & ranges);

/// Lineage information: from which table, part and mark range does the chunk come from?
/// This information is needed by the query condition cache.
class MarkRangesInfo : public ChunkInfoCloneable<MarkRangesInfo>
{
public:
    MarkRangesInfo(UUID table_uuid_, const String & part_name_, size_t marks_count_, bool has_final_mark_, MarkRanges mark_ranges_);
    void appendMarkRanges(const MarkRanges & mark_ranges_);

    UUID table_uuid;
    String part_name;
    size_t marks_count;
    bool has_final_mark;
    MarkRanges mark_ranges;
};
using MarkRangesInfoPtr = std::shared_ptr<MarkRangesInfo>;

}


template <>
struct fmt::formatter<DB::MarkRange>
{
    constexpr static auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::MarkRange & range, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", fmt::format("({}, {})", range.begin, range.end));
    }
};

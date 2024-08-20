#pragma once

#include <cstddef>
#include <deque>

#include <fmt/core.h>
#include <fmt/format.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{


/** A pair of marks that defines the range of rows in a part. Specifically,
 * the range has the form [begin * index_granularity, end * index_granularity).
 */
struct MarkRange
{
    size_t begin;
    size_t end;

    MarkRange() = default;
    MarkRange(const size_t begin_, const size_t end_) : begin{begin_}, end{end_} {}

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

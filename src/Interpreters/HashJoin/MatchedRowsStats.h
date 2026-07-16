#pragma once

#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <base/types.h>
#include <Core/Joins.h>

namespace DB
{

enum class LeftMatchedSource
{
    MatchedRowsSize,
    MatchedRowsComplement,
    NotInOutput,
    Unsupported,
};

enum class RightMatchedSource
{
    NonJoinedComplement,
    LeftMatchCount,
    RefsBitmap,
    AllRight,
    NotInOutput,
    Unsupported,
};

constexpr LeftMatchedSource leftMatchedSource(JoinKind kind, JoinStrictness strictness)
{
    if (kind == JoinKind::Cross || kind == JoinKind::Comma)
        return LeftMatchedSource::MatchedRowsSize;

    const bool is_any = strictness == JoinStrictness::Any;
    const bool is_anti = strictness == JoinStrictness::Anti;
    const bool is_semi = strictness == JoinStrictness::Semi;

    if (kind == JoinKind::Full && is_any)
        return LeftMatchedSource::Unsupported;

    if (kind == JoinKind::Left && is_anti)
        return LeftMatchedSource::MatchedRowsComplement;

    if (kind == JoinKind::Right && (is_semi || is_anti))
        return LeftMatchedSource::NotInOutput;

    return LeftMatchedSource::MatchedRowsSize;
}

constexpr RightMatchedSource rightMatchedSource(JoinKind kind, JoinStrictness strictness)
{
    if (kind == JoinKind::Cross || kind == JoinKind::Comma)
        return RightMatchedSource::AllRight;

    const bool is_any = strictness == JoinStrictness::Any;
    const bool is_anti = strictness == JoinStrictness::Anti;
    const bool is_semi = strictness == JoinStrictness::Semi;

    if (kind == JoinKind::Full && is_any)
        return RightMatchedSource::Unsupported;

    if (kind == JoinKind::Left && (is_semi || is_anti))
        return RightMatchedSource::NotInOutput;

    if ((kind == JoinKind::Inner && is_any) || (kind == JoinKind::Right && is_semi))
        return RightMatchedSource::LeftMatchCount;

    if (kind == JoinKind::Right || kind == JoinKind::Full)
        return RightMatchedSource::NonJoinedComplement;

    return RightMatchedSource::RefsBitmap;
}

class MatchedRowsStats
{
public:

    MatchedRowsStats(JoinKind, JoinStrictness, UInt64 right_total_);

    void collectProbeBlock(UInt64 probed_block_size, UInt64 matched_left_rows);

    void markRightMatched(UInt64 ref_word);

    void collectNonJoined(UInt64 non_joined_rows);

    void prepareRightBitmap(const HashJoin::StoredBlocksList & stored_blocks);

    UInt64 getInputLeft() const { return left_rows_total.load(std::memory_order_relaxed); }
    UInt64 getInputRight() const { return right_rows_total; }
    UInt64 getMatchedLeft() const;
    UInt64 getMatchedRight() const;

private:

    using MatchedRightRows = JoinStuff::JoinUsedFlags;

    std::unique_ptr<MatchedRightRows> right_rows_bitmap;
    UInt64 right_rows_total = 0;
    std::atomic<UInt64> left_rows_total = 0;
    std::atomic<UInt64> left_rows_matched = 0;
    std::atomic<UInt64> non_joined_right_rows = 0;

    JoinKind join_kind;
    JoinStrictness join_strictness;
};

}

#include <Core/Joins.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/RowRefs.h>
#include <base/types.h>

namespace DB
{

MatchedRowsStats::MatchedRowsStats(JoinKind kind, JoinStrictness strictness, JoinAnalyzeMode analyze_mode_)
: join_kind(kind)
, join_strictness(strictness)
, analyze_mode(analyze_mode_)
{
}

void MatchedRowsStats::prepareRightFlagsIfNeeded(const HashJoin::StoredBlocksList & stored_blocks)
{
    if (analyze_mode == JoinAnalyzeMode::Exact && rightMatchedSource(join_kind, join_strictness) == RightMatchedSource::RefsFlags)
        prepareRightFlags(stored_blocks);
}

void MatchedRowsStats::collectProbeBlock(UInt64 probed_block_size, std::optional<UInt64> matched_left)
{
    left_rows_total.fetch_add(probed_block_size, std::memory_order_relaxed);

    if (matched_left)
        left_rows_matched.fetch_add(*matched_left, std::memory_order_relaxed);
    else
        left_matched_available.store(false, std::memory_order_relaxed);
}

static size_t rowsAddressableBySelector(const ScatteredBlock::Selector & selector)
{
    if (selector.isContinuousRange())
        return selector.getRange().second;

    size_t rows = 0;
    for (const size_t row : selector.getIndexes().getData())
        rows = std::max(rows, row + 1);
    return rows;
}

void MatchedRowsStats::prepareRightFlags(const HashJoin::StoredBlocksList & stored_blocks)
{
    right_rows_flags = std::make_unique<MatchedRightFlags>();
    for (const auto & block : stored_blocks)
        right_rows_flags->allocate(block.block_no, rowsAddressableBySelector(block.selector));
}

void MatchedRowsStats::collectNonJoined(UInt64 non_joined_rows)
{
    non_joined_right_rows.fetch_add(non_joined_rows);
}

void MatchedRowsStats::markRightMatched(UInt64 ref_word)
{
    for (const UInt64 word : refsOf(ref_word))
        right_rows_flags->setMatched(refWordBlockNo(word), refWordRowNo(word));
}

std::optional<UInt64> MatchedRowsStats::getMatchedLeft() const
{
    if (!left_matched_available.load(std::memory_order_relaxed))
        return std::nullopt;

    return left_rows_matched.load(std::memory_order_relaxed);
}

std::optional<UInt64> MatchedRowsStats::getMatchedRight(UInt64 right_rows_total) const
{
    switch (rightMatchedSource(join_kind, join_strictness))
    {
        case RightMatchedSource::RefsFlags:
            if (right_rows_flags)
                return right_rows_flags->countMatched();
            return std::nullopt;
        case RightMatchedSource::NonJoinedComplement:
            return right_rows_total - non_joined_right_rows.load(std::memory_order_relaxed);
        case RightMatchedSource::Unsupported:
            return std::nullopt;
    }
}

}

#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/RowRefs.h>
#include <base/types.h>

namespace DB
{

MatchedRowsStats::MatchedRowsStats(JoinKind kind, JoinStrictness strictness, UInt64 right_total_)
: right_rows_total(right_total_)
, join_kind(kind)
, join_strictness(strictness)
{
}


void MatchedRowsStats::collectProbeBlock(UInt64 probed_block_size, UInt64 matched_left_rows)
{
    left_rows_total.fetch_add(probed_block_size);
    left_rows_matched.fetch_add(matched_left_rows);
}

void MatchedRowsStats::prepareRightBitmap(const HashJoin::StoredBlocksList & stored_blocks)
{
    right_rows_bitmap = std::make_unique<JoinStuff::JoinUsedFlags>();
    for (const auto & block : stored_blocks)
        right_rows_bitmap->allocPerRowFalse(block.block_no, block.columns.at(0)->size());
}

void MatchedRowsStats::collectNonJoined(UInt64 non_joined_rows)
{
    non_joined_right_rows.fetch_add(non_joined_rows);
}

void MatchedRowsStats::markRightMatched(UInt64 ref_word)
{
    for (const UInt64 word : refsOf(ref_word))
        right_rows_bitmap->setPerRow(refWordBlockNo(word), refWordRowNo(word));
}

UInt64 MatchedRowsStats::getMatchedLeft() const
{
    switch (leftMatchedSource(join_kind, join_strictness))
    {
        case LeftMatchedSource::MatchedRowsSize:
            return left_rows_matched.load(std::memory_order_relaxed);
        case LeftMatchedSource::MatchedRowsComplement:
            return left_rows_total.load(std::memory_order_relaxed) - left_rows_matched.load(std::memory_order_relaxed);
        case LeftMatchedSource::NotInOutput:
        case LeftMatchedSource::Unsupported:
            return 0;
    }
}

UInt64 MatchedRowsStats::getMatchedRight() const
{
    switch (rightMatchedSource(join_kind, join_strictness))
    {
        case RightMatchedSource::RefsBitmap:
            return right_rows_bitmap ? right_rows_bitmap->countUsed() : 0;
        case RightMatchedSource::LeftMatchCount:
            return left_rows_matched.load(std::memory_order_relaxed);
        case RightMatchedSource::NonJoinedComplement:
            return right_rows_total - non_joined_right_rows.load(std::memory_order_relaxed);
        case RightMatchedSource::AllRight:
            return right_rows_total;
        case RightMatchedSource::NotInOutput:
        case RightMatchedSource::Unsupported:
            return 0;
    }
}

}

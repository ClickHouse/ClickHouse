#pragma once

#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/JoinUtils.h>
#include <base/types.h>
#include <Core/Joins.h>

#include <atomic>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

enum class LeftMatchedSource
{
    ReplicationOffsets,
    DefaultRowMarkers,
    OutputFilter,
    OutputFilterComplement,
    Unsupported,
};

enum class RightMatchedSource
{
    NonJoinedComplement,
    RefsFlags,
    Unsupported,
};

constexpr LeftMatchedSource leftMatchedSource(JoinKind kind, JoinStrictness strictness)
{
    if (strictness == JoinStrictness::All && isLeftOrFull(kind))
        return LeftMatchedSource::DefaultRowMarkers;

    /// RightAny emits one row per left row just like ALL, so the markers mean the same
    if (strictness == JoinStrictness::RightAny && isLeftOrFull(kind))
        return LeftMatchedSource::DefaultRowMarkers;

    if (strictness == JoinStrictness::All)
        return LeftMatchedSource::ReplicationOffsets;

    if (kind == JoinKind::Left && strictness == JoinStrictness::Anti)
        return LeftMatchedSource::OutputFilterComplement;

    if (kind == JoinKind::Left && strictness == JoinStrictness::Semi)
        return LeftMatchedSource::OutputFilter;

    if (kind == JoinKind::Inner && (strictness == JoinStrictness::RightAny || strictness == JoinStrictness::Asof))
        return LeftMatchedSource::OutputFilter;

    if (kind == JoinKind::Right && strictness == JoinStrictness::RightAny)
        return LeftMatchedSource::OutputFilter;

    return LeftMatchedSource::Unsupported;
}

constexpr RightMatchedSource rightMatchedSource(JoinKind kind, JoinStrictness strictness)
{
    if (JoinCommon::hasNonJoinedBlocks(kind, strictness))
        return RightMatchedSource::NonJoinedComplement;

    if (isInnerOrLeft(kind) && strictness == JoinStrictness::All)
        return RightMatchedSource::RefsFlags;

    return RightMatchedSource::Unsupported;
}

/// Per-block/per-row flags of right-table rows that found a match, maintained ONLY for
/// EXPLAIN ANALYZE statistics. It is intentionally separate from `JoinStuff::JoinUsedFlags`
/// (which the join maintains for RIGHT/FULL non-joined output): mixing a profiling concern into
/// that correctness-critical structure would violate single-responsibility. This owns just the
/// small subset of functionality the statistics need.
class MatchedRightFlags
{
public:
    /// Allocate flags (all false) for a stored right block. Called once per block after build.
    void allocate(UInt32 block_no, size_t rows)
    {
        per_row_flags[block_no] = std::vector<std::atomic_bool>(rows);
    }

    /// Mark a single right row as matched (idempotent, thread-safe).
    void setMatched(UInt32 block_no, size_t row_num)
    {
        auto it = per_row_flags.find(block_no);
        chassert(it != per_row_flags.end());

        auto & flag = it->second[row_num];
        if (!flag.load(std::memory_order_relaxed))
            flag.store(true, std::memory_order_relaxed);
    }

    /// Number of distinct right rows marked as matched.
    size_t countMatched() const
    {
        size_t result = 0;
        for (const auto & [_, flags] : per_row_flags)
            for (const auto & flag : flags)
                result += flag.load(std::memory_order_relaxed);
        return result;
    }

private:
    /// Keyed by RowRef::block_no
    std::unordered_map<UInt32, std::vector<std::atomic_bool>> per_row_flags;
};

class MatchedRowsStats
{
public:

    MatchedRowsStats(JoinKind, JoinStrictness, JoinAnalyzeMode);

    void collectProbeBlock(UInt64 probed_block_size, std::optional<UInt64> matched_left);

    void markRightMatched(UInt64 ref_word);

    void collectNonJoined(UInt64 non_joined_rows);

    void prepareRightFlagsIfNeeded(const HashJoin::StoredBlocksList & stored_blocks);
    void prepareRightFlags(const HashJoin::StoredBlocksList & stored_blocks);
    bool hasRightFlags() const { return right_rows_flags != nullptr; }

    UInt64 getInputLeft() const { return left_rows_total.load(std::memory_order_relaxed); }
    std::optional<UInt64> getMatchedLeft() const;
    std::optional<UInt64> getMatchedRight(UInt64 right_rows_total) const;

private:

    std::unique_ptr<MatchedRightFlags> right_rows_flags;
    std::atomic<UInt64> left_rows_total = 0;
    std::atomic<UInt64> left_rows_matched = 0;
    std::atomic<bool> left_matched_available = true;
    std::atomic<UInt64> non_joined_right_rows = 0;

    JoinKind join_kind;
    JoinStrictness join_strictness;
    JoinAnalyzeMode analyze_mode;
};

template <JoinKind KIND, JoinStrictness STRICTNESS, typename AddedColumns>
static std::optional<UInt64> countMatchedLeftRows(const AddedColumns & added_columns, size_t probed_rows)
{
    constexpr auto source = leftMatchedSource(KIND, STRICTNESS);

    if constexpr (source == LeftMatchedSource::ReplicationOffsets)
    {
        const auto & offsets = added_columns.offsets_to_replicate;
        UInt64 matched = 0;
        for (size_t i = 0; i < probed_rows; ++i)
            matched += offsets[i] > offsets[i - 1];
        return matched;
    }
    else if constexpr (source == LeftMatchedSource::DefaultRowMarkers)
    {
        if (added_columns.additional_filter_expression)
            return added_columns.matched_left_rows;

        /// The markers live in LazyOutput::row_refs, which gets allocated and filled only
        /// when a flag gets turned on
        if (!added_columns.record_row_refs)
            return std::nullopt;

        UInt64 not_matched = 0;
        for (const UInt64 ref_word : added_columns.lazy_output.getRowRefs())
            not_matched += ref_word == 0;
        return probed_rows - not_matched;
    }
    else if constexpr (source == LeftMatchedSource::OutputFilter)
    {
        chassert(added_columns.need_filter);
        return countBytesInFilter(added_columns.filter);
    }
    else if constexpr (source == LeftMatchedSource::OutputFilterComplement)
    {
        chassert(added_columns.need_filter);
        return probed_rows - countBytesInFilter(added_columns.filter);
    }
    else
    {
        return std::nullopt;
    }
}

template <typename AddedColumns>
static void markRightMatchedFromRowRefs(MatchedRowsStats & stats, const AddedColumns & added_columns)
{
    UInt64 prev = 0;
    for (const UInt64 ref_word : added_columns.lazy_output.getRowRefs())
    {
        if (ref_word == 0 || ref_word == prev)
            continue;

        stats.markRightMatched(ref_word);
        prev = ref_word;
    }
}
}

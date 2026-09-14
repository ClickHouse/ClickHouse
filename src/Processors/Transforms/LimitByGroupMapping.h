#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/AggregationMethod.h>
#include <Processors/Transforms/ChunkRowRange.h>
#include <Common/ColumnsHashing.h>
#include <Common/Exception.h>

#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// The exclusive end of the kept per-group interval `[offset, offset + length)`, saturated at the
/// maximum of `UInt64` so that an unbounded length does not wrap.
UInt64 computeGroupLimitEnd(UInt64 length, UInt64 offset);

struct GroupingKeys
{
    Names names;
    std::vector<size_t> positions;
};

/// The grouping keys whose header-sample column is not `ColumnConst`. Constant columns do not help
/// distinguish groups because they have the same value on every row.
GroupingKeys filterNonConstKeys(const Block & header, const Names & column_names);

/// The chunk-local part of a same-group run that falls into the kept interval
/// `[group_offset, group_limit_end)`. `run_start_row` and `run_row_count` describe the run inside the
/// current chunk, `group_rows_seen_before_run` is the number of rows already seen for this group
/// before the run starts. A returned `length == 0` means the run contributes no rows.
ChunkRowRange shrinkRunToLimitWindow(
    UInt64 run_start_row, UInt64 run_row_count, UInt64 group_rows_seen_before_run, UInt64 group_offset, UInt64 group_limit_end);

/// The grouping state of the hash-based `LIMIT BY`: a hash table mapping each grouping key to an index
/// into a vector holding the number of input rows seen for that group so far.
///
/// The group index lives in the mapped slot of a cell, so a set method (which `GROUP BY` without
/// aggregate functions uses) cannot carry it; `chooseMethod` never returns one for these keys.
class LimitByGroupMapping
{
public:
    LimitByGroupMapping(const Block & header, const Names & column_names);

    /// Every grouping key is constant, so the whole input is one group and no hash table is built.
    bool isTrivial() const { return data.type == AggregatedDataVariants::Type::without_key; }

    const GroupingKeys & getKeys() const { return keys; }

    UInt64 getRowsSeen(size_t group_idx) const { return group_counts[group_idx]; }
    void setRowsSeen(size_t group_idx, UInt64 rows_seen) { group_counts[group_idx] = rows_seen; }
    size_t getNumGroups() const { return group_counts.size(); }

    /// The hash-table buffer plus the arena holding the keys; the dominant part of what a spill frees.
    size_t allocatedBytes() const;

    /// Splits the `row_count` rows of one chunk into maximal runs of rows that map to the same group and
    /// calls `on_run(run_start_row, run_row_count, group_idx)` for each of them. `on_row(row_idx)` is
    /// called before every row; returning false stops the mapping, and the run being accumulated is not
    /// reported, so the caller must treat the chunk as unprocessed.
    template <typename OnRun, typename OnRow>
    void mapChunk(const ColumnRawPtrs & key_columns, UInt64 row_count, OnRun && on_run, OnRow && on_row)
    {
        switch (data.type)
        {
#define M(NAME, IS_TWO_LEVEL) \
            case AggregatedDataVariants::Type::NAME: \
                mapChunkImpl(*data.NAME, key_columns, row_count, on_run, on_row); \
                break;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

            case AggregatedDataVariants::Type::EMPTY:
            case AggregatedDataVariants::Type::without_key:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AggregatedDataVariants type in LimitByGroupMapping");
        }
    }

    /// Appends the grouping key of every group to `key_columns` (which follow the header types at
    /// `getKeys().positions`) and the number of rows seen for it to `rows_seen`. The hash table is left
    /// behind: the caller destroys the whole mapping to release it.
    void extractGroups(MutableColumns & key_columns, PaddedPODArray<UInt64> & rows_seen);

private:
    template <typename Method, typename OnRun, typename OnRow>
    requires SetAggregationMethod<Method>
    void mapChunkImpl(Method &, const ColumnRawPtrs &, UInt64, OnRun &, OnRow &)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT BY does not support void-mapped aggregation methods");
    }

    template <typename Method, typename OnRun, typename OnRow>
    requires MapAggregationMethod<Method>
    void mapChunkImpl(Method & hash_method, const ColumnRawPtrs & key_columns, UInt64 row_count, OnRun & on_run, OnRow & on_row)
    {
        typename Method::State state(key_columns, data.key_sizes, hash_method_context);

        UInt64 current_run_start_row = 0;
        size_t current_run_group_idx = 0;

        for (UInt64 row_idx = 0; row_idx < row_count; ++row_idx)
        {
            if (!on_row(row_idx))
                return;

            auto key_emplace_result = state.emplaceKey(hash_method.data, row_idx, *data.aggregates_pool);
            size_t row_group_idx = 0;
            if (key_emplace_result.isInserted()) /// New grouping key
            {
                /// Assign a stable index into `group_counts` to the grouping key we just inserted.
                row_group_idx = group_counts.size();
                group_counts.push_back(0);
                key_emplace_result.setMapped(mappedFromGroupIndex(row_group_idx));
            }
            else /// Existing grouping key
            {
                row_group_idx = groupIndexFromMapped(key_emplace_result.getMapped());

                chassert(row_group_idx < group_counts.size());
            }

            if (row_idx == 0)
                current_run_group_idx = row_group_idx;
            else if (row_group_idx != current_run_group_idx) /// Local run ended
            {
                on_run(current_run_start_row, row_idx - current_run_start_row, current_run_group_idx);
                current_run_group_idx = row_group_idx;
                current_run_start_row = row_idx;
            }
        }

        /// Flush the final run, which extends to the end of the chunk.
        on_run(current_run_start_row, row_count - current_run_start_row, current_run_group_idx);
    }

    template <bool is_two_level, typename Method>
    requires SetAggregationMethod<Method>
    void extractGroupsImpl(Method &, MutableColumns &, PaddedPODArray<UInt64> &)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT BY does not support void-mapped aggregation methods");
    }

    template <bool is_two_level, typename Method>
    requires MapAggregationMethod<Method>
    void extractGroupsImpl(Method & hash_method, MutableColumns & key_columns, PaddedPODArray<UInt64> & rows_seen);

    /// The mapped slot holds a group index biased by one, so that the zeroth group is not a null pointer.
    static AggregateDataPtr mappedFromGroupIndex(size_t group_index)
    {
        return reinterpret_cast<AggregateDataPtr>(static_cast<uintptr_t>(group_index) + 1);
    }

    static size_t groupIndexFromMapped(AggregateDataPtr mapped)
    {
        return static_cast<size_t>(reinterpret_cast<uintptr_t>(mapped) - 1);
    }

    GroupingKeys keys;

    AggregatedDataVariants data;
    ColumnsHashing::HashMethodContextPtr hash_method_context;

    /// The total number of input rows already seen for the group, indexed by the group index stored in
    /// the cell. This is what decides whether the next row of that group is emitted.
    std::vector<UInt64> group_counts;
};

}

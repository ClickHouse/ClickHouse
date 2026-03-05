#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/TopKThresholdTracker.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/PODArray.h>

namespace DB
{

/// Builds the intermediate header used between partial and merge transforms.
/// Keys are pass-through; aggregates become DataTypeAggregateFunction columns.
Block buildIntermediateHeader(const Block & input_header, const Names & key_names, const AggregateDescriptions & aggregates);

/// Fused GROUP BY + ORDER BY aggregate + LIMIT K transform.
///
/// Mode 1 (sorted_input=true):  reads data already sorted by the aggregate
///   argument.  Stops after K distinct groups (early termination).
///
/// Mode 2 (sorted_input=false): direct aggregation using HashMap +
///   per-group IAggregateFunction states (no Aggregator framework), followed
///   by partial sort + LIMIT.  Optionally pushes a dynamic __topKFilter
///   prewhere via TopKThresholdTracker for storage-level skipping.
///
/// When partial=true (Mode 2 parallel), outputs intermediate aggregate state
/// columns instead of final results, for merging by TopNAggregatingMergeTransform.
class TopNAggregatingTransform : public IAccumulatingTransform
{
public:
    TopNAggregatingTransform(
        const Block & input_header_,
        const Block & output_header_,
        const Names & key_names_,
        const AggregateDescriptions & aggregates_,
        const SortDescription & sort_description_,
        size_t limit_,
        bool sorted_input_,
        bool partial_ = false,
        bool enable_threshold_pruning_ = false,
        TopKThresholdTrackerPtr threshold_tracker_ = nullptr);

    ~TopNAggregatingTransform() override;

    String getName() const override { return "TopNAggregating"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    /// --- Configuration (immutable after construction) ---
    Names key_names;
    AggregateDescriptions aggregates;
    SortDescription sort_description;
    size_t limit;
    bool sorted_input;
    bool partial;
    bool enable_threshold_pruning;
    TopKThresholdTrackerPtr threshold_tracker;
    Block stored_input_header;

    /// --- Column index mapping (computed once in constructor) ---
    ColumnNumbers key_column_indices;
    size_t order_by_agg_index = 0;
    int sort_direction = 0;
    std::vector<ColumnNumbers> agg_arg_columns;

    /// --- Aggregate state layout (computed once in constructor) ---
    std::vector<size_t> agg_state_offsets;
    size_t total_state_size = 0;
    size_t state_align = 1;

    /// --- Per-group state (grows during consume) ---
    /// Keep a wrapper instead of raw AggregateDataPtr so we can extend per-group
    /// metadata later (for example cached order value or flags) without refactors.
    struct GroupState { AggregateDataPtr state = nullptr; };

    ArenaPtr arena;
    HashMapWithSavedHash<std::string_view, size_t> group_indices;
    std::vector<GroupState> group_states;
    size_t num_groups = 0;
    bool generated = false;

    /// Mode 1: result columns (key + aggregate results, filled incrementally).
    MutableColumns result_columns;

    /// Mode 2: accumulated key columns (one row per group).
    /// Memory is O(number of unique groups) because all groups are aggregated
    /// before partial-sort selects the top K.  The dynamic __topKFilter prewhere
    /// is the primary mechanism for reducing data volume reaching the transform.
    MutableColumns mode2_accumulated_keys;

    /// --- In-transform threshold pruning (Mode 2, level >= 1) ---
    size_t order_agg_arg_col_idx = 0;
    MutableColumnPtr boundary_column;
    bool threshold_active = false;
    /// Adaptive threshold refresh cadence:
    /// start with per-chunk refresh for fast convergence, then back off as
    /// more chunks are processed to reduce repeated materialize+partial-sort cost.
    size_t mode2_chunks_seen = 0;
    size_t chunks_since_last_threshold_refresh = 0;

    /// --- Consume / generate per mode ---
    void consumeMode1(Chunk & chunk);
    void consumeMode2(Chunk & chunk);
    Chunk generateMode1();
    Chunk generateMode2();
    Chunk generateMode2Partial();

    /// --- Helpers: column indices and key serialization ---
    void initColumnIndices(const Block & input_header_);
    SerializedKeyHolder serializeGroupKey(const Columns & columns, size_t row) const;

    /// Populates agg_arg_column_ptrs from the current chunk columns.
    std::vector<ColumnRawPtrs> agg_arg_column_ptrs;
    void prepareArgColumnPtrs(const Columns & columns);

    /// --- Helpers: aggregate state lifecycle ---
    void createAggregateStates(AggregateDataPtr place) const;
    void destroyAggregateStates(AggregateDataPtr place) const;
    void addRowToAggregateStates(AggregateDataPtr place, size_t row);
    void insertResultsFromStates(AggregateDataPtr place, MutableColumns & output_columns);

    /// --- Helpers: sort + permute + limit ---
    IColumn::Permutation getSortPermutation(const IColumn & order_col, size_t output_limit) const;

    /// --- Helpers: threshold ---
    void refreshThresholdFromStates();
    void maybeRefreshThreshold();
    ColumnPtr buildThresholdKeepMask(const ColumnPtr & column, size_t rows);
    bool isBelowThreshold(const IColumn & col, size_t row) const;
};

/// Merges partial intermediate results from N parallel TopNAggregatingTransform
/// workers into a single final output. Uses direct HashMap + IAggregateFunction
/// merge (no Aggregator framework).
class TopNAggregatingMergeTransform : public IAccumulatingTransform
{
public:
    TopNAggregatingMergeTransform(
        const Block & intermediate_header_,
        const Block & output_header_,
        const Names & key_names_,
        const AggregateDescriptions & aggregates_,
        const SortDescription & sort_description_,
        size_t limit_);

    ~TopNAggregatingMergeTransform() override;

    String getName() const override { return "TopNAggregatingMerge"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    /// --- Configuration ---
    Names key_names;
    AggregateDescriptions aggregates;
    SortDescription sort_description;
    size_t limit;
    Block stored_header;

    /// --- Column index mapping ---
    ColumnNumbers key_column_indices;
    ColumnNumbers agg_column_indices;
    size_t order_by_agg_index = 0;
    int sort_direction = 0;

    /// --- Aggregate state layout ---
    std::vector<size_t> agg_state_offsets;
    size_t total_state_size = 0;
    size_t state_align = 1;

    /// --- Per-group state ---
    /// Keep a wrapper instead of raw AggregateDataPtr so we can extend per-group
    /// metadata later (for example cached order value or flags) without refactors.
    struct GroupState { AggregateDataPtr state = nullptr; };

    ArenaPtr arena;
    HashMapWithSavedHash<std::string_view, size_t> group_indices;
    std::vector<GroupState> group_states;
    MutableColumns accumulated_keys;
    size_t num_groups = 0;
    bool generated = false;
};

}

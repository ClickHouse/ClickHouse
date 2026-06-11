#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/TopKThresholdTracker.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/PODArray.h>

namespace DB
{

/// Builds the intermediate header used between partial and merge transforms.
/// Keys are pass-through; aggregates become DataTypeAggregateFunction columns.
Block buildIntermediateHeader(const Block & input_header, const Names & key_names, const AggregateDescriptions & aggregates);

/// GROUP BY limits from `Aggregator::Params`. The TopN transforms bypass `Aggregator`,
/// so they enforce `max_rows_to_group_by` / `group_by_overflow_mode` themselves,
/// mirroring `Aggregator::checkLimits`: `throw` raises `TOO_MANY_ROWS`, `break` stops
/// consuming input, `any` stops creating new groups but keeps aggregating existing ones.
/// The count is checked per transform (= per stream), like the per-thread check in
/// `Aggregator`, and only counts groups actually created — rows skipped by threshold
/// pruning or Mode 1 early termination do not contribute, in the same way that rows
/// skipped by a primary-key index do not contribute to `max_rows_to_read`.
struct TopNGroupByLimits
{
    size_t max_rows = 0;
    OverflowMode overflow_mode = OverflowMode::THROW;
};

/// Base class for fused GROUP BY + ORDER BY aggregate + LIMIT K transforms.
/// Contains shared infrastructure: column index mapping, aggregate state
/// layout, key serialization, and aggregate lifecycle helpers.
class TopNAggregatingTransformBase : public IAccumulatingTransform
{
public:
    TopNAggregatingTransformBase(
        const Block & input_header_,
        const Block & output_header_,
        const Names & key_names_,
        const AggregateDescriptions & aggregates_,
        const SortDescription & sort_description_,
        size_t limit_,
        const TopNGroupByLimits & group_by_limits_);

    ~TopNAggregatingTransformBase() override = default;

protected:
    /// --- Configuration (immutable after construction) ---
    Names key_names;
    AggregateDescriptions aggregates;
    SortDescription sort_description;
    size_t limit;
    TopNGroupByLimits group_by_limits;
    Block stored_input_header;

    /// Set when `group_by_overflow_mode = 'any'` triggered: keep aggregating
    /// existing groups but do not create new ones (see `Aggregator::checkLimits`).
    bool no_more_keys = false;

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
    ArenaPtr arena;
    HashMapWithSavedHash<std::string_view, size_t> group_indices;
    size_t num_groups = 0;
    bool generated = false;

    /// --- Helpers: column indices and key serialization ---
    void initColumnIndices(const Block & input_header_);
    SerializedKeyHolder serializeGroupKey(const Columns & columns, size_t row) const;

    /// Strips Const/Sparse wrappers from key columns but preserves
    /// LowCardinality so the accumulated key columns match the output
    /// header type (which keeps LowCardinality).
    ColumnRawPtrs key_column_ptrs;
    Columns key_column_holders;
    void prepareKeyColumnPtrs(const Columns & columns);

    /// Populates agg_arg_column_ptrs from the current chunk columns,
    /// unwrapping Const/Sparse/LowCardinality so aggregate functions
    /// receive the same column representation as the standard Aggregator
    /// (which calls recursiveRemoveLowCardinality on aggregate arguments).
    std::vector<ColumnRawPtrs> agg_arg_column_ptrs;
    Columns agg_arg_column_holders;
    void prepareArgColumnPtrs(const Columns & columns);

    /// --- Helpers: aggregate state lifecycle ---
    void createAggregateStates(AggregateDataPtr place) const;
    void destroyAggregateStates(AggregateDataPtr place) const;
    void addRowToAggregateStates(AggregateDataPtr place, size_t row);
    void insertResultsFromStates(AggregateDataPtr place, MutableColumns & output_columns);
};


/// Mode 1: sorted-input early-termination transform.
///
/// Input is physically sorted by the ORDER BY aggregate's determining column
/// (e.g. `start_time` for `max(start_time)`).  Processes rows one by one; the
/// first row for each new group determines its aggregate result.  Stops after
/// K distinct groups -- no need to see the rest of the data.
class TopNSortedAggregatingTransform : public TopNAggregatingTransformBase
{
public:
    TopNSortedAggregatingTransform(
        const Block & input_header_,
        const Block & output_header_,
        const Names & key_names_,
        const AggregateDescriptions & aggregates_,
        const SortDescription & sort_description_,
        size_t limit_,
        const TopNGroupByLimits & group_by_limits_);

    String getName() const override { return "TopNSortedAggregating"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    MutableColumns result_columns;
};


/// Mode 2: direct hash aggregation with optional threshold pruning.
///
/// Accumulates all groups in a HashMap with per-group IAggregateFunction
/// states (no Aggregator framework), then partial-sorts and limits at
/// output time.  Optionally prunes input rows whose ORDER BY aggregate
/// argument falls below the current K-th threshold.
///
/// When partial=true, outputs intermediate aggregate state columns (for
/// merging by TopNAggregatingMergeTransform) instead of final results.
class TopNDirectAggregatingTransform : public TopNAggregatingTransformBase
{
public:
    TopNDirectAggregatingTransform(
        const Block & input_header_,
        const Block & output_header_,
        const Names & key_names_,
        const AggregateDescriptions & aggregates_,
        const SortDescription & sort_description_,
        size_t limit_,
        const TopNGroupByLimits & group_by_limits_,
        bool partial_ = false,
        bool enable_threshold_pruning_ = false,
        TopKThresholdTrackerPtr threshold_tracker_ = nullptr);

    ~TopNDirectAggregatingTransform() override;

    String getName() const override { return "TopNDirectAggregating"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    bool partial;
    bool enable_threshold_pruning;
    TopKThresholdTrackerPtr threshold_tracker;

    struct GroupState { AggregateDataPtr state = nullptr; };
    std::vector<GroupState> group_states;

    /// Accumulated key columns (one row per group).
    MutableColumns accumulated_keys;

    /// --- In-transform threshold pruning ---
    size_t order_agg_arg_col_idx = 0;
    MutableColumnPtr boundary_column;
    bool threshold_active = false;
    size_t chunks_seen = 0;
    size_t chunks_since_last_threshold_refresh = 0;

    Chunk generateFull();
    Chunk generatePartial();

    IColumn::Permutation getSortPermutation(const IColumn & order_col, size_t output_limit) const;
    void refreshThresholdFromStates();
    void maybeRefreshThreshold();
    ColumnPtr buildThresholdKeepMask(const ColumnPtr & column, size_t rows);
    bool isBelowThreshold(const IColumn & col, size_t row) const;
};


/// Merges partial intermediate results from N parallel TopNDirectAggregatingTransform
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
        size_t limit_,
        const TopNGroupByLimits & group_by_limits_);

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
    TopNGroupByLimits group_by_limits;
    Block stored_header;

    /// See `TopNAggregatingTransformBase::no_more_keys`.
    bool no_more_keys = false;

    /// --- Column index mapping ---
    ColumnNumbers key_column_indices;
    ColumnNumbers agg_column_indices;
    size_t order_by_agg_index = 0;
    int sort_direction = 0;

    /// --- Aggregate state layout ---
    std::vector<size_t> agg_state_offsets;
    size_t total_state_size = 0;
    size_t state_align = 1;

    struct GroupState { AggregateDataPtr state = nullptr; };

    ArenaPtr arena;
    HashMapWithSavedHash<std::string_view, size_t> group_indices;
    std::vector<GroupState> group_states;
    MutableColumns accumulated_keys;
    size_t num_groups = 0;
    bool generated = false;
};

}

#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/TopKThresholdTracker.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>

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
    Names key_names;
    AggregateDescriptions aggregates;
    SortDescription sort_description;
    size_t limit;
    bool sorted_input;
    bool partial;
    bool enable_threshold_pruning;
    TopKThresholdTrackerPtr threshold_tracker;

    ColumnNumbers key_column_indices;
    size_t order_by_agg_index = 0;
    int sort_direction = 0;

    Block stored_input_header;

    /// Mode 1 result columns (key + aggregate results, filled incrementally).
    MutableColumns result_columns;
    size_t num_groups = 0;
    bool generated = false;

    /// Hash map: group key hash → index in group_states / mode2_accumulated_keys.
    HashMap<UInt128, size_t, UInt128TrivialHash> group_indices;

    struct GroupState
    {
        AggregateDataPtr state = nullptr;
    };

    ArenaPtr arena;
    std::vector<GroupState> group_states;
    size_t total_state_size = 0;
    size_t state_align = 1;

    std::vector<ColumnNumbers> agg_arg_columns;
    std::vector<size_t> agg_state_offsets;
    std::vector<ColumnRawPtrs> agg_arg_column_ptrs;

    /// Mode 2: accumulated key columns (one row per group).
    MutableColumns mode2_accumulated_keys;

    /// Dynamic-filter boundary column for TopKThresholdTracker.
    MutableColumnPtr boundary_column;

    void initColumnIndices(const Block & input_header_);
    void consumeMode1(Chunk & chunk);
    void consumeMode2(Chunk & chunk);
    Chunk generateMode1();
    Chunk generateMode2();
    Chunk generateMode2Partial();

    UInt128 hashGroupKey(const Columns & columns, size_t row) const;

    void createAggregateStates(AggregateDataPtr place) const;
    void destroyAggregateStates(AggregateDataPtr place) const;
    void addRowToAggregateStates(AggregateDataPtr place, size_t row);
    void insertResultsFromStates(AggregateDataPtr place, MutableColumns & output_columns);
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
    Names key_names;
    AggregateDescriptions aggregates;
    SortDescription sort_description;
    size_t limit;

    size_t order_by_agg_index = 0;
    int sort_direction = 0;

    Block stored_header;

    ColumnNumbers key_column_indices;
    ColumnNumbers agg_column_indices;

    HashMap<UInt128, size_t, UInt128TrivialHash> group_indices;

    struct GroupState
    {
        AggregateDataPtr state = nullptr;
    };

    ArenaPtr arena;
    std::vector<GroupState> group_states;
    size_t total_state_size = 0;
    size_t state_align = 1;
    std::vector<size_t> agg_state_offsets;

    MutableColumns accumulated_keys;
    size_t num_groups = 0;
    bool generated = false;
};

}

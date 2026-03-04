#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/TopKThresholdTracker.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>

namespace DB
{

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

    MutableColumns result_columns;
    size_t num_groups = 0;
    bool generated = false;

    /// Open-addressing hash map: SipHash-128 → group index.
    /// SipHash-128 has negligible collision probability (~2^-64 for practical sizes),
    /// so we treat it as a unique key without per-entry exact comparison.
    HashMap<UInt128, size_t, UInt128TrivialHash> group_indices;

    struct GroupState
    {
        AggregateDataPtr state = nullptr;
    };

    ArenaPtr arena;
    std::vector<GroupState> group_states;
    size_t total_state_size = 0;
    size_t state_align = 1;

    /// Pre-computed column indices for each aggregate's arguments to avoid
    /// per-row getPositionByName lookups.
    std::vector<ColumnNumbers> agg_arg_columns;

    /// Pre-computed aggregate state offsets to avoid per-row alignment math.
    std::vector<size_t> agg_state_offsets;

    /// Per-chunk cached raw column pointers for aggregate arguments,
    /// avoiding heap allocation per row inside addRowToAggregateStates.
    std::vector<ColumnRawPtrs> agg_arg_column_ptrs;

    /// Threshold pruning: a single-element boundary column holds the K-th best
    /// aggregate result value, enabling O(1) per-row comparison. Periodically
    /// refreshed from actual aggregate states to keep the boundary tight.
    MutableColumnPtr boundary_column;
    bool threshold_active = false;
    size_t order_agg_arg_col_idx = 0;

    bool isBelowThreshold(const IColumn & col, size_t row) const;
    void refreshThresholdFromStates();

    void initColumnIndices(const Block & input_header_);
    void consumeMode1(Chunk & chunk);
    void consumeMode2(Chunk & chunk);
    Chunk generateMode1();
    Chunk generateMode2();
    Chunk generateMode2Partial();

    UInt128 hashGroupKey(const Columns & columns, size_t row) const;

    void createAggregateStates(AggregateDataPtr place) const;
    void destroyAggregateStates(AggregateDataPtr place) const;
    void addRowToAggregateStates(AggregateDataPtr place, const Columns & columns, size_t row);
    void insertResultsFromStates(AggregateDataPtr place, MutableColumns & output_columns);
};

Block buildIntermediateHeader(const Block & input_header, const Names & key_names, const AggregateDescriptions & aggregates);

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

    ColumnNumbers key_column_indices;
    size_t order_by_agg_index = 0;
    int sort_direction = 0;

    Block stored_header;
    Block final_header;

    MutableColumns key_columns;

    HashMap<UInt128, size_t, UInt128TrivialHash> group_indices;

    struct GroupState
    {
        AggregateDataPtr state = nullptr;
    };

    Arena arena;
    std::vector<GroupState> group_states;
    size_t total_state_size = 0;
    size_t state_align = 1;
    size_t num_groups = 0;
    bool generated = false;

    UInt128 hashGroupKey(const Columns & columns, size_t row) const;
    size_t findGroupIndex(UInt128 hash, const Columns & columns, size_t row) const;
    void createAggregateStates(AggregateDataPtr place) const;
    void destroyAggregateStates(AggregateDataPtr place) const;
};

}

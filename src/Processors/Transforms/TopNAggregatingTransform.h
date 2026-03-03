#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Processors/IAccumulatingTransform.h>
#include <Common/Arena.h>

#include <unordered_map>

namespace DB
{

/// Fused operator for GROUP BY ... ORDER BY aggregate LIMIT K.
///
/// Mode 1 (sorted_input=true): input is sorted by the ORDER BY aggregate's argument.
/// Each group's aggregate result is determined by its first row.
/// Stops reading after `limit` unique groups.
///
/// Mode 2 (sorted_input=false): input is not sorted.
/// Aggregates all groups, then partial-sorts to select the top K.
/// Eliminates the separate full sort step.
/// NOTE: threshold-based group pruning (as described in the proposal) is not
/// yet implemented; this is an intentional v1 simplification.
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
        bool sorted_input_);

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

    ColumnNumbers key_column_indices;

    /// Which aggregate function is used for ORDER BY (index into `aggregates`).
    size_t order_by_agg_index = 0;

    /// Sort direction: 1=ASC, -1=DESC.
    int sort_direction = 0;

    Block stored_input_header;

    /// Mode 1 storage: output columns built incrementally.
    MutableColumns result_columns;
    size_t num_groups = 0;
    bool generated = false;

    /// Hash map: serialized group key -> row index in result_columns.
    std::unordered_map<std::string, size_t> group_indices;

    struct GroupState
    {
        AggregateDataPtr state = nullptr;
    };

    Arena arena;
    std::vector<GroupState> group_states;
    size_t total_state_size = 0;
    size_t state_align = 1;

    void initColumnIndices(const Block & input_header_);
    void consumeMode1(Chunk & chunk);
    void consumeMode2(Chunk & chunk);
    Chunk generateMode1();
    Chunk generateMode2();

    std::string serializeGroupKey(const Columns & columns, size_t row) const;
    void createAggregateStates(AggregateDataPtr place) const;
    void destroyAggregateStates(AggregateDataPtr place) const;
    void addRowToAggregateStates(AggregateDataPtr place, const Columns & columns, size_t row);
    void insertResultsFromStates(AggregateDataPtr place, MutableColumns & output_columns);
};

}

#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/Algorithms/MergedData.h>

namespace DB
{

/** Merges several inputs to one row. When merging, the data is pre-aggregated - merge of states of
  * aggregate functions. For columns that do not have the AggregateFunction type, when merged, the
  * first value is selected.
  */
class AggregatingNoSortedAlgorithm final : public IMergingAlgorithm
{
public:
    AggregatingNoSortedAlgorithm(const Block & header);
    void initialize(Inputs inputs) override;
    void consume(Input &, size_t) override { }
    Status merge() override;

    struct SimpleAggregateDescription;
    struct AggregateDescription;

    /// This structure define columns into one of three types:
    /// * columns which are not aggregate functions and not needed to be aggregated
    /// * usual aggregate functions, which stores states into ColumnAggregateFunction
    /// * simple aggregate functions, which store states into ordinary columns
    struct ColumnsDefinition
    {
        ColumnsDefinition(); /// Is needed because destructor is defined.
        ColumnsDefinition(ColumnsDefinition &&) noexcept; /// Is needed because destructor is defined.
        ~ColumnsDefinition(); /// Is needed because otherwise std::vector's destructor uses incomplete types.

        /// Columns with which numbers should not be aggregated.
        ColumnNumbers column_numbers_not_to_aggregate;
        std::vector<AggregateDescription> columns_to_aggregate;
        std::vector<SimpleAggregateDescription> columns_to_simple_aggregate;

        /// Does SimpleAggregateFunction allocates memory in arena?
        bool allocates_memory_in_arena = false;
    };

private:
    ColumnsDefinition columns_definition;
    MutableColumns columns;
    std::unique_ptr<Arena> arena;
    size_t arena_size = 0;
    Inputs inputs;
};

}

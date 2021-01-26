#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/**
 * The second step of aggregation in order of sorting key.
 * The transform recieves k inputs with partialy aggregated data,
 * sorted by group by key (prefix of sorting key).
 * Then it merges aggregated data from inputs by the following algorithm:
 *  - At each step find the smallest value X of the sorting key among last rows of current blocks of inputs.
 *    Since the data is sorted in order of sorting key and has no duplicates (because of aggregation),
 *    X will never appear later in any of input streams.
 *  - Aggregate all rows in current blocks of inputs upto the upper_bound of X using
 *    regular hash table algorithm (Aggregator::mergeBlock).
 */
class FinishAggregatingInOrderAlgorithm final : public IMergingAlgorithm
{
public:
    FinishAggregatingInOrderAlgorithm(
        const Block & header_,
        size_t num_inputs_,
        AggregatingTransformParamsPtr params_,
        SortDescription description_);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

private:
    Chunk aggregate();

    struct State
    {
        size_t num_rows;
        Columns all_columns;
        ColumnRawPtrs sorting_columns;

        /// Number of row starting from which need to aggregate.
        size_t current_row = 0;

        /// Number of row upto which need to aggregate (not included).
        size_t to_row = 0;

        State(const Chunk & chunk, const SortDescription & description);
        bool isValid() const { return current_row < num_rows; }
    };

    Block header;
    size_t num_inputs;
    AggregatingTransformParamsPtr params;
    SortDescription description;
    Inputs current_inputs;
    std::vector<State> states;
};

}

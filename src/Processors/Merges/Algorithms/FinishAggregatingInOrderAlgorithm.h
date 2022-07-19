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
 * The transform receives k inputs with partially aggregated data,
 * sorted by group by key (prefix of sorting key).
 * Then it merges aggregated data from inputs by the following algorithm:
 *  - At each step find the smallest value X of the sorting key among last rows of current blocks of inputs.
 *    Since the data is sorted in order of sorting key and has no duplicates in single input stream (because of aggregation),
 *    X will never appear later in any of input streams.
 *  - Aggregate all rows in current blocks of inputs up to the upper_bound of X using
 *    regular hash table algorithm (Aggregator::mergeBlock).
 * The hash table at one step will contain all keys <= X from all blocks.
 * There is another, simpler algorithm (AggregatingSortedAlgorithm), that merges
 * and aggregates sorted data for one key at a time, using one aggregation state.
 * It is a simple k-way merge algorithm and it makes O(n*log(k)) comparisons,
 * where * n -- number of rows, k -- number of parts. In comparison, this algorithm
 * makes about * O(n + k * log(n)) operations, n -- for hash table, k * log(n)
 * -- for finding positions in blocks. It is better than O(n*log(k)), when k is not
 * too big and not too small (about 100-1000).
 */
class FinishAggregatingInOrderAlgorithm final : public IMergingAlgorithm
{
public:
    FinishAggregatingInOrderAlgorithm(
        const Block & header_,
        size_t num_inputs_,
        AggregatingTransformParamsPtr params_,
        SortDescription description_,
        size_t max_block_size_);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

private:
    Chunk aggregate();
    void addToAggregation();

    struct State
    {
        size_t num_rows;
        Columns all_columns;
        ColumnRawPtrs sorting_columns;

        /// Number of row starting from which need to aggregate.
        size_t current_row = 0;

        /// Number of row up to which need to aggregate (not included).
        size_t to_row = 0;

        State(const Chunk & chunk, const SortDescription & description);
        bool isValid() const { return current_row < num_rows; }
    };

    Block header;
    size_t num_inputs;
    AggregatingTransformParamsPtr params;
    SortDescription description;
    size_t max_block_size;

    Inputs current_inputs;
    std::vector<State> states;
    std::vector<size_t> inputs_to_update;
    BlocksList blocks;
    size_t accumulated_rows = 0;
};

}

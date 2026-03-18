#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>
#include <Common/ThreadPool.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/**
 * The second step of aggregation in order of sorting key.
 * The transform receives k inputs with partially aggregated data,
 * sorted by group by key (prefix of sorting key).
 *
 * Then it merges aggregated data from inputs by the following algorithm:
 *  - At each step find the smallest value X of the sorting key among last rows of current blocks of inputs.
 *    Since the data is sorted in order of sorting key and has no duplicates in single input stream (because of aggregation),
 *    X will never appear later in any of input streams.
 *  - Add to current group all rows from current blocks of inputs up to the upper_bound of X.
 *  - Pass current group to the next step (MergingAggregatedBucketTransform), which do actual
 *    merge of aggregated data in parallel by regular hash table algorithm (Aggregator::mergeBlock).
 *
 * The hash table at one step will contain all keys <= X from all blocks.
 * There is another, simpler algorithm (AggregatingSortedAlgorithm), that merges
 * and aggregates sorted data for one key at a time, using one aggregation state.
 * It is a simple k-way merge algorithm and it makes O(n * log(k)) comparisons,
 * where n -- number of rows, k -- number of parts. In comparison, this algorithm
 * makes about * O(n + k * log(n)) operations, n -- for hash table,
 * k * log(n) -- for finding positions in blocks. It is better than O(n*log(k)),
 * when k is not too big and not too small (about 100-1000).
 */
class FinishAggregatingInOrderAlgorithm final : public IMergingAlgorithm
{
public:
    FinishAggregatingInOrderAlgorithm(
        const Block & header_,
        size_t num_inputs_,
        AggregatingTransformParamsPtr params_,
        const SortDescription & description_,
        size_t max_block_size_rows_,
        size_t max_block_size_bytes_);

    const char * getName() const override { return "FinishAggregatingInOrderAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override { return {.bytes = accumulated_bytes, .rows = accumulated_rows, .blocks = chunk_num}; }

private:
    Chunk prepareToMerge();
    void addToAggregation();

    struct State
    {
        Columns all_columns;
        ColumnRawPtrs sorting_columns;

        size_t num_rows = 0;

        /// Number of row starting from which need to aggregate.
        size_t current_row = 0;

        /// Number of row up to which need to aggregate (not included).
        size_t to_row = 0;

        /// Number of bytes in all columns + number of bytes in arena, related to current chunk.
        size_t total_bytes = 0;

        State(const Chunk & chunk, const SortDescriptionWithPositions & description, Int64 total_bytes_);
        State() = default;

        bool isValid() const { return current_row < num_rows; }
    };

    Block header;
    size_t num_inputs;
    AggregatingTransformParamsPtr params;
    SortDescriptionWithPositions description;
    size_t max_block_size_rows;
    size_t max_block_size_bytes;

    Inputs current_inputs;

    std::vector<State> states;
    std::vector<size_t> inputs_to_update;

    std::vector<Chunk> chunks;
    UInt64 chunk_num = 0;
    size_t accumulated_rows = 0;
    size_t accumulated_bytes = 0;

    size_t total_merged_rows = 0;
    size_t total_merged_bytes = 0;
};

}

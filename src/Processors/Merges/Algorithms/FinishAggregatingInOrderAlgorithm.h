#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

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

    struct State
    {
        size_t num_rows;
        Columns all_columns;
        ColumnRawPtrs sorting_columns;

        size_t current_row = 0;
        size_t to_row = 0;

        State(const Chunk & chunk, const SortDescription & description);
        bool isValid() const { return current_row < num_rows; }
    };

private:
    Block aggregate();

    MergedData merged_data;
    Block header;
    size_t num_inputs;
    AggregatingTransformParamsPtr params;
    SortDescription description;
    Inputs current_inputs;
    std::vector<State> states;
};

}

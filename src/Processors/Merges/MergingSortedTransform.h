#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/MergingSortedAlgorithm.h>


namespace DB
{

/// Merges several sorted inputs into one sorted output.
class MergingSortedTransform final : public IMergingTransform2<MergingSortedAlgorithm>
{
public:
    MergingSortedTransform(
        const Block & header,
        size_t num_inputs,
        SortDescription description,
        size_t max_block_size,
        UInt64 limit_ = 0,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool quiet_ = false,
        bool use_average_block_sizes = false,
        bool have_all_inputs_ = true);

    String getName() const override { return "MergingSortedTransform"; }

protected:
    void onNewInput() override;
    void onFinish() override;

private:
    bool quiet = false;
};

}

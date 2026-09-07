#pragma once

#include <Processors/Merges/Algorithms/DistinctSortedAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace ProfileEvents
{
    extern const Event MergingSortedMilliseconds;
}

namespace DB
{

/// Merges external `DISTINCT` runs and removes their already-emitted flag from the output.
class DistinctSortedTransform final : public IMergingTransform<DistinctSortedAlgorithm>
{
public:
    DistinctSortedTransform(
        SharedHeader input_header, SharedHeader output_header, size_t num_inputs,
        SortDescription description, size_t flag_column_pos, size_t max_block_size_rows,
        bool have_all_inputs = true)
        : IMergingTransform(
            num_inputs, input_header, output_header, have_all_inputs, /*limit_hint_=*/ 0,
            /*always_read_till_end_=*/ false, input_header, num_inputs, std::move(description),
            flag_column_pos, max_block_size_rows)
    {
    }

    String getName() const override { return "DistinctSortedTransform"; }

protected:
    void onNewInput() override { algorithm.addInput(); }
    void onFinish() override
    {
        logMergedStats(ProfileEvents::MergingSortedMilliseconds, "Merged distinct", getLogger("DistinctSortedTransform"));
    }
};

}

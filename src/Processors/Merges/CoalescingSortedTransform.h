#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/SummingSortedAlgorithm.h>

namespace ProfileEvents
{
    extern const Event CoalescingSortedMilliseconds;
}

namespace DB
{

class CoalescingSortedTransform final : public IMergingTransform<SummingSortedAlgorithm>
{
public:

    CoalescingSortedTransform(
        SharedHeader header, size_t num_inputs,
        SortDescription description_,
        const Names & partition_and_sorting_required_columns,
        const Names & partition_key_columns,
        size_t max_block_size_rows,
        size_t max_block_size_bytes
        )
        : IMergingTransform(
            num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
            header,
            num_inputs,
            std::move(description_),
            partition_and_sorting_required_columns,
            partition_key_columns,
            max_block_size_rows,
            max_block_size_bytes,
            "last_value")
    {
    }

    String getName() const override { return "CoalescingSortedTransform"; }

    void onFinish() override
    {
        logMergedStats(ProfileEvents::CoalescingSortedMilliseconds, "Coalescing sorted", getLogger("CoalescingSortedTransform"));
    }
};

}

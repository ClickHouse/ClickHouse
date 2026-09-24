#pragma once

#include <Processors/Merges/Algorithms/DistinctSortedAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace ProfileEvents
{
    extern const Event MergingSortedMilliseconds;
}

namespace DB
{

/// Merges external `DISTINCT` runs, returning ordinary payload and discarding suppression keys.
class DistinctSortedTransform final : public IMergingTransform<DistinctSortedAlgorithm>
{
public:
    DistinctSortedTransform(
        SharedHeaders input_headers, SharedHeader output_header, SortDescription description,
        size_t num_key_columns, size_t max_block_size_rows, bool have_all_inputs = true)
        : IMergingTransform(
            input_headers, output_header, have_all_inputs, /*limit_hint_=*/ 0,
            /*always_read_till_end_=*/ false, /*empty_chunk_on_finish_=*/ false,
            input_headers, output_header, std::move(description), num_key_columns, max_block_size_rows)
    {
    }

    String getName() const override { return "DistinctSortedTransform"; }

protected:
    void onNewInput() override { algorithm.addInput(inputs.back().getSharedHeader()); }
    void onFinish() override
    {
        logMergedStats(ProfileEvents::MergingSortedMilliseconds, "Merged distinct", getLogger("DistinctSortedTransform"));
    }
};

}

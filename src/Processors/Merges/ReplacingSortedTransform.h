#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>

namespace ProfileEvents
{
    extern const Event ReplacingSortedMilliseconds;
}

namespace DB
{

/// Implementation of IMergingTransform via ReplacingSortedAlgorithm.
class ReplacingSortedTransform final : public IMergingTransform<ReplacingSortedAlgorithm>
{
public:
    ReplacingSortedTransform(
        SharedHeader header, size_t num_inputs,
        SortDescription description_,
        const String & is_deleted_column, const String & version_column,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        std::optional<size_t> max_dynamic_subcolumns_,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false,
        bool cleanup = false,
        bool enable_vertical_final = false)
        : IMergingTransform(
            num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
            header,
            num_inputs,
            std::move(description_),
            is_deleted_column,
            version_column,
            max_block_size_rows,
            max_block_size_bytes,
            max_dynamic_subcolumns_,
            out_row_sources_buf_,
            use_average_block_sizes,
            cleanup,
            enable_vertical_final)
    {
    }

    String getName() const override { return "ReplacingSorted"; }

    void onFinish() override
    {
        logMergedStats(ProfileEvents::ReplacingSortedMilliseconds, "Replaced sorted", getLogger("ReplacingSortedTransform"));
    }
};

}

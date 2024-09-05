#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/VersionedCollapsingAlgorithm.h>

namespace ProfileEvents
{
    extern const Event VersionedCollapsingSortedMilliseconds;
}

namespace DB
{

/// Implementation of IMergingTransform via VersionedCollapsingAlgorithm.
class VersionedCollapsingTransform final : public IMergingTransform<VersionedCollapsingAlgorithm>
{
public:
    /// Don't need version column. It's in primary key.
    VersionedCollapsingTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & sign_column_,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false)
        : IMergingTransform(
            num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
            header,
            num_inputs,
            std::move(description_),
            sign_column_,
            max_block_size_rows,
            max_block_size_bytes,
            out_row_sources_buf_,
            use_average_block_sizes)
    {
    }

    String getName() const override { return "VersionedCollapsingTransform"; }

    void onFinish() override
    {
        logMergedStats(ProfileEvents::VersionedCollapsingSortedMilliseconds, "Versioned collapsed sorted", getLogger("VersionedCollapsingTransform"));
    }
};

}

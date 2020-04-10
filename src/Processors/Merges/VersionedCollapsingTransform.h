#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/VersionedCollapsingAlgorithm.h>


namespace DB
{

/** Merges several sorted ports to one.
  * For each group of consecutive identical values of the sorting key
  *   (the columns by which the data is sorted, including specially specified version column),
  *   merges any pair of consecutive rows with opposite sign.
  */
class VersionedCollapsingTransform final : public IMergingTransform2<VersionedCollapsingAlgorithm>
{
public:
    /// Don't need version column. It's in primary key.
    VersionedCollapsingTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & sign_column_,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false)
        : IMergingTransform2(
            num_inputs, header, header, true,
            header,
            num_inputs,
            std::move(description_),
            sign_column_,
            max_block_size,
            out_row_sources_buf_,
            use_average_block_sizes)
    {
    }

    String getName() const override { return "VersionedCollapsingTransform"; }
};

}

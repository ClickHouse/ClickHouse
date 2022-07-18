#pragma once

#include <Processors/Transforms/SortingTransform.h>
#include <Core/SortDescription.h>
#include <Common/filesystemHelpers.h>
#include <base/logger_useful.h>


namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

/// Takes sorted separate chunks of data. Sorts them.
/// Returns stream with globally sorted data.
class MergeSortingTransform : public SortingTransform
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingTransform(
        const Block & header,
        const SortDescription & description_,
        size_t max_merged_block_size_,
        UInt64 limit_,
        size_t max_bytes_before_remerge_,
        double remerge_lowered_memory_bytes_ratio_,
        size_t max_bytes_before_external_sort_,
        VolumePtr tmp_volume_,
        size_t min_free_disk_space_);

    String getName() const override { return "MergeSortingTransform"; }

protected:
    void consume(Chunk chunk) override;
    void serialize() override;
    void generate() override;

    Processors expandPipeline() override;

private:
    size_t max_bytes_before_remerge;
    double remerge_lowered_memory_bytes_ratio;
    size_t max_bytes_before_external_sort;
    VolumePtr tmp_volume;
    size_t min_free_disk_space;

    size_t sum_rows_in_blocks = 0;
    size_t sum_bytes_in_blocks = 0;

    Poco::Logger * log = &Poco::Logger::get("MergeSortingTransform");

    /// If remerge doesn't save memory at least several times, mark it as useless and don't do it anymore.
    bool remerge_is_useful = true;

    /// Everything below is for external sorting.
    std::vector<std::unique_ptr<TemporaryFile>> temporary_files;

    /// Merge all accumulated blocks to keep no more than limit rows.
    void remerge();

    ProcessorPtr external_merging_sorted;
};

}

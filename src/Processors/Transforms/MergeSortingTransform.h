#pragma once

#include <Processors/Transforms/SortingTransform.h>
#include <Core/SortDescription.h>
#include <Common/filesystemHelpers.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Interpreters/TemporaryDataOnDisk.h>


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
        size_t max_block_bytes,
        UInt64 limit_,
        bool increase_sort_description_compile_attempts,
        size_t max_bytes_before_remerge_,
        double remerge_lowered_memory_bytes_ratio_,
        size_t max_bytes_before_external_sort_,
        TemporaryDataOnDiskPtr tmp_data_,
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
    TemporaryDataOnDiskPtr tmp_data;
    size_t min_free_disk_space;
    size_t max_block_bytes;

    size_t sum_rows_in_blocks = 0;
    size_t sum_bytes_in_blocks = 0;

    LoggerPtr log = getLogger("MergeSortingTransform");

    /// If remerge doesn't save memory at least several times, mark it as useless and don't do it anymore.
    bool remerge_is_useful = true;

    /// Merge all accumulated blocks to keep no more than limit rows.
    void remerge();

    ProcessorPtr external_merging_sorted;
};

}

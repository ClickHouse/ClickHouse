#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    Names required_columns_,
    MarkRanges mark_ranges_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    bool check_columns_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    bool one_range_per_task_)
    : MergeTreeBaseSelectProcessor{
        metadata_snapshot_->getSampleBlockForColumns(required_columns_, storage_.getVirtuals(), storage_.getStorageID()),
        storage_, metadata_snapshot_, prewhere_info_, max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_},
    required_columns{std::move(required_columns_)},
    data_part{owned_data_part_},
    all_mark_ranges(std::move(mark_ranges_)),
    one_range_per_task(one_range_per_task_),
    check_columns(check_columns_),
    total_rows(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
{
    /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    task_columns = getReadTaskColumns(
        storage, metadata_snapshot, data_part,
        required_columns, prewhere_info, check_columns);

    if (use_uncompressed_cache)
        owned_uncompressed_cache = storage.getContext()->getUncompressedCache();

    owned_mark_cache = storage.getContext()->getMarkCache();

    reader = data_part->getReader(task_columns.columns, metadata_snapshot, all_mark_ranges,
        owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

    if (prewhere_info)
        pre_reader = data_part->getReader(task_columns.pre_columns, metadata_snapshot, all_mark_ranges,
            owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

    addTotalRowsApprox(total_rows);
    ordered_names = header_without_virtual_columns.getNames();
}

void MergeTreeSelectProcessor::finish()
{
    /** Close the files (before destroying the object).
    * When many sources are created, but simultaneously reading only a few of them,
    * buffers don't waste memory.
    */
    reader.reset();
    pre_reader.reset();
    data_part.reset();
}

MergeTreeSelectProcessor::~MergeTreeSelectProcessor() = default;

}

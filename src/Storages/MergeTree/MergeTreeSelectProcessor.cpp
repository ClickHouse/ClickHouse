#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>


namespace DB
{

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
    ExpressionActionsSettings actions_settings,
    bool check_columns_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    bool has_limit_below_one_block_)
    : MergeTreeBaseSelectProcessor{
        metadata_snapshot_->getSampleBlockForColumns(required_columns_, storage_.getVirtuals(), storage_.getStorageID()),
        storage_, metadata_snapshot_, prewhere_info_, std::move(actions_settings), max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_},
    required_columns{std::move(required_columns_)},
    data_part{owned_data_part_},
    sample_block(metadata_snapshot_->getSampleBlock()),
    all_mark_ranges(std::move(mark_ranges_)),
    part_index_in_query(part_index_in_query_),
    has_limit_below_one_block(has_limit_below_one_block_),
    check_columns(check_columns_),
    total_rows(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
{
    addTotalRowsApprox(total_rows);
    ordered_names = header_without_virtual_columns.getNames();
}

void MergeTreeSelectProcessor::initializeReaders()
{
    task_columns = getReadTaskColumns(
        storage, metadata_snapshot, data_part,
        required_columns, prewhere_info, check_columns);

    /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    if (use_uncompressed_cache)
        owned_uncompressed_cache = storage.getContext()->getUncompressedCache();

    owned_mark_cache = storage.getContext()->getMarkCache();

    reader = data_part->getReader(task_columns.columns, metadata_snapshot, all_mark_ranges,
        owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

    if (prewhere_info)
        pre_reader = data_part->getReader(task_columns.pre_columns, metadata_snapshot, all_mark_ranges,
            owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

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

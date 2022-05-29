#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>


namespace DB
{

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    Names required_columns_,
    MarkRanges mark_ranges_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    ExpressionActionsSettings actions_settings,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    bool has_limit_below_one_block_,
    std::optional<ParallelReadingExtension> extension_)
    : MergeTreeBaseSelectProcessor{
        storage_snapshot_->getSampleBlockForColumns(required_columns_),
        storage_, storage_snapshot_, prewhere_info_, std::move(actions_settings), max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_, extension_},
    required_columns{std::move(required_columns_)},
    data_part{owned_data_part_},
    sample_block(storage_snapshot_->metadata->getSampleBlock()),
    all_mark_ranges(std::move(mark_ranges_)),
    part_index_in_query(part_index_in_query_),
    has_limit_below_one_block(has_limit_below_one_block_),
    total_rows(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
{
    /// Actually it means that parallel reading from replicas enabled
    /// and we have to collaborate with initiator.
    /// In this case we won't set approximate rows, because it will be accounted multiple times.
    /// Also do not count amount of read rows if we read in order of sorting key,
    /// because we don't know actual amount of read rows in case when limit is set.
    if (!extension_.has_value() && !reader_settings.read_in_order)
        addTotalRowsApprox(total_rows);

    ordered_names = header_without_virtual_columns.getNames();
}

void MergeTreeSelectProcessor::initializeReaders()
{
    task_columns = getReadTaskColumns(
        storage, storage_snapshot, data_part,
        required_columns, prewhere_info, /*with_subcolumns=*/ true);

    /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    if (use_uncompressed_cache)
        owned_uncompressed_cache = storage.getContext()->getUncompressedCache();

    owned_mark_cache = storage.getContext()->getMarkCache();

    reader = data_part->getReader(task_columns.columns, storage_snapshot->getMetadataForQuery(),
        all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

    if (prewhere_info)
        pre_reader = data_part->getReader(task_columns.pre_columns, storage_snapshot->getMetadataForQuery(),
            all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

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

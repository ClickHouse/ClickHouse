#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Interpreters/Context.h>


namespace DB
{

MergeTreeSelectAlgorithm::MergeTreeSelectAlgorithm(
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
    MergeTreeInOrderReadPoolParallelReplicasPtr pool_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    bool has_limit_below_one_block_)
    : IMergeTreeSelectAlgorithm{
        storage_snapshot_->getSampleBlockForColumns(required_columns_),
        storage_, storage_snapshot_, prewhere_info_, std::move(actions_settings), max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_},
    required_columns{std::move(required_columns_)},
    data_part{owned_data_part_},
    sample_block(storage_snapshot_->metadata->getSampleBlock()),
    all_mark_ranges(std::move(mark_ranges_)),
    part_index_in_query(part_index_in_query_),
    has_limit_below_one_block(has_limit_below_one_block_),
    pool(pool_),
    total_rows(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
{
    ordered_names = header_without_const_virtual_columns.getNames();
}

void MergeTreeSelectAlgorithm::initializeReaders()
{
    task_columns = getReadTaskColumns(
        LoadedMergeTreeDataPartInfoForReader(data_part), storage_snapshot,
        required_columns, virt_column_names, prewhere_info, /*with_subcolumns=*/ true);

    /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    if (use_uncompressed_cache)
        owned_uncompressed_cache = storage.getContext()->getUncompressedCache();

    owned_mark_cache = storage.getContext()->getMarkCache();

    initializeMergeTreeReadersForPart(data_part, task_columns, storage_snapshot->getMetadataForQuery(),
        all_mark_ranges, {}, {});
}


void MergeTreeSelectAlgorithm::finish()
{
    /** Close the files (before destroying the object).
    * When many sources are created, but simultaneously reading only a few of them,
    * buffers don't waste memory.
    */
    reader.reset();
    pre_reader_for_step.clear();
    data_part.reset();
}

MergeTreeSelectAlgorithm::~MergeTreeSelectAlgorithm() = default;

}

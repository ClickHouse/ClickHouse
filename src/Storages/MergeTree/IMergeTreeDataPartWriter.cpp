#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeWriterSettings & settings_)
    : data_part(data_part_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , columns_list(columns_list_)
    , settings(settings_)
    , with_final_mark(storage.getSettings()->write_final_mark && settings.can_use_adaptive_granularity){}

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeIndices & skip_indices_,
    const MergeTreeIndexGranularity & index_granularity_,
    const MergeTreeWriterSettings & settings_)
    : data_part(data_part_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , columns_list(columns_list_)
    , skip_indices(skip_indices_)
    , index_granularity(index_granularity_)
    , settings(settings_)
    , with_final_mark(storage.getSettings()->write_final_mark && settings.can_use_adaptive_granularity) {}

Columns IMergeTreeDataPartWriter::releaseIndexColumns()
{
    return Columns(
        std::make_move_iterator(index_columns.begin()),
        std::make_move_iterator(index_columns.end()));
}

void IMergeTreeDataPartWriter::next()
{
    current_mark = next_mark;
    index_offset = next_index_offset;
}

bool IMergeTreeDataPartWriter::adjustLastUnfinishedMark(size_t new_block_index_granularity)
{
    /// If amount of rest rows in the last granule more then granularity of the new block
    /// than finish it.
    if (!index_granularity.empty() && index_offset > new_block_index_granularity)
    {
        size_t already_written_rows_in_last_granule = index_granularity.getLastMarkRows() - index_offset;
        /// We can still write some rows to the last granule
        if (already_written_rows_in_last_granule < new_block_index_granularity)
        {
            index_granularity.setLastMarkRows(new_block_index_granularity);
            index_offset = new_block_index_granularity - already_written_rows_in_last_granule;
        }
        else /// Our last granule is already full, let's start the new one
        {
            index_granularity.setLastMarkRows(already_written_rows_in_last_granule);
            index_offset = 0;
        }
        return true;
    }
    return false;
}

IMergeTreeDataPartWriter::~IMergeTreeDataPartWriter() = default;

}

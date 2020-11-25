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

IMergeTreeDataPartWriter::~IMergeTreeDataPartWriter() = default;

}

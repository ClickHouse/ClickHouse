#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData & storage_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeWriterSettings & settings_)
    : storage(storage_)
    , columns_list(columns_list_)
    , settings(settings_) {}

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData & storage_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeIndices & skip_indices_,
    const MergeTreeIndexGranularity & index_granularity_,
    const MergeTreeWriterSettings & settings_)
    : storage(storage_)
    , columns_list(columns_list_)
    , skip_indices(skip_indices_)
    , index_granularity(index_granularity_)
    , settings(settings_) {}

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

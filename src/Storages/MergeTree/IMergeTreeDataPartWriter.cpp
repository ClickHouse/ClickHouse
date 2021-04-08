#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation)
{
    Block result;
    for (size_t i = 0, size = names.size(); i < size; ++i)
    {
        const auto & name = names[i];
        result.insert(i, block.getByName(name));

        /// Reorder primary key columns in advance and add them to `primary_key_columns`.
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }

    return result;
}

Block permuteBlockIfNeeded(const Block & block, const IColumn::Permutation * permutation)
{
    Block result;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        result.insert(i, block.getByPosition(i));
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }
    return result;
}

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : data_part(data_part_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , columns_list(columns_list_)
    , settings(settings_)
    , index_granularity(index_granularity_)
    , with_final_mark(storage.getSettings()->write_final_mark && settings.can_use_adaptive_granularity)
{
}

Columns IMergeTreeDataPartWriter::releaseIndexColumns()
{
    return Columns(
        std::make_move_iterator(index_columns.begin()),
        std::make_move_iterator(index_columns.end()));
}

IMergeTreeDataPartWriter::~IMergeTreeDataPartWriter() = default;

}

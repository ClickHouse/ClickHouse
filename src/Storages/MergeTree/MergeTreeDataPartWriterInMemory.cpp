#include <Storages/MergeTree/MergeTreeDataPartWriterInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeDataPartWriterInMemory::MergeTreeDataPartWriterInMemory(
    const DataPartInMemoryPtr & part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeWriterSettings & settings_)
    : IMergeTreeDataPartWriter(part_, columns_list_, metadata_snapshot_, settings_)
    , part_in_memory(part_) {}

void MergeTreeDataPartWriterInMemory::write(
    const Block & block, const IColumn::Permutation * permutation)
{
    if (part_in_memory->block)
        throw Exception("DataPartWriterInMemory supports only one write", ErrorCodes::LOGICAL_ERROR);

    Block primary_key_block;
    if (settings.rewrite_primary_key)
        primary_key_block = getBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);

    Block result_block;
    if (permutation)
    {
        for (const auto & col : columns_list)
        {
            if (primary_key_block.has(col.name))
                result_block.insert(primary_key_block.getByName(col.name));
            else
            {
                auto permuted = block.getByName(col.name);
                permuted.column = permuted.column->permute(*permutation, 0);
                result_block.insert(permuted);
            }
        }
    }
    else
    {
        for (const auto & col : columns_list)
            result_block.insert(block.getByName(col.name));
    }

    index_granularity.appendMark(result_block.rows());
    if (with_final_mark)
        index_granularity.appendMark(0);
    part_in_memory->block = std::move(result_block);

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block);
}

void MergeTreeDataPartWriterInMemory::calculateAndSerializePrimaryIndex(const Block & primary_index_block)
{
    size_t rows = primary_index_block.rows();
    if (!rows)
        return;

    size_t primary_columns_num = primary_index_block.columns();
    index_columns.resize(primary_columns_num);
    for (size_t i = 0; i < primary_columns_num; ++i)
    {
        const auto & primary_column = *primary_index_block.getByPosition(i).column;
        index_columns[i] = primary_column.cloneEmpty();
        index_columns[i]->insertFrom(primary_column, 0);
        if (with_final_mark)
            index_columns[i]->insertFrom(primary_column, rows - 1);
    }
}

void MergeTreeDataPartWriterInMemory::fillChecksums(IMergeTreeDataPart::Checksums & checksums)
{
    /// If part is empty we still need to initialize block by empty columns.
    if (!part_in_memory->block)
        for (const auto & column : columns_list)
            part_in_memory->block.insert(ColumnWithTypeAndName{column.type, column.name});

    checksums.files["data.bin"] = part_in_memory->calculateBlockChecksum();
}

}

#include <Storages/MergeTree/MergeTreeDataPartWriterBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartBuffer.h>
#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeDataPartWriterBuffer::MergeTreeDataPartWriterBuffer(
    const MutableDataPartBufferPtr & part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeWriterSettings & settings_)
    : IMergeTreeDataPartWriter(part_, columns_list_, metadata_snapshot_, settings_)
    , part_buffer(part_) {}

void MergeTreeDataPartWriterBuffer::write(
    const Block & block, const IColumn::Permutation * permutation)
{
    if (part_buffer->block)
        throw Exception("DataPartWriterBuffer supports only one write", ErrorCodes::LOGICAL_ERROR);

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
    part_buffer->block = std::move(result_block);

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block);
}

void MergeTreeDataPartWriterBuffer::calculateAndSerializePrimaryIndex(const Block & primary_index_block)
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

void MergeTreeDataPartWriterBuffer::fillChecksums(IMergeTreeDataPart::Checksums & checksums)
{
    /// If part is empty we still need to initialize block by empty columns.
    if (!part_buffer->block)
        for (const auto & column : columns_list)
            part_buffer->block.insert(ColumnWithTypeAndName{column.type, column.name});

    checksums.files["data.bin"] = part_buffer->calculateBlockChecksum();
}

}

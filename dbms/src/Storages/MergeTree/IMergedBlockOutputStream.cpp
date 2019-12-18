#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    // constexpr auto DATA_FILE_EXTENSION = ".bin";
    // constexpr auto INDEX_FILE_EXTENSION = ".idx";
}


IMergedBlockOutputStream::IMergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part)
    : storage(data_part->storage)
    , part_path(data_part->getFullPath())
{
}

Block IMergedBlockOutputStream::getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation)
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

/// Implementation of IMergedBlockOutputStream::ColumnStream.

}

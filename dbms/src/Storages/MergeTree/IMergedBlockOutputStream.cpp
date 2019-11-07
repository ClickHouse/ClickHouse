#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeReaderSettings.h>
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

/// Implementation of IMergedBlockOutputStream::ColumnStream.

}

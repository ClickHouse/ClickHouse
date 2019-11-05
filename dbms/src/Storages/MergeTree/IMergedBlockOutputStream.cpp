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
    constexpr auto INDEX_FILE_EXTENSION = ".idx";
}


IMergedBlockOutputStream::IMergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part,
    CompressionCodecPtr codec_,
    const WriterSettings & writer_settings_,
    bool blocks_are_granules_size_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    bool can_use_adaptive_granularity_)
    : storage(data_part->storage)
    , part_path(data_part->getFullPath())
    , writer_settings(writer_settings_)
    , can_use_adaptive_granularity(can_use_adaptive_granularity_)
    , marks_file_extension(data_part->getMarksFileExtension())
    , blocks_are_granules_size(blocks_are_granules_size_)
    , index_granularity(data_part->index_granularity)
    , compute_granularity(index_granularity.empty())
    , codec(std::move(codec_))
    , skip_indices(indices_to_recalc)
    , with_final_mark(storage.getSettings()->write_final_mark && can_use_adaptive_granularity)
{
    if (blocks_are_granules_size && !index_granularity.empty())
        throw Exception("Can't take information about index granularity from blocks, when non empty index_granularity array specified", ErrorCodes::LOGICAL_ERROR);
}

/// Implementation of IMergedBlockOutputStream::ColumnStream.

}

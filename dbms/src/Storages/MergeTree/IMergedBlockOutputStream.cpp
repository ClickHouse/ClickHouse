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

void fillIndexGranularityImpl(
    const Block & block,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    size_t index_offset,
    MergeTreeIndexGranularity & index_granularity,
    bool can_use_adaptive_index_granularity)
{
    size_t rows_in_block = block.rows();
    size_t index_granularity_for_block;
    if (!can_use_adaptive_index_granularity)
        index_granularity_for_block = fixed_index_granularity_rows;
    else
    {
        size_t block_size_in_memory = block.bytes();
        if (blocks_are_granules)
            index_granularity_for_block = rows_in_block;
        else if (block_size_in_memory >= index_granularity_bytes)
        {
            size_t granules_in_block = block_size_in_memory / index_granularity_bytes;
            index_granularity_for_block = rows_in_block / granules_in_block;
        }
        else
        {
            size_t size_of_row_in_bytes = block_size_in_memory / rows_in_block;
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }
    if (index_granularity_for_block == 0) /// very rare case when index granularity bytes less then single row
        index_granularity_for_block = 1;

    /// We should be less or equal than fixed index granularity
    index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);

    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void IMergedBlockOutputStream::fillIndexGranularity(const Block & block)
{
    const auto storage_settings = storage.getSettings();
    fillIndexGranularityImpl(
        block,
        storage_settings->index_granularity_bytes,
        storage_settings->index_granularity,
        blocks_are_granules_size,
        index_offset,
        index_granularity,
        can_use_adaptive_granularity);
}

void IMergedBlockOutputStream::initSkipIndices()
{
    for (const auto & index : skip_indices)
    {
        String stream_name = index->getFileName();
        skip_indices_streams.emplace_back(
                std::make_unique<IMergeTreeDataPartWriter::ColumnStream>(
                        stream_name,
                        part_path + stream_name, INDEX_FILE_EXTENSION,
                        part_path + stream_name, marks_file_extension,
                        codec, writer_settings.max_compress_block_size,
                        0, writer_settings.aio_threshold));
        skip_indices_aggregators.push_back(index->createIndexAggregator());
        skip_index_filling.push_back(0);
    }
}

void IMergedBlockOutputStream::calculateAndSerializeSkipIndices(
        const Block & skip_indexes_block, size_t rows)
{
    size_t skip_index_current_data_mark = 0;

    /// Filling and writing skip indices like in IMergedBlockOutputStream::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index = skip_indices[i];
        auto & stream = *skip_indices_streams[i];
        size_t prev_pos = 0;
        skip_index_current_data_mark = skip_index_data_mark;
        while (prev_pos < rows)
        {
            UInt64 limit = 0;
            if (prev_pos == 0 && index_offset != 0)
            {
                limit = index_offset;
            }
            else
            {
                limit = index_granularity.getMarkRows(skip_index_current_data_mark);
                if (skip_indices_aggregators[i]->empty())
                {
                    skip_indices_aggregators[i] = index->createIndexAggregator();
                    skip_index_filling[i] = 0;

                    if (stream.compressed.offset() >= writer_settings.min_compress_block_size)
                        stream.compressed.next();

                    writeIntBinary(stream.plain_hashing.count(), stream.marks);
                    writeIntBinary(stream.compressed.offset(), stream.marks);
                    /// Actually this numbers is redundant, but we have to store them
                    /// to be compatible with normal .mrk2 file format
                    if (can_use_adaptive_granularity)
                        writeIntBinary(1UL, stream.marks);
                }
                /// this mark is aggregated, go to the next one
                skip_index_current_data_mark++;
            }

            size_t pos = prev_pos;
            skip_indices_aggregators[i]->update(skip_indexes_block, &pos, limit);

            if (pos == prev_pos + limit)
            {
                ++skip_index_filling[i];

                /// write index if it is filled
                if (skip_index_filling[i] == index->granularity)
                {
                    skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
                    skip_index_filling[i] = 0;
                }
            }
            prev_pos = pos;
        }
    }
    skip_index_data_mark = skip_index_current_data_mark;
}

void IMergedBlockOutputStream::finishSkipIndicesSerialization(
        MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->finalize();
        stream->addToChecksums(checksums);
    }

    skip_indices_streams.clear();
    skip_indices_aggregators.clear();
    skip_index_filling.clear();
}

/// Implementation of IMergedBlockOutputStream::ColumnStream.

}

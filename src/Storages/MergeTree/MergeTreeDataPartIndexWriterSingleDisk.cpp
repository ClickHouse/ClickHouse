#include "MergeTreeDataPartIndexWriterSingleDisk.h"

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeDataPartIndexWriterSingleDisk::MergeTreeDataPartIndexWriterSingleDisk(IMergeTreeDataPartWriter & part_writer_)
    : IMergeTreeDataPartIndexWriter(part_writer_)
{
}

void MergeTreeDataPartIndexWriterSingleDisk::initPrimaryIndex()
{
    if (part_writer.storage.hasPrimaryKey())
    {
        part_writer.index_file_stream = part_writer.data_part->volume->getDisk()->writeFile(
            part_writer.part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite
        );
        part_writer.index_stream = std::make_unique<HashingWriteBuffer>(*part_writer.index_file_stream);
    }

    part_writer.primary_index_initialized = true;
}

void MergeTreeDataPartIndexWriterSingleDisk::calculateAndSerializePrimaryIndex(const Block & primary_index_block, size_t rows)
{
    if (!part_writer.primary_index_initialized)
        throw Exception("Primary index is not initialized", ErrorCodes::LOGICAL_ERROR);

    size_t primary_columns_num = primary_index_block.columns();
    if (part_writer.index_columns.empty())
    {
        part_writer.index_types = primary_index_block.getDataTypes();
        part_writer.index_columns.resize(primary_columns_num);
        part_writer.last_index_row.resize(primary_columns_num);
        for (size_t i = 0; i < primary_columns_num; ++i)
            part_writer.index_columns[i] = primary_index_block.getByPosition(i).column->cloneEmpty();
    }

    /** While filling index (index_columns), disable memory tracker.
     * Because memory is allocated here (maybe in context of INSERT query),
     *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
     * And otherwise it will look like excessively growing memory consumption in context of query.
     *  (observed in long INSERT SELECTs)
     */
    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    /// Write index. The index contains Primary Key value for each `index_granularity` row.

    size_t current_row = part_writer.index_offset;
    size_t total_marks = part_writer.index_granularity.getMarksCount();

    while (part_writer.index_mark < total_marks && current_row < rows)
    {
        if (part_writer.storage.hasPrimaryKey())
        {
            for (size_t j = 0; j < primary_columns_num; ++j)
            {
                const auto & primary_column = primary_index_block.getByPosition(j);
                part_writer.index_columns[j]->insertFrom(*primary_column.column, current_row);
                primary_column.type->serializeBinary(*primary_column.column, current_row, *part_writer.index_stream);
            }
        }

        current_row += part_writer.index_granularity.getMarkRows(part_writer.index_mark++);
    }

    /// store last index row to write final mark at the end of column
    for (size_t j = 0; j < primary_columns_num; ++j)
    {
        const IColumn & primary_column = *primary_index_block.getByPosition(j).column.get();
        primary_column.get(rows - 1, part_writer.last_index_row[j]);
    }
}

void MergeTreeDataPartIndexWriterSingleDisk::finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & checksums)
{
    bool write_final_mark = (part_writer.with_final_mark && part_writer.data_written);
    if (write_final_mark && part_writer.compute_granularity)

        part_writer.index_granularity.appendMark(0);

    if (part_writer.index_stream)
    {
        if (write_final_mark)
        {
            for (size_t j = 0; j < part_writer.index_columns.size(); ++j)
            {
                part_writer.index_columns[j]->insert(part_writer.last_index_row[j]);
                part_writer.index_types[j]->serializeBinary(part_writer.last_index_row[j], *part_writer.index_stream);
            }

            part_writer.last_index_row.clear();
        }

        part_writer.index_stream->next();
        checksums.files["primary.idx"].file_size = part_writer.index_stream->count();
        checksums.files["primary.idx"].file_hash = part_writer.index_stream->getHash();
        part_writer.index_stream = nullptr;
    }
}


void MergeTreeDataPartIndexWriterSingleDisk::initSkipIndices()
{
    for (const auto & index : part_writer.skip_indices)
    {
        String stream_name = index->getFileName();
        part_writer.skip_indices_streams.emplace_back(
            std::make_unique<IMergeTreeDataPartWriter::Stream>(
                stream_name,
                part_writer.data_part->volume->getDisk(),
                part_writer.part_path + stream_name, INDEX_FILE_EXTENSION,
                part_writer.part_path + stream_name, part_writer.marks_file_extension,
                part_writer.default_codec, part_writer.settings.max_compress_block_size,
                0, part_writer.settings.aio_threshold));
        part_writer.skip_indices_aggregators.push_back(index->createIndexAggregator());
        part_writer.skip_index_filling.push_back(0);
    }

    part_writer.skip_indices_initialized = true;
}

void MergeTreeDataPartIndexWriterSingleDisk::calculateAndSerializeSkipIndices(
    const Block & skip_indexes_block, size_t rows)
{
    if (!part_writer.skip_indices_initialized)
        throw Exception("Skip indices are not initialized", ErrorCodes::LOGICAL_ERROR);

    size_t skip_index_current_data_mark = 0;

    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < part_writer.skip_indices.size(); ++i)
    {
        const auto index = part_writer.skip_indices[i];
        auto & stream = *part_writer.skip_indices_streams[i];
        size_t prev_pos = 0;
        skip_index_current_data_mark = part_writer.skip_index_data_mark;
        while (prev_pos < rows)
        {
            UInt64 limit = 0;
            if (prev_pos == 0 && part_writer.index_offset != 0)
            {
                limit = part_writer.index_offset;
            }
            else
            {
                limit = part_writer.index_granularity.getMarkRows(skip_index_current_data_mark);
                if (part_writer.skip_indices_aggregators[i]->empty())
                {
                    part_writer.skip_indices_aggregators[i] = index->createIndexAggregator();
                    part_writer.skip_index_filling[i] = 0;

                    if (stream.compressed.offset() >= part_writer.settings.min_compress_block_size)
                        stream.compressed.next();

                    writeIntBinary(stream.plain_hashing.count(), stream.marks);
                    writeIntBinary(stream.compressed.offset(), stream.marks);
                    /// Actually this numbers is redundant, but we have to store them
                    /// to be compatible with normal .mrk2 file format
                    if (part_writer.settings.can_use_adaptive_granularity)
                        writeIntBinary(1UL, stream.marks);
                }
                /// this mark is aggregated, go to the next one
                skip_index_current_data_mark++;
            }

            size_t pos = prev_pos;
            part_writer.skip_indices_aggregators[i]->update(skip_indexes_block, &pos, limit);

            if (pos == prev_pos + limit)
            {
                ++part_writer.skip_index_filling[i];

                /// write index if it is filled
                if (part_writer.skip_index_filling[i] == index->granularity)
                {
                    part_writer.skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
                    part_writer.skip_index_filling[i] = 0;
                }
            }
            prev_pos = pos;
        }
    }
    part_writer.skip_index_data_mark = skip_index_current_data_mark;
}

void MergeTreeDataPartIndexWriterSingleDisk::finishSkipIndicesSerialization(
    MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < part_writer.skip_indices.size(); ++i)
    {
        auto & stream = *part_writer.skip_indices_streams[i];
        if (!part_writer.skip_indices_aggregators[i]->empty())
            part_writer.skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
    }

    for (auto & stream : part_writer.skip_indices_streams)
    {
        stream->finalize();
        stream->addToChecksums(checksums);
    }

    part_writer.skip_indices_streams.clear();
    part_writer.skip_indices_aggregators.clear();
    part_writer.skip_index_filling.clear();
}


}

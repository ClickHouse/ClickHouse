#include "MergeTreeDataPartIndexWriterSingleDisk.h"

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_VOLUME_TYPE;
}

MergeTreeDataPartIndexWriterSingleDisk::MergeTreeDataPartIndexWriterSingleDisk(IMergeTreeDataPartWriter & part_writer_)
    : IMergeTreeDataPartIndexWriter(part_writer_)
{
    if (std::dynamic_pointer_cast<SingleDiskVolume>(part_writer_.data_part->volume) == nullptr &&
        std::dynamic_pointer_cast<VolumeJBOD>(part_writer_.data_part->volume) == nullptr)
    {
        throw Exception("Invalid volume type for single disk index writer: " +
                            volumeTypeToString(part_writer_.data_part->volume->getType()),
                        ErrorCodes::INVALID_VOLUME_TYPE);
    }
}

void MergeTreeDataPartIndexWriterSingleDisk::initPrimaryIndex()
{
    if (part_writer.storage.hasPrimaryKey())
    {
        index_file_stream = part_writer.data_part->volume->getDisk()->writeFile(
            part_writer.part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite
        );
        index_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);
    }

    primary_index_initialized = true;
}

void MergeTreeDataPartIndexWriterSingleDisk::calculateAndSerializePrimaryIndex(const Block & primary_index_block, size_t rows)
{
    if (!primary_index_initialized)
        throw Exception("Primary index is not initialized", ErrorCodes::LOGICAL_ERROR);

    size_t primary_columns_num = primary_index_block.columns();
    if (index_columns.empty())
    {
        index_types = primary_index_block.getDataTypes();
        index_columns.resize(primary_columns_num);
        last_index_row.resize(primary_columns_num);
        for (size_t i = 0; i < primary_columns_num; ++i)
            index_columns[i] = primary_index_block.getByPosition(i).column->cloneEmpty();
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

    while (index_mark < total_marks && current_row < rows)
    {
        if (part_writer.storage.hasPrimaryKey())
        {
            for (size_t j = 0; j < primary_columns_num; ++j)
            {
                const auto & primary_column = primary_index_block.getByPosition(j);
                index_columns[j]->insertFrom(*primary_column.column, current_row);
                primary_column.type->serializeBinary(*primary_column.column, current_row, *index_stream);
            }
        }

        current_row += part_writer.index_granularity.getMarkRows(index_mark++);
    }

    /// store last index row to write final mark at the end of column
    for (size_t j = 0; j < primary_columns_num; ++j)
    {
        const IColumn & primary_column = *primary_index_block.getByPosition(j).column.get();
        primary_column.get(rows - 1, last_index_row[j]);
    }
}

void MergeTreeDataPartIndexWriterSingleDisk::finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & checksums)
{
    bool write_final_mark = (part_writer.with_final_mark && part_writer.data_written);
    if (write_final_mark && part_writer.compute_granularity)

        part_writer.index_granularity.appendMark(0);

    if (index_stream)
    {
        if (write_final_mark)
        {
            for (size_t j = 0; j < index_columns.size(); ++j)
            {
                index_columns[j]->insert(last_index_row[j]);
                index_types[j]->serializeBinary(last_index_row[j], *index_stream);
            }

            last_index_row.clear();
        }

        index_stream->next();
        checksums.files["primary.idx"].file_size = index_stream->count();
        checksums.files["primary.idx"].file_hash = index_stream->getHash();
        index_stream = nullptr;
    }
}

}

#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Interpreters/getColumnFromBlock.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


MergeTreeReaderInMemory::MergeTreeReaderInMemory(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    DataPartInMemoryPtr data_part_,
    NamesAndTypesList columns_,
    const StorageSnapshotPtr & storage_snapshot_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_)
    : IMergeTreeReader(
        data_part_info_for_read_,
        columns_,
        storage_snapshot_,
        nullptr,
        nullptr,
        mark_ranges_,
        settings_,
        {})
    , part_in_memory(std::move(data_part_))
{
    for (const auto & column_to_read : columns_to_read)
    {
        /// If array of Nested column is missing in part,
        /// we have to read its offsets if they exist.
        if (typeid_cast<const DataTypeArray *>(column_to_read.type.get())
            && !tryGetColumnFromBlock(part_in_memory->block, column_to_read))
        {
            if (auto offsets_position = findColumnForOffsets(column_to_read))
            {
                positions_for_offsets[column_to_read.name] = *data_part_info_for_read->getColumnPosition(offsets_position->first);
                partially_read_columns.insert(column_to_read.name);
            }
        }
    }
}

size_t MergeTreeReaderInMemory::readRows(
    size_t from_mark, size_t /* current_task_last_mark */, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (!continue_reading)
        total_rows_read = 0;

    size_t total_marks = data_part_info_for_read->getIndexGranularity().getMarksCount();
    if (from_mark >= total_marks)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Mark {} is out of bound. Max mark: {}",
            toString(from_mark), toString(total_marks));

    size_t num_columns = res_columns.size();
    checkNumberOfColumns(num_columns);

    size_t part_rows = part_in_memory->block.rows();
    if (total_rows_read >= part_rows)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read data in MergeTreeReaderInMemory. "
            "Rows already read: {}. Rows in part: {}", total_rows_read, part_rows);

    size_t rows_to_read = std::min(max_rows_to_read, part_rows - total_rows_read);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column_to_read = columns_to_read[i];

        /// Copy offsets, if array of Nested column is missing in part.
        auto offsets_it = positions_for_offsets.find(column_to_read.name);
        if (offsets_it != positions_for_offsets.end() && !column_to_read.isSubcolumn())
        {
            const auto & source_offsets = assert_cast<const ColumnArray &>(
                *part_in_memory->block.getByPosition(offsets_it->second).column).getOffsets();

            if (res_columns[i] == nullptr)
                res_columns[i] = column_to_read.type->createColumn();

            auto mutable_column = res_columns[i]->assumeMutable();
            auto & res_offstes = assert_cast<ColumnArray &>(*mutable_column).getOffsets();
            size_t start_offset = total_rows_read ? source_offsets[total_rows_read - 1] : 0;
            for (size_t row = 0; row < rows_to_read; ++row)
                res_offstes.push_back(source_offsets[total_rows_read + row] - start_offset);

            res_columns[i] = std::move(mutable_column);
        }
        else if (part_in_memory->hasColumnFiles(column_to_read))
        {
            auto block_column = getColumnFromBlock(part_in_memory->block, column_to_read);
            if (rows_to_read == part_rows)
            {
                res_columns[i] = block_column;
            }
            else
            {
                if (res_columns[i] == nullptr)
                    res_columns[i] = column_to_read.type->createColumn();

                auto mutable_column = res_columns[i]->assumeMutable();
                mutable_column->insertRangeFrom(*block_column, total_rows_read, rows_to_read);
                res_columns[i] = std::move(mutable_column);
            }
        }
    }

    total_rows_read += rows_to_read;
    return rows_to_read;
}

}

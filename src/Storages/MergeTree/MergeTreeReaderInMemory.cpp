#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


MergeTreeReaderInMemory::MergeTreeReaderInMemory(
    DataPartInMemoryPtr data_part_,
    NamesAndTypesList columns_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_)
    : IMergeTreeReader(data_part_, std::move(columns_),
        nullptr, nullptr, std::move(mark_ranges_),
        std::move(settings_), {})
    , part_in_memory(std::move(data_part_))
{
}

size_t MergeTreeReaderInMemory::readRows(size_t from_mark, bool /* continue_reading */, size_t max_rows_to_read, Columns & res_columns)
{
    size_t total_marks = data_part->index_granularity.getMarksCount();
    if (from_mark >= total_marks)
        throw Exception("Mark " + toString(from_mark) + " is out of bound. Max mark: "
            + toString(total_marks), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    size_t num_columns = res_columns.size();
    checkNumberOfColumns(num_columns);

    size_t part_rows = part_in_memory->block.rows();
    if (total_rows_read >= part_rows)
        throw Exception("Cannot read data in MergeTreeReaderInMemory. Rows already read: "
            + toString(total_rows_read) + ". Rows in part: " + toString(part_rows), ErrorCodes::CANNOT_READ_ALL_DATA);

    auto column_it = columns.begin();
    size_t rows_read = 0;
    for (size_t i = 0; i < num_columns; ++i, ++column_it)
    {
        auto [name, type] = getColumnFromPart(*column_it);
        if (!part_in_memory->block.has(name))
            continue;

        const auto & block_column = part_in_memory->block.getByName(name).column;
        if (total_rows_read == 0 && part_rows <= max_rows_to_read)
        {
            res_columns[i] = block_column;
            rows_read = part_rows;
        }
        else
        {
            if (res_columns[i] == nullptr)
                res_columns[i] = type->createColumn();

            auto mutable_column = res_columns[i]->assumeMutable();
            rows_read = std::min(max_rows_to_read, part_rows - total_rows_read);
            mutable_column->insertRangeFrom(*block_column, total_rows_read, rows_read);
            res_columns[i] = std::move(mutable_column);
        }
    }

    total_rows_read += rows_read;
    return rows_read;
}

}

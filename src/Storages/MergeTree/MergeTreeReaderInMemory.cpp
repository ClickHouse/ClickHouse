#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}


MergeTreeReaderInMemory::MergeTreeReaderInMemory(
    DataPartInMemoryPtr data_part_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_)
    : IMergeTreeReader(data_part_, std::move(columns_), metadata_snapshot_,
        nullptr, nullptr, std::move(mark_ranges_),
        std::move(settings_), {})
    , part_in_memory(std::move(data_part_))
{
    for (const auto & name_and_type : columns)
    {
        auto [name, type] = getColumnFromPart(name_and_type);

        /// If array of Nested column is missing in part,
        /// we have to read its offsets if they exist.
        if (!part_in_memory->block.has(name) && typeid_cast<const DataTypeArray *>(type.get()))
            if (auto offset_position = findColumnForOffsets(name))
                positions_for_offsets[name] = *offset_position;
    }
}

static ColumnPtr getColumnFromBlock(const Block & block, const NameAndTypePair & name_and_type)
{
    auto storage_name = name_and_type.getNameInStorage();
    if (!block.has(storage_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found column '{}' in block", storage_name);

    const auto & column = block.getByName(storage_name).column;
    if (name_and_type.isSubcolumn())
        return name_and_type.getTypeInStorage()->getSubcolumn(name_and_type.getSubcolumnName(), *column);

    return column;
}

size_t MergeTreeReaderInMemory::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (!continue_reading)
        total_rows_read = 0;

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

    size_t rows_to_read = std::min(max_rows_to_read, part_rows - total_rows_read);
    auto column_it = columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++column_it)
    {
        auto name_type = getColumnFromPart(*column_it);

        /// Copy offsets, if array of Nested column is missing in part.
        auto offsets_it = positions_for_offsets.find(name_type.name);
        if (offsets_it != positions_for_offsets.end() && !name_type.isSubcolumn())
        {
            const auto & source_offsets = assert_cast<const ColumnArray &>(
                *part_in_memory->block.getByPosition(offsets_it->second).column).getOffsets();

            if (res_columns[i] == nullptr)
                res_columns[i] = name_type.type->createColumn();

            auto mutable_column = res_columns[i]->assumeMutable();
            auto & res_offstes = assert_cast<ColumnArray &>(*mutable_column).getOffsets();
            size_t start_offset = total_rows_read ? source_offsets[total_rows_read - 1] : 0;
            for (size_t row = 0; row < rows_to_read; ++row)
                res_offstes.push_back(source_offsets[total_rows_read + row] - start_offset);

            res_columns[i] = std::move(mutable_column);
        }
        else if (part_in_memory->hasColumnFiles(name_type))
        {
            auto block_column = getColumnFromBlock(part_in_memory->block, name_type);
            if (rows_to_read == part_rows)
            {
                res_columns[i] = block_column;
            }
            else
            {
                if (res_columns[i] == nullptr)
                    res_columns[i] = name_type.type->createColumn();

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

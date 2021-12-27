#include <Processors/Chunk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSITION_OUT_OF_BOUND;
}

Chunk::Chunk(DB::Columns columns_, UInt64 num_rows_) : columns(std::move(columns_)), num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(Columns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_)
    : columns(std::move(columns_)), num_rows(num_rows_), chunk_info(std::move(chunk_info_))
{
    checkNumRowsIsConsistent();
}

static Columns unmuteColumns(MutableColumns && mut_columns)
{
    Columns columns;
    columns.reserve(mut_columns.size());
    for (auto & col : mut_columns)
        columns.emplace_back(std::move(col));

    return columns;
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_), chunk_info(std::move(chunk_info_))
{
    checkNumRowsIsConsistent();
}

Chunk Chunk::clone() const
{
    return Chunk(getColumns(), getNumRows(), chunk_info);
}

void Chunk::setColumns(Columns columns_, UInt64 num_rows_)
{
    columns = std::move(columns_);
    num_rows = num_rows_;
    checkNumRowsIsConsistent();
}

void Chunk::setColumns(MutableColumns columns_, UInt64 num_rows_)
{
    columns = unmuteColumns(std::move(columns_));
    num_rows = num_rows_;
    checkNumRowsIsConsistent();
}

void Chunk::checkNumRowsIsConsistent()
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto & column = columns[i];
        if (column->size() != num_rows)
            throw Exception("Invalid number of rows in Chunk column " + column->getName()+ " position " + toString(i) + ": expected " +
                            toString(num_rows) + ", got " + toString(column->size()), ErrorCodes::LOGICAL_ERROR);
    }
}

MutableColumns Chunk::mutateColumns()
{
    size_t num_columns = columns.size();
    MutableColumns mut_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mut_columns[i] = IColumn::mutate(std::move(columns[i]));

    columns.clear();
    num_rows = 0;

    return mut_columns;
}

MutableColumns Chunk::cloneEmptyColumns() const
{
    size_t num_columns = columns.size();
    MutableColumns mut_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mut_columns[i] = columns[i]->cloneEmpty();
    return mut_columns;
}

Columns Chunk::detachColumns()
{
    num_rows = 0;
    return std::move(columns);
}

void Chunk::addColumn(ColumnPtr column)
{
    if (column->size() != num_rows)
        throw Exception("Invalid number of rows in Chunk column " + column->getName()+ ": expected " +
                        toString(num_rows) + ", got " + toString(column->size()), ErrorCodes::LOGICAL_ERROR);

    columns.emplace_back(std::move(column));
}

void Chunk::erase(size_t position)
{
    if (columns.empty())
        throw Exception("Chunk is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= columns.size())
        throw Exception("Position " + toString(position) + " out of bound in Chunk::erase(), max position = "
                        + toString(columns.size() - 1), ErrorCodes::POSITION_OUT_OF_BOUND);

    columns.erase(columns.begin() + position);
}

UInt64 Chunk::bytes() const
{
    UInt64 res = 0;
    for (const auto & column : columns)
        res += column->byteSize();

    return res;
}

UInt64 Chunk::allocatedBytes() const
{
    UInt64 res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();

    return res;
}

std::string Chunk::dumpStructure() const
{
    WriteBufferFromOwnString out;
    for (const auto & column : columns)
        out << ' ' << column->dumpStructure();

    return out.str();
}


void ChunkMissingValues::setBit(size_t column_idx, size_t row_idx)
{
    RowsBitMask & mask = rows_mask_by_column_id[column_idx];
    mask.resize(row_idx + 1);
    mask[row_idx] = true;
}

const ChunkMissingValues::RowsBitMask & ChunkMissingValues::getDefaultsBitmask(size_t column_idx) const
{
    static RowsBitMask none;
    auto it = rows_mask_by_column_id.find(column_idx);
    if (it != rows_mask_by_column_id.end())
        return it->second;
    return none;
}

}

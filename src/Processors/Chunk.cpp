#include <Processors/Chunk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSITION_OUT_OF_BOUND;
}

Chunk::Chunk(Chunk && other) noexcept
    : columns{std::move(other.columns)}
    , num_rows{std::move(other.num_rows)}
{
    for (size_t i = 0; i < kChunkInfoCount; ++i)
        chunk_infos[i] = std::move(other.chunk_infos[i]);
}

Chunk::Chunk(DB::Columns columns_, UInt64 num_rows_)
    : columns(std::move(columns_))
    , num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(Columns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_)
    : columns(std::move(columns_))
    , num_rows(num_rows_)
{
    chunk_infos[0] = std::move(chunk_info_);
    checkNumRowsIsConsistent();
}

static Columns unmuteColumns(MutableColumns && mutable_columns)
{
    Columns columns;
    columns.reserve(mutable_columns.size());
    for (auto & col : mutable_columns)
        columns.emplace_back(std::move(col));

    return columns;
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_)
    : columns(unmuteColumns(std::move(columns_)))
    , num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_)
    : columns(unmuteColumns(std::move(columns_)))
    , num_rows(num_rows_)
{
    chunk_infos[0] = std::move(chunk_info_);
    checkNumRowsIsConsistent();
}

Chunk & Chunk::operator=(Chunk && other) noexcept
{
    columns = std::move(other.columns);

    num_rows = other.num_rows;
    other.num_rows = 0;

    for (size_t i = 0; i < kChunkInfoCount; ++i)
        chunk_infos[i] = std::move(other.chunk_infos[i]);

    return *this;
}

Chunk Chunk::clone() const
{
    Chunk clonned_chunk(getColumns(), getNumRows());

    for (size_t i = 0; i < kChunkInfoCount; ++i)
        clonned_chunk.chunk_infos[i] = chunk_infos[i];

    return clonned_chunk;
}

void Chunk::swap(Chunk & other) noexcept
{
    columns.swap(other.columns);
    std::swap(num_rows, other.num_rows);

    for (size_t i = 0; i < kChunkInfoCount; ++i)
        chunk_infos[i].swap(other.chunk_infos[i]);
}

void Chunk::clear()
{
    num_rows = 0;
    columns.clear();

    for (auto & chunk_info : chunk_infos)
        chunk_info = nullptr;
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of rows in Chunk column {}: expected {}, got {}",
                            column->getName() + " position " + toString(i), toString(num_rows), toString(column->size()));
    }
}

MutableColumns Chunk::mutateColumns()
{
    size_t num_columns = columns.size();
    MutableColumns mutable_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mutable_columns[i] = IColumn::mutate(std::move(columns[i]));

    columns.clear();
    num_rows = 0;

    return mutable_columns;
}

MutableColumns Chunk::cloneEmptyColumns() const
{
    size_t num_columns = columns.size();
    MutableColumns mutable_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mutable_columns[i] = columns[i]->cloneEmpty();
    return mutable_columns;
}

Columns Chunk::detachColumns()
{
    num_rows = 0;
    return std::move(columns);
}

bool Chunk::hasChunkInfo(UInt8 index) const
{
    chassert(index < kChunkInfoCount);
    return chunk_infos[index] != nullptr;
}

const ChunkInfoPtr & Chunk::getChunkInfo(UInt8 index) const
{
    chassert(index < kChunkInfoCount);
    return chunk_infos[index];
}

void Chunk::setChunkInfo(ChunkInfoPtr chunk_info_, UInt8 index)
{
    chassert(index < kChunkInfoCount);
    chunk_infos[index] = std::move(chunk_info_);
}

void Chunk::addColumn(ColumnPtr column)
{
    if (empty())
        num_rows = column->size();
    else if (column->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of rows in Chunk column {}, got {}",
                        column->getName()+ ": expected " + toString(num_rows), toString(column->size()));

    columns.emplace_back(std::move(column));
}

void Chunk::addColumn(size_t position, ColumnPtr column)
{
    if (position >= columns.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND,
                        "Position {} out of bound in Chunk::addColumn(), max position = {}",
                        position, columns.size() - 1);
    if (empty())
        num_rows = column->size();
    else if (column->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Invalid number of rows in Chunk column {}: expected {}, got {}",
                        column->getName(), num_rows, column->size());

    columns.emplace(columns.begin() + position, std::move(column));
}

void Chunk::erase(size_t position)
{
    if (columns.empty())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Chunk is empty");

    if (position >= columns.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position {} out of bound in Chunk::erase(), max position = {}",
                        toString(position), toString(columns.size() - 1));

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

void Chunk::append(const Chunk & chunk)
{
    append(chunk, 0, chunk.getNumRows());
}

void Chunk::append(const Chunk & chunk, size_t from, size_t length)
{
    MutableColumns mutable_columns = mutateColumns();
    for (size_t position = 0; position < mutable_columns.size(); ++position)
    {
        auto column = chunk.getColumns()[position];
        mutable_columns[position]->insertRangeFrom(*column, from, length);
    }
    size_t rows = mutable_columns[0]->size();
    setColumns(std::move(mutable_columns), rows);
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

void convertToFullIfConst(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();
    chunk.setColumns(std::move(columns), num_rows);
}

void convertToFullIfSparse(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = recursiveRemoveSparse(column);
    chunk.setColumns(std::move(columns), num_rows);
}

Chunk cloneConstWithDefault(const Chunk & chunk, size_t num_rows)
{
    auto columns = chunk.cloneEmptyColumns();
    for (auto & column : columns)
    {
        column->insertDefault();
        column = ColumnConst::create(std::move(column), num_rows);
    }

    return Chunk(std::move(columns), num_rows);
}

}

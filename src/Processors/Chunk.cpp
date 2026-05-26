#include <Processors/Chunk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <DataTypes/DataTypeLowCardinality.h>

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

static Columns unmuteColumns(MutableColumns && mutable_columns)
{
    Columns columns;
    columns.reserve(mutable_columns.size());
    for (auto & col : mutable_columns)
        columns.emplace_back(std::move(col));

    return columns;
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk Chunk::clone() const
{
    auto tmp = Chunk(getColumns(), getNumRows());
    tmp.setChunkInfos(chunk_infos.clone());
    return tmp;
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of rows in Chunk {} column {} at position {}: expected {}, got {}",
                dumpStructure(), column->getName(), i, num_rows, column->size());
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

void Chunk::addColumn(ColumnPtr column)
{
    if (empty())
        num_rows = column->size();
    else if (column->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of rows in Chunk {} column {}: expected {}, got {}",
            dumpStructure(), column->getName(), num_rows, column->size());

    columns.emplace_back(std::move(column));
}

void Chunk::addColumn(size_t position, ColumnPtr column)
{
    if (position >= columns.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND,
                        "Position {} out of bound in Chunk::addColumn(), max position = {}",
                        position, !columns.empty() ? columns.size() - 1 : 0);
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
                        toString(position), toString(!columns.empty() ? columns.size() - 1 : 0));

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
    bool first = true;
    for (const auto & column : columns)
    {
        if (!first)
            out << ", ";
        out << column->dumpStructure();
        first = false;
    }

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

void removeSpecialColumnRepresentations(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = removeSpecialRepresentations(column);
    chunk.setColumns(std::move(columns), num_rows);
}

void materializeChunk(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = removeSpecialRepresentations(column->convertToFullColumnIfConst());
    chunk.setColumns(std::move(columns), num_rows);
}

void compactReplicatedColumns(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    /// Step 1: Materialize columns where replication provides no benefit.
    for (auto & col : columns)
        col = convertToFullColumnIfReplicationNotUseful(col);

    /// Step 2: Compact remaining ColumnReplicated columns.
    /// First map shared indexes to the corresponding nested columns and their positions in the original columns.
    struct IndexWithNestedColumns
    {
        ColumnPtr shared_index;
        Columns nested_columns;
        std::vector<size_t> positions;
    };

    std::unordered_map<const IColumn *, IndexWithNestedColumns> index_to_nested_cols_map;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!columns[i]->isReplicated())
            continue;

        const auto & rep = typeid_cast<const ColumnReplicated &>(*columns[i]);
        const auto & src_index = rep.getIndexesColumn();
        const auto * src_index_ptr = src_index.get();
        if (auto it = index_to_nested_cols_map.find(src_index_ptr); it != index_to_nested_cols_map.end())
        {
            it->second.nested_columns.push_back(rep.getNestedColumn());
            it->second.positions.push_back(i);
        }
        else
            index_to_nested_cols_map[src_index_ptr] = {src_index, {rep.getNestedColumn()}, {i}};
    }

    /// Second compact the indexes with their nested columns and assign them back to the original columns.
    for (const auto & [_, index_to_nested_cols] : index_to_nested_cols_map)
    {
        const auto & [shared_index, nested_columns, positions] = index_to_nested_cols;

        ColumnIndex column_index(shared_index);
        auto result = column_index.buildCompactIndexedColumns(nested_columns);
        if (result.compact_indexes.get() != shared_index.get())
        {
            for (size_t j = 0; j < positions.size(); ++j)
                columns[positions[j]] = ColumnReplicated::create(
                    result.compact_indexed_columns[j], result.compact_indexes);
        }
    }

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

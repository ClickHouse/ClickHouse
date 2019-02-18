#include <Processors/Port.h>
#include <IO/WriteHelpers.h>

namespace DB
{

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
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_), chunk_info(std::move(chunk_info_))
{
}

Chunk::Chunk(Chunk && other) noexcept
    : columns(std::move(other.columns))
    , num_rows(other.num_rows)
    , chunk_info(std::move(other.chunk_info))
{
    other.num_rows = 0;
}

Chunk & Chunk::operator=(Chunk && other) noexcept
{
    columns = std::move(other.columns);
    chunk_info = std::move(other.chunk_info);
    num_rows = other.num_rows;
    other.num_rows = 0;
    return *this;
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
    for (auto & column : columns)
        if (column->size() != num_rows)
            throw Exception("Invalid number of rows in Chunk column " + column->getName()+ ": expected " +
                            toString(num_rows) + ", got " + toString(column->size()), ErrorCodes::LOGICAL_ERROR);
}

MutableColumns Chunk::mutateColumns()
{
    size_t num_columns = columns.size();
    MutableColumns mut_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mut_columns[i] = (*std::move(columns[i])).mutate();

    columns.clear();
    num_rows = 0;

    return mut_columns;
}

Columns Chunk::detachColumns()
{
    num_rows = 0;
    return std::move(columns);
}

void Chunk::clear()
{
    num_rows = 0;
    columns.clear();
    chunk_info.reset();
}

void connect(OutputPort & output, InputPort & input)
{
    if (input.state || output.state)
        throw Exception("Port is already connected", ErrorCodes::LOGICAL_ERROR);

    input.output_port = &output;
    output.input_port = &input;
    input.state = std::make_shared<Port::State>();
    output.state = input.state;
}

}

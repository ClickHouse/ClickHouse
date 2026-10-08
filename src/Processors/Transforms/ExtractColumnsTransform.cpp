#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Interpreters/getColumnFromBlock.h>

namespace DB
{

ExtractColumnsTransform::ExtractColumnsTransform(SharedHeader header_, const NamesAndTypesList & requested_columns_)
    : ISimpleTransform(header_, std::make_shared<const Block>(transformHeader(*header_, requested_columns_)), false), requested_columns(requested_columns_)
{

}

Block ExtractColumnsTransform::transformHeader(Block header, const NamesAndTypesList & requested_columns_)
{
    ColumnsWithTypeAndName columns;
    columns.reserve(requested_columns_.size());
    for (const auto & required_column : requested_columns_)
        columns.emplace_back(getColumnFromBlock(header, required_column), required_column.type, required_column.name);

    return Block(std::move(columns));
}

void ExtractColumnsTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    Columns columns;
    columns.reserve(requested_columns.size());
    for (const auto & required_column : requested_columns)
        columns.emplace_back(getColumnFromBlock(block, required_column));

    chunk.setColumns(std::move(columns), num_rows);
}

}

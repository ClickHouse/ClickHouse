#include <Processors/Transforms/ColumnLazyTransform.h>
#include <Columns/ColumnLazy.h>

namespace DB
{

Block ColumnLazyTransform::transformHeader(Block header)
{
    for (auto & it : header)
    {
        if (isColumnLazy(*(it.column)))
        {
            it.column = it.type->createColumn();
        }
    }
    return header;
}

ColumnLazyTransform::ColumnLazyTransform(const Block & header_)
    : ISimpleTransform(
    header_,
    transformHeader(header_),
    true)
{}

void ColumnLazyTransform::transform(Chunk & chunk)
{
    const size_t rows_size = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    Block block = getInputPort().getHeader().cloneWithColumns(columns);

    ColumnsWithTypeAndName res_columns;
    for (const auto & column : columns)
    {
        if (isColumnLazy(*column))
        {
            const auto * column_lazy = typeid_cast<const ColumnLazy *>(column.get());
            column_lazy->transform(res_columns);
        }
    }

    for (auto & column_with_type_and_name : res_columns)
    {
        const auto & alias_name = column_with_type_and_name.name;
        block.getByName(alias_name).column = std::move(column_with_type_and_name.column);
    }

    chunk.setColumns(block.getColumns(), rows_size);
}

}

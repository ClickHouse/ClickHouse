#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Adds a materialized const column to the chunk with a specified value.
class AddingConstColumnTransform : public ISimpleTransform
{
public:
    AddingConstColumnTransform(const Block & header, ColumnWithTypeAndName column_)
        : ISimpleTransform(header, transformHeader(header, column_), false)
        , column(std::move(column_))
    {
        if (!column.column || !isColumnConst(*column.column) || !column.column->empty())
            throw Exception("AddingConstColumnTransform expected empty const column", ErrorCodes::LOGICAL_ERROR);
    }

    String getName() const override { return "AddingConstColumnTransform"; }

    static Block transformHeader(Block header, ColumnWithTypeAndName & column_)
    {
        header.insert(column_);
        return header;
    }

protected:
    void transform(Chunk & chunk) override
    {
        auto num_rows = chunk.getNumRows();
        chunk.addColumn(column.column->cloneResized(num_rows)->convertToFullColumnIfConst());
    }

private:
    ColumnWithTypeAndName column;
};

}

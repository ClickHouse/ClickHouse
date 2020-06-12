#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Adds a materialized const column to the chunk with a specified value.
template <typename T>
class AddingConstColumnTransform : public ISimpleTransform
{
public:
    AddingConstColumnTransform(const Block & header, DataTypePtr data_type_, T value_, const String & column_name_)
        : ISimpleTransform(header, addColumn(header, data_type_, column_name_), false)
        , data_type(std::move(data_type_)), value(value_) {}

    String getName() const override { return "AddingConstColumnTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        auto num_rows = chunk.getNumRows();
        chunk.addColumn(data_type->createColumnConst(num_rows, value)->convertToFullColumnIfConst());
    }

private:
    static Block addColumn(Block header, const DataTypePtr & data_type, const String & column_name)
    {
        header.insert({data_type->createColumn(), data_type, column_name});
        return header;
    }

    DataTypePtr data_type;
    T value;
};

}

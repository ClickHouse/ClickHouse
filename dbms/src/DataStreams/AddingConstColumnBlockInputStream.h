#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Core/ColumnWithTypeAndName.h>

namespace DB
{

/** Adds a materialized const column to the block with a specified value.
  */
template <typename T>
class AddingConstColumnBlockInputStream : public IProfilingBlockInputStream
{
public:
    AddingConstColumnBlockInputStream(
        BlockInputStreamPtr input_,
        DataTypePtr data_type_,
        T value_,
        String column_name_)
        : data_type(data_type_), value(value_), column_name(column_name_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "AddingConstColumn"; }

    Block getHeader() const override
    {
        Block res = children.back()->getHeader();
        res.insert({data_type->createColumn(), data_type, column_name});
        return res;
    }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return res;

        res.insert({data_type->createColumnConst(res.rows(), value)->convertToFullColumnIfConst(), data_type, column_name});
        return res;
    }

private:
    DataTypePtr data_type;
    T value;
    String column_name;
};

}

#pragma once

#include <unordered_set>
#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ReplaceWindowColumnBlockInputStream : public IBlockInputStream
{
public:
    ReplaceWindowColumnBlockInputStream(
        BlockInputStreamPtr input_,
        String window_column_name_,
        UInt32 window_start_,
        UInt32 window_end_)
        : window_column_name(window_column_name_)
    {
        children.push_back(input_);
        window_column_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeDateTime>()});
        window_value.emplace_back(window_start_);
        window_value.emplace_back(window_end_);

        position = -1;
        Block header = children.back()->getHeader();
        for (auto & column_ : header.getColumnsWithTypeAndName())
        {
            if (startsWith(column_.name, "HOP_SLICE"))
            {
                position = header.getPositionByName(column_.name);
                hop_slice_name = column_.name;
                break;
            }
        }
        if (position < 0)
            throw Exception("Not found column HOP_SLICE", ErrorCodes::LOGICAL_ERROR);
    }

    String getName() const override { return "ReplaceWindowColumn"; }

    Block getHeader() const override
    {
        Block res = children.back()->getHeader();
        res.erase(position);
        res.insert(position, {window_column_type->createColumn(), window_column_type, window_column_name});
        return res;
    }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return res;

        res.erase(position);
        res.insert(position, {window_column_type->createColumnConst(res.rows(), window_value)->convertToFullColumnIfConst(), window_column_type, window_column_name});
        return res;
    }

private:
    String window_column_name;
    String hop_slice_name;
    DataTypePtr window_column_type;
    int position;
    Tuple window_value;
};

}

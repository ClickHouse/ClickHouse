#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class ConstantValue
{
public:
    ConstantValue(ColumnPtr column_, DataTypePtr data_type_)
        : column(std::move(column_))
        , data_type(std::move(data_type_))
    {}

    ConstantValue(const Field & field_, DataTypePtr data_type_)
        : column(data_type_->createColumnConst(1, field_))
        , data_type(std::move(data_type_))
    {}

    const ColumnPtr & getColumn() const
    {
        return column;
    }

    const DataTypePtr & getType() const
    {
        return data_type;
    }
private:
    ColumnPtr column;
    DataTypePtr data_type;
};

}

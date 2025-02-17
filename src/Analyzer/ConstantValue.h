#pragma once

#include <Common/FieldVisitorToString.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/FieldToDataType.h>

namespace DB
{

class ConstantValue
{
public:
    ConstantValue(ColumnPtr column_, DataTypePtr data_type_)
        : column(wrapToColumnConst(column_))
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

    std::pair<String, DataTypePtr> getValueNameAndType() const
    {
        return column->getValueNameAndType(0);
    }

private:

    static ColumnPtr wrapToColumnConst(ColumnPtr column_)
    {
        if (!isColumnConst(*column_))
            return ColumnConst::create(column_, 1);
        return column_;
    }

    ColumnPtr column;
    DataTypePtr data_type;
};

}

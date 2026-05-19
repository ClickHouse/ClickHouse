#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class ConstantValue
{
public:
    ConstantValue(const ColumnPtr & column_, DataTypePtr data_type_)
        : column(wrapToColumnConst(column_))
        , data_type(std::move(data_type_))
    {}

    ConstantValue(const Field & field_, DataTypePtr data_type_)
        : column(data_type_->createColumnConst(1, field_))
        , data_type(std::move(data_type_))
    {}

    const ColumnConstPtr & getColumn() const
    {
        return column;
    }

    const DataTypePtr & getType() const
    {
        return data_type;
    }

    String getValueName(const IColumn::Options & options) const
    {
        return column->getValueName(0, options);
    }

private:

    static ColumnConstPtr wrapToColumnConst(const ColumnPtr & column_)
    {
        if (const auto * column_const = typeid_cast<const ColumnConst *>(column_.get()))
            return column_const->getPtr();
        return ColumnConst::create(column_, 1);
    }

    ColumnConstPtr column;
    DataTypePtr data_type;
};

}

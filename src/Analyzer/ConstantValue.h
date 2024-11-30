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
        , field_cache(applyVisitor(FieldVisitorToString(), field_), field_.getType(), applyVisitor(FieldToDataType(), field_))
    {}

    const ColumnPtr & getColumn() const
    {
        return column;
    }

    const DataTypePtr & getType() const
    {
        return data_type;
    }

    const std::tuple<String, Field::Types::Which, DataTypePtr> & getFieldAttributes() const &
    {
        if (std::get<String>(field_cache).empty())
        {
            Field field;
            column->get(0, field);
            field_cache = {applyVisitor(FieldVisitorToString(), field), field.getType(), applyVisitor(FieldToDataType(), field)};
        }

        return field_cache;
    }

    std::tuple<String, Field::Types::Which, DataTypePtr> getFieldAttributes() const &&
    {
        return getFieldAttributes();
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
    mutable std::tuple<String, Field::Types::Which, DataTypePtr> field_cache;
};

}

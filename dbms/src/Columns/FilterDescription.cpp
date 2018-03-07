#include <Columns/FilterDescription.h>

#include <Common/typeid_cast.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


ConstantFilterDescription::ConstantFilterDescription(const IColumn & column)
{
    if (column.onlyNull())
    {
        always_false = true;
        return;
    }

    if (column.isColumnConst())
    {
        const ColumnConst & column_const = static_cast<const ColumnConst &>(column);
        const IColumn & column_nested = column_const.getDataColumn();

        if (!typeid_cast<const ColumnUInt8 *>(&column_nested))
        {
            const ColumnNullable * column_nested_nullable = typeid_cast<const ColumnNullable *>(&column_nested);
            if (!column_nested_nullable || !typeid_cast<const ColumnUInt8 *>(&column_nested_nullable->getNestedColumn()))
            {
                throw Exception("Illegal type " + column_nested.getName() + " of column for constant filter. Must be UInt8 or Nullable(UInt8).",
                                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
            }
        }

        if (column_const.getValue<UInt64>())
            always_true = true;
        else
            always_false = true;
        return;
    }
}


FilterDescription::FilterDescription(const IColumn & column)
{
    if (const ColumnUInt8 * concrete_column = typeid_cast<const ColumnUInt8 *>(&column))
    {
        data = &concrete_column->getData();
        return;
    }

    if (const ColumnNullable * nullable_column = typeid_cast<const ColumnNullable *>(&column))
    {
        MutableColumnPtr mutable_holder = nullable_column->getNestedColumn().mutate();

        ColumnUInt8 * concrete_column = typeid_cast<ColumnUInt8 *>(mutable_holder.get());
        if (!concrete_column)
            throw Exception("Illegal type " + column.getName() + " of column for filter. Must be UInt8 or Nullable(UInt8).",
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

        const NullMap & null_map = nullable_column->getNullMapData();
        IColumn::Filter & res =  concrete_column->getData();

        size_t size = res.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = res[i] && !null_map[i];

        data = &res;
        data_holder = std::move(mutable_holder);
        return;
    }

    throw Exception("Illegal type " + column.getName() + " of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of them.",
        ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
}

}

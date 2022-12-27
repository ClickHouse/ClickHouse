#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/** arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  */
struct ArrayFilterImpl
{
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    /// If there are several arrays, the first one is passed here.
    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
                return array.clone();
            else
                return ColumnArray::create(
                    array.getDataPtr()->cloneEmpty(),
                    ColumnArray::ColumnOffsets::create(array.size(), 0));
        }

        const IColumn::Filter & filter = column_filter->getData();
        ColumnPtr filtered = array.getData().filter(filter, -1);

        const IColumn::Offsets & in_offsets = array.getOffsets();
        auto column_offsets = ColumnArray::ColumnOffsets::create(in_offsets.size());
        IColumn::Offsets & out_offsets = column_offsets->getData();

        size_t in_pos = 0;
        size_t out_pos = 0;
        for (size_t i = 0; i < in_offsets.size(); ++i)
        {
            for (; in_pos < in_offsets[i]; ++in_pos)
            {
                if (filter[in_pos])
                    ++out_pos;
            }
            out_offsets[i] = out_pos;
        }

        return ColumnArray::create(filtered, std::move(column_offsets));
    }
};

struct NameArrayFilter { static constexpr auto name = "arrayFilter"; };
using FunctionArrayFilter = FunctionArrayMapped<ArrayFilterImpl, NameArrayFilter>;

void registerFunctionArrayFilter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFilter>();
}

}



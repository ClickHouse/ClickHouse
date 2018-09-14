#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

/** arrayCount(x1,...,xn -> expression, array1,...,arrayn) - for how many elements of the array the expression is true.
  * An overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */
struct ArrayCountImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnUInt32::create(offsets.size());
                ColumnUInt32::Container & out_counts = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_counts[i] = offsets[i] - pos;
                    pos = offsets[i];
                }

                return out_column;
            }
            else
                return DataTypeUInt32().createColumnConst(array.size(), UInt64(0));
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnUInt32::create(offsets.size());
        ColumnUInt32::Container & out_counts = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t count = 0;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                    ++count;
            }
            out_counts[i] = count;
        }

        return out_column;
    }
};

struct NameArrayCount { static constexpr auto name = "arrayCount"; };
using FunctionArrayCount = FunctionArrayMapped<ArrayCountImpl, NameArrayCount>;

void registerFunctionArrayCount(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayCount>();
}

}



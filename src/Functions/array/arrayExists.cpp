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

/** arrayExists(x1,...,xn -> expression, array1,...,arrayn) - is the expression true for at least one array element.
  * An overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */
struct ArrayExistsImpl
{
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnUInt8::create(offsets.size());
                ColumnUInt8::Container & out_exists = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_exists[i] = offsets[i] - pos > 0;
                    pos = offsets[i];
                }

                return out_column;
            }
            else
                return DataTypeUInt8().createColumnConst(array.size(), 0u);
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnUInt8::create(offsets.size());
        ColumnUInt8::Container & out_exists = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            UInt8 exists = 0;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                {
                    exists = 1;
                    pos = offsets[i];
                    break;
                }
            }
            out_exists[i] = exists;
        }

        return out_column;
    }
};

struct NameArrayExists { static constexpr auto name = "arrayExists"; };
using FunctionArrayExists = FunctionArrayMapped<ArrayExistsImpl, NameArrayExists>;

REGISTER_FUNCTION(ArrayExists)
{
    factory.registerFunction<FunctionArrayExists>();
}

}



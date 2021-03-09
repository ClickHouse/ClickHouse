#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/** arrayCumSumNonNegative() - returns an array with cumulative sums of the original. (If value < 0 -> 0).
  */
struct ArrayCumSumNonNegativeImpl
{
    static bool useDefaultImplementationForConstants() { return true; }
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        WhichDataType which(expression_return);

        if (which.isNativeUInt())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());

        if (which.isNativeInt())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());

        if (which.isFloat())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

        if (which.isDecimal())
        {
            UInt32 scale = getDecimalScale(*expression_return);
            DataTypePtr nested = std::make_shared<DataTypeDecimal<Decimal128>>(DecimalUtils::maxPrecision<Decimal128>(), scale);
            return std::make_shared<DataTypeArray>(nested);
        }

        throw Exception("arrayCumSumNonNegativeImpl cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    template <typename Element, typename Result>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
    {
        using ColVecType = std::conditional_t<IsDecimalNumber<Element>, ColumnDecimal<Element>, ColumnVector<Element>>;
        using ColVecResult = std::conditional_t<IsDecimalNumber<Result>, ColumnDecimal<Result>, ColumnVector<Result>>;

        const ColVecType * column = checkAndGetColumn<ColVecType>(&*mapped);

        if (!column)
            return false;

        const IColumn::Offsets & offsets = array.getOffsets();
        const typename ColVecType::Container & data = column->getData();

        typename ColVecResult::MutablePtr res_nested;
        if constexpr (IsDecimalNumber<Element>)
            res_nested = ColVecResult::create(0, data.getScale());
        else
            res_nested = ColVecResult::create();

        typename ColVecResult::Container & res_values = res_nested->getData();
        res_values.resize(data.size());

        size_t pos = 0;
        Result accum_sum = 0;
        for (auto offset : offsets)
        {
            // skip empty arrays
            if (pos < offset)
            {
                accum_sum = data[pos] > 0 ? data[pos] : Element(0); // NOLINT
                res_values[pos] = accum_sum;
                for (++pos; pos < offset; ++pos)
                {
                    accum_sum = accum_sum + data[pos];
                    if (accum_sum < 0)
                        accum_sum = 0;

                    res_values[pos] = accum_sum;
                }
            }
        }
        res_ptr = ColumnArray::create(std::move(res_nested), array.getOffsetsPtr());
        return true;

    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        ColumnPtr res;

        if (executeType< UInt8 , UInt64>(mapped, array, res) ||
            executeType< UInt16, UInt64>(mapped, array, res) ||
            executeType< UInt32, UInt64>(mapped, array, res) ||
            executeType< UInt64, UInt64>(mapped, array, res) ||
            executeType<  Int8 ,  Int64>(mapped, array, res) ||
            executeType<  Int16,  Int64>(mapped, array, res) ||
            executeType<  Int32,  Int64>(mapped, array, res) ||
            executeType<  Int64,  Int64>(mapped, array, res) ||
            executeType<Float32,Float64>(mapped, array, res) ||
            executeType<Float64,Float64>(mapped, array, res) ||
            executeType<Decimal32, Decimal128>(mapped, array, res) ||
            executeType<Decimal64, Decimal128>(mapped, array, res) ||
            executeType<Decimal128, Decimal128>(mapped, array, res))
            return res;
        else
            throw Exception("Unexpected column for arrayCumSumNonNegativeImpl: " + mapped->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

};

struct NameArrayCumSumNonNegative { static constexpr auto name = "arrayCumSumNonNegative"; };
using FunctionArrayCumSumNonNegative = FunctionArrayMapped<ArrayCumSumNonNegativeImpl, NameArrayCumSumNonNegative>;

void registerFunctionArrayCumSumNonNegative(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayCumSumNonNegative>();
}

}


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
            DataTypePtr nested = std::make_shared<DataTypeDecimal<Decimal128>>(DecimalUtils::max_precision<Decimal128>, scale);
            return std::make_shared<DataTypeArray>(nested);
        }

        throw Exception("arrayCumSumNonNegativeImpl cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    template <typename Src, typename Dst>
    static void NO_SANITIZE_UNDEFINED implVector(
        size_t size, const IColumn::Offset * __restrict offsets, Dst * __restrict res_values, const Src * __restrict src_values)
    {
        size_t pos = 0;
        for (const auto * end = offsets + size; offsets < end; ++offsets)
        {
            auto offset = *offsets;
            Dst accumulated{};
            for (; pos < offset; ++pos)
            {
                accumulated += src_values[pos];
                if (accumulated < Dst{})
                    accumulated = {};
                res_values[pos] = accumulated;
            }
        }
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
        implVector(offsets.size(), offsets.data(), res_values.data(), data.data());
        res_ptr = ColumnArray::create(std::move(res_nested), array.getOffsetsPtr());
        return true;

    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        ColumnPtr res;

        mapped = mapped->convertToFullColumnIfConst();
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


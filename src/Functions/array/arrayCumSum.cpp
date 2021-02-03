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

struct ArrayCumSumImpl
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
            DataTypePtr nested = std::make_shared<DataTypeDecimal<Decimal128>>(DecimalUtils::maxPrecision<Decimal128>(), scale);
            return std::make_shared<DataTypeArray>(nested);
        }

        throw Exception("arrayCumSum cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    template <typename Element, typename Result>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
    {
        using ColVecType = std::conditional_t<IsDecimalNumber<Element>, ColumnDecimal<Element>, ColumnVector<Element>>;
        using ColVecResult = std::conditional_t<IsDecimalNumber<Result>, ColumnDecimal<Result>, ColumnVector<Result>>;

        const ColVecType * column = checkAndGetColumn<ColVecType>(&*mapped);

        if (!column)
        {
            const ColumnConst * column_const = checkAndGetColumnConst<ColVecType>(&*mapped);

            if (!column_const)
                return false;

            const Element x = column_const->template getValue<Element>();
            const IColumn::Offsets & offsets = array.getOffsets();

            typename ColVecResult::MutablePtr res_nested;
            if constexpr (IsDecimalNumber<Element>)
            {
                const typename ColVecType::Container & data =
                    checkAndGetColumn<ColVecType>(&column_const->getDataColumn())->getData();
                res_nested = ColVecResult::create(0, data.getScale());
            }
            else
                res_nested = ColVecResult::create();

            typename ColVecResult::Container & res_values = res_nested->getData();
            res_values.resize(column_const->size());

            size_t pos = 0;
            for (auto offset : offsets)
            {
                // skip empty arrays
                if (pos < offset)
                {
                    res_values[pos++] = x; // NOLINT
                    for (; pos < offset; ++pos)
                        res_values[pos] = res_values[pos - 1] + x;
                }
            }

            res_ptr = ColumnArray::create(std::move(res_nested), array.getOffsetsPtr());
            return true;
        }

        const typename ColVecType::Container & data = column->getData();
        const IColumn::Offsets & offsets = array.getOffsets();

        typename ColVecResult::MutablePtr res_nested;
        if constexpr (IsDecimalNumber<Element>)
            res_nested = ColVecResult::create(0, data.getScale());
        else
            res_nested = ColVecResult::create();

        typename ColVecResult::Container & res_values = res_nested->getData();
        res_values.resize(data.size());

        size_t pos = 0;
        for (auto offset : offsets)
        {
            // skip empty arrays
            if (pos < offset)
            {
                res_values[pos] = data[pos]; // NOLINT
                for (++pos; pos < offset; ++pos)
                    res_values[pos] = res_values[pos - 1] + data[pos];
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
            throw Exception("Unexpected column for arrayCumSum: " + mapped->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

};

struct NameArrayCumSum { static constexpr auto name = "arrayCumSum"; };
using FunctionArrayCumSum = FunctionArrayMapped<ArrayCumSumImpl, NameArrayCumSum>;

void registerFunctionArrayCumSum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayCumSum>();
}

}


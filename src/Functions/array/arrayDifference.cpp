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

/** arrayDifference() - returns an array with the difference between all pairs of neighboring elements.
  */
struct ArrayDifferenceImpl
{
    static bool useDefaultImplementationForConstants() { return true; }
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        WhichDataType which(expression_return);

        if (which.isUInt8() || which.isInt8())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt16>());

        if (which.isUInt16() || which.isInt16())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());

        if (which.isUInt32() || which.isUInt64() || which.isInt32() || which.isInt64())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());

        if (which.isFloat32() || which.isFloat64())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

        if (which.isDecimal())
            return std::make_shared<DataTypeArray>(expression_return);

        throw Exception("arrayDifference cannot process values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
        for (auto offset : offsets)
        {
            // skip empty arrays
            if (pos < offset)
            {
                res_values[pos] = 0;
                for (++pos; pos < offset; ++pos)
                    res_values[pos] = static_cast<Result>(data[pos]) - static_cast<Result>(data[pos - 1]);
            }
        }
        res_ptr = ColumnArray::create(std::move(res_nested), array.getOffsetsPtr());
        return true;

    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        ColumnPtr res;

        if (executeType< UInt8 ,  Int16>(mapped, array, res) ||
            executeType< UInt16,  Int32>(mapped, array, res) ||
            executeType< UInt32,  Int64>(mapped, array, res) ||
            executeType< UInt64,  Int64>(mapped, array, res) ||
            executeType<  Int8 ,  Int16>(mapped, array, res) ||
            executeType<  Int16,  Int32>(mapped, array, res) ||
            executeType<  Int32,  Int64>(mapped, array, res) ||
            executeType<  Int64,  Int64>(mapped, array, res) ||
            executeType<Float32,Float64>(mapped, array, res) ||
            executeType<Float64,Float64>(mapped, array, res) ||
            executeType<Decimal32, Decimal32>(mapped, array, res) ||
            executeType<Decimal64, Decimal64>(mapped, array, res) ||
            executeType<Decimal128, Decimal128>(mapped, array, res))
            return res;
        else
            throw Exception("Unexpected column for arrayDifference: " + mapped->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

};

struct NameArrayDifference { static constexpr auto name = "arrayDifference"; };
using FunctionArrayDifference = FunctionArrayMapped<ArrayDifferenceImpl, NameArrayDifference>;

void registerFunctionArrayDifference(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayDifference>();
}

}


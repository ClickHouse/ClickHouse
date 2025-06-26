#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


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

        if (which.isUInt())
        {
            if (which.isNativeUInt())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
            if (which.isUInt128())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt128>());
            if (which.isUInt256())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt256>());
            UNREACHABLE();
        }

        if (which.isInt())
        {
            if (which.isNativeInt())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());
            if (which.isInt128())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt128>());
            if (which.isInt256())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt256>());
            UNREACHABLE();
        }

        if (which.isFloat())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
        if (which.isDecimal())
        {
            UInt32 scale = getDecimalScale(*expression_return);
            DataTypePtr nested;
            if (which.isDecimal256())
                nested = std::make_shared<DataTypeDecimal<Decimal256>>(DecimalUtils::max_precision<Decimal256>, scale);
            else
                nested = std::make_shared<DataTypeDecimal<Decimal128>>(DecimalUtils::max_precision<Decimal128>, scale);
            return std::make_shared<DataTypeArray>(nested);
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "arrayCumSum cannot add values of type {}", expression_return->getName());
    }


    template <typename Src, typename Dst>
    static void NO_SANITIZE_UNDEFINED implConst(
        size_t size, const IColumn::Offset * __restrict offsets, Dst * __restrict res_values, Src src_value)
    {
        size_t pos = 0;
        for (const auto * end = offsets + size; offsets < end; ++offsets)
        {
            auto offset = *offsets;
            Dst accumulated{};
            for (; pos < offset; ++pos)
            {
                accumulated += src_value;
                res_values[pos] = accumulated;
            }
        }
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
                res_values[pos] = accumulated;
            }
        }
    }


    template <typename Element, typename Result>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
    {
        using ColVecType = ColumnVectorOrDecimal<Element>;
        using ColVecResult = ColumnVectorOrDecimal<Result>;

        const ColVecType * column = checkAndGetColumn<ColVecType>(&*mapped);

        if (!column)
        {
            const ColumnConst * column_const = checkAndGetColumnConst<ColVecType>(&*mapped);

            if (!column_const)
                return false;

            const Element x = column_const->template getValue<Element>();
            const IColumn::Offsets & offsets = array.getOffsets();

            typename ColVecResult::MutablePtr res_nested;
            if constexpr (is_decimal<Element>)
            {
                const ColVecType * column_typed = checkAndGetColumn<ColVecType>(&column_const->getDataColumn());
                res_nested = ColVecResult::create(0, column_typed->getScale());
            }
            else
                res_nested = ColVecResult::create();

            typename ColVecResult::Container & res_values = res_nested->getData();
            res_values.resize(column_const->size());
            implConst(offsets.size(), offsets.data(), res_values.data(), x);
            res_ptr = ColumnArray::create(std::move(res_nested), array.getOffsetsPtr());
            return true;
        }

        const typename ColVecType::Container & data = column->getData();
        const IColumn::Offsets & offsets = array.getOffsets();

        typename ColVecResult::MutablePtr res_nested;
        if constexpr (is_decimal<Element>)
            res_nested = ColVecResult::create(0, column->getScale());
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

        if (executeType<UInt8, UInt64>(mapped, array, res) || executeType<UInt16, UInt64>(mapped, array, res)
            || executeType<UInt32, UInt64>(mapped, array, res) || executeType<UInt64, UInt64>(mapped, array, res)
            || executeType<UInt128, UInt128>(mapped, array, res) || executeType<UInt256, UInt256>(mapped, array, res)
            || executeType<Int8, Int64>(mapped, array, res) || executeType<Int16, Int64>(mapped, array, res)
            || executeType<Int32, Int64>(mapped, array, res) || executeType<Int64, Int64>(mapped, array, res)
            || executeType<Int128, Int128>(mapped, array, res) || executeType<Int256, Int256>(mapped, array, res)
            || executeType<Float32, Float64>(mapped, array, res) || executeType<Float64, Float64>(mapped, array, res)
            || executeType<Decimal32, Decimal128>(mapped, array, res) || executeType<Decimal64, Decimal128>(mapped, array, res)
            || executeType<Decimal128, Decimal128>(mapped, array, res) || executeType<Decimal256, Decimal256>(mapped, array, res))
            return res;
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column for arrayCumSum: {}", mapped->getName());
    }
};

struct NameArrayCumSum
{
    static constexpr auto name = "arrayCumSum";
};
using FunctionArrayCumSum = FunctionArrayMapped<ArrayCumSumImpl, NameArrayCumSum>;

REGISTER_FUNCTION(ArrayCumSum)
{
    FunctionDocumentation::Description description = "Returns an array of the partial (running) sums of the elements in the source array. If a lambda function is specified, the sum is computed from applying the lambda to the array elements at each position.";
    FunctionDocumentation::Syntax syntax = "arrayCumSum([func,] arr1[, arr2, ... , arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"func", "Optional. A lambda function to apply to the array elements at each position.", {"Lambda function"}},
        {"arr1", "The source array of numeric values.", {"Array(T)"}},
        {"[arr2, ..., arrN]", "Optional. Additional arrays of the same size, passed as arguments to the lambda function if specified.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the partial sums of the elements in the source array. The result type matches the input array's numeric type.", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT arrayCumSum([1, 1, 1, 1]) AS res", "[1, 2, 3, 4]"},
        {"With lambda", "SELECT arrayCumSum(x -> x * 2, [1, 2, 3]) AS res", "[2, 6, 12]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayCumSum>(documentation);
}

}

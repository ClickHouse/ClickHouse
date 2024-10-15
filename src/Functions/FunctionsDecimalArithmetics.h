#pragma once

#include <type_traits>
#include <Core/AccurateComparison.h>

#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>

#include <Poco/Logger.h>
#include <Loggers/Loggers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


struct DecimalOpHelpers
{
    /* These functions perform main arithmetic logic.
     * As soon as intermediate results may not fit Decimal256 (e.g. 1e36, scale 10),
     * we may not operate with Decimals. Later on this big number may be shrunk (e.g. result scale is 0 in the case above).
     * That's why we need to store intermediate results in a flexible extendable storage (here we use std::vector)
     * Here we operate on numbers using simple digit arithmetic.
     * This is the reason these functions are slower than traditional ones.
     *
     * Here and below we use UInt8 for storing digits (0-9 range with maximum carry of 9 will definitely fit this)
     */
    static std::vector<UInt8> multiply(const std::vector<UInt8> & num1, const std::vector<UInt8> & num2)
    {
        UInt16 const len1 = num1.size();
        UInt16 const len2 = num2.size();
        if (len1 == 0 || len2 == 0)
            return {0};

        std::vector<UInt8> result(len1 + len2, 0);
        UInt16 i_n1 = 0;
        UInt16 i_n2;

        for (Int32 i = len1 - 1; i >= 0; --i)
        {
            UInt16 carry = 0;
            i_n2 = 0;
            for (Int32 j = len2 - 1; j >= 0; --j)
            {
                if (unlikely(i_n1 + i_n2 >= len1 + len2))
                    throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow: result bigger that Decimal256");
                UInt16 sum = num1[i] * num2[j] + result[i_n1 + i_n2] + carry;
                carry = sum / 10;
                result[i_n1 + i_n2] = sum % 10;
                ++i_n2;
            }

            if (carry > 0)
            {
                if (unlikely(i_n1 + i_n2 >= len1 + len2))
                    throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow: result bigger that Decimal256");
                result[i_n1 + i_n2] += carry;
            }

            ++i_n1;
        }

        // Maximum Int32 value exceeds 2 billion, we can safely use it for array length storing
        Int32 i = static_cast<Int32>(result.size() - 1);

        while (i >= 0 && result[i] == 0)
        {
            result.pop_back();
            --i;
        }
        if (i == -1)
            return {0};

        std::reverse(result.begin(), result.end());
        return result;
    }

    static std::vector<UInt8> divide(const std::vector<UInt8> & number, const Int256 & divisor)
    {
        std::vector<UInt8> result;
        const auto max_index = number.size() - 1;

        UInt16 idx = 0;
        Int256 temp = 0;

        while (temp < divisor && max_index > idx)
        {
            temp = temp * 10 + number[idx];
            ++idx;
        }

        if (unlikely(temp == 0))
            return {0};

        while (max_index >= idx)
        {
            result.push_back(temp / divisor);
            temp = (temp % divisor) * 10 + number[idx];
            ++idx;
        }
        result.push_back(temp / divisor);

        return result;
    }

    static std::vector<UInt8> toDigits(Int256 x)
    {
        std::vector<UInt8> result;
        if (x >= 10)
            result = toDigits(x / 10);

        result.push_back(x % 10);
        return result;
    }

    static UInt256 fromDigits(const std::vector<UInt8> & digits)
    {
        Int256 result = 0;
        Int256 scale = 0;
        for (auto i = digits.rbegin(); i != digits.rend(); ++i)
        {
            result += DecimalUtils::scaleMultiplier<Decimal256>(scale) * (*i);
            ++scale;
        }
        return result;
    }
};


template <typename ResultType, typename Transform>
struct Processor
{
    const Transform transform;

    explicit Processor(Transform transform_)
        : transform(std::move(transform_))
    {}

    template <typename FirstArgVectorType, typename SecondArgType>
    void NO_INLINE
    vectorConstant(const FirstArgVectorType & vec_first, const SecondArgType second_value,
                   PaddedPODArray<typename ResultType::FieldType> & vec_to, UInt16 scale_a, UInt16 scale_b, UInt16 result_scale,
                   size_t input_rows_count) const
    {
        vec_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
            vec_to[i] = transform.execute(vec_first[i], second_value, scale_a, scale_b, result_scale);
    }

    template <typename FirstArgVectorType, typename SecondArgVectorType>
    void NO_INLINE
    vectorVector(const FirstArgVectorType & vec_first, const SecondArgVectorType & vec_second,
                 PaddedPODArray<typename ResultType::FieldType> & vec_to, UInt16 scale_a, UInt16 scale_b, UInt16 result_scale,
                 size_t input_rows_count) const
    {
        vec_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
            vec_to[i] = transform.execute(vec_first[i], vec_second[i], scale_a, scale_b, result_scale);
    }

    template <typename FirstArgType, typename SecondArgVectorType>
    void NO_INLINE
    constantVector(const FirstArgType & first_value, const SecondArgVectorType & vec_second,
                   PaddedPODArray<typename ResultType::FieldType> & vec_to, UInt16 scale_a, UInt16 scale_b, UInt16 result_scale,
                   size_t input_rows_count) const
    {
        vec_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
            vec_to[i] = transform.execute(first_value, vec_second[i], scale_a, scale_b, result_scale);
    }
};


template <typename FirstArgType, typename SecondArgType, typename ResultType, typename Transform>
struct DecimalArithmeticsImpl
{
    static ColumnPtr execute(Transform transform, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
    {
        using FirstArgValueType = typename FirstArgType::FieldType;
        using FirstArgColumnType = typename FirstArgType::ColumnType;
        using SecondArgValueType = typename SecondArgType::FieldType;
        using SecondArgColumnType = typename SecondArgType::ColumnType;
        using ResultColumnType = typename ResultType::ColumnType;

        UInt16 scale_a = getDecimalScale(*arguments[0].type);
        UInt16 scale_b = getDecimalScale(*arguments[1].type);
        UInt16 result_scale = getDecimalScale(*result_type->getPtr());

        auto op = Processor<ResultType, Transform>{std::move(transform)};

        auto result_col = result_type->createColumn();
        auto col_to = assert_cast<ResultColumnType *>(result_col.get());

        const auto * first_col = checkAndGetColumn<FirstArgColumnType>(arguments[0].column.get());
        const auto * second_col = checkAndGetColumn<SecondArgColumnType>(arguments[1].column.get());
        const auto * first_col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const auto * second_col_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());

        if (first_col)
        {
            if (second_col_const)
                op.vectorConstant(first_col->getData(), second_col_const->template getValue<SecondArgValueType>(), col_to->getData(), scale_a, scale_b, result_scale, input_rows_count);
            else
                op.vectorVector(first_col->getData(), second_col->getData(), col_to->getData(), scale_a, scale_b, result_scale, input_rows_count);
        }
        else if (first_col_const)
        {
            op.constantVector(first_col_const->template getValue<FirstArgValueType>(), second_col->getData(), col_to->getData(), scale_a, scale_b, result_scale, input_rows_count);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                            arguments[0].column->getName(), Transform::name);
        }

        return result_col;
    }
};


template <typename Transform>
class FunctionsDecimalArithmetics : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionsDecimalArithmetics>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} does not match: 2 or 3 expected", getName());

        if (!isDecimal(arguments[0].type) || !isDecimal(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments for {} function must be Decimal", getName());

        UInt8 scale = std::max(getDecimalScale(*arguments[0].type->getPtr()), getDecimalScale(*arguments[1].type->getPtr()));

        if (arguments.size() == 3)
        {
            WhichDataType which_scale(arguments[2].type.get());

            if (!which_scale.isUInt8())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of function {}. "
                    "Should be constant UInt8 from range[0, 76]", arguments[2].type->getName(), getName());

            const ColumnConst * scale_column = checkAndGetColumnConst<ColumnUInt8>(arguments[2].column.get());

            if (!scale_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column of third argument of function {}. "
                    "Should be constant UInt8", getName());

            scale = scale_column->getValue<UInt8>();
        }

        /**
        At compile time, result is unknown. We only know the Scale (number of fractional digits) at runtime.
        Also nothing is known about size of whole part.
        As in simple division/multiplication for decimals, we scale the result up, but it is explicit here and no downscale is performed.
        It guarantees that result will have given scale and it can also be MANUALLY converted to other decimal types later.
        **/
        if (scale > DecimalUtils::max_precision<Decimal256>)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal value of third argument of function {}: "
                            "must be integer in range [0, 76]", this->getName());

        return std::make_shared<DataTypeDecimal256>(DecimalUtils::max_precision<Decimal256>, scale);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return resolveOverload(arguments, result_type, input_rows_count);
    }

private:
    // long resolver to call proper templated func
    ColumnPtr resolveOverload(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        WhichDataType which_dividend(arguments[0].type.get());
        WhichDataType which_divisor(arguments[1].type.get());

        if (which_dividend.isDecimal32())
        {
            using DividendType = DataTypeDecimal32;
            if (which_divisor.isDecimal32())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal32, DataTypeDecimal256, Transform>::execute(Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal64())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal64, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal128())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal128, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal256())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal256, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
        }

        else if (which_dividend.isDecimal64())
        {
            using DividendType = DataTypeDecimal64;
            if (which_divisor.isDecimal32())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal32, DataTypeDecimal256, Transform>::execute(Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal64())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal64, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal128())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal128, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal256())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal256, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
        }

        else if (which_dividend.isDecimal128())
        {
            using DividendType = DataTypeDecimal128;
            if (which_divisor.isDecimal32())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal32, DataTypeDecimal256, Transform>::execute(Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal64())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal64, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal128())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal128, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal256())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal256, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
        }

        else if (which_dividend.isDecimal256())
        {
            using DividendType = DataTypeDecimal256;
            if (which_divisor.isDecimal32())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal32, DataTypeDecimal256, Transform>::execute(Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal64())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal64, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal128())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal128, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
            if (which_divisor.isDecimal256())
                return DecimalArithmeticsImpl<DividendType, DataTypeDecimal256, DataTypeDecimal256, Transform>::execute(
                    Transform{}, arguments, result_type, input_rows_count);
        }

        // the compiler is happy now
        return nullptr;
    }
};

}

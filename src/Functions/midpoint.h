#pragma once

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/castColumn.h>
#include <base/TypeList.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionMidpoint : public IFunction
{
public:
    static constexpr auto name = "midpoint";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionMidpoint>(context); }

    explicit FunctionMidpoint(ContextPtr) { }

    static DataTypePtr resolveReturnType(const DataTypes & types)
    {
        DataTypes withoutNulls;
        for (const auto & type : types)
        {
            if (type->onlyNull())
                continue;
            withoutNulls.emplace_back(type);
        }
        if (!withoutNulls.empty())
            return getLeastSupertype(withoutNulls);
        return getLeastSupertype(types);
    }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());
        return resolveReturnType(types);
    }

    template <typename ColumnType>
    static void calculateForType(auto & res_data, Columns & converted_columns, size_t input_rows_count)
    {
        using FieldType = typename ColumnType::ValueType;
        res_data.resize(input_rows_count);
        // For each row, sum across all input columns
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            FieldType sum{};
            for (const auto & col : converted_columns)
            {
                const auto * col_vec = assert_cast<const ColumnType *>(col.get());
                sum += col_vec->getData()[row];
            }
            res_data[row] = sum / static_cast<FieldType>(converted_columns.size());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        // allocate result column dynamically using result_type
        auto col_res = result_type->createColumn();
        Columns converted_columns;
        for (const auto & argument : arguments)
        {
            if (argument.type->onlyNull())
                continue; // ignore NULL arguments
            auto converted_col = castColumn(argument, result_type)->convertToFullColumnIfConst();
            converted_columns.push_back(converted_col);
        }

        // if all columns are nulls, we can return any of them
        if (converted_columns.empty())
            return arguments[0].column;
        // if there's just one non-null column then there's nothing to calculate
        else if (converted_columns.size() == 1)
            return converted_columns[0];

        if (isDecimal(result_type) || isDateTime64(result_type) || isTime64(result_type))
        {
            using DecimalAndTemporalTypes = TypeList<
                DataTypeDecimal32,
                DataTypeDecimal64,
                DataTypeDecimal128,
                DataTypeDecimal256,
                DataTypeDateTime64,
                DataTypeTime64>; // those temporal type behave like decimals
            castTypeToEither(
                DecimalAndTemporalTypes{},
                result_type.get(),
                [&](const auto & type) -> bool
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using FieldType = typename DataType::FieldType;
                    auto * col_res_vec = assert_cast<ColumnDecimal<FieldType> *>(col_res.get());
                    auto & res_data = col_res_vec->getData();
                    calculateForType<ColumnDecimal<FieldType>>(res_data, converted_columns, input_rows_count);
                    return true;
                });
        }
        else if (isNumber(result_type) || isDate(result_type) || isDateTime(result_type) || isTime(result_type) || isDate32(result_type))
        {
            using NumericAndTemporalTypes = TypeList<
                DataTypeUInt8,
                DataTypeUInt16,
                DataTypeUInt32,
                DataTypeUInt64,
                DataTypeUInt128,
                DataTypeUInt256,
                DataTypeInt8,
                DataTypeInt16,
                DataTypeInt32,
                DataTypeInt64,
                DataTypeInt128,
                DataTypeInt256,
                DataTypeBFloat16,
                DataTypeFloat32,
                DataTypeFloat64,
                DataTypeDate,
                DataTypeDateTime,
                DataTypeTime,
                DataTypeDate32>; // those temporal type behave like numeric
            castTypeToEither(
                NumericAndTemporalTypes{},
                result_type.get(),
                [&](const auto & type) -> bool
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using FieldType = typename DataType::FieldType;
                    auto * col_res_vec = assert_cast<ColumnVector<FieldType> *>(col_res.get());
                    auto & res_data = col_res_vec->getData();
                    calculateForType<ColumnVector<FieldType>>(res_data, converted_columns, input_rows_count);
                    return true;
                });
        }

        return col_res;
    }
};

template <typename A, typename B>
struct MidpointImpl
{
    //using ResultType = NumberTraits::ResultOfMidpoint<A, B>;
    using ResultType = NumberTraits::ResultOfIf<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if constexpr ( // Fast path: built-in integers
            std::is_integral_v<A> && std::is_integral_v<B> &&
            sizeof(A) <= 8 && sizeof(B) <= 8
        )
        {
            Result x = static_cast<Result>(a);
            Result y = static_cast<Result>(b);
            return (x & y) + ((x ^ y) >> 1);
        }
        else // Safe path: everything else
        {
            return static_cast<Result>((static_cast<double>(a) + static_cast<double>(b)) / 2.0);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        auto * ty = left->getType();

        // Floating point
        if (!ty->isIntegerTy())
        {
            auto * sum = b.CreateFAdd(left, right);
            auto * two = llvm::ConstantFP::get(ty, 2.0);
            return b.CreateFDiv(sum, two);
        }

        // Integer: widen to avoid overflow
        unsigned bits = ty->getScalarSizeInBits();
        auto * wide_ty = llvm::IntegerType::get(ty->getContext(), bits * 2);

        llvm::Value * ext_l = is_signed ? b.CreateSExt(left, wide_ty) : b.CreateZExt(left, wide_ty);
        llvm::Value * ext_r = is_signed ? b.CreateSExt(right, wide_ty) : b.CreateZExt(right, wide_ty);

        auto * sum = b.CreateAdd(ext_l, ext_r);
        auto * avg = b.CreateUDiv(sum, llvm::ConstantInt::get(wide_ty, 2));

        return b.CreateTrunc(avg, ty);
    }
#endif
};

template <typename SpecializedFunction>
class MidpointResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "midpoint";
    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<MidpointResolver<SpecializedFunction>>(context);
    }

    explicit MidpointResolver(ContextPtr context_)
        : context(context_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2)
        {
            const auto & arg_0_type = arguments[0].type;
            const auto & arg_1_type = arguments[1].type;
            if (isNumber(arg_0_type) && isNumber(arg_1_type))
                return std::make_unique<FunctionToFunctionBaseAdaptor>(SpecializedFunction::create(context), argument_types, return_type);
        }

        return std::make_unique<FunctionToFunctionBaseAdaptor>(FunctionMidpoint::create(context), argument_types, return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());

        if (types.size() == 2)
        {
            const auto & arg_0_type = types[0];
            const auto & arg_1_type = types[1];
            if (isNumber(arg_0_type) && isNumber(arg_1_type))
                return SpecializedFunction::create(context)->getReturnTypeImpl(types);
        }
        return FunctionMidpoint::resolveReturnType(types);
    }

protected:
    ContextPtr context;
};

}

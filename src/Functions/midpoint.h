#pragma once

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <Core/Types.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>

#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/castTypeToEither.h>

#include <Interpreters/castColumn.h>

#include <base/TypeList.h>
#include <base/extended_types.h>

#include <limits>
#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionMidpoint : public IFunction
{
public:
    static constexpr auto name = "midpoint";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionMidpoint>(context); }

    explicit FunctionMidpoint(ContextPtr) { }

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

        return getLeastSupertype(types);
    }

    template <typename T>
    static constexpr bool is_integer_like_v = std::is_integral_v<T> || is_big_int_v<T>;

    template <typename T>
    struct WiderIntImpl
    {
        using type = T;
    };

    template <typename T>
    requires is_integer_like_v<T> && (sizeof(T) <= 16) /// cannot widen beyond 256-bit
    struct WiderIntImpl<T>
    {
        static constexpr bool is_signed_v = std::numeric_limits<T>::is_signed;
        using type = std::conditional_t<
            (sizeof(T) <= 8),
            std::conditional_t<is_signed_v, Int128, UInt128>,
            std::conditional_t<is_signed_v, Int256, UInt256>>;
    };

    template <typename T>
    using WiderInt = typename WiderIntImpl<T>::type;

    template <typename ResultColumnType>
    static void calculateForNumericType(auto & result_data, const Columns & input_columns, size_t input_rows_count)
    {
        using ResultType = typename ResultColumnType::ValueType;

        /// We try to temporary use wider type to avoid overflow in summation
        /// Otherwise, midpoint(UINT64_MAX, UINT64_MAX, UINT64_MAX) would overflow.
        using SumType = WiderInt<ResultType>;

        result_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            SumType sum{};
            for (const auto & column : input_columns)
            {
                const auto * column_vec = assert_cast<const ResultColumnType *>(column.get());
                sum += static_cast<SumType>(column_vec->getData()[row]);
            }

            const auto denom = static_cast<SumType>(input_columns.size());
            result_data[row] = static_cast<ResultType>(sum / denom); /// trunc toward 0
        }
    }

    /// Nullable-aware: ignore NULL arguments per row; result is NULL only if all args are NULL in that row.
    template <typename ResultColumnType>
    static void calculateForNumericType(
        auto & result_data,
        const Columns & input_columns,
        const std::vector<const ColumnUInt8::Container *> & input_null_maps,
        ColumnUInt8::Container & result_null_map_data,
        size_t input_rows_count)
    {
        using ResultType = typename ResultColumnType::ValueType;

        /// We try to temporary use wider type to avoid overflow in summation
        /// Otherwise, midpoint(Nullable(UINT64_MAX), UINT64_MAX, UINT64_MAX) would overflow.
        using SumType = WiderInt<ResultType>;

        result_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            SumType sum{};

            /// If no non-NULL values found, result is NULL.
            size_t count = 0;

            for (size_t i = 0; i < input_columns.size(); ++i)
            {
                chassert(input_null_maps[i] != nullptr);

                /// Ignore the entries that are NULL so that we can compute average over non-NULLs only.
                if ((*input_null_maps[i])[row])
                    continue;

                const auto * col_vec = assert_cast<const ResultColumnType *>(input_columns[i].get());
                sum += static_cast<SumType>(col_vec->getData()[row]);
                ++count;
            }

            if (count == 0)
            {
                result_null_map_data[row] = 1;
                result_data[row] = ResultType{};
            }
            else
            {
                result_null_map_data[row] = 0;
                result_data[row] = static_cast<ResultType>(sum / static_cast<SumType>(count)); /// trunc toward 0
            }
        }
    }

    template <typename ResultColumnType>
    static void calculateForDecimalType(auto & result_data, const Columns & input_columns, size_t input_rows_count)
    {
        using ResultType = typename ResultColumnType::ValueType;
        using Native = typename ResultType::NativeType;

        using SumType = WiderInt<Native>;

        result_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            SumType sum{};
            for (const auto & column : input_columns)
            {
                const auto * column_vec = assert_cast<const ResultColumnType *>(column.get());
                sum += static_cast<SumType>(column_vec->getData()[row].value);
            }

            result_data[row].value = static_cast<Native>(sum / static_cast<SumType>(input_columns.size())); /// trunc toward 0
        }
    }

    template <typename ResultColumnType>
    static void calculateForDecimalType(
        auto & result_data,
        const Columns & input_columns,
        const std::vector<const ColumnUInt8::Container *> & input_null_maps,
        ColumnUInt8::Container & result_null_map_data,
        size_t input_rows_count)
    {
        using ResultType = typename ResultColumnType::ValueType;
        using Native = typename ResultType::NativeType;

        using SumType = WiderInt<Native>;

        result_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            SumType sum{};
            size_t count = 0;

            for (size_t i = 0; i < input_columns.size(); ++i)
            {
                chassert(input_null_maps[i] != nullptr);
                if ((*input_null_maps[i])[row])
                    continue;

                const auto * col_vec = assert_cast<const ResultColumnType *>(input_columns[i].get());
                sum += static_cast<SumType>(col_vec->getData()[row].value);
                ++count;
            }

            if (count == 0)
            {
                result_null_map_data[row] = 1;
                result_data[row].value = Native{};
            }
            else
            {
                result_null_map_data[row] = 0;
                result_data[row].value = static_cast<Native>(sum / static_cast<SumType>(count)); /// trunc toward 0
            }
        }
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const bool result_is_nullable = result_type->isNullable();
        const DataTypePtr nested_result_type = result_is_nullable ? removeNullable(result_type) : result_type;

        auto nested_result_column = nested_result_type->createColumn();

        Columns converted_columns;
        converted_columns.reserve(arguments.size());

        for (const auto & argument : arguments)
        {
            if (argument.type->onlyNull())
                continue;

            auto converted_col = castColumn(argument, result_type)->convertToFullColumnIfConst();
            converted_columns.emplace_back(std::move(converted_col));
        }

        if (converted_columns.empty())
            return arguments[0].column->cloneResized(input_rows_count);

        if (converted_columns.size() == 1)
            return converted_columns[0];

        Columns columns;
        columns.reserve(converted_columns.size());

        std::vector<const ColumnUInt8::Container *> input_null_maps;
        ColumnUInt8::MutablePtr result_null_map;
        ColumnUInt8::Container * result_null_map_data = nullptr;

        if (result_is_nullable)
        {
            input_null_maps.reserve(converted_columns.size());

            result_null_map = ColumnUInt8::create();
            result_null_map_data = &result_null_map->getData();
            result_null_map_data->resize_fill(input_rows_count, 0);

            for (const auto & col : converted_columns)
            {
                const auto * col_nullable = assert_cast<const ColumnNullable *>(col.get());
                columns.emplace_back(col_nullable->getNestedColumnPtr());
                input_null_maps.emplace_back(&col_nullable->getNullMapData());
            }
        }
        else
        {
            std::swap(columns, converted_columns);
        }

        if (isDecimal(nested_result_type) || isDateTime64(nested_result_type) || isTime64(nested_result_type))
        {
            using DecimalAndTemporalTypes = TypeList<
                DataTypeDecimal32,
                DataTypeDecimal64,
                DataTypeDecimal128,
                DataTypeDecimal256,
                DataTypeDateTime64,
                DataTypeTime64>; /// these temporal types behave like decimals

            castTypeToEither(
                DecimalAndTemporalTypes{},
                nested_result_type.get(),
                [&](const auto & type) -> bool
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using FieldType = typename DataType::FieldType;
                    using ResultColumn = ColumnDecimal<FieldType>;

                    auto & result_column_data = assert_cast<ResultColumn *>(nested_result_column.get())->getData();

                    if (result_is_nullable)
                        calculateForDecimalType<ResultColumn>(
                            result_column_data, columns, input_null_maps, *result_null_map_data, input_rows_count);
                    else
                        calculateForDecimalType<ResultColumn>(result_column_data, columns, input_rows_count);

                    return true;
                });
        }
        else if (
            isNumber(nested_result_type) || isDate(nested_result_type) || isDateTime(nested_result_type) || isTime(nested_result_type)
            || isDate32(nested_result_type))
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
                DataTypeDate32>; /// these temporal types behave like numeric

            castTypeToEither(
                NumericAndTemporalTypes{},
                nested_result_type.get(),
                [&](const auto & type) -> bool
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using FieldType = typename DataType::FieldType;
                    using ResultColumn = ColumnVector<FieldType>;

                    auto & result_column_data = assert_cast<ResultColumn *>(nested_result_column.get())->getData();

                    if (result_is_nullable)
                        calculateForNumericType<ResultColumn>(
                            result_column_data, columns, input_null_maps, *result_null_map_data, input_rows_count);
                    else
                        calculateForNumericType<ResultColumn>(result_column_data, columns, input_rows_count);

                    return true;
                });
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} supports only numeric/temporal types, got {}",
                getName(),
                result_type->getName());
        }

        if (result_is_nullable)
            return ColumnNullable::create(std::move(nested_result_column), std::move(result_null_map));

        return nested_result_column;
    }
};

/// Two-arg midpoint used by FunctionBinaryArithmetic fast-path.
/// Integer rounding is trunc-toward-zero.
template <typename A, typename B>
struct MidpointImpl
{
    template <typename T>
    static constexpr bool is_integer_like_v = std::is_integral_v<T> || is_big_int_v<T>;

    using ResultType = typename NumberTraits::ResultOfIf<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if constexpr (is_integer_like_v<Result>)
        {
            Result x = static_cast<Result>(a);
            Result y = static_cast<Result>(b);

            constexpr bool is_signed_res = std::numeric_limits<Result>::is_signed;

            if constexpr (!is_signed_res)
            {
                /// Unsigned: trunc0 == floor, overflow-safe midpoint.
                return (x & y) + ((x ^ y) >> 1);
            }
            else
            {
                /// Signed: compute floor midpoint, then adjust to trunc-toward-0 when sum is negative and odd.
                Result floor_avg = (x & y) + ((x ^ y) >> 1);

                const bool odd = (((x ^ y) & 1) != 0);

                /// Sum sign without overflow:
                /// - if same sign, sign is that sign
                /// - if opposite signs, x + y cannot overflow, so we can check (x + y) < 0 safely
                const bool sum_negative = ((x < 0) == (y < 0)) ? (x < 0) : ((x + y) < 0);

                if (odd && sum_negative)
                    ++floor_avg;

                return floor_avg;
            }
        }
        else
        {
            return (static_cast<Result>(a) + static_cast<Result>(b)) / static_cast<Result>(2);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        auto * ty = left->getType();

        /// Floating point
        if (!ty->isIntegerTy())
        {
            auto * sum = b.CreateFAdd(left, right);
            auto * two = llvm::ConstantFP::get(ty, 2.0);
            return b.CreateFDiv(sum, two);
        }

        /// Integer: widen to avoid overflow
        unsigned bits = ty->getScalarSizeInBits();
        auto * wide_ty = llvm::IntegerType::get(ty->getContext(), bits * 2);

        llvm::Value * ext_l = is_signed ? b.CreateSExt(left, wide_ty) : b.CreateZExt(left, wide_ty);
        llvm::Value * ext_r = is_signed ? b.CreateSExt(right, wide_ty) : b.CreateZExt(right, wide_ty);

        auto * sum = b.CreateAdd(ext_l, ext_r);
        auto * two = llvm::ConstantInt::get(wide_ty, 2);
        auto * avg = is_signed ? b.CreateSDiv(sum, two) : b.CreateUDiv(sum, two);

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

    /// Fast-path only for:
    /// - exactly 2 args
    /// - both non-Nullable (we need ignore-NULL semantics for Nullable, handled by FunctionMidpoint)
    /// - both non-Decimal numbers (Decimal handled by FunctionMidpoint)
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        if (arguments.size() == 2)
        {
            const auto & a0 = arguments[0].type;
            const auto & a1 = arguments[1].type;

            if (!a0->isNullable() && !a1->isNullable())
            {
                if (isNumber(a0) && isNumber(a1) && !isDecimal(a0) && !isDecimal(a1))
                    return std::make_unique<FunctionToFunctionBaseAdaptor>(
                        SpecializedFunction::create(context), argument_types, return_type);
            }
        }

        return std::make_unique<FunctionToFunctionBaseAdaptor>(FunctionMidpoint::create(context), argument_types, return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());

        return getLeastSupertype(types);
    }

protected:
    ContextPtr context;
};

}

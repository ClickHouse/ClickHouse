#pragma once

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IsOperation.h>
#include <Functions/castTypeToEither.h>

#include "config.h"
#include <Common/TargetSpecific.h>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

template <typename A, typename Op>
struct UnaryOperationImpl
{
    using ResultType = typename Op::ResultType;
    using ColVecA = ColumnVectorOrDecimal<A>;
    using ColVecC = ColumnVectorOrDecimal<ResultType>;
    using ArrayA = typename ColVecA::Container;
    using ArrayC = typename ColVecC::Container;

    MULTITARGET_FUNCTION_X86_V4_V3(
    MULTITARGET_FUNCTION_HEADER(static void NO_INLINE), vectorImpl, MULTITARGET_FUNCTION_BODY((const ArrayA & a, ArrayC & c) /// NOLINT
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::apply(a[i]);
    }))

    static void NO_INLINE vector(const ArrayA & a, ArrayC & c)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v4))
        {
            vectorImpl_x86_64_v4(a, c);
            return;
        }

        if (isArchSupported(TargetArch::x86_64_v3))
        {
            vectorImpl_x86_64_v3(a, c);
            return;
        }
#endif

        vectorImpl(a, c);
    }

    static void constant(A a, ResultType & c)
    {
        c = Op::apply(a);
    }
};


template <typename Op>
struct FixedStringUnaryOperationImpl
{
    MULTITARGET_FUNCTION_X86_V4_V3(
    MULTITARGET_FUNCTION_HEADER(static void NO_INLINE), vectorImpl, MULTITARGET_FUNCTION_BODY((const ColumnFixedString::Chars & a, /// NOLINT
        ColumnFixedString::Chars & c)
    {
        size_t size = a.size();

        for (size_t i = 0; i < size; ++i)
            c[i] = Op::apply(a[i]);
    }))

    static void NO_INLINE vector(const ColumnFixedString::Chars & a, ColumnFixedString::Chars & c)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v4))
        {
            vectorImpl_x86_64_v4(a, c);
            return;
        }

        if (isArchSupported(TargetArch::x86_64_v3))
        {
            vectorImpl_x86_64_v3(a, c);
            return;
        }
#endif

        vectorImpl(a, c);
    }
};

template <typename Op>
struct StringUnaryOperationReduceImpl
{
    MULTITARGET_FUNCTION_X86_V4_V3(
        MULTITARGET_FUNCTION_HEADER(static UInt64 NO_INLINE),
        vectorImpl,
        MULTITARGET_FUNCTION_BODY((const UInt8 * start, const UInt8 * end) /// NOLINT
        {
            UInt64 res = 0;
            while (start < end)
                res += Op::apply(*start++);
            return res;
        }))

    static UInt64 NO_INLINE vector(const UInt8 * start, const UInt8 * end)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v4))
        {
            return vectorImpl_x86_64_v4(start, end);
        }

        if (isArchSupported(TargetArch::x86_64_v3))
        {
            return vectorImpl_x86_64_v3(start, end);
        }
#endif

        return vectorImpl(start, end);
    }
};

template <typename FunctionName>
struct FunctionUnaryArithmeticMonotonicity;

/// Used to indicate undefined operation
struct InvalidType;


template <template <typename> class Op, typename Name, bool is_injective>
class FunctionUnaryArithmetic : public IFunction
{
    static constexpr bool allow_decimal = IsUnaryOperation<Op>::negate || IsUnaryOperation<Op>::abs || IsUnaryOperation<Op>::sign;
    static constexpr bool allow_string_or_fixed_string = Op<UInt8>::allow_string_or_fixed_string;
    static constexpr bool is_bit_count = IsUnaryOperation<Op>::bit_count;
    static constexpr bool is_sign_function = IsUnaryOperation<Op>::sign;

    ContextPtr context;

    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<
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
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeDecimal<Decimal32>,
            DataTypeDecimal<Decimal64>,
            DataTypeDecimal<Decimal128>,
            DataTypeDecimal<Decimal256>,
            DataTypeFixedString,
            DataTypeString,
            DataTypeInterval>(type, std::forward<F>(f));
    }

    static FunctionOverloadResolverPtr
    getFunctionForTupleArithmetic(const DataTypePtr & type, ContextPtr context)
    {
        if (!isTuple(type))
            return {};

        /// Special case when the function is negate, argument is tuple.
        /// We construct another function (example: tupleNegate) and call it.

        if constexpr (!IsUnaryOperation<Op>::negate)
            return {};

        return FunctionFactory::instance().get("tupleNegate", context);
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUnaryArithmetic>(); }

    FunctionUnaryArithmetic() = default;

    explicit FunctionUnaryArithmetic(ContextPtr context_) : context(context_) {}

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return is_injective; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return getReturnTypeImplStatic(arguments, context);
    }

    static DataTypePtr getReturnTypeImplStatic(const DataTypes & arguments, ContextPtr context)
    {
        /// Special case when the function is negate, argument is tuple.
        if (auto function_builder = getFunctionForTupleArithmetic(arguments[0], context))
        {
            ColumnsWithTypeAndName new_arguments(1);

            new_arguments[0].type = arguments[0];

            auto function = function_builder->build(new_arguments);
            return function->getResultType();
        }

        DataTypePtr result;
        bool valid = castType(arguments[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<DataTypeFixedString, DataType>)
            {
                if constexpr (!allow_string_or_fixed_string)
                    return false;
                /// For `bitCount`, when argument is FixedString, it's return type
                /// should be integer instead of FixedString, the return value is
                /// the sum of `bitCount` apply to each chars.
                else
                {
                    /// UInt16 can save bitCount of FixedString less than 8192,
                    /// it's should enough for almost all cases, and the setting
                    /// `allow_suspicious_fixed_string_types` is disabled by default.
                    if constexpr (is_bit_count)
                        result = std::make_shared<DataTypeUInt16>();
                    else
                        result = std::make_shared<DataType>(type.getN());
                }
            }
            else if constexpr (std::is_same_v<DataTypeString, DataType>)
            {
                if constexpr (!allow_string_or_fixed_string)
                    return false;
                else
                {
                    if constexpr (is_bit_count)
                        result = std::make_shared<DataTypeUInt64>();
                    else
                        result = std::make_shared<DataType>();
                }
            }
            else if constexpr (std::is_same_v<DataTypeInterval, DataType>)
            {
                if constexpr (!IsUnaryOperation<Op>::negate)
                    return false;
                result = std::make_shared<DataTypeInterval>(type.getKind());
            }
            else
            {
                using T0 = typename DataType::FieldType;

                if constexpr (IsDataTypeDecimal<DataType> && !is_sign_function)
                {
                    if constexpr (!allow_decimal)
                        return false;
                    result = std::make_shared<DataType>(type.getPrecision(), type.getScale());
                }
                else
                    result = std::make_shared<DataTypeNumber<typename Op<T0>::ResultType>>();
            }
            return true;
        });
        if (!valid)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), String(name));
        return result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Special case when the function is negate, argument is tuple.
        if (auto function_builder = getFunctionForTupleArithmetic(arguments[0].type, context))
        {
            return function_builder->build(arguments)->execute(arguments, result_type, input_rows_count, /* dry_run = */ false);
        }

        ColumnPtr result_column;
        bool valid = castType(arguments[0].type.get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;

            if constexpr (std::is_same_v<DataTypeFixedString, DataType>)
            {
                if constexpr (allow_string_or_fixed_string)
                {
                    if (const auto * col = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
                    {
                        if constexpr (is_bit_count)
                        {
                            auto size = col->size();

                            auto col_res = ColumnUInt16::create(size);
                            auto & vec_res = col_res->getData();
                            vec_res.resize(col->size());

                            const auto & chars = col->getChars();
                            auto n = col->getN();
                            for (size_t i = 0; i < size; ++i)
                            {
                                vec_res[i] = static_cast<UInt16>(
                                    StringUnaryOperationReduceImpl<Op<UInt8>>::vector(chars.data() + n * i, chars.data() + n * (i + 1)));
                            }
                            result_column = std::move(col_res);
                            return true;
                        }
                        else
                        {
                            auto col_res = ColumnFixedString::create(col->getN());
                            auto & vec_res = col_res->getChars();
                            vec_res.resize(col->size() * col->getN());
                            FixedStringUnaryOperationImpl<Op<UInt8>>::vector(col->getChars(), vec_res);
                            result_column = std::move(col_res);
                            return true;
                        }
                    }
                }
            }
            else if constexpr (std::is_same_v<DataTypeString, DataType>)
            {
                if constexpr (allow_string_or_fixed_string)
                {
                    if (const auto * col = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
                    {
                        if constexpr (is_bit_count)
                        {
                            auto size = col->size();

                            auto col_res = ColumnUInt64::create(size);
                            auto & vec_res = col_res->getData();

                            const auto & chars = col->getChars();
                            const auto & offsets = col->getOffsets();
                            for (size_t i = 0; i < size; ++i)
                            {
                                vec_res[i] = StringUnaryOperationReduceImpl<Op<UInt8>>::vector(
                                    chars.data() + offsets[i - 1], chars.data() + offsets[i]);
                            }
                            result_column = std::move(col_res);
                            return true;
                        }
                        else
                        {
                            auto col_res = ColumnString::create();
                            auto & vec_res = col_res->getChars();
                            auto & offset_res = col_res->getOffsets();

                            const auto & vec_col = col->getChars();
                            const auto & offset_col = col->getOffsets();

                            vec_res.resize(vec_col.size());
                            offset_res.resize(offset_col.size());
                            memcpy(offset_res.data(), offset_col.data(), offset_res.size() * sizeof(UInt64));

                            FixedStringUnaryOperationImpl<Op<UInt8>>::vector(vec_col, vec_res);
                            result_column = std::move(col_res);
                            return true;
                        }
                    }
                }
            }
            else if constexpr (IsDataTypeDecimal<DataType>)
            {
                using T0 = typename DataType::FieldType;
                if constexpr (allow_decimal)
                {
                    if (auto col = checkAndGetColumn<ColumnDecimal<T0>>(arguments[0].column.get()))
                    {
                        if constexpr (is_sign_function)
                        {
                            auto col_res = ColumnVector<typename Op<T0>::ResultType>::create();
                            auto & vec_res = col_res->getData();
                            vec_res.resize(col->getData().size());
                            UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);
                            result_column = std::move(col_res);
                            return true;
                        }
                        else
                        {
                            auto col_res = ColumnDecimal<typename Op<T0>::ResultType>::create(0, type.getScale());
                            auto & vec_res = col_res->getData();
                            vec_res.resize(col->getData().size());
                            UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);
                            result_column = std::move(col_res);
                            return true;
                        }
                    }
                }
            }
            else
            {
                using T0 = typename DataType::FieldType;
                if (auto col = checkAndGetColumn<ColumnVector<T0>>(arguments[0].column.get()))
                {
                    auto col_res = ColumnVector<typename Op<T0>::ResultType>::create();
                    auto & vec_res = col_res->getData();
                    vec_res.resize(col->getData().size());
                    UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);
                    result_column = std::move(col_res);
                    return true;
                }
            }

            return false;
        });
        if (!valid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{}'s argument does not match the expected data type", getName());

        return result_column;
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr & result_type) const override
    {
        if (1 != arguments.size())
            return false;

        if (!canBeNativeType(*arguments[0]) || !canBeNativeType(*result_type))
            return false;

        return castType(arguments[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<DataTypeFixedString, DataType> || std::is_same_v<DataTypeString, DataType>)
                return false;
            else
            {
                using T0 = typename DataType::FieldType;
                using T1 = typename Op<T0>::ResultType;
                if constexpr (!std::is_same_v<T1, InvalidType> && !IsDataTypeDecimal<DataType> && Op<T0>::compilable)
                    return true;
            }

            return false;
        });
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        assert(1 == arguments.size());

        llvm::Value * result = nullptr;
        castType(arguments[0].type.get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<DataTypeFixedString, DataType> || std::is_same_v<DataTypeString, DataType>)
                return false;
            else
            {
                using T0 = typename DataType::FieldType;
                using T1 = typename Op<T0>::ResultType;
                if constexpr (!std::is_same_v<T1, InvalidType> && !IsDataTypeDecimal<DataType> && Op<T0>::compilable)
                {
                    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
                    if constexpr (std::is_same_v<Op<T0>, AbsImpl<T0>> || std::is_same_v<Op<T0>, BitCountImpl<T0>> || std::is_same_v<Op<T0>, SignImpl<T0>>)
                    {
                        /// We don't need to cast the argument to the result type if it's abs/bitcount/sign function.
                        result = Op<T0>::compile(b, arguments[0].value, is_signed_v<T0>);
                    }
                    else
                    {
                        auto * v = nativeCast(b, arguments[0], result_type);
                        result = Op<T0>::compile(b, v, is_signed_v<T1>);
                    }

                    return true;
                }
            }

            return false;
        });

        return result;
    }
#endif

    bool hasInformationAboutMonotonicity() const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::has();
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::get(type, left, right);
    }
};


struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { .is_monotonic = true };
    }
};

}

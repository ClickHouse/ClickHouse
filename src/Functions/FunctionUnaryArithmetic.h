#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/Native.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IsOperation.h>
#include <Functions/castTypeToEither.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_EMBEDDED_COMPILER
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wunused-parameter"
#    include <llvm/IR/IRBuilder.h>
#    pragma GCC diagnostic pop
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
    using ColVecA = std::conditional_t<IsDecimalNumber<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecC = std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>, ColumnVector<ResultType>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayC = typename ColVecC::Container;

    static void NO_INLINE vector(const ArrayA & a, ArrayC & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::apply(a[i]);
    }

    static void constant(A a, ResultType & c)
    {
        c = Op::apply(a);
    }
};


template <typename Op>
struct FixedStringUnaryOperationImpl
{
    static void NO_INLINE vector(const ColumnFixedString::Chars & a, ColumnFixedString::Chars & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::apply(a[i]);
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
    static constexpr bool allow_fixed_string = Op<UInt8>::allow_fixed_string;
    static constexpr bool is_sign_function = IsUnaryOperation<Op>::sign;

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
            DataTypeFixedString
        >(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUnaryArithmetic>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return is_injective; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr result;
        bool valid = castType(arguments[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<DataTypeFixedString, DataType>)
            {
                if constexpr (!Op<DataTypeFixedString>::allow_fixed_string)
                    return false;
                result = std::make_shared<DataType>(type.getN());
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
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        ColumnPtr result_column;
        bool valid = castType(arguments[0].type.get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;

            if constexpr (std::is_same_v<DataTypeFixedString, DataType>)
            {
                if constexpr (allow_fixed_string)
                {
                    if (const auto * col = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
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
            throw Exception(getName() + "'s argument does not match the expected data type", ErrorCodes::LOGICAL_ERROR);

        return result_column;
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments) const override
    {
        if (1 != arguments.size())
            return false;

        return castType(arguments[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<DataTypeFixedString, DataType>)
                return false;
            else
                return !IsDataTypeDecimal<DataType> && Op<typename DataType::FieldType>::compilable;
        });
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, Values values) const override
    {
        assert(1 == types.size() && 1 == values.size());

        llvm::Value * result = nullptr;
        castType(types[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<DataTypeFixedString, DataType>)
                return false;
            else
            {
                using T0 = typename DataType::FieldType;
                using T1 = typename Op<T0>::ResultType;
                if constexpr (!std::is_same_v<T1, InvalidType> && !IsDataTypeDecimal<DataType> && Op<T0>::compilable)
                {
                    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
                    auto * v = nativeCast(b, types[0], values[0], std::make_shared<DataTypeNumber<T1>>());
                    result = Op<T0>::compile(b, v, is_signed_v<T1>);
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

    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left, const Field & right) const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::get(left, right);
    }
};


struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return { true };
    }
};

}

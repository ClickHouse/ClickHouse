#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/TargetSpecific.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Impl, typename Name>
class FunctionIntHash : public IFunction
{
public:
    static constexpr auto name = Name::name;

private:
    using ToType = typename Impl::ReturnType;

    MULTITARGET_FUNCTION_AVX512F_AVX2(
        MULTITARGET_FUNCTION_HEADER(template <typename FromType> static void),
        executeManyTypeImpl,
        MULTITARGET_FUNCTION_BODY((const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to) /// NOLINT
        {
            size_t size = vec_from.size();

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Impl::apply(vec_from[i]);
        })
    )

    template <typename FromType>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments) const
    {
        using ColVecType = ColumnVectorOrDecimal<FromType>;

        if (const ColVecType * col_from = checkAndGetColumn<ColVecType>(arguments[0].column.get()))
        {
            const typename ColVecType::Container & vec_from = col_from->getData();
            auto col_to = ColumnVector<ToType>::create(vec_from.size());
            typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

#if USE_MULTITARGET_CODE
            if (isArchSupported(TargetArch::AVX512F))
                executeManyTypeImplAVX512F(vec_from, vec_to);
            else if (isArchSupported(TargetArch::AVX2))
                executeManyTypeImplAVX2(vec_from, vec_to);
            else
#endif
            {
                executeManyTypeImpl(vec_from, vec_to);
            }
            return col_to;
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(), Name::name);
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isValueRepresentedByNumber())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        return std::make_shared<DataTypeNumber<typename Impl::ReturnType>>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeNumber<typename Impl::ReturnType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isUInt8())
            return executeType<UInt8>(arguments);
        if (which.isUInt16())
            return executeType<UInt16>(arguments);
        if (which.isUInt32())
            return executeType<UInt32>(arguments);
        if (which.isUInt64())
            return executeType<UInt64>(arguments);
        if (which.isInt8())
            return executeType<Int8>(arguments);
        if (which.isInt16())
            return executeType<Int16>(arguments);
        if (which.isInt32())
            return executeType<Int32>(arguments);
        if (which.isInt64())
            return executeType<Int64>(arguments);
        if (which.isDate())
            return executeType<UInt16>(arguments);
        if (which.isDate32())
            return executeType<Int32>(arguments);
        if (which.isDateTime())
            return executeType<UInt32>(arguments);
        if (which.isDecimal32())
            return executeType<Decimal32>(arguments);
        if (which.isDecimal64())
            return executeType<Decimal64>(arguments);
        if (which.isIPv4())
            return executeType<IPv4>(arguments);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
            arguments[0].type->getName(), getName());
    }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIntHash>(); }
};


struct NameIntHash32 { static constexpr auto name = "intHash32"; };
struct NameIntHash64 { static constexpr auto name = "intHash64"; };
using FunctionIntHash32 = FunctionIntHash<IntHash32Impl, NameIntHash32>;
using FunctionIntHash64 = FunctionIntHash<IntHash64Impl, NameIntHash64>;

}

REGISTER_FUNCTION(HashingInt)
{
    factory.registerFunction<FunctionIntHash32>();
    factory.registerFunction<FunctionIntHash64>();
}
}

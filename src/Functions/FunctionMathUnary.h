#pragma once

#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include "config.h"

/** FastOps is a fast vector math library from Mikhail Parakhin, https://www.linkedin.com/in/mikhail-parakhin/
  * Enabled by default.
  */
#if USE_FASTOPS
#    include <fastops/fastops.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


template <typename Impl>
class FunctionMathUnary : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMathUnary>(); }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & argument = arguments.front();

        if (!isNumber(argument))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                argument->getName(),
                getName());

        /// Integers are converted to Float64.
        if (Impl::always_returns_float64 || !isFloat(argument))
            return std::make_shared<DataTypeFloat64>();
        return argument;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return Impl::always_returns_float64 ? std::make_shared<DataTypeFloat64>() : nullptr;
    }

    template <typename T, typename ReturnType>
    static void executeInIterations(const T * src_data, ReturnType * dst_data, size_t size)
    {
        if constexpr (Impl::rows_per_iteration == 0)
        {
            /// Process all data as a whole and use FastOps implementation

            /// If the argument is integer, convert to Float64 beforehand
            if constexpr (!std::is_floating_point_v<T>)
            {
                PODArray<Float64> tmp_vec(size);
                for (size_t i = 0; i < size; ++i)
                    tmp_vec[i] = static_cast<Float64>(src_data[i]);

                Impl::execute(tmp_vec.data(), size, dst_data);
            }
            else
            {
                Impl::execute(src_data, size, dst_data);
            }
        }
        else
        {
            const size_t rows_remaining = size % Impl::rows_per_iteration;
            const size_t rows_size = size - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&src_data[i], &dst_data[i]);

            if (rows_remaining != 0)
            {
                T src_remaining[Impl::rows_per_iteration];
                memcpy(src_remaining, &src_data[rows_size], rows_remaining * sizeof(T));
                memset(src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(T));
                ReturnType dst_remaining[Impl::rows_per_iteration];

                Impl::execute(src_remaining, dst_remaining);

                if constexpr (is_big_int_v<T> || std::is_same_v<T, Decimal256>)
                    for (size_t i = 0; i < rows_remaining; ++i)
                        dst_data[rows_size + i] = dst_remaining[i];
                else
                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(ReturnType));
            }
        }
    }

    template <typename T, typename ReturnType>
    static ColumnPtr execute(const ColumnVector<T> * col, size_t input_rows_count)
    {
        const auto & src_data = col->getData();

        auto dst = ColumnVector<ReturnType>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        executeInIterations(src_data.data(), dst_data.data(), input_rows_count);

        return dst;
    }

    template <typename T, typename ReturnType>
    static ColumnPtr execute(const ColumnDecimal<T> * col, size_t input_rows_count)
    {
        const auto & src_data = col->getData();
        UInt32 scale = col->getScale();

        auto dst = ColumnVector<ReturnType>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
            dst_data[i] = DecimalUtils::convertTo<ReturnType>(src_data[i], scale);

        executeInIterations(dst_data.data(), dst_data.data(), input_rows_count);

        return dst;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col = arguments[0];
        ColumnPtr res;

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
            using ReturnType = std::conditional_t<Impl::always_returns_float64 || !std::is_floating_point_v<Type>, Float64, Type>;
            using ColVecType = ColumnVectorOrDecimal<Type>;

            const auto col_vec = checkAndGetColumn<ColVecType>(col.column.get());
            if (col_vec == nullptr)
                return false;
            return (res = execute<Type, ReturnType>(col_vec, input_rows_count)) != nullptr;
        };

        if (!callOnBasicType<void, true, true, true, false>(col.type->getTypeId(), call))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                col.column->getName(),
                getName());

        return res;
    }
};


template <typename Name, Float64(Function)(Float64)>
struct UnaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;
    static constexpr bool always_returns_float64 = true;

    template <typename T>
    static void execute(const T * __restrict src, Float64 * __restrict dst)
    {
        *dst = Function(static_cast<Float64>(*src));
    }
};

}

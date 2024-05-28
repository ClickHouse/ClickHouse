#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}


template <typename Impl>
class FunctionMathBinaryFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMathBinaryFloat64>(); }
    static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto check_argument_type = [this](const IDataType * arg)
        {
            if (!isNativeNumber(arg) && !isDecimal(arg))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arg->getName(), getName());
        };

        check_argument_type(arguments.front().get());
        check_argument_type(arguments.back().get());

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename LeftType, typename RightType>
    static void executeInIterationsStaticLeft(const LeftType left_src_data[Impl::rows_per_iteration], const RightType * right_src_data, Float64 * dst_data, size_t src_size)
    {
        const auto rows_remaining = src_size % Impl::rows_per_iteration;
        const auto rows_size = src_size - rows_remaining;

        for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
            Impl::execute(left_src_data, &right_src_data[i], &dst_data[i]);

        if (rows_remaining != 0)
        {
            RightType right_src_remaining[Impl::rows_per_iteration];
            memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
            memset(right_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));

            Float64 dst_remaining[Impl::rows_per_iteration];

            Impl::execute(left_src_data, right_src_remaining, dst_remaining);

            if constexpr (is_big_int_v<LeftType> || std::is_same_v<LeftType, Decimal256> || is_big_int_v<RightType> || std::is_same_v<RightType, Decimal256>)
                for (size_t i = 0; i < rows_remaining; ++i)
                    dst_data[rows_size + i] = dst_remaining[i];
            else
                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
        }
    }

    template <typename LeftType, typename RightType>
    static void executeInIterationsStaticRight(const LeftType* left_src_data, const RightType right_src_data[Impl::rows_per_iteration], Float64 * dst_data, size_t src_size)
    {
        const auto rows_remaining = src_size % Impl::rows_per_iteration;
        const auto rows_size = src_size - rows_remaining;

        for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
            Impl::execute(&left_src_data[i], right_src_data, &dst_data[i]);

        if (rows_remaining != 0)
        {
            RightType left_src_remaining[Impl::rows_per_iteration];
            memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
            memset(left_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));

            Float64 dst_remaining[Impl::rows_per_iteration];

            Impl::execute(left_src_remaining, right_src_data, dst_remaining);

            if constexpr (is_big_int_v<LeftType> || std::is_same_v<LeftType, Decimal256> || is_big_int_v<RightType> || std::is_same_v<RightType, Decimal256>)
                for (size_t i = 0; i < rows_remaining; ++i)
                    dst_data[rows_size + i] = dst_remaining[i];
            else
                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
        }
    }

    template <typename LeftType, typename RightType>
    static void executeInIterations(const LeftType * left_src_data, const RightType * right_src_data, Float64 * dst_data, size_t src_size)
    {
        const auto rows_remaining = src_size % Impl::rows_per_iteration;
        const auto rows_size = src_size - rows_remaining;

        for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
            Impl::execute(&left_src_data[i], &right_src_data[i], &dst_data[i]);

        if (rows_remaining != 0)
        {
            LeftType left_src_remaining[Impl::rows_per_iteration];
            memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
            memset(left_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));

            RightType right_src_remaining[Impl::rows_per_iteration];
            memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
            memset(right_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));

            Float64 dst_remaining[Impl::rows_per_iteration];

            Impl::execute(left_src_remaining, right_src_remaining, dst_remaining);

            if constexpr (is_big_int_v<LeftType> || std::is_same_v<LeftType, Decimal256> || is_big_int_v<RightType> || std::is_same_v<RightType, Decimal256>)
                for (size_t i = 0; i < rows_remaining; ++i)
                    dst_data[rows_size + i] = dst_remaining[i];
            else
                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
        }
    }

    template <typename LeftType, typename RightType>
    static ColumnPtr executeTyped(const ColumnConst * left_arg, const IColumn * right_arg)
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVectorOrDecimal<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();
            auto & dst_data = dst->getData();
            const auto & right_src_data = right_arg_typed->getData();
            const auto src_size = right_src_data.size();
            dst_data.resize(src_size);

            if constexpr (is_decimal<LeftType>)
            {
                Float64 left_src_data[Impl::rows_per_iteration];
                const auto left_arg_typed = checkAndGetColumn<ColumnVectorOrDecimal<LeftType>>(left_arg->getDataColumnPtr().get());
                UInt32 left_scale = left_arg_typed->getScale();
                for (size_t i = 0; i < src_size; ++i)
                    left_src_data[i] = DecimalUtils::convertTo<Float64>(left_arg->template getValue<LeftType>(), left_scale);

                if constexpr (is_decimal<RightType>)
                {
                    UInt32 right_scale = right_arg_typed->getScale();
                    for (size_t i = 0; i < src_size; ++i)
                        dst_data[i] = DecimalUtils::convertTo<Float64>(right_src_data[i], right_scale);

                    executeInIterationsStaticLeft(left_src_data, dst_data.data(), dst_data.data(), src_size);
                }
                else
                {
                    executeInIterationsStaticLeft(left_src_data, right_src_data.data(), dst_data.data(), src_size);
                }
            }
            else
            {
                LeftType left_src_data[Impl::rows_per_iteration];
                std::fill(std::begin(left_src_data), std::end(left_src_data), left_arg->template getValue<LeftType>());

                if constexpr (is_decimal<RightType>)
                {
                    UInt32 right_scale = right_arg_typed->getScale();
                    for (size_t i = 0; i < src_size; ++i)
                        dst_data[i] = DecimalUtils::convertTo<Float64>(right_src_data[i], right_scale);

                    executeInIterationsStaticLeft(left_src_data, dst_data.data(), dst_data.data(), src_size);
                }
                else
                {
                    executeInIterationsStaticLeft(left_src_data, right_src_data.data(), dst_data.data(), src_size);
                }
            }
            return dst;
        }
        return nullptr;
    }

    template <typename LeftType, typename RightType>
    static ColumnPtr executeTyped(const ColumnVectorOrDecimal<LeftType> * left_arg, const IColumn * right_arg)
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVectorOrDecimal<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            const auto & right_src_data = right_arg_typed->getData();
            auto & dst_data = dst->getData();
            const auto src_size = left_src_data.size();
            dst_data.resize(src_size);

            if constexpr (is_decimal<LeftType> && is_decimal<RightType>)
            {
                auto left = ColumnVector<Float64>::create();
                auto & left_data = left->getData();
                left_data.resize(src_size);
                UInt32 left_scale = left_arg->getScale();
                UInt32 right_scale = right_arg_typed->getScale();
                for (size_t i = 0; i < src_size; ++i)
                {
                    left_data[i] = DecimalUtils::convertTo<Float64>(left_src_data[i], left_scale);
                    dst_data[i] = DecimalUtils::convertTo<Float64>(right_src_data[i], right_scale);
                }

                executeInIterations(left_data.data(), dst_data.data(), dst_data.data(), src_size);
            }
            else if constexpr (!is_decimal<LeftType> && is_decimal<RightType>)
            {
                UInt32 scale = right_arg_typed->getScale();
                for (size_t i = 0; i < src_size; ++i)
                    dst_data[i] = DecimalUtils::convertTo<Float64>(right_src_data[i], scale);

                executeInIterations(left_src_data.data(), dst_data.data(), dst_data.data(), src_size);
            }
            else if constexpr (is_decimal<LeftType> && !is_decimal<RightType>)
            {
                UInt32 scale = left_arg->getScale();
                for (size_t i = 0; i < src_size; ++i)
                    dst_data[i] = DecimalUtils::convertTo<Float64>(left_src_data[i], scale);

                executeInIterations(dst_data.data(), right_src_data.data(), dst_data.data(), src_size);
            }
            else
            {
                executeInIterations(left_src_data.data(), right_src_data.data(), dst_data.data(), src_size);
            }

            return dst;
        }
        if (const auto right_arg_typed = checkAndGetColumnConst<ColumnVectorOrDecimal<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            auto & dst_data = dst->getData();
            const auto src_size = left_src_data.size();
            dst_data.resize(src_size);

            if constexpr (is_decimal<RightType>)
            {
                Float64 right_src_data[Impl::rows_per_iteration];
                UInt32 right_scale
                        = checkAndGetColumn<ColumnVectorOrDecimal<RightType>>(right_arg_typed->getDataColumnPtr().get())->getScale();
                for (size_t i = 0; i < src_size; ++i)
                    right_src_data[i] = DecimalUtils::convertTo<Float64>(right_arg_typed->template getValue<RightType>(), right_scale);

                if constexpr (is_decimal<LeftType>)
                {
                    UInt32 left_scale = left_arg->getScale();
                    for (size_t i = 0; i < src_size; ++i)
                        dst_data[i] = DecimalUtils::convertTo<Float64>(left_src_data[i], left_scale);

                    executeInIterationsStaticRight(dst_data.data(), right_src_data, dst_data.data(), src_size);
                }
                else
                {
                    executeInIterationsStaticRight(left_src_data.data(), right_src_data, dst_data.data(), src_size);
                }
            }
            else
            {
                RightType right_src_data[Impl::rows_per_iteration];
                std::fill(std::begin(right_src_data), std::end(right_src_data), right_arg_typed->template getValue<RightType>());

                if constexpr (is_decimal<LeftType>)
                {
                    UInt32 left_scale = left_arg->getScale();
                    for (size_t i = 0; i < src_size; ++i)
                        dst_data[i] = DecimalUtils::convertTo<Float64>(left_src_data[i], left_scale);

                    executeInIterationsStaticRight(dst_data.data(), right_src_data, dst_data.data(), src_size);
                }
                else
                {
                    executeInIterationsStaticRight(left_src_data.data(), right_src_data, dst_data.data(), src_size);
                }
            }

            return dst;
        }
        return nullptr;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & col_left = arguments[0];
        const ColumnWithTypeAndName & col_right = arguments[1];
        ColumnPtr res;

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftType = typename Types::LeftType;
            using RightType = typename Types::RightType;
            using ColVecLeft = ColumnVectorOrDecimal<LeftType>;

            const IColumn * left_arg = col_left.column.get();
            const IColumn * right_arg = col_right.column.get();

            if (const auto left_arg_typed = checkAndGetColumn<ColVecLeft>(left_arg))
            {
                if ((res = executeTyped<LeftType, RightType>(left_arg_typed, right_arg)))
                    return true;

                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}", right_arg->getName(), getName());
            }
            if (const auto left_arg_typed = checkAndGetColumnConst<ColVecLeft>(left_arg))
            {
                if ((res = executeTyped<LeftType, RightType>(left_arg_typed, right_arg)))
                    return true;

                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}", right_arg->getName(), getName());
            }

            return false;
        };

        TypeIndex left_index = col_left.type->getTypeId();
        TypeIndex right_index = col_right.type->getTypeId();

        if (!callOnBasicTypes<true, true, true, false>(left_index, right_index, call))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col_left.column->getName(), getName());

        return res;
    }
};

template <typename Name, Float64(Function)(Float64, Float64)>
struct BinaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * __restrict src_left, const T2 * __restrict src_right, Float64 * __restrict dst)
    {
        *dst = Function(static_cast<Float64>(*src_left), static_cast<Float64>(*src_right));
    }
};

}

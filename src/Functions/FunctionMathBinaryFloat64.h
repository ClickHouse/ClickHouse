#pragma once

#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>

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
        const auto check_argument_type = [this] (const IDataType * arg)
        {
            if (!isNativeNumber(arg) && !isDecimal(arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                    arg->getName(), getName());
        };

        check_argument_type(arguments.front().get());
        check_argument_type(arguments.back().get());

        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    template <typename Type>
    static ColumnPtr executeTyped(const ColumnConst * left_arg, const IColumn * right_arg, size_t input_rows_count)
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<Type>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            Type left_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(left_src_data), std::end(left_src_data), left_arg->template getValue<Type>());
            const auto & right_src_data = right_arg_typed->getData();
            auto & dst_data = dst->getData();
            dst_data.resize(input_rows_count);

            const auto rows_remaining = input_rows_count % Impl::rows_per_iteration;
            const auto rows_size = input_rows_count - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(left_src_data, &right_src_data[i], &dst_data[i]);

            if (rows_remaining != 0)
            {
                Type right_src_remaining[Impl::rows_per_iteration];
                memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(Type));
                memset(right_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(Type));
                Float64 dst_remaining[Impl::rows_per_iteration];

                Impl::execute(left_src_data, right_src_remaining, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            return dst;
        }

        return nullptr;
    }

    template <typename Type>
    static ColumnPtr executeTyped(const ColumnVector<Type> * left_arg, const IColumn * right_arg, size_t input_rows_count)
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<Type>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            const auto & right_src_data = right_arg_typed->getData();
            auto & dst_data = dst->getData();
            dst_data.resize(input_rows_count);

            const auto rows_remaining = input_rows_count % Impl::rows_per_iteration;
            const auto rows_size = input_rows_count - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&left_src_data[i], &right_src_data[i], &dst_data[i]);

            if (rows_remaining != 0)
            {
                Type left_src_remaining[Impl::rows_per_iteration];
                memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(Type));
                memset(left_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(Type));

                Type right_src_remaining[Impl::rows_per_iteration];
                memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(Type));
                memset(right_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(Type));

                Float64 dst_remaining[Impl::rows_per_iteration];

                Impl::execute(left_src_remaining, right_src_remaining, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            return dst;
        }
        if (const auto right_arg_typed = checkAndGetColumnConst<ColumnVector<Type>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            Type right_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(right_src_data), std::end(right_src_data), right_arg_typed->template getValue<Type>());
            auto & dst_data = dst->getData();
            dst_data.resize(input_rows_count);

            const auto rows_remaining = input_rows_count % Impl::rows_per_iteration;
            const auto rows_size = input_rows_count - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&left_src_data[i], right_src_data, &dst_data[i]);

            if (rows_remaining != 0)
            {
                Type left_src_remaining[Impl::rows_per_iteration];
                memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(Type));
                memset(left_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(Type));

                Float64 dst_remaining[Impl::rows_per_iteration];

                Impl::execute(left_src_remaining, right_src_data, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            return dst;
        }

        return nullptr;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col_left = arguments[0];
        const ColumnWithTypeAndName & col_right = arguments[1];

        ColumnPtr col_ptr_left = col_left.column;
        ColumnPtr col_ptr_right = col_right.column;

        TypeIndex left_index = col_left.type->getTypeId();
        TypeIndex right_index = col_right.type->getTypeId();

        /// There are two cases: if both types are float with 32-bit or less, we use the Float32 specialization,
        /// otherwise we the Float64 version of the function.

        bool use_32bit = WhichDataType(left_index).isFloat() && WhichDataType(right_index).isFloat()
            && col_left.type->getSizeOfValueInMemory() <= 4 && col_right.type->getSizeOfValueInMemory() <= 4;

        if (use_32bit)
        {
            col_ptr_left = castColumn(col_left, std::make_shared<DataTypeFloat32>());
            col_ptr_right = castColumn(col_right, std::make_shared<DataTypeFloat32>());
        }
        else
        {
            col_ptr_left = castColumn(col_left, std::make_shared<DataTypeFloat64>());
            col_ptr_right = castColumn(col_right, std::make_shared<DataTypeFloat64>());
        }

        ColumnPtr res;

        auto call = [&](const auto & type) -> bool
        {
            using Type = std::decay_t<decltype(type)>;
            using Column = ColumnVector<Type>;

            const IColumn * left_arg = col_ptr_left.get();
            const IColumn * right_arg = col_ptr_right.get();

            if (const auto left_arg_typed = checkAndGetColumn<Column>(left_arg))
            {
                if ((res = executeTyped<Type>(left_arg_typed, right_arg, input_rows_count)))
                    return true;

                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}",
                    right_arg->getName(), getName());
            }
            if (const auto left_arg_typed = checkAndGetColumnConst<Column>(left_arg))
            {
                if ((res = executeTyped<Type>(left_arg_typed, right_arg, input_rows_count)))
                    return true;

                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}",
                    right_arg->getName(), getName());
            }

            return false;
        };

        if (!(call(Float64()) || call(Float32())))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                col_left.column->getName(), getName());

        return res;
    }
};


template <typename Name, Float64(Function)(Float64, Float64)>
struct BinaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        dst[0] = Function(static_cast<Float64>(src_left[0]), static_cast<Float64>(src_right[0]));
    }
};

}

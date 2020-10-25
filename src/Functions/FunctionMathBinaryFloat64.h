#pragma once

#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

/** More efficient implementations of mathematical functions are possible when using a separate library.
  * Disabled due to license compatibility limitations.
  * To enable: download http://www.agner.org/optimize/vectorclass.zip and unpack to contrib/vectorclass
  * Then rebuild with -DENABLE_VECTORCLASS=1
  */


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
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathBinaryFloat64>(); }
    static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto check_argument_type = [this] (const IDataType * arg)
        {
            if (!isNativeNumber(arg))
                throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        };

        check_argument_type(arguments.front().get());
        check_argument_type(arguments.back().get());

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename LeftType, typename RightType>
    bool executeTyped(Block & block, const size_t result, const ColumnConst * left_arg, const IColumn * right_arg) const
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            LeftType left_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(left_src_data), std::end(left_src_data), left_arg->template getValue<LeftType>());
            const auto & right_src_data = right_arg_typed->getData();
            const auto src_size = right_src_data.size();
            auto & dst_data = dst->getData();
            dst_data.resize(src_size);

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

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            block.getByPosition(result).column = std::move(dst);
            return true;
        }

        return false;
    }

    template <typename LeftType, typename RightType>
    bool executeTyped(Block & block, const size_t result, const ColumnVector<LeftType> * left_arg, const IColumn * right_arg) const
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            const auto & right_src_data = right_arg_typed->getData();
            const auto src_size = left_src_data.size();
            auto & dst_data = dst->getData();
            dst_data.resize(src_size);

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

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            block.getByPosition(result).column = std::move(dst);
            return true;
        }
        if (const auto right_arg_typed = checkAndGetColumnConst<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            RightType right_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(right_src_data), std::end(right_src_data), right_arg_typed->template getValue<RightType>());
            const auto src_size = left_src_data.size();
            auto & dst_data = dst->getData();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&left_src_data[i], right_src_data, &dst_data[i]);

            if (rows_remaining != 0)
            {
                LeftType left_src_remaining[Impl::rows_per_iteration];
                memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
                memset(left_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
                Float64 dst_remaining[Impl::rows_per_iteration];

                Impl::execute(left_src_remaining, right_src_data, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            block.getByPosition(result).column = std::move(dst);
            return true;
        }

        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & col_left = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & col_right = block.getByPosition(arguments[1]);

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftType = typename Types::LeftType;
            using RightType = typename Types::RightType;
            using ColVecLeft = ColumnVector<LeftType>;

            const IColumn * left_arg = col_left.column.get();
            const IColumn * right_arg = col_right.column.get();

            if (const auto left_arg_typed = checkAndGetColumn<ColVecLeft>(left_arg))
            {
                if (executeTyped<LeftType, RightType>(block, result, left_arg_typed, right_arg))
                    return true;

                throw Exception{"Illegal column " + right_arg->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
            }
            if (const auto left_arg_typed = checkAndGetColumnConst<ColVecLeft>(left_arg))
            {
                if (executeTyped<LeftType, RightType>(block, result, left_arg_typed, right_arg))
                    return true;

                throw Exception{"Illegal column " + right_arg->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
            }

            return false;
        };

        TypeIndex left_index = col_left.type->getTypeId();
        TypeIndex right_index = col_right.type->getTypeId();

        if (!callOnBasicTypes<true, true, false, false>(left_index, right_index, call))
            throw Exception{"Illegal column " + col_left.column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }
};


template <typename Name, Float64(Function)(Float64, Float64)>
struct BinaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src_left[0]), static_cast<Float64>(src_right[0])));
    }
};

#define BinaryFunctionVectorized BinaryFunctionPlain

}

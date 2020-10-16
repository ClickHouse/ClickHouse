#pragma once

#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
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


/** FastOps is a fast vector math library from Mikhail Parakhin (former Yandex CTO),
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
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathUnary>(); }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & arg = arguments.front();
        if (!isNumber(arg))
            throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        /// Integers are converted to Float64.
        if (Impl::always_returns_float64 || !isFloat(arg))
            return std::make_shared<DataTypeFloat64>();
        else
            return arg;
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
                    tmp_vec[i] = src_data[i];

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

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(ReturnType));
            }
        }
    }

    template <typename T, typename ReturnType>
    static bool execute(Block & block, const ColumnVector<T> * col, const size_t result)
    {
        const auto & src_data = col->getData();
        const size_t size = src_data.size();

        auto dst = ColumnVector<ReturnType>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(size);

        executeInIterations(src_data.data(), dst_data.data(), size);

        block.getByPosition(result).column = std::move(dst);
        return true;
    }

    template <typename T, typename ReturnType>
    static bool execute(Block & block, const ColumnDecimal<T> * col, const size_t result)
    {
        const auto & src_data = col->getData();
        const size_t size = src_data.size();
        UInt32 scale = src_data.getScale();

        auto dst = ColumnVector<ReturnType>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(size);

        for (size_t i = 0; i < size; ++i)
            dst_data[i] = convertFromDecimal<DataTypeDecimal<T>, DataTypeNumber<ReturnType>>(src_data[i], scale);

        executeInIterations(dst_data.data(), dst_data.data(), size);

        block.getByPosition(result).column = std::move(dst);
        return true;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & col = block.getByPosition(arguments[0]);

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
            using ReturnType = std::conditional_t<Impl::always_returns_float64 || !std::is_floating_point_v<Type>, Float64, Type>;
            using ColVecType = std::conditional_t<IsDecimalNumber<Type>, ColumnDecimal<Type>, ColumnVector<Type>>;

            const auto col_vec = checkAndGetColumn<ColVecType>(col.column.get());
            return execute<Type, ReturnType>(block, col_vec, result);
        };

        if (!callOnBasicType<void, true, true, true, false>(col.type->getTypeId(), call))
            throw Exception{"Illegal column " + col.column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }
};


template <typename Name, Float64(Function)(Float64)>
struct UnaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;
    static constexpr bool always_returns_float64 = true;

    template <typename T>
    static void execute(const T * src, Float64 * dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src[0])));
    }
};

#define UnaryFunctionVectorized UnaryFunctionPlain

}

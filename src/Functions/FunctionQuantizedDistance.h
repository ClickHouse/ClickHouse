#pragma once

#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "Columns/ColumnFixedString.h"
#include "DataTypes/DataTypeFixedString.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"

namespace DB
{

template <typename Kernel, const char * function_name>
class FunctionQuantizedL2Distance : public IFunction
{
public:
    static constexpr auto name = function_name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantizedL2Distance<Kernel, function_name>>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeFloat32>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeWithResultTypeAndLeftTypeAndRightType(arguments[0].column, arguments[1].column, input_rows_count, arguments);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    using T = typename Kernel::ValueType;

    ColumnPtr executeWithResultTypeAndLeftTypeAndRightType(
        ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        using ResultType = Float32;

        if (col_x->isConst())
            return executeWithLeftArgConst(col_x, col_y, input_rows_count, arguments);
        if (col_y->isConst())
            return executeWithLeftArgConst(col_y, col_x, input_rows_count, arguments);

        const auto & array_x = *assert_cast<const ColumnFixedString *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnFixedString *>(col_y.get());

        const T * __restrict__ data_x = reinterpret_cast<const T *>(array_x.getChars().data());
        const T * __restrict__ data_y = reinterpret_cast<const T *>(array_y.getChars().data());

        size_t fixed_size = array_x.getN() / sizeof(T);

        auto col_res = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = col_res->getData();

        ColumnArray::Offset prev = 0;
        size_t row = 0;

        for (size_t off = fixed_size; off <= fixed_size * input_rows_count; off += fixed_size)
        {
            static constexpr size_t VEC_SIZE = 4;
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; prev + VEC_SIZE < off; prev += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                    Kernel::template accumulate<ResultType>(states[s], data_x[prev + s], data_y[prev + s]);
            }

            typename Kernel::template State<ResultType> state;
            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state);

            for (; prev < off; ++prev)
            {
                Kernel::template accumulate<ResultType>(state, data_x[prev], data_y[prev]);
            }
            result_data[row] = Kernel::finalize(state);
            ++row;
        }

        return col_res;
    }

    ColumnPtr executeWithLeftArgConst(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName &) const
    {
        using ResultType = Float32;

        col_x = assert_cast<const ColumnConst *>(col_x.get())->getDataColumnPtr();
        col_y = col_y->convertToFullColumnIfConst();

        const auto & array_x = *assert_cast<const ColumnFixedString *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnFixedString *>(col_y.get());

        const T * __restrict__ data_x = reinterpret_cast<const T *>(array_x.getChars().data());
        const T * __restrict__ data_y = reinterpret_cast<const T *>(array_y.getChars().data());

        size_t fixed_size = array_x.getN() / sizeof(T);

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        size_t prev = 0;
        size_t row = 0;

        for (size_t off = fixed_size; off <= fixed_size * input_rows_count; off += fixed_size)
        {
            size_t i = 0;
            typename Kernel::template State<ResultType> state;

            static constexpr size_t VEC_SIZE = 32;
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; prev + VEC_SIZE < off; i += VEC_SIZE, prev += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                    Kernel::template accumulate<ResultType>(states[s], data_x[i + s], data_y[prev + s]);
            }

            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state);


            for (; prev < off; ++i, ++prev)
            {
                Kernel::template accumulate<ResultType>(state, data_x[i], data_y[prev]);
            }
            result_data[row] = Kernel::finalize(state);
            row++;
        }

        return result;
    }
};
}

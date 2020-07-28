#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Interpreters/castColumn.h>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionRange : public IFunction
{
public:
    static constexpr auto name = "range";
    static constexpr size_t max_elements = 100'000'000;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRange>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 3 || arguments.empty())
        {
            throw Exception{"Function " + getName() + " needs 1..3 arguments; passed "
                            + std::to_string(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
        }

        for (const auto & arg : arguments)
        {
            if (!isUnsignedInteger(arg))
                throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        DataTypePtr common_type = getLeastSupertype(arguments);
        return std::make_shared<DataTypeArray>(common_type);
    }

    template <typename T>
    bool executeInternal(Block & block, const IColumn * arg, const size_t result) const
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(arg))
        {
            const auto & in_data = in->getData();
            const auto total_values = std::accumulate(std::begin(in_data), std::end(in_data), size_t{},
                [this] (const size_t lhs, const size_t rhs)
                {
                    const auto sum = lhs + rhs;
                    if (sum < lhs)
                        throw Exception{"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

                    return sum;
                });

            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            auto data_col = ColumnVector<T>::create(total_values);
            auto offsets_col = ColumnArray::ColumnOffsets::create(in->size());

            auto & out_data = data_col->getData();
            auto & out_offsets = offsets_col->getData();

            IColumn::Offset offset{};
            for (size_t row_idx = 0, rows = in->size(); row_idx < rows; ++row_idx)
            {
                for (size_t elem_idx = 0, elems = in_data[row_idx]; elem_idx < elems; ++elem_idx)
                    out_data[offset + elem_idx] = elem_idx;

                offset += in_data[row_idx];
                out_offsets[row_idx] = offset;
            }

            block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
            return true;
        }
        else
            return false;
    }

    template <typename T>
    bool executeConstStartStep(Block & block, const IColumn * end_arg, const T start, const T step, const size_t input_rows_count, const size_t result) const
    {
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        if (!end_column)
        {
            return false;
        }

        const auto & end_data = end_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start < end_data[row_idx] && step == 0)
                throw Exception{"A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            pre_values += start >= end_data[row_idx] ? 0
                            : (end_data[row_idx] - start - 1) / step + 1;

            if (pre_values < total_values)
                throw Exception{"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t st = start, ed = end_data[row_idx]; st < ed; st += step)
                out_data[offset++] = st;

            out_offsets[row_idx] = offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        return true;
    }

    template <typename T>
    bool executeConstStep(Block & block, const IColumn * start_arg, const IColumn * end_arg, const T step, const size_t input_rows_count, const size_t result) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_arg);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        if (!end_column || !start_column)
        {
            return false;
        }

        const auto & start_data = start_column->getData();
        const auto & end_data = end_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start_data[row_idx] < end_data[row_idx] && step == 0)
                throw Exception{"A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            pre_values += start_data[row_idx] >= end_data[row_idx] ? 0
                            : (end_data[row_idx] - start_data[row_idx] - 1) / step + 1;

            if (pre_values < total_values)
                throw Exception{"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t st = start_data[row_idx], ed = end_data[row_idx]; st < ed; st += step)
                out_data[offset++] = st;

            out_offsets[row_idx] = offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        return true;
    }

    template <typename T>
    bool executeConstStart(Block & block, const IColumn * end_arg, const IColumn * step_arg, const T start, const size_t input_rows_count, const size_t result) const
    {
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        auto step_column = checkAndGetColumn<ColumnVector<T>>(step_arg);
        if (!end_column || !step_column)
        {
            return false;
        }

        const auto & end_data = end_column->getData();
        const auto & step_data = step_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start < end_data[row_idx] && step_data[row_idx] == 0)
                throw Exception{"A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            pre_values += start >= end_data[row_idx] ? 0
                            : (end_data[row_idx] - start - 1) / step_data[row_idx] + 1;

            if (pre_values < total_values)
                throw Exception{"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t st = start, ed = end_data[row_idx]; st < ed; st += step_data[row_idx])
                out_data[offset++] = st;

            out_offsets[row_idx] = offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        return true;
    }

    template <typename T>
    bool executeGeneric(Block & block, const IColumn * start_col, const IColumn * end_col, const IColumn * step_col, const size_t input_rows_count, const size_t result) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_col);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_col);
        auto step_column = checkAndGetColumn<ColumnVector<T>>(step_col);

        if (!start_column || !end_column || !step_column)
        {
            return false;
        }

        const auto & start_data = start_column->getData();
        const auto & end_start = end_column->getData();
        const auto & step_data = step_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (start_data[row_idx] < end_start[row_idx] && step_data[row_idx] == 0)
                throw Exception{"A call to function " + getName() + " overflows, the 3rd argument step can't be zero",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            pre_values += start_data[row_idx] >= end_start[row_idx] ? 0
                            : (end_start[row_idx] -start_data[row_idx] - 1) / (step_data[row_idx]) + 1;

            if (pre_values < total_values)
                throw Exception{"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t st = start_data[row_idx], ed = end_start[row_idx]; st < ed; st += step_data[row_idx])
                out_data[offset++] = st;

            out_offsets[row_idx] = offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        return true;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        if (arguments.size() == 1)
        {
            const auto * col = block.getByPosition(arguments[0]).column.get();
            if (!executeInternal<UInt8>(block, col, result) &&
                !executeInternal<UInt16>(block, col, result) &&
                !executeInternal<UInt32>(block, col, result) &&
                !executeInternal<UInt64>(block, col, result))
            {
                throw Exception{"Illegal column " + col->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
            }
            return;
        }

        Columns columns_holder(3);
        ColumnRawPtrs columns(3);

        const auto return_type = checkAndGetDataType<DataTypeArray>(block.getByPosition(result).type.get())->getNestedType();

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (i == 1)
                columns_holder[i] = castColumn(block.getByPosition(arguments[i]), return_type)->convertToFullColumnIfConst();
            else
                columns_holder[i] = castColumn(block.getByPosition(arguments[i]), return_type);

            columns[i] = columns_holder[i].get();
        }

        // for step column, defaults to 1
        if (arguments.size() == 2)
        {
            columns_holder[2] = return_type->createColumnConst(input_rows_count, 1);
            columns[2] = columns_holder[2].get();
        }

        bool is_start_const = isColumnConst(*columns[0]);
        bool is_step_const = isColumnConst(*columns[2]);
        bool ok;
        if (is_start_const && is_step_const)
        {
            UInt64 start = assert_cast<const ColumnConst &>(*columns[0]).getUInt(0);
            UInt64 step = assert_cast<const ColumnConst &>(*columns[2]).getUInt(0);

            ok = executeConstStartStep<UInt8>(block, columns[1], start, step, input_rows_count, result) ||
                executeConstStartStep<UInt16>(block, columns[1], start, step, input_rows_count, result) ||
                executeConstStartStep<UInt32>(block, columns[1], start, step, input_rows_count, result) ||
                executeConstStartStep<UInt64>(block, columns[1], start, step, input_rows_count, result);
        }
        else if (is_start_const && !is_step_const)
        {
            UInt64 start = assert_cast<const ColumnConst &>(*columns[0]).getUInt(0);

            ok = executeConstStart<UInt8>(block, columns[1], columns[2], start, input_rows_count, result) ||
                executeConstStart<UInt16>(block, columns[1], columns[2], start, input_rows_count, result) ||
                executeConstStart<UInt32>(block, columns[1], columns[2], start, input_rows_count, result) ||
                executeConstStart<UInt64>(block, columns[1], columns[2], start, input_rows_count, result);
        }
        else if (!is_start_const && is_step_const)
        {
            UInt64 step = assert_cast<const ColumnConst &>(*columns[2]).getUInt(0);

            ok = executeConstStep<UInt8>(block, columns[0], columns[1], step, input_rows_count, result) ||
                executeConstStep<UInt16>(block, columns[0], columns[1], step, input_rows_count, result) ||
                executeConstStep<UInt32>(block, columns[0], columns[1], step, input_rows_count, result) ||
                executeConstStep<UInt64>(block, columns[0], columns[1], step, input_rows_count, result);
        }
        else
        {
            ok = executeGeneric<UInt8>(block, columns[0], columns[1], columns[2], input_rows_count, result) ||
                executeGeneric<UInt16>(block, columns[0], columns[1], columns[2], input_rows_count, result) ||
                executeGeneric<UInt32>(block, columns[0], columns[1], columns[2], input_rows_count, result) ||
                executeGeneric<UInt64>(block, columns[0], columns[1], columns[2], input_rows_count, result);
        }

        if (!ok)
        {
            throw Exception{"Illegal columns " + columns[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }

};


void registerFunctionRange(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRange>();
}

}

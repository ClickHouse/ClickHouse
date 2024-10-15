#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Settings.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>
#include <numeric>
#include <vector>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 function_range_max_elements_in_block;
}

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


/** Generates array
  * range(size): [0, size)
  * range(start, end): [start, end)
  * range(start, end, step): [start, end) with step increments.
  */
class FunctionRange : public IFunction
{
public:
    static constexpr auto name = "range";

    const size_t max_elements;
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionRange>(std::move(context_)); }
    explicit FunctionRange(ContextPtr context) : max_elements(context->getSettingsRef()[Setting::function_range_max_elements_in_block]) { }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 3 || arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs 1..3 arguments; passed {}.",
                getName(), arguments.size());
        }

        if (std::find_if (arguments.cbegin(), arguments.cend(), [](const auto & arg) { return arg->onlyNull(); }) != arguments.cend())
            return makeNullable(std::make_shared<DataTypeNothing>());

        DataTypes arg_types;
        for (size_t i = 0, size = arguments.size(); i < size; ++i)
        {
            DataTypePtr type_no_nullable = removeNullable(arguments[i]);

            if (i < 2 && WhichDataType(type_no_nullable).isIPv4())
                arg_types.emplace_back(std::make_shared<DataTypeUInt32>());
            else if (isInteger(type_no_nullable))
                arg_types.push_back(type_no_nullable);
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                    arguments[i]->getName(), getName());
        }

        DataTypePtr common_type = getLeastSupertype(arg_types);
        return std::make_shared<DataTypeArray>(common_type);
    }

    template <typename T>
    ColumnPtr executeInternal(const IColumn * arg) const
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(arg))
        {
            const auto & in_data = in->getData();
            const auto total_values = std::accumulate(std::begin(in_data), std::end(in_data), size_t{},
                [this] (const size_t lhs, const T rhs)
                {
                    if (rhs < 0)
                        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                        "A call to function {} overflows, only support positive values when only end is provided", getName());

                    const auto sum = lhs + rhs;
                    if (sum < lhs)
                        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                        "A call to function {} overflows, investigate the values "
                                        "of arguments you are passing", getName());

                    return sum;
                });

            if (total_values > max_elements)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "A call to function {} would produce {} array elements, which "
                                "is greater than the allowed maximum of {}",
                                getName(), std::to_string(total_values), std::to_string(max_elements));

            auto data_col = ColumnVector<T>::create(total_values);
            auto offsets_col = ColumnArray::ColumnOffsets::create(in->size());

            auto & out_data = data_col->getData();
            auto & out_offsets = offsets_col->getData();

            IColumn::Offset offset{};
            for (size_t row_idx = 0, rows = in->size(); row_idx < rows; ++row_idx)
            {
                for (T elem_idx = 0, elems = in_data[row_idx]; elem_idx < elems; ++elem_idx)
                    out_data[offset + elem_idx] = elem_idx;

                offset += in_data[row_idx];
                out_offsets[row_idx] = offset;
            }

            return ColumnArray::create(std::move(data_col), std::move(offsets_col));
        }
        return nullptr;
    }

    template <typename T>
    ColumnPtr executeConstStartStep(
            const IColumn * end_arg, const T start, const T step, const size_t input_rows_count) const
    {
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        if (!end_column)
            return nullptr;

        const auto & end_data = end_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;
        PODArray<size_t> row_length(input_rows_count);

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step == 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "A call to function {} overflows, the 3rd argument step can't be zero", getName());

            if (start < end_data[row_idx] && step > 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_data[row_idx]) - static_cast<__int128_t>(start) - 1) / static_cast<__int128_t>(step) + 1;
            else if (start > end_data[row_idx] && step < 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_data[row_idx]) - static_cast<__int128_t>(start) + 1) / static_cast<__int128_t>(step) + 1;
            else
                row_length[row_idx] = 0;

            pre_values += row_length[row_idx];

            if (pre_values < total_values)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                        "A call to function {} overflows, investigate the values "
                                        "of arguments you are passing", getName());

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "A call to function {} would produce {} array elements, which "
                                "is greater than the allowed maximum of {}",
                                getName(), std::to_string(total_values), std::to_string(max_elements));
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t idx = 0; idx < row_length[row_idx]; ++idx)
            {
                out_data[offset] = static_cast<T>(start + idx * step);
                ++offset;
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    template <typename T>
    ColumnPtr executeConstStep(
        const IColumn * start_arg, const IColumn * end_arg, const T step, const size_t input_rows_count) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_arg);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        if (!end_column || !start_column)
            return nullptr;

        const auto & start_data = start_column->getData();
        const auto & end_data = end_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;
        PODArray<size_t> row_length(input_rows_count);

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step == 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "A call to function {} overflows, the 3rd argument step can't be zero", getName());

            if (start_data[row_idx] < end_data[row_idx] && step > 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_data[row_idx]) - static_cast<__int128_t>(start_data[row_idx]) - 1) / static_cast<__int128_t>(step) + 1;
            else if (start_data[row_idx] > end_data[row_idx] && step < 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_data[row_idx]) - static_cast<__int128_t>(start_data[row_idx]) + 1) / static_cast<__int128_t>(step) + 1;
            else
                row_length[row_idx] = 0;

            pre_values += row_length[row_idx];

            if (pre_values < total_values)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                        "A call to function {} overflows, investigate the values "
                                        "of arguments you are passing", getName());

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "A call to function {} would produce {} array elements, which "
                                "is greater than the allowed maximum of {}",
                                getName(), std::to_string(total_values), std::to_string(max_elements));
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t idx = 0; idx < row_length[row_idx]; ++idx)
            {
                out_data[offset] = static_cast<T>(start_data[row_idx] + idx * step);
                ++offset;
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    template <typename T>
    ColumnPtr executeConstStart(
            const IColumn * end_arg, const IColumn * step_arg, const T start, const size_t input_rows_count) const
    {
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_arg);
        auto step_column = checkAndGetColumn<ColumnVector<T>>(step_arg);
        if (!end_column || !step_column)
            return nullptr;

        const auto & end_data = end_column->getData();
        const auto & step_data = step_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;
        PODArray<size_t> row_length(input_rows_count);

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step_data[row_idx] == 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "A call to function {} overflows, the 3rd argument step can't be zero", getName());

            if (start < end_data[row_idx] && step_data[row_idx] > 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_data[row_idx]) - static_cast<__int128_t>(start) - 1) / static_cast<__int128_t>(step_data[row_idx]) + 1;
            else if (start > end_data[row_idx] && step_data[row_idx] < 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_data[row_idx]) - static_cast<__int128_t>(start) + 1) / static_cast<__int128_t>(step_data[row_idx]) + 1;
            else
                row_length[row_idx] = 0;

            pre_values += row_length[row_idx];

            if (pre_values < total_values)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                        "A call to function {} overflows, investigate the values "
                                        "of arguments you are passing", getName());

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "A call to function {} would produce {} array elements, which "
                                "is greater than the allowed maximum of {}",
                                getName(), std::to_string(total_values), std::to_string(max_elements));
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t idx = 0; idx < row_length[row_idx]; ++idx)
            {
                out_data[offset] = static_cast<T>(start + idx * step_data[row_idx]);
                ++offset;
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    template <typename T>
    ColumnPtr executeGeneric(
        const IColumn * start_col, const IColumn * end_col, const IColumn * step_col, const size_t input_rows_count) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_col);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_col);
        auto step_column = checkAndGetColumn<ColumnVector<T>>(step_col);

        if (!start_column || !end_column || !step_column)
            return nullptr;

        const auto & start_data = start_column->getData();
        const auto & end_start = end_column->getData();
        const auto & step_data = step_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;
        PODArray<size_t> row_length(input_rows_count);

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if (step_data[row_idx] == 0)
                throw Exception{ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "A call to function {} underflows, the 3rd argument step can't be less or equal to zero", getName()};
            if (start_data[row_idx] < end_start[row_idx] && step_data[row_idx] > 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_start[row_idx]) - static_cast<__int128_t>(start_data[row_idx]) - 1) / static_cast<__int128_t>(step_data[row_idx]) + 1;
            else if (start_data[row_idx] > end_start[row_idx] && step_data[row_idx] < 0)
                row_length[row_idx] = (static_cast<__int128_t>(end_start[row_idx]) - static_cast<__int128_t>(start_data[row_idx]) + 1) / static_cast<__int128_t>(step_data[row_idx]) + 1;
            else
                row_length[row_idx] = 0;

            pre_values += row_length[row_idx];

            if (pre_values < total_values)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                        "A call to function {} overflows, investigate the values "
                                        "of arguments you are passing", getName());

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "A call to function {} would produce {} array elements, which "
                                "is greater than the allowed maximum of {}",
                                getName(), std::to_string(total_values), std::to_string(max_elements));
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            for (size_t idx = 0; idx < row_length[row_idx]; ++idx)
            {
                out_data[offset] = static_cast<T>(start_data[row_idx] + idx * step_data[row_idx]);
                ++offset;
            }
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        NullPresence null_presence = getNullPresense(arguments);
        if (null_presence.has_null_constant)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        DataTypePtr elem_type = checkAndGetDataType<DataTypeArray>(result_type.get())->getNestedType();
        WhichDataType which(elem_type);

        if (!which.isNativeUInt() && !which.isNativeInt())
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal columns of arguments of function {}, the function only implemented "
                            "for unsigned/signed integers up to 64 bit", getName());
        }

        auto throwIfNullValue = [&](const ColumnWithTypeAndName & col)
        {
            if (!col.type->isNullable())
                return;
            const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(col.column.get());
            if (!nullable_col)
                nullable_col = checkAndGetColumnConstData<ColumnNullable>(col.column.get());
            if (!nullable_col)
                return;
            const auto & null_map = nullable_col->getNullMapData();
            if (!memoryIsZero(null_map.data(), 0, null_map.size()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal (null) value column {} of argument of function {}", col.column->getName(), getName());
        };

        ColumnPtr res;
        if (arguments.size() == 1)
        {
            throwIfNullValue(arguments[0]);
            const auto * col = arguments[0].column.get();
            if (arguments[0].type->isNullable())
            {
                const auto & nullable = checkAndGetColumn<ColumnNullable>(*arguments[0].column);
                col = nullable.getNestedColumnPtr().get();
            }

            if (!((res = executeInternal<UInt8>(col)) || (res = executeInternal<UInt16>(col)) || (res = executeInternal<UInt32>(col))
                  || (res = executeInternal<UInt64>(col)) || (res = executeInternal<Int8>(col)) || (res = executeInternal<Int16>(col))
                  || (res = executeInternal<Int32>(col)) || (res = executeInternal<Int64>(col))))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col->getName(), getName());
            }
            return res;
        }

        Columns columns_holder(3);
        ColumnRawPtrs column_ptrs(3);

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            throwIfNullValue(arguments[i]);
            if (i == 1)
                columns_holder[i] = castColumn(arguments[i], elem_type)->convertToFullColumnIfConst();
            else
                columns_holder[i] = castColumn(arguments[i], elem_type);

            column_ptrs[i] = columns_holder[i].get();
        }

        /// Step is one by default.
        if (arguments.size() == 2)
        {
            /// Convert a column with constant 1 to the result type.
            columns_holder[2] = castColumn(
                {DataTypeUInt8().createColumnConst(input_rows_count, 1), std::make_shared<DataTypeUInt8>(), {}},
                elem_type);

            column_ptrs[2] = columns_holder[2].get();
        }

        bool is_start_const = isColumnConst(*column_ptrs[0]);
        bool is_step_const = isColumnConst(*column_ptrs[2]);
        if (is_start_const && is_step_const)
        {
            if (which.isNativeUInt())
            {
                UInt64 start = assert_cast<const ColumnConst &>(*column_ptrs[0]).getUInt(0);
                UInt64 step = assert_cast<const ColumnConst &>(*column_ptrs[2]).getUInt(0);

                if ((res = executeConstStartStep<UInt8>(column_ptrs[1], start, step, input_rows_count))
                    || (res = executeConstStartStep<UInt16>(column_ptrs[1], start, step, input_rows_count))
                    || (res = executeConstStartStep<UInt32>(
                            column_ptrs[1], static_cast<UInt32>(start), static_cast<UInt32>(step), input_rows_count))
                    || (res = executeConstStartStep<UInt64>(column_ptrs[1], start, step, input_rows_count)))
                {
                }
            }
            else if (which.isNativeInt())
            {
                Int64 start = assert_cast<const ColumnConst &>(*column_ptrs[0]).getInt(0);
                Int64 step = assert_cast<const ColumnConst &>(*column_ptrs[2]).getInt(0);

                if ((res = executeConstStartStep<Int8>(column_ptrs[1], start, step, input_rows_count))
                    || (res = executeConstStartStep<Int16>(column_ptrs[1], start, step, input_rows_count))
                    || (res = executeConstStartStep<Int32>(
                            column_ptrs[1], static_cast<Int32>(start), static_cast<Int32>(step), input_rows_count))
                    || (res = executeConstStartStep<Int64>(column_ptrs[1], start, step, input_rows_count)))
                {
                }
            }
        }
        else if (is_start_const && !is_step_const)
        {
            if (which.isNativeUInt())
            {
                UInt64 start = assert_cast<const ColumnConst &>(*column_ptrs[0]).getUInt(0);

                if ((res = executeConstStart<UInt8>(column_ptrs[1], column_ptrs[2], start, input_rows_count))
                    || (res = executeConstStart<UInt16>(column_ptrs[1], column_ptrs[2], start, input_rows_count))
                    || (res = executeConstStart<UInt32>(column_ptrs[1], column_ptrs[2], static_cast<UInt32>(start), input_rows_count))
                    || (res = executeConstStart<UInt64>(column_ptrs[1], column_ptrs[2], start, input_rows_count)))
                {
                }
            }
            else if (which.isNativeInt())
            {
                Int64 start = assert_cast<const ColumnConst &>(*column_ptrs[0]).getInt(0);

                if ((res = executeConstStart<Int8>(column_ptrs[1], column_ptrs[2], start, input_rows_count))
                    || (res = executeConstStart<Int16>(column_ptrs[1], column_ptrs[2], start, input_rows_count))
                    || (res = executeConstStart<Int32>(column_ptrs[1], column_ptrs[2], static_cast<Int32>(start), input_rows_count))
                    || (res = executeConstStart<Int64>(column_ptrs[1], column_ptrs[2], start, input_rows_count)))
                {
                }
            }
        }
        else if (!is_start_const && is_step_const)
        {
            if (which.isNativeUInt())
            {
                UInt64 step = assert_cast<const ColumnConst &>(*column_ptrs[2]).getUInt(0);

                if ((res = executeConstStep<UInt8>(column_ptrs[0], column_ptrs[1], step, input_rows_count))
                    || (res = executeConstStep<UInt16>(column_ptrs[0], column_ptrs[1], step, input_rows_count))
                    || (res = executeConstStep<UInt32>(column_ptrs[0], column_ptrs[1], static_cast<UInt32>(step), input_rows_count))
                    || (res = executeConstStep<UInt64>(column_ptrs[0], column_ptrs[1], step, input_rows_count)))
                {
                }
            }
            else if (which.isNativeInt())
            {
                Int64 step = assert_cast<const ColumnConst &>(*column_ptrs[2]).getInt(0);

                if ((res = executeConstStep<Int8>(column_ptrs[0], column_ptrs[1], step, input_rows_count))
                    || (res = executeConstStep<Int16>(column_ptrs[0], column_ptrs[1], step, input_rows_count))
                    || (res = executeConstStep<Int32>(column_ptrs[0], column_ptrs[1], static_cast<Int32>(step), input_rows_count))
                    || (res = executeConstStep<Int64>(column_ptrs[0], column_ptrs[1], step, input_rows_count)))
                {
                }
            }
        }
        else
        {
            if ((res = executeGeneric<UInt8>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<UInt16>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<UInt32>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<UInt64>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int8>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int16>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int32>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count))
                || (res = executeGeneric<Int64>(column_ptrs[0], column_ptrs[1], column_ptrs[2], input_rows_count)))
            {
            }
        }

        if (!res)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns {} of argument of function {}", column_ptrs[0]->getName(), getName());
        }

        return res;
    }

};


REGISTER_FUNCTION(Range)
{
    factory.registerFunction<FunctionRange>();
}

}

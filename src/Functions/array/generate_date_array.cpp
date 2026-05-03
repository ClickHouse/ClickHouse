#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/IntervalKind.h>


namespace DB
{
namespace Setting
{
extern const SettingsUInt64 function_generate_date_array_max_elements_in_block;
}

namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
}


/** Generates date array
  * GENERATE_DATE_ARRAY(start_date_expr, end_date_expr[, step_expr])
  */
class FunctionGenerateDateArray : public IFunction
{
public:
    static constexpr auto name = "generate_date_array";

    const size_t max_elements;
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGenerateDateArray>(std::move(context_)); }
    explicit FunctionGenerateDateArray(ContextPtr context)
        : max_elements(context->getSettingsRef()[Setting::function_generate_date_array_max_elements_in_block])
    {
    }

private:
    String getName() const override { return name; }
    // Return 0 since this is a variadic function
    size_t getNumberOfArguments() const override { return 0; }
    // Can take 2 or 3 arguments (default argument counts as new one)
    bool isVariadic() const override { return true; }
    // While any null in the input should cause it to return NULL,
    //  it seems (from the documentation) that if some arguments are nullable, the code still gets called
    //  So we would still need to write code that is "safe" regardless of what is passed
    //  At the very least, this would cause massive allocation and inefficient execution
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 3)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs 2..3 arguments; passed {}.", getName(), arguments.size());
        }

        // If any argument is NULL, returns NULL
        if (std::find_if(arguments.cbegin(), arguments.cend(), [](const auto & arg) { return arg->onlyNull(); }) != arguments.cend())
            return makeNullable(std::make_shared<DataTypeNothing>());

        DataTypes arg_types;
        for (size_t i = 0, size = arguments.size(); i < size; ++i)
        {
            DataTypePtr type_no_nullable = removeNullable(arguments[i]);
            if (i == 2)
            {
                if (!isInterval(type_no_nullable))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument of function {}. Expected an interval type.",
                        arguments[i]->getName(),
                        getName());
                const auto * data_type_interval{checkAndGetDataType<DataTypeInterval>(type_no_nullable.get())};
                switch (data_type_interval->getKind())
                {
                    case IntervalKind::Kind::Day:
                    case IntervalKind::Kind::Week:
                    case IntervalKind::Kind::Month:
                    case IntervalKind::Kind::Quarter:
                    case IntervalKind::Kind::Year:
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument of function {}. Expected an interval type of day, week, month, quarter or year "
                            "kind.",
                            arguments[i]->getName(),
                            getName());
                }
            }
            else
            {
                // Treat String as Date — strings will be cast to the resolved date type at execution time.
                // This allows passing date strings directly without explicit casting.
                DataTypePtr resolved = isString(type_no_nullable) ? std::make_shared<DataTypeDate>() : type_no_nullable;
                arg_types.push_back(resolved);
            }
        }

        DataTypePtr common_type = getLeastSupertype(arg_types);
        return std::make_shared<DataTypeArray>(common_type);
    }

    template <typename T>
    T addInterval(T value, const IntervalKind kind, Int64 step) const
    {
        switch (kind)
        {
            case IntervalKind::Kind::Day:
                return AddDaysImpl::execute(value, step, DateLUT::instance(), DateLUT::instance("UTC"), 0);
            case IntervalKind::Kind::Week:
                return AddWeeksImpl::execute(value, step, DateLUT::instance(), DateLUT::instance("UTC"), 0);
            case IntervalKind::Kind::Month:
                return AddMonthsImpl::execute(value, step, DateLUT::instance(), DateLUT::instance("UTC"), 0);
            case IntervalKind::Kind::Quarter:
                return AddQuartersImpl::execute(value, step, DateLUT::instance(), DateLUT::instance("UTC"), 0);
            case IntervalKind::Kind::Year:
                return AddYearsImpl::execute(value, step, DateLUT::instance(), DateLUT::instance("UTC"), 0);
            default:
                UNREACHABLE();
        }
    }

    template <typename T>
    size_t getIntervalCount(T start, T end, Int64 interval_step, IntervalKind interval_kind) const
    {
        if (interval_step == 0)
            throw Exception{
                ErrorCodes::BAD_ARGUMENTS, "Invalid argument to function {}, the 3rd argument step can't equal to zero", getName()};

        size_t count = 0;
        T currentDate = start;
        if (interval_step > 0)
        {
            while (currentDate <= end)
            {
                currentDate = addInterval(currentDate, interval_kind, interval_step);
                ++count;
            }
            return count;
        }
        else
        {
            while (currentDate >= end)
            {
                currentDate = addInterval(currentDate, interval_kind, interval_step);
                ++count;
            }
            return count;
        }
    }

    template <typename T>
    ColumnPtr executeGeneric(
        const IColumn * start_col,
        const IColumn * end_col,
        const IColumn * step_col,
        const IntervalKind interval_kind,
        const size_t input_rows_count) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_col);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_col);
        auto step_column = checkAndGetColumn<ColumnVector<Int64>>(step_col);

        if (!start_column || !end_column || !step_column)
            return nullptr;

        const auto & start_data = start_column->getData();
        const auto & end_start = end_column->getData();
        const auto & step_data = step_column->getData();

        size_t total_values = 0;
        PODArray<size_t> row_length(input_rows_count);

        // Count the output size
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            row_length[row_idx] = getIntervalCount(start_data[row_idx], end_start[row_idx], step_data[row_idx], interval_kind);
            total_values += row_length[row_idx];

            if (total_values > max_elements)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "A call to function {} would produce {} array elements, which "
                    "is greater than the allowed maximum of {}",
                    getName(),
                    std::to_string(total_values),
                    std::to_string(max_elements));
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            T currentDate;
            currentDate = start_data[row_idx];
            Int64 step = step_data[row_idx];
            for (size_t idx = 0; idx < row_length[row_idx]; ++idx)
            {
                out_data[offset] = currentDate;
                currentDate = addInterval(currentDate, interval_kind, step);
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

        if (!which.isDateOrDate32())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal columns of arguments of function {}, the function only implemented for Date/Date32 types",
                getName());
        }

        auto throw_if_null_value = [&](const ColumnWithTypeAndName & col)
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
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Illegal (null) value column {} of argument of function {}",
                    col.column->getName(),
                    getName());
        };

        ColumnPtr res;
        Columns columns_holder(3);
        ColumnRawPtrs column_ptrs(3);

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            throw_if_null_value(arguments[i]);
            if (i == 2)
                columns_holder[i] = castColumn(arguments[i], std::make_shared<DataTypeInt64>())->convertToFullColumnIfConst();
            else
                columns_holder[i] = castColumn(arguments[i], elem_type);

            column_ptrs[i] = columns_holder[i].get();
        }

        IntervalKind interval_kind = IntervalKind::Kind::Day;
        if (arguments.size() == 3)
        {
            if (const auto * interval_type = checkAndGetDataType<DataTypeInterval>(removeNullable(arguments[2].type).get()))
                interval_kind = interval_type->getKind();
        }

        /// Step is one Day by default.
        if (arguments.size() == 2)
        {
            /// Convert a column with constant 1 to the result type.
            columns_holder[2] = DataTypeInt64().createColumnConst(input_rows_count, Int64{1})->convertToFullColumnIfConst();
            column_ptrs[2] = columns_holder[2].get();
        }

        if ((res = executeGeneric<UInt16>(column_ptrs[0], column_ptrs[1], column_ptrs[2], interval_kind, input_rows_count))
            || (res = executeGeneric<Int32>(column_ptrs[0], column_ptrs[1], column_ptrs[2], interval_kind, input_rows_count)))
        {
        }

        if (!res)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal columns {} of argument of function {}", column_ptrs[0]->getName(), getName());
        }

        return res;
    }
};


REGISTER_FUNCTION(GenerateDateArray)
{
    FunctionDocumentation::Description description = R"(
Returns an array of dates from `start` to `end` (inclusive) incremented by `step`.

- `start` and `end` accept `Date`, `Date32`, or a string literal that is implicitly cast to `Date`.
- When `start` and `end` have different types, the element type of the returned array is their least supertype (e.g. mixing `Date` and `Date32` yields `Array(Date32)`).
- `step` must be a non-zero interval of kind `Day`, `Week`, `Month`, `Quarter`, or `Year`. When omitted, the default step is `INTERVAL 1 DAY`.
- If `step` is positive the sequence is ascending. If `step` is negative the sequence is descending.
- If `end` is unreachable in the given direction, an empty array is returned.
- Returns `NULL` if any argument is of type `Nullable(Nothing)` (i.e. a bare `NULL` literal). Throws an exception if any argument has a runtime `NULL` value inside a `Nullable` column.
- Throws an exception if the total number of elements produced exceeds the limit set by [`function_generate_date_array_max_elements_in_block`](../../operations/settings/settings.md#function_generate_date_array_max_elements_in_block).
    )";
    FunctionDocumentation::Syntax syntax = "generate_date_array(start, end[, step])";
    FunctionDocumentation::Arguments arguments = {
        {"start", "The first date of the array. `Date`, `Date32`, or a string literal implicitly cast to `Date`."},
        {"end", "The last date of the array (inclusive). `Date`, `Date32`, or a string literal implicitly cast to `Date`."},
        {"step", "Optional. The interval between consecutive dates. Must be `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, or `IntervalYear`. Must not be zero. Default value: `INTERVAL 1 DAY`."},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Array of dates from `start` to `end` (inclusive) by `step`.", {"Array(Date)", "Array(Date32)"}};
    FunctionDocumentation::Examples examples = {
        {"Daily step (default)", "SELECT generate_date_array('2024-01-01', '2024-01-05');", R"(
┌─generate_date_array('2024-01-01', '2024-01-05')────────────────────┐
│ ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05'] │
└────────────────────────────────────────────────────────────────────┘
        )"},
        {"Monthly step", "SELECT generate_date_array('2024-01-01', '2024-06-01', INTERVAL 1 MONTH);", R"(
┌─generate_date_array('2024-01-01', '2024-06-01', toIntervalMonth(1))─────────────┐
│ ['2024-01-01','2024-02-01','2024-03-01','2024-04-01','2024-05-01','2024-06-01'] │
└─────────────────────────────────────────────────────────────────────────────────┘
        )"},
        {"Descending (negative step)", "SELECT generate_date_array('2024-01-05', '2024-01-01', INTERVAL -1 DAY);", R"(
┌─generate_date_array('2024-01-05', '2024-01-01', toIntervalDay(-1))─┐
│ ['2024-01-05','2024-01-04','2024-01-03','2024-01-02','2024-01-01'] │
└────────────────────────────────────────────────────────────────────┘
        )"},
        {"Same start and end — single-element array", "SELECT generate_date_array('2024-03-15', '2024-03-15');", R"(
┌─generate_date_array('2024-03-15'::Date32, '2024-03-15'::Date, INTERVAL 1 DAY)─┐
│ ['2024-03-15']                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
        )"},
        {"end before start with positive step — empty array", "SELECT generate_date_array('2024-01-05', '2024-01-01');", R"(
┌─generate_date_array('2024-01-05', '2024-01-01')─┐
│ []                                              │
└─────────────────────────────────────────────────┘
        )"},
        {"NULL argument — returns NULL", "SELECT generate_date_array(NULL, '2024-01-05');", R"(
┌─generate_date_array(NULL, '2024-01-05')─┐
│ NULL                                    │
└─────────────────────────────────────────┘
        )"},
        {"Generate an array column from a subquery", R"(
SELECT generate_date_array(date_start, date_end) AS date_range
FROM (
    SELECT toDate('2024-01-01') AS date_start, toDate('2024-01-07') AS date_end
    UNION ALL SELECT toDate('2024-02-15') AS date_start, toDate('2024-02-21') AS date_end
) ORDER BY date_range;
        )", R"(
┌─generate_date_array(date_start, date_end)─AS date_range──────────────────────────────────────┐
│ ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-06','2024-01-07'] │
│ ['2024-02-15','2024-02-16','2024-02-17','2024-02-18','2024-02-19','2024-02-20','2024-02-21'] │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
        )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 0};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGenerateDateArray>(documentation);
}

}

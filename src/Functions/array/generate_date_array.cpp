#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
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
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/IntervalKind.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
}


/** Generates date array
  * GENERATE_DATE_ARRAY(start_date_expr, end_date_expr[, step_expr])
  * step_expr must be a constant interval expression.
  */
class FunctionGenerateDateArray : public IFunction
{
public:
    static constexpr auto name = "generate_date_array";
    static constexpr size_t max_elements = 500'000'000;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGenerateDateArray>(); }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    /// While any null in the input should cause it to return NULL,
    ///  it seems (from the documentation) that if some arguments are nullable, the code still gets called
    ///  So we would still need to write code that is "safe" regardless of what is passed
    ///  At the very least, this would cause massive allocation and inefficient execution
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Infer the smallest Date type that can represent a constant string argument.
    ///
    /// - If the column is a `ColumnConst`, the string value is parsed as a date and
    ///   `DataTypeDate` is returned when the day number fits in [0, 65535] (the
    ///   `UInt16` / `Date` range), or `DataTypeDate32` otherwise.
    /// - If the column is non-constant (a full `ColumnString`), we cannot inspect
    ///   the values at type-resolution time, so we conservatively return `DataTypeDate32`
    ///   to avoid silent clamping for pre-1970 dates.
    /// - If parsing fails, `DataTypeDate32` is returned; `castColumn` at execution time
    ///   will surface a proper `CANNOT_PARSE_DATE` exception.
    static DataTypePtr resolveStringArgType(const ColumnWithTypeAndName & col)
    {
        const auto * col_const = checkAndGetColumnConst<ColumnString>(col.column.get());
        if (!col_const)
            return std::make_shared<DataTypeDate32>();

        const String & s = col_const->getValue<String>();
        ReadBufferFromString buf(s);
        ExtendedDayNum day_num;
        if (!tryReadDateText(day_num, buf) || !buf.eof())
            return std::make_shared<DataTypeDate32>();

        /// DayNum (UInt16) covers 1970-01-01 .. 2149-06-06 (values 0..65535).
        if (day_num >= 0 && day_num <= std::numeric_limits<UInt16>::max())
            return std::make_shared<DataTypeDate>();
        return std::make_shared<DataTypeDate32>();
    }

    /// Type validator for `start` / `end`: accepts Date, Date32, or String, with optional Nullable wrapper.
    static bool isDateOrDate32OrStringMaybeNullable(const IDataType & type)
    {
        const IDataType & inner = type.isNullable()
            ? *static_cast<const DataTypeNullable &>(type).getNestedType()
            : type;
        return isDate(inner) || isDate32(inner) || isString(inner);
    }

    /// Type validator for `step`: accepts only Interval kinds that make sense for date ranges
    /// (Day, Week, Month, Quarter, Year), with optional Nullable wrapper.
    static bool isDateIntervalForRange(const IDataType & type)
    {
        const IDataType & inner = type.isNullable()
            ? *static_cast<const DataTypeNullable &>(type).getNestedType()
            : type;
        const auto * interval = checkAndGetDataType<DataTypeInterval>(&inner);
        if (!interval)
            return false;
        switch (interval->getKind())
        {
            case IntervalKind::Kind::Day:
            case IntervalKind::Kind::Week:
            case IntervalKind::Kind::Month:
            case IntervalKind::Kind::Quarter:
            case IntervalKind::Kind::Year:
                return true;
            default:
                return false;
        }
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// If any argument is NULL, returns NULL.
        if (std::find_if(arguments.cbegin(), arguments.cend(), [](const auto & arg) { return arg.type->onlyNull(); }) != arguments.cend())
            return makeNullable(std::make_shared<DataTypeNothing>());

        FunctionArgumentDescriptors mandatory_args{
            {"start", &isDateOrDate32OrStringMaybeNullable, nullptr, "Date, Date32, or String"},
            {"end", &isDateOrDate32OrStringMaybeNullable, nullptr, "Date, Date32, or String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"step", &isDateIntervalForRange, nullptr, "IntervalDay | IntervalWeek | IntervalMonth | IntervalQuarter | IntervalYear"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        DataTypes arg_types;
        for (size_t i = 0; i < 2; ++i)
        {
            DataTypePtr type_no_nullable = removeNullable(arguments[i].type);
            if (isString(type_no_nullable))
                /// For string arguments, inspect the constant value to pick the smallest
                /// fitting date type (Date or Date32). Non-constant string columns default
                /// to Date32 since their values are not visible at type-resolution time.
                arg_types.push_back(resolveStringArgType(arguments[i]));
            else
                arg_types.push_back(type_no_nullable);
        }

        DataTypePtr common_type = getLeastSupertype(arg_types);
        return std::make_shared<DataTypeArray>(common_type);
    }

    /// Advance a calendar date by a Month/Quarter/Year interval.
    /// Only used in the slow path — Day and Week are handled arithmetically.
    template <typename T>
    T addCalendarInterval(T value, const IntervalKind kind, Int64 step) const
    {
        switch (kind)
        {
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

    /// Iterative count for Month/Quarter/Year intervals (calendar arithmetic).
    /// Early-exits as soon as the running total exceeds max_elements so the
    /// caller can raise ARGUMENT_OUT_OF_BOUND without completing the full loop.
    template <typename T>
    size_t getCalendarCount(T start, T end, Int64 step, IntervalKind kind) const
    {
        size_t count = 0;
        T current = start;
        if (step > 0)
        {
            while (current <= end)
            {
                current = addCalendarInterval(current, kind, step);
                ++count;
                if (count > max_elements)
                    return count; /// caller will throw
            }
        }
        else
        {
            while (current >= end)
            {
                current = addCalendarInterval(current, kind, step);
                ++count;
                if (count > max_elements)
                    return count; /// caller will throw
            }
        }
        return count;
    }

    /// Returns the number of dates in the inclusive range [start, end] with the given step.
    /// Returns 0 when end is unreachable from start with the given step direction.
    static size_t getDaysCount(Int64 start, Int64 end, Int64 day_step)
    {
        if (day_step > 0 && start <= end)
            return static_cast<size_t>((end - start) / day_step + 1);
        if (day_step < 0 && start >= end)
            return static_cast<size_t>((end - start) / day_step + 1);
        return 0;
    }

    /// Unified execute helper templated on date storage type T and on whether the
    /// step is day-based (Day/Week) or calendar-based (Month/Quarter/Year).
    ///
    /// IsDayBased = true  — count is O(1) via getDaysCount; fill uses plain integer
    ///                      arithmetic (start + idx * step).  interval_kind is ignored.
    /// IsDayBased = false — count is iterative via getCalendarCount (throws on limit);
    ///                      fill advances with addCalendarInterval.
    ///
    /// if constexpr eliminates the dead branch entirely at compile time, so both
    /// instantiations have the same performance as the original separate functions.
    template <typename T, bool IsDayBased>
    ColumnPtr executeGeneric(
        const IColumn * start_col, const IColumn * end_col, Int64 step_value, IntervalKind interval_kind, size_t input_rows_count) const
    {
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_col);
        auto end_column = checkAndGetColumn<ColumnVector<T>>(end_col);
        if (!start_column || !end_column)
            return nullptr;

        const auto & start_data = start_column->getData();
        const auto & end_data = end_column->getData();

        size_t total_values = 0;
        PODArray<size_t> row_length(input_rows_count);

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            if constexpr (IsDayBased)
            {
                row_length[row_idx] = getDaysCount(Int64(start_data[row_idx]), Int64(end_data[row_idx]), step_value);
            }
            else
            {
                row_length[row_idx] = getCalendarCount(start_data[row_idx], end_data[row_idx], step_value, interval_kind);
            }
            total_values += row_length[row_idx];

            if (total_values > max_elements)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "A call to function {} would produce {} array elements, which "
                    "is greater than the allowed maximum of {}",
                    getName(),
                    total_values,
                    max_elements);
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            T current = start_data[row_idx];
            size_t len = row_length[row_idx];
            for (size_t idx = 0; idx < len; ++idx)
            {
                out_data[offset + idx] = current;
                if constexpr (IsDayBased)
                {
                    current += step_value;
                }
                else
                {
                    current = addCalendarInterval(current, interval_kind, step_value);
                }
            }
            offset += len;
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    template <typename T>
    ColumnPtr executeGenericDays(const IColumn * start_col, const IColumn * end_col, Int64 step_value, size_t input_rows_count) const
    {
        return executeGeneric<T, true>(start_col, end_col, step_value, IntervalKind::Kind::Day, input_rows_count);
    }

    template <typename T>
    ColumnPtr executeGenericCalendar(
        const IColumn * start_col, const IColumn * end_col, Int64 step_value, IntervalKind interval_kind, size_t input_rows_count) const
    {
        return executeGeneric<T, false>(start_col, end_col, step_value, interval_kind, input_rows_count);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        NullPresence null_presence = getNullPresense(arguments);
        if (null_presence.has_null_constant)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

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

        for (size_t i = 0; i < arguments.size(); ++i)
            throw_if_null_value(arguments[i]);

        /// Default interval kind and constant step value.
        IntervalKind interval_kind = IntervalKind::Kind::Day;
        Int64 step_value = 1;

        if (arguments.size() == 3)
        {
            if (const auto * interval_type = checkAndGetDataType<DataTypeInterval>(arguments[2].type.get()))
                interval_kind = interval_type->getKind();

            step_value = assert_cast<const ColumnConst &>(*arguments[2].column).getValue<Int64>();

            if (step_value == 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Invalid argument to function {}, the 3rd argument step can't equal to zero", getName());
        }

        DataTypePtr elem_type = checkAndGetDataType<DataTypeArray>(result_type.get())->getNestedType();
        ColumnPtr start_col = castColumn(arguments[0], elem_type)->convertToFullColumnIfConst();
        ColumnPtr end_col = castColumn(arguments[1], elem_type)->convertToFullColumnIfConst();

        ColumnPtr res;
        const bool is_day_based = (interval_kind == IntervalKind::Kind::Day || interval_kind == IntervalKind::Kind::Week);

        if (is_day_based)
        {
            /// Convert weeks to days once, outside any loop.
            Int64 day_step = step_value * (interval_kind == IntervalKind::Kind::Week ? 7 : 1);
            if ((res = executeGenericDays<UInt16>(start_col.get(), end_col.get(), day_step, input_rows_count))
                || (res = executeGenericDays<Int32>(start_col.get(), end_col.get(), day_step, input_rows_count)))
                return res;
        }
        else
        {
            if ((res = executeGenericCalendar<UInt16>(start_col.get(), end_col.get(), step_value, interval_kind, input_rows_count))
                || (res = executeGenericCalendar<Int32>(start_col.get(), end_col.get(), step_value, interval_kind, input_rows_count)))
            {
                return res;
            }
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns {} of argument of function {}", start_col->getName(), getName());
    }
};


REGISTER_FUNCTION(GenerateDateArray)
{
    FunctionDocumentation::Description description = R"(
Returns an array of dates from `start` to `end` (inclusive) incremented by `step`.

- `start` and `end` accept `Date`, `Date32`, or a string literal that is implicitly cast to `Date` or `Date32` depending on the value.
- When `start` and `end` have different types, the element type of the returned array is their least supertype (e.g. mixing `Date` and `Date32` yields `Array(Date32)`).
- `step` must be a **constant** non-zero interval of kind `Day`, `Week`, `Month`, `Quarter`, or `Year`. It cannot be a column reference or a non-constant expression. When omitted, the default step is `INTERVAL 1 DAY`.
- If `step` is positive the sequence is ascending. If `step` is negative the sequence is descending.
- If `end` is unreachable in the given direction, an empty array is returned.
- Returns `NULL` if any argument is of type `Nullable(Nothing)` (i.e. a bare `NULL` literal). Throws an exception if any argument has a runtime `NULL` value inside a `Nullable` column.
- Throws an exception if the total number of elements produced exceeds 500 million.
    )";
    FunctionDocumentation::Syntax syntax = "generate_date_array(start, end[, step])";
    FunctionDocumentation::Arguments arguments = {
        {"start",
         "The first date of the array. `Date`, `Date32`, or a string literal implicitly cast to `Date` or `Date32` depending on the "
         "value.", {"String", "Date", "Date32", "NULL"}},
        {"end",
         "The last date of the array (inclusive). `Date`, `Date32`, or a string literal implicitly cast to `Date` or `Date32` depending on "
         "the value.", {"String", "Date", "Date32", "NULL"}},
        {"step",
         "Optional. A **constant** interval between consecutive dates. Must be `IntervalDay`, `IntervalWeek`, `IntervalMonth`, "
         "`IntervalQuarter`, or `IntervalYear`. Must not be zero. Cannot be a column reference. Default value: `INTERVAL 1 DAY`.",
        {"Interval"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Array of dates from `start` to `end` (inclusive) by `step`.", {"Array(Date)", "Array(Date32)", "NULL"}};
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
        {"Pre-1970 string dates are inferred as Date32", "SELECT generate_date_array('1960-01-01', '1960-01-08');", R"(
┌─generate_date_array('1960-01-01', '1960-01-08')───────────────────────────────────────────────────────────┐
│ ['1960-01-01','1960-01-02','1960-01-03','1960-01-04','1960-01-05','1960-01-06','1960-01-07','1960-01-08'] │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"},
        {"Generate an array column from a subquery (start/end from columns, constant step)",
         R"(
SELECT generate_date_array(date_start, date_end) AS date_range
FROM (
    SELECT toDate('2024-01-01') AS date_start, toDate('2024-01-07') AS date_end
    UNION ALL SELECT toDate('2024-02-15') AS date_start, toDate('2024-02-21') AS date_end
) ORDER BY date_range;
        )",
         R"(
┌─generate_date_array(date_start, date_end)─AS date_range──────────────────────────────────────┐
│ ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-06','2024-01-07'] │
│ ['2024-02-15','2024-02-16','2024-02-17','2024-02-18','2024-02-19','2024-02-20','2024-02-21'] │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
        )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGenerateDateArray>(documentation);
}

}

#include <algorithm>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>
#include <Common/IntervalKind.h>
#include <Common/PODArray.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
}


/// Generates an array of dates:
/// generate_date_array(start, end[, step])
/// step must be a constant interval expression.
class FunctionGenerateDateArray : public IFunction
{
public:
    static constexpr auto name = "generate_date_array";
    static constexpr size_t max_elements = 1'000'000;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGenerateDateArray>(); }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Infer the smallest Date type that can represent a constant string argument: Date if the parsed
    /// value fits into the Date range, Date32 otherwise. Non-constant strings and strings that fail to
    /// parse resolve to Date32 - parsing errors surface in castColumn during execution.
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

        if (day_num >= 0 && day_num <= std::numeric_limits<UInt16>::max())
            return std::make_shared<DataTypeDate>();
        return std::make_shared<DataTypeDate32>();
    }

    /// Type validator for start / end.
    static bool isDateOrDate32OrString(const IDataType & type)
    {
        return isDate(type) || isDate32(type) || isString(type);
    }

    /// Type validator for step: accepts only Interval kinds that make sense for date ranges.
    static bool isDateInterval(const IDataType & type)
    {
        const auto * interval = checkAndGetDataType<DataTypeInterval>(&type);
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
        FunctionArgumentDescriptors mandatory_args{
            {"start", &isDateOrDate32OrString, nullptr, "Date, Date32, or String"},
            {"end", &isDateOrDate32OrString, nullptr, "Date, Date32, or String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"step", &isDateInterval, nullptr, "IntervalDay | IntervalWeek | IntervalMonth | IntervalQuarter | IntervalYear"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        DataTypes arg_types;
        for (size_t i = 0; i < 2; ++i)
        {
            if (isString(arguments[i].type))
                arg_types.push_back(resolveStringArgType(arguments[i]));
            else
                arg_types.push_back(arguments[i].type);
        }

        DataTypePtr common_type = getLeastSupertype(arg_types);
        return std::make_shared<DataTypeArray>(common_type);
    }

    /// The largest |step| the date arithmetic below handles without overflowing intermediate values.
    /// Any larger step overshoots the whole Date32 range [1900-01-01, 2299-12-31] in a single step,
    /// exactly like the clamped one, so clamping does not change the result.
    static Int64 maxAbsStep(IntervalKind kind)
    {
        switch (kind)
        {
            case IntervalKind::Kind::Day:
                return 200'000;
            case IntervalKind::Kind::Week:
                return 30'000;
            case IntervalKind::Kind::Month:
            case IntervalKind::Kind::Quarter:
            case IntervalKind::Kind::Year:
                return 10'000;
            default:
                UNREACHABLE();
        }
    }

    /// Advance a date by one step of the given interval kind. The Date32 representation (Int32 day
    /// number) is used for Date values too, so the arithmetic cannot wrap around the UInt16 boundary.
    static Int32 addInterval(Int32 value, IntervalKind kind, Int64 step)
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

    /// Returns the number of dates in the inclusive range [start, end] with the given step.
    /// Stops counting as soon as the result exceeds max_elements - the caller throws in that case.
    static size_t getRangeLength(Int32 start, Int32 end, Int64 step, IntervalKind kind)
    {
        size_t count = 0;
        Int32 current = start;
        while (step > 0 ? current <= end : current >= end)
        {
            ++count;
            if (count > max_elements)
                break;
            Int32 next = addInterval(current, kind, step);
            /// The date addition saturates at the boundary of the supported date range,
            /// no further dates exist in the range.
            if (step > 0 ? next <= current : next >= current)
                break;
            current = next;
        }
        return count;
    }

    template <typename T>
    ColumnPtr executeGeneric(
        const IColumn * start_col, const IColumn * end_col, Int64 step, IntervalKind interval_kind, size_t input_rows_count) const
    {
        const auto * start_column = checkAndGetColumn<ColumnVector<T>>(start_col);
        const auto * end_column = checkAndGetColumn<ColumnVector<T>>(end_col);
        if (!start_column || !end_column)
            return nullptr;

        const auto & start_data = start_column->getData();
        const auto & end_data = end_column->getData();

        size_t total_values = 0;
        PODArray<size_t> row_length(input_rows_count);

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            row_length[row_idx] = getRangeLength(Int32(start_data[row_idx]), Int32(end_data[row_idx]), step, interval_kind);
            total_values += row_length[row_idx];

            if (total_values > max_elements)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "A call to function {} would produce more than the allowed maximum of {} array elements",
                    getName(),
                    max_elements);
        }

        auto data_col = ColumnVector<T>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            Int32 current = start_data[row_idx];
            for (size_t idx = 0; idx < row_length[row_idx]; ++idx)
            {
                out_data[offset + idx] = static_cast<T>(current);
                current = addInterval(current, interval_kind, step);
            }
            offset += row_length[row_idx];
            out_offsets[row_idx] = offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Default step is 1 day, the explicit step argument is extracted below.
        IntervalKind interval_kind = IntervalKind::Kind::Day;
        Int64 step_value = 1;

        if (arguments.size() == 3)
        {
            interval_kind = assert_cast<const DataTypeInterval &>(*arguments[2].type).getKind();
            step_value = assert_cast<const ColumnConst &>(*arguments[2].column).getValue<Int64>();

            if (step_value == 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Invalid argument to function {}, the 3rd argument step can't equal to zero", getName());

            step_value = std::clamp(step_value, -maxAbsStep(interval_kind), maxAbsStep(interval_kind));
        }

        DataTypePtr elem_type = assert_cast<const DataTypeArray &>(*result_type).getNestedType();
        ColumnPtr start_col = castColumn(arguments[0], elem_type)->convertToFullColumnIfConst();
        ColumnPtr end_col = castColumn(arguments[1], elem_type)->convertToFullColumnIfConst();

        ColumnPtr res;
        if ((res = executeGeneric<UInt16>(start_col.get(), end_col.get(), step_value, interval_kind, input_rows_count))
            || (res = executeGeneric<Int32>(start_col.get(), end_col.get(), step_value, interval_kind, input_rows_count)))
            return res;

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
- Throws an exception if the total number of elements produced exceeds 1 million.
    )";
    FunctionDocumentation::Syntax syntax = "generate_date_array(start, end[, step])";
    FunctionDocumentation::Arguments arguments = {
        {"start",
         "The first date of the array. `Date`, `Date32`, or a string literal implicitly cast to `Date` or `Date32` depending on the "
         "value.", {"String", "Date", "Date32"}},
        {"end",
         "The last date of the array (inclusive). `Date`, `Date32`, or a string literal implicitly cast to `Date` or `Date32` depending on "
         "the value.", {"String", "Date", "Date32"}},
        {"step",
         "Optional. A **constant** interval between consecutive dates. Must be `IntervalDay`, `IntervalWeek`, `IntervalMonth`, "
         "`IntervalQuarter`, or `IntervalYear`. Must not be zero. Cannot be a column reference. Default value: `INTERVAL 1 DAY`.",
        {"Interval"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Array of dates from `start` to `end` (inclusive) by `step`.", {"Array(Date)", "Array(Date32)"}};
    FunctionDocumentation::Examples examples = {
        {"Daily step (default)", "SELECT generate_date_array('2024-01-01', '2024-01-05');", R"(
┌─generate_date_array('2024-01-01', '2024-01-05')────────────────────┐
│ ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05'] │
└────────────────────────────────────────────────────────────────────┘
        )"},
        {"Monthly step", "SELECT generate_date_array('2024-03-07', '2024-08-07', INTERVAL 1 MONTH);", R"(
┌─generate_date_array('2024-03-07', '2024-08-07', toIntervalMonth(1))─────────────┐
│ ['2024-03-07','2024-04-07','2024-05-07','2024-06-07','2024-07-07','2024-08-07'] │
└─────────────────────────────────────────────────────────────────────────────────┘
        )"},
        {"Descending (negative step)", "SELECT generate_date_array('2024-03-02', '2024-02-27', INTERVAL -1 DAY);", R"(
┌─generate_date_array('2024-03-02', '2024-02-27', toIntervalDay(-1))─┐
│ ['2024-03-02','2024-03-01','2024-02-29','2024-02-28','2024-02-27'] │
└────────────────────────────────────────────────────────────────────┘
        )"},
        {"Same start and end — single-element array", "SELECT generate_date_array('2024-03-15', '2024-03-15');", R"(
┌─generate_date_array('2024-03-15', '2024-03-15')─┐
│ ['2024-03-15']                                  │
└─────────────────────────────────────────────────┘
        )"},
        {"end before start with positive step — empty array", "SELECT generate_date_array('2024-01-05', '2024-01-01');", R"(
┌─generate_date_array('2024-01-05', '2024-01-01')─┐
│ []                                              │
└─────────────────────────────────────────────────┘
        )"},
        {"Pre-1970 string dates are inferred as Date32", "SELECT generate_date_array('1960-01-01', '1960-01-08');", R"(
┌─generate_date_array('1960-01-01', '1960-01-08')───────────────────────────────────────────────────────────┐
│ ['1960-01-01','1960-01-02','1960-01-03','1960-01-04','1960-01-05','1960-01-06','1960-01-07','1960-01-08'] │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGenerateDateArray>(documentation);
}

}

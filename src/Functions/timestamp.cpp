#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsDateTime.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/** timestamp(expr[, expr_time])
 *
 * Emulates MySQL's TIMESTAMP() but supports only input format 'yyyy-mm-dd[ hh:mm:ss[.mmmmmm]]' instead of
 * MySQLs possible input formats (https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html).
  */
class FunctionTimestamp final : public IFunction
{
public:
    static constexpr UInt32 DATETIME_SCALE = 6;

    static constexpr auto name = "timestamp";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimestamp>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    /// Throws when a row cannot be parsed.
    [[nodiscard]] std::optional<bool> canThrowIfKnown(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    /// An empty time and a zero-filled `FixedString`, the defaults behind a `NULL`, cannot be parsed.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(StringOrFixedString, [String]) -> DateTime64(6)";
    }

    static DateTime64 parseDateTime(std::string_view str, const DateLUTImpl & time_zone)
    {
        ReadBufferFromMemory read_buffer(str);
        DateTime64 value = 0;
        readDateTime64Text(value, DATETIME_SCALE, read_buffer, time_zone);
        return value;
    }

    static Decimal64 parseTime(std::string_view str)
    {
        ReadBufferFromMemory read_buffer(str);
        Decimal64 value = 0;
        readTime64Text(value, DATETIME_SCALE, read_buffer);
        return value;
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const DateLUTImpl & local_time_zone = DateLUT::instance();

        auto col_result = ColumnDateTime64::create(input_rows_count, DATETIME_SCALE);
        ColumnDateTime64::Container & vec_result = col_result->getData();

        const IColumn * col_timestamp = arguments[0].column.get();

        if (const ColumnConst * col_timestamp_const = checkAndGetColumn<ColumnConst>(col_timestamp))
        {
            std::fill(vec_result.begin(), vec_result.end(), parseDateTime(col_timestamp_const->getDataAt(0), local_time_zone));
        }
        else if (const ColumnString * col_timestamp_string = checkAndGetColumn<ColumnString>(col_timestamp))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                vec_result[i] = parseDateTime(col_timestamp_string->getDataAt(i), local_time_zone);
        }
        else if (const ColumnFixedString * col_timestamp_fixed_string = checkAndGetColumn<ColumnFixedString>(col_timestamp))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                vec_result[i] = parseDateTime(col_timestamp_fixed_string->getDataAt(i), local_time_zone);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of 1st argument of function {}. Must be String or FixedString",
                col_timestamp->getName(),
                getName());
        }

        if (arguments.size() == 1)
            return col_result;

        const IColumn * col_time = arguments[1].column.get();

        if (const ColumnConst * col_time_const = checkAndGetColumn<ColumnConst>(col_time))
        {
            const Decimal64 time_value = parseTime(col_time_const->getDataAt(0));
            for (auto & value : vec_result)
                value.addOverflow(time_value);
        }
        else if (const ColumnString * col_time_string = checkAndGetColumn<ColumnString>(col_time))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                vec_result[i].addOverflow(parseTime(col_time_string->getDataAt(i)));
        }
        else if (const ColumnFixedString * col_time_fixed_string = checkAndGetColumn<ColumnFixedString>(col_time))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                vec_result[i].addOverflow(parseTime(col_time_fixed_string->getDataAt(i)));
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of 2nd argument of function {}. Must be String or FixedString",
                col_time->getName(),
                getName());
        }

        return col_result;
    }
};

}

REGISTER_FUNCTION(Timestamp)
{
    FunctionDocumentation::Description description = R"(
Converts the first argument `expr` to type [`DateTime64(6)`](/reference/data-types/datetime64).
If a second argument `expr_time` is provided, it adds the specified time to the converted value.
    )";
    FunctionDocumentation::Syntax syntax = R"(
    timestamp(expr[, expr_time])
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"expr", "Date or date with time.", {"String"}},
        {"expr_time", "Optional. Time to add to the converted value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the converted value of `expr`, or `expr` with added time", {"DateTime64(6)"}};
    FunctionDocumentation::Examples examples = {
        {"Convert date string to DateTime64(6)", R"(
SELECT timestamp('2023-12-31') AS ts;
    )",
        R"(
┌─────────────────────────ts─┐
│ 2023-12-31 00:00:00.000000 │
└────────────────────────────┘
    )"},
        {"Add time to date string", R"(
SELECT timestamp('2023-12-31 12:00:00', '12:00:00.11') AS ts;
    )",
        R"(
┌─────────────────────────ts─┐
│ 2024-01-01 00:00:00.110000 │
└────────────────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimestamp>(documentation, FunctionFactory::Case::Insensitive);
}

}

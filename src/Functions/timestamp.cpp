#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
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
class FunctionTimestamp : public IFunction
{
public:
    static constexpr UInt32 DATETIME_SCALE = 6;

    static constexpr auto name = "timestamp";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimestamp>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"timestamp", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}
        };
        FunctionArgumentDescriptors optional_args{
            {"time", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeDateTime64>(DATETIME_SCALE);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const DateLUTImpl * local_time_zone = &DateLUT::instance();

        auto col_result = ColumnDateTime64::create(input_rows_count, DATETIME_SCALE);
        ColumnDateTime64::Container & vec_result = col_result->getData();

        const IColumn * col_timestamp = arguments[0].column.get();

        if (const ColumnString * col_timestamp_string = checkAndGetColumn<ColumnString>(col_timestamp))
        {
            const ColumnString::Chars * chars = &col_timestamp_string->getChars();
            const IColumn::Offsets * offsets = &col_timestamp_string->getOffsets();

            size_t current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = (*offsets)[i];
                const size_t string_size = next_offset - current_offset - 1;

                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);

                DateTime64 value = 0;
                readDateTime64Text(value, col_result->getScale(), read_buffer, *local_time_zone);
                vec_result[i] = value;

                current_offset = next_offset;
            }
        }
        else if (const ColumnFixedString * col_timestamp_fixed_string = checkAndGetColumn<ColumnFixedString>(col_timestamp))
        {
            const ColumnString::Chars * chars = &col_timestamp_fixed_string->getChars();
            const size_t fixed_string_size = col_timestamp_fixed_string->getN();

            size_t current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = current_offset + fixed_string_size;

                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], fixed_string_size);

                DateTime64 value = 0;
                readDateTime64Text(value, col_result->getScale(), read_buffer, *local_time_zone);
                vec_result[i] = value;

                current_offset = next_offset;
            }
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

        if (const ColumnString * col_time_string = checkAndGetColumn<ColumnString>(col_time))
        {
            const ColumnString::Chars * chars = &col_time_string->getChars();
            const IColumn::Offsets * offsets = &col_time_string->getOffsets();

            size_t current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = (*offsets)[i];
                const size_t string_size = next_offset - current_offset - 1;

                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);

                Decimal64 value = 0;
                readTime64Text(value, col_result->getScale(), read_buffer);
                vec_result[i].addOverflow(value);

                current_offset = next_offset;
            }
        }
        else if (const ColumnFixedString * col_time_fixed_string = checkAndGetColumn<ColumnFixedString>(col_time))
        {
            const ColumnString::Chars * chars = &col_time_fixed_string->getChars();
            const size_t fixed_string_size = col_time_fixed_string->getN();

            size_t current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = current_offset + fixed_string_size;

                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], fixed_string_size);

                Decimal64 value = 0;
                readTime64Text(value, col_result->getScale(), read_buffer);
                vec_result[i].addOverflow(value);

                current_offset = next_offset;
            }
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
    factory.registerFunction<FunctionTimestamp>(FunctionDocumentation{
        .description = R"(
Converts the first argument 'expr' to type DateTime64(6).
If the second argument 'expr_time' is provided, it adds the specified time to the converted value.
:::)",
        .syntax = "timestamp(expr[, expr_time])",
        .arguments = {
            {"expr", "Date or date with time. Type: String."},
            {"expr_time", "Time to add. Type: String."}
        },
        .returned_value = "The result of conversion and, optionally, addition. Type: DateTime64(6).",
        .examples = {
            {"timestamp", "SELECT timestamp('2013-12-31')", "2013-12-31 00:00:00.000000"},
            {"timestamp", "SELECT timestamp('2013-12-31 12:00:00')", "2013-12-31 12:00:00.000000"},
            {"timestamp", "SELECT timestamp('2013-12-31 12:00:00', '12:00:00.11')", "2014-01-01 00:00:00.110000"},
        },
        .categories{"DateTime"}}, FunctionFactory::Case::Insensitive);
}

}

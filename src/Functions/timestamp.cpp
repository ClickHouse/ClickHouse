#include <Functions/FunctionFactory.h>
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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionTimestamp : public IFunction
{
public:
    static constexpr UInt32 DATETIME_SCALE = 6;

    static constexpr auto name = "timestamp";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimestamp>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Arguments size of function {} should be 1 or 2",
                getName());

        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be string or fixed string",
                arguments[0].type->getName(),
                getName());

        if (arguments.size() == 2)
            if (!isStringOrFixedString(arguments[1].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd argument of function {}. Should be string or fixed string",
                    arguments[1].type->getName(),
                    getName());

        return std::make_shared<DataTypeDateTime64>(DATETIME_SCALE);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Arguments size of function {} should be 1 or 2",
                getName());

        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be string or fixed string",
                arguments[0].type->getName(),
                getName());

        if (arguments.size() == 2)
            if (!isStringOrFixedString(arguments[1].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd argument of function {}. Should be string or fixed string",
                    arguments[1].type->getName(),
                    getName());

        const DateLUTImpl * local_time_zone = &DateLUT::instance();

        auto col_to = ColumnDateTime64::create(input_rows_count, DATETIME_SCALE);
        ColumnDateTime64::Container & vec_to = col_to->getData();

        const IColumn * col_date = arguments[0].column.get();

        if (const ColumnString * col_date_string = checkAndGetColumn<ColumnString>(col_date))
        {
            const ColumnString::Chars * chars = &col_date_string->getChars();
            const IColumn::Offsets * offsets = &col_date_string->getOffsets();

            size_t current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = (*offsets)[i];
                const size_t string_size = next_offset - current_offset - 1;

                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);

                DateTime64 value = 0;
                readDateTime64Text(value, col_to->getScale(), read_buffer, *local_time_zone);
                vec_to[i] = value;

                current_offset = next_offset;
            }
        }
        else if (const ColumnFixedString * col_date_fixed_string = checkAndGetColumn<ColumnFixedString>(col_date))
        {
            const ColumnString::Chars * chars = &col_date_fixed_string->getChars();
            const size_t fixed_string_size = col_date_fixed_string->getN();

            size_t current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = current_offset + fixed_string_size;

                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], fixed_string_size);

                DateTime64 value = 0;
                readDateTime64Text(value, col_to->getScale(), read_buffer, *local_time_zone);
                vec_to[i] = value;

                current_offset = next_offset;
            }
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of 1st argument of function {}. Must be string or fixed string",
                col_date->getName(),
                getName());
        }

        if (arguments.size() == 1)
            return col_to;

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
                readTime64Text(value, col_to->getScale(), read_buffer);
                vec_to[i] += value;

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
                readTime64Text(value, col_to->getScale(), read_buffer);
                vec_to[i] += value;

                current_offset = next_offset;
            }
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of 2nd argument of function {}. Must be string or fixed string",
                col_time->getName(),
                getName());
        }

        return col_to;
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
        .categories{"DateTime"}}, FunctionFactory::CaseInsensitive);
}

}

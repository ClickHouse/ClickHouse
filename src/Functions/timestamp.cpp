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

                // if (!isAllRead(read_buffer))
                //     throwExceptionForIncompletelyParsedValue(read_buffer, *res_type);

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

                // if (!isAllRead(read_buffer))
                //     throwExceptionForIncompletelyParsedValue(read_buffer, *res_type);

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

        /// hh-mm-ss
        static constexpr auto time_length = 8;

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

                if (string_size != time_length)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                        "Illegal size of argument of argument of 2nd argument of function {}",
                        col_date->getName());

                const UInt8 * s = chars->data() + current_offset;

                UInt8 hour = (s[0] - '0') * 10 + (s[1] - '0');
                UInt8 minute = (s[3] - '0') * 10 + (s[4] - '0');
                UInt8 second = (s[6] - '0') * 10 + (s[7] - '0');

                time_t time_offset = hour * 3600 + minute * 60 + second;

                vec_to[i] += time_offset * common::exp10_i32(DATETIME_SCALE);

                current_offset = next_offset;
            }
        }
        else if (const ColumnFixedString * col_time_fixed_string = checkAndGetColumn<ColumnFixedString>(col_time))
        {
            const ColumnString::Chars * chars = &col_time_fixed_string->getChars();
            const size_t fixed_string_size = col_time_fixed_string->getN();

            if (fixed_string_size != time_length)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal size of argument of argument of 2nd argument of function {}",
                    getName());

            size_t current_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = current_offset + fixed_string_size;

                const UInt8 * s = chars->data() + current_offset;

                UInt8 hour = (s[0] - '0') * 10 + (s[1] - '0');
                UInt8 minute = (s[3] - '0') * 10 + (s[4] - '0');
                UInt8 second = (s[6] - '0') * 10 + (s[7] - '0');

                time_t time_offset = hour * 3600 + minute * 60 + second;

                vec_to[i] += time_offset * common::exp10_i32(DATETIME_SCALE);

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
    factory.registerFunction<FunctionTimestamp>({}, FunctionFactory::CaseInsensitive);
}

}

#include "config.h"

#if USE_ULID

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <ulid.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionULIDStringToDateTime : public IFunction
{
public:
    static constexpr size_t ULID_LENGTH = 26;
    static constexpr UInt32 DATETIME_SCALE = 3;

    static constexpr auto name = "ULIDStringToDateTime";

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionULIDStringToDateTime>();
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Wrong number of arguments for function {}: should be 1 or 2",
                getName());

        const auto * arg_fixed_string = checkAndGetDataType<DataTypeFixedString>(arguments[0].type.get());
        const auto * arg_string = checkAndGetDataType<DataTypeString>(arguments[0].type.get());

        if (!arg_string && !(arg_fixed_string && arg_fixed_string->getN() == ULID_LENGTH))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Must be String or FixedString(26).",
                arguments[0].type->getName(),
                getName());

        String timezone;
        if (arguments.size() == 2)
        {
            timezone = extractTimeZoneNameFromColumn(arguments[1].column.get(), arguments[1].name);

            if (timezone.empty())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                    getName());
        }

        return std::make_shared<DataTypeDateTime64>(DATETIME_SCALE, timezone);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnDateTime64::create(input_rows_count, DATETIME_SCALE);
        auto & vec_res = col_res->getData();

        const ColumnPtr column = arguments[0].column;

        const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(column.get());
        const auto * column_string = checkAndGetColumn<ColumnString>(column.get());

        if (column_fixed_string)
        {
            if (column_fixed_string->getN() != ULID_LENGTH)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of argument of function {}, expected String or FixedString({})",
                    arguments[0].name, getName(), ULID_LENGTH
                );

            const auto & vec_src = column_fixed_string->getChars();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                DateTime64 time = decode(vec_src.data() + i * ULID_LENGTH);
                vec_res[i] = time;
            }
        }
        else if (column_string)
        {
            const auto & vec_src = column_string->getChars();
            const auto & offsets_src = column_string->getOffsets();

            size_t src_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                DateTime64 time = 0;

                size_t string_size = offsets_src[i] - src_offset;
                if (string_size != ULID_LENGTH + 1)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Illegal column {} of argument of function {}, ULID must be {} characters long",
                        arguments[0].name, getName(), ULID_LENGTH
                    );

                time = decode(vec_src.data() + src_offset);

                src_offset += string_size;
                vec_res[i] = time;
            }
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}, expected String or FixedString({})",
                arguments[0].name, getName(), ULID_LENGTH
            );

        return col_res;
    }

    static DateTime64 decode(const UInt8 * data)
    {
        unsigned char buffer[16];
        int ret = ulid_decode(buffer, reinterpret_cast<const char *>(data));
        if (ret != 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse ULID {}",
                std::string_view(reinterpret_cast<const char *>(data), ULID_LENGTH)
            );

        /// Timestamp in milliseconds is the first 48 bits of the decoded ULID
        Int64 ms = 0;
        memcpy(reinterpret_cast<UInt8 *>(&ms) + 2, buffer, 6);

#    if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        ms = std::byteswap(ms);
#    endif

        return DecimalUtils::decimalFromComponents<DateTime64>(ms / intExp10(DATETIME_SCALE), ms % intExp10(DATETIME_SCALE), DATETIME_SCALE);
    }
};


REGISTER_FUNCTION(ULIDStringToDateTime)
{
    factory.registerFunction<FunctionULIDStringToDateTime>(FunctionDocumentation
        {
            .description=R"(
This function extracts the timestamp from a ULID and returns it as a DateTime64(3) typed value.
The function expects the ULID to be provided as the first argument, which can be either a String or a FixedString(26) data type.
An optional second argument can be passed to specify a timezone for the timestamp.
)",
            .examples{
                {"ulid", "SELECT ULIDStringToDateTime(generateULID())", ""},
                {"timezone", "SELECT ULIDStringToDateTime(generateULID(), 'Asia/Istanbul')", ""}},
            .categories{"ULID"}
        });
}

}

#endif

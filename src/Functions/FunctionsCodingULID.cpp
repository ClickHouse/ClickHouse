#include "config.h"

#if USE_ULID

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <ulid.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <bool to_binary>
class FunctionULIDConversion final : public IFunction
{
public:
    static constexpr auto name = to_binary ? "ULIDStringToNum" : "ULIDNumToString";
    static constexpr size_t input_size = to_binary ? 26 : 16;
    static constexpr size_t output_size = to_binary ? 16 : 26;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionULIDConversion>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    /// An empty `String` is invalid, including unused dictionary entries and values behind `NULL`.
    bool canBeExecutedOnDefaultArguments() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * fixed_string = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!isString(arguments[0]) && !(fixed_string && fixed_string->getN() == input_size))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected String or FixedString({})",
                arguments[0]->getName(),
                name,
                input_size);

        return std::make_shared<DataTypeFixedString>(output_size);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result = ColumnFixedString::create(output_size);
        auto & result_data = result->getChars();
        result_data.resize(input_rows_count * output_size);

        const auto convert_column = [&](const auto & column)
        {
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const auto value = column.getDataAt(row);
                if (value.size() != input_size)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid length {} of argument of function {}, expected {} bytes",
                        value.size(),
                        name,
                        input_size);

                auto * destination = result_data.data() + row * output_size;
                if constexpr (to_binary)
                {
                    /// `ulid_decode` uses plain `char` values as table indices; validate before calling it.
                    for (unsigned char c : value)
                    {
                        if (c >= 128)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse ULID: non-ASCII character");
                    }

                    if (ulid_decode(reinterpret_cast<unsigned char *>(destination), value.data()) != 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse ULID {}", value);
                }
                else
                {
                    ulid_encode(reinterpret_cast<char *>(destination), reinterpret_cast<const unsigned char *>(value.data()));
                }
            }
        };

        const auto & column = arguments[0].column;
        if (const auto * string_column = checkAndGetColumn<ColumnString>(column.get()))
            convert_column(*string_column);
        else if (const auto * fixed_string_column = checkAndGetColumn<ColumnFixedString>(column.get()))
            convert_column(*fixed_string_column);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", column->getName(), name);

        return result;
    }
};

using FunctionULIDStringToNum = FunctionULIDConversion<true>;
using FunctionULIDNumToString = FunctionULIDConversion<false>;

}

REGISTER_FUNCTION(ULIDConversion)
{
    FunctionDocumentation::Description decode_description = R"(
Converts the text representation of a [ULID](https://github.com/ulid/spec) to its 16-byte binary representation.
The bytes are in big-endian (network) order: the 48-bit timestamp followed by the 80-bit random component.

The input must contain exactly 26 ASCII characters. Decoding is case-insensitive and accepts the Crockford Base32 aliases `I` and `L` for `1`, and `O` for `0`.
An exception is thrown for an invalid length, invalid characters, or a value that exceeds 128 bits.
    )";
    FunctionDocumentation::Syntax decode_syntax = "ULIDStringToNum(ulid)";
    FunctionDocumentation::Arguments decode_arguments = {{"ulid", "Text representation of a ULID.", {"String", "FixedString(26)"}}};
    FunctionDocumentation::ReturnedValue decode_returned_value
        = {"Returns the 16-byte binary representation of the ULID.", {"FixedString(16)"}};
    FunctionDocumentation::Examples decode_examples
        = {{"Convert a ULID to binary", "SELECT hex(ULIDStringToNum('00000000000000000000000001'))", "00000000000000000000000000000001"}};
    FunctionDocumentation decode_documentation
        = {decode_description,
           decode_syntax,
           decode_arguments,
           {},
           decode_returned_value,
           decode_examples,
           {26, 10},
           FunctionDocumentation::Category::ULID};
    factory.registerFunction<FunctionULIDStringToNum>(decode_documentation);

    FunctionDocumentation::Description encode_description = R"(
Converts a 16-byte binary [ULID](https://github.com/ulid/spec) to its canonical 26-character uppercase text representation.
The input bytes are in big-endian (network) order: the 48-bit timestamp followed by the 80-bit random component.

Every 16-byte value is valid, including values containing zero bytes.
An exception is thrown if a `String` argument does not contain exactly 16 bytes.
    )";
    FunctionDocumentation::Syntax encode_syntax = "ULIDNumToString(binary)";
    FunctionDocumentation::Arguments encode_arguments
        = {{"binary", "16-byte binary representation of a ULID.", {"String", "FixedString(16)"}}};
    FunctionDocumentation::ReturnedValue encode_returned_value
        = {"Returns the canonical uppercase text representation of the ULID.", {"FixedString(26)"}};
    FunctionDocumentation::Examples encode_examples
        = {{"Convert a binary ULID to text",
            "SELECT ULIDNumToString(unhex('00000000000000000000000000000001'))",
            "00000000000000000000000001"}};
    FunctionDocumentation encode_documentation
        = {encode_description,
           encode_syntax,
           encode_arguments,
           {},
           encode_returned_value,
           encode_examples,
           {26, 10},
           FunctionDocumentation::Category::ULID};
    factory.registerFunction<FunctionULIDNumToString>(encode_documentation);
}

}

#endif

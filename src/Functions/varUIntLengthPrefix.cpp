#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/VarInt.h>

#include <cstring>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
}

namespace
{

/// A string that is stored with its length in front of it, as `RowBinary` and `Native` store a `String` and as the state of
/// some aggregate functions is stored, has the length as a `VarUInt`: an unsigned LEB128 number, 7 bits per byte, the lowest
/// bits first, the high bit of every byte but the last one is set. The functions below convert between such a string and its
/// contents, and encode one number.

void checkIsStringOrFixedString(const String & function_name, const DataTypePtr & type)
{
    if (!isStringOrFixedString(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, expected String or FixedString", type->getName(), function_name);
}

class FunctionAddVarUIntLengthPrefix final : public IFunction
{
public:
    static constexpr auto name = "addVarUIntLengthPrefix";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionAddVarUIntLengthPrefix>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkIsStringOrFixedString(getName(), arguments[0]);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & column = *arguments[0].column;

        auto result = ColumnString::create();
        auto & chars = result->getChars();
        auto & offsets = result->getOffsets();
        offsets.resize(input_rows_count);
        chars.reserve(column.byteSize() + input_rows_count * 2);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const std::string_view value = column.getDataAt(row);
            const size_t prefix_size = getLengthOfVarUInt(value.size());

            const size_t old_size = chars.size();
            chars.resize(old_size + prefix_size + value.size());

            char * pos = reinterpret_cast<char *>(chars.data() + old_size);
            pos = writeVarUInt(value.size(), pos);
            if (!value.empty())
                memcpy(pos, value.data(), value.size());

            offsets[row] = chars.size();
        }

        return result;
    }
};

class FunctionRemoveVarUIntLengthPrefix final : public IFunction
{
public:
    static constexpr auto name = "removeVarUIntLengthPrefix";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRemoveVarUIntLengthPrefix>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkIsStringOrFixedString(getName(), arguments[0]);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & column = *arguments[0].column;

        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const std::string_view value = column.getDataAt(row);

            UInt64 length = 0;
            size_t prefix_size = 0;
            bool complete = false;
            while (prefix_size < value.size() && prefix_size < VAR_UINT_MAX_SIZE)
            {
                const UInt64 byte = static_cast<unsigned char>(value[prefix_size]);

                /// The tenth byte can only carry the highest bit of a `UInt64`.
                if (prefix_size == VAR_UINT_MAX_SIZE - 1 && byte > 1)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "The length prefix in the argument of function {} does not fit UInt64", getName());

                length |= (byte & 0x7F) << (7 * prefix_size);
                ++prefix_size;

                if (!(byte & 0x80))
                {
                    complete = true;
                    break;
                }
            }

            if (!complete)
                throw Exception(ErrorCodes::INCORRECT_DATA, "The argument of function {} does not start with a complete VarUInt length", getName());

            const size_t rest = value.size() - prefix_size;
            if (length != rest)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "The length prefix in the argument of function {} is {}, but {} bytes follow it",
                    getName(),
                    length,
                    rest);

            result->insertData(value.data() + prefix_size, rest);
        }

        return result;
    }
};

class FunctionToVarUInt final : public IFunction
{
public:
    static constexpr auto name = "toVarUInt";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToVarUInt>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isNativeUInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected UInt8, UInt16, UInt32 or UInt64",
                arguments[0]->getName(),
                getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & column = *arguments[0].column;

        auto result = ColumnString::create();
        auto & chars = result->getChars();
        auto & offsets = result->getOffsets();
        offsets.resize(input_rows_count);
        chars.reserve(input_rows_count * 2);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 value = column.getUInt(row);

            const size_t old_size = chars.size();
            chars.resize(old_size + getLengthOfVarUInt(value));
            writeVarUInt(value, reinterpret_cast<char *>(chars.data() + old_size));

            offsets[row] = chars.size();
        }

        return result;
    }
};

}

REGISTER_FUNCTION(VarUIntLengthPrefix)
{
    {
        FunctionDocumentation::Description description = R"(
Puts the length of a string in front of it, encoded as a `VarUInt` (an unsigned LEB128 number).
This is how `RowBinary` and `Native` store a `String`, and how the state of some aggregate functions (for example `uniqApacheHLL` and `uniqTheta`) stores an external sketch.
It is the inverse of [`removeVarUIntLengthPrefix`](#removeVarUIntLengthPrefix).

A sketch that was written by another system, without the length, can be read as a state of such a function after adding the prefix: `CAST(addVarUIntLengthPrefix(sketch) AS AggregateFunction(...))`.
)";
        FunctionDocumentation::Syntax syntax = "addVarUIntLengthPrefix(s)";
        FunctionDocumentation::Arguments arguments = {{"s", "The string.", {"String", "FixedString"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The string with its length in front of it.", {"String"}};
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT hex(addVarUIntLengthPrefix('abc'))", "03616263"},
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionAddVarUIntLengthPrefix>(documentation);
    }

    {
        FunctionDocumentation::Description description = R"(
Removes the length that is stored in front of a string as a `VarUInt` (an unsigned LEB128 number), and returns the string.
It is the inverse of [`addVarUIntLengthPrefix`](#addVarUIntLengthPrefix).

The length must be complete and equal to the number of bytes that follow it, otherwise the function throws an exception.
The encoding of the length is read as the `RowBinary` reader reads it, so a length that is padded with zero groups is accepted.

It gives another system the bytes of a sketch that the state of an aggregate function stores, for example `removeVarUIntLengthPrefix(toString(state))`.
Apply it only to a state that is stored as a length followed by bytes, such as the state of `uniqApacheHLL`.
)";
        FunctionDocumentation::Syntax syntax = "removeVarUIntLengthPrefix(s)";
        FunctionDocumentation::Arguments arguments = {{"s", "The string that starts with its length.", {"String", "FixedString"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The string without its length.", {"String"}};
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT removeVarUIntLengthPrefix(unhex('03616263'))", "abc"},
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRemoveVarUIntLengthPrefix>(documentation);
    }

    {
        FunctionDocumentation::Description description = R"(
Encodes an unsigned integer as a `VarUInt` (an unsigned LEB128 number): 7 bits per byte, the lowest bits first, the high bit of every byte but the last one is set.
It takes from one to ten bytes.
)";
        FunctionDocumentation::Syntax syntax = "toVarUInt(x)";
        FunctionDocumentation::Arguments arguments = {{"x", "The number.", {"UInt8", "UInt16", "UInt32", "UInt64"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The bytes of the encoded number.", {"String"}};
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT hex(toVarUInt(300))", "AC02"},
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionToVarUInt>(documentation);
    }
}

}

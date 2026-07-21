#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/UUIDv7Utils.h>
#include <Common/TypeID.h>

/// Functions for the TypeID format (https://github.com/jetify-com/typeid/tree/main/spec):
/// a type prefix plus a 26-character Crockford base32 suffix encoding a 128-bit UUID,
/// e.g. `user_01h455vb4pex5vsknk084sn02q`.

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int INCORRECT_DATA;
}

namespace
{

UUID makeUUIDFromBytes(UInt64 high_bytes, UInt64 low_bytes)
{
    UUID uuid;
    UUIDHelpers::getHighBytes(uuid) = high_bytes;
    UUIDHelpers::getLowBytes(uuid) = low_bytes;
    return uuid;
}

const ColumnString & getStringColumn(const ColumnPtr & column, const char * function_name, const char * argument_name)
{
    const auto * col_string = checkAndGetColumn<ColumnString>(column.get());
    if (!col_string)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of {} argument of function {}, must be String",
            column->getName(), argument_name, function_name);
    return *col_string;
}

/// TypeIDs are often stored with an exact width (e.g. FixedString(26) for the prefixless
/// form), so the textual inputs accept both String and FixedString. Both column types
/// return the row value from getDataAt, so the callers can read rows generically.
const IColumn & getStringOrFixedStringColumn(const ColumnPtr & column, const char * function_name, const char * argument_name)
{
    if (!checkAndGetColumn<ColumnString>(column.get()) && !checkAndGetColumn<ColumnFixedString>(column.get()))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of {} argument of function {}, must be String or FixedString",
            column->getName(), argument_name, function_name);
    return *column;
}

enum class TypeIDErrorHandling : uint8_t
{
    ThrowException,
    ReturnNull
};

template <typename Name, TypeIDErrorHandling error_handling>
class FunctionTypeIDToUUID : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTypeIDToUUID>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"type_id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
        validateFunctionArguments(*this, arguments, args);

        if constexpr (error_handling == TypeIDErrorHandling::ReturnNull)
            return makeNullable(std::make_shared<DataTypeUUID>());
        else
            return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & col_string = getStringOrFixedStringColumn(arguments[0].column, name, "first");

        auto col_res = ColumnUUID::create();
        typename ColumnUUID::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        ColumnUInt8::MutablePtr col_null_map;
        if constexpr (error_handling == TypeIDErrorHandling::ReturnNull)
            col_null_map = ColumnUInt8::create(input_rows_count, UInt8{0});

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view value = col_string.getDataAt(row);
            std::string_view prefix;
            std::string_view suffix;
            UInt64 high_bytes = 0;
            UInt64 low_bytes = 0;

            if (splitTypeID(value, prefix, suffix) && decodeTypeIDSuffix(suffix, high_bytes, low_bytes))
            {
                vec_res[row] = makeUUIDFromBytes(high_bytes, low_bytes);
            }
            else
            {
                if constexpr (error_handling == TypeIDErrorHandling::ThrowException)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid TypeID value ({}) in function {}", value, name);
                else
                {
                    vec_res[row] = UUID{};
                    col_null_map->getData()[row] = 1;
                }
            }
        }

        if constexpr (error_handling == TypeIDErrorHandling::ReturnNull)
            return ColumnNullable::create(std::move(col_res), std::move(col_null_map));
        else
            return col_res;
    }
};

struct NameTypeIDToUUID
{
    static constexpr auto name = "typeIDToUUID";
};

struct NameTryTypeIDToUUID
{
    static constexpr auto name = "tryTypeIDToUUID";
};

using FunctionTypeIDToUUIDThrow = FunctionTypeIDToUUID<NameTypeIDToUUID, TypeIDErrorHandling::ThrowException>;
using FunctionTryTypeIDToUUID = FunctionTypeIDToUUID<NameTryTypeIDToUUID, TypeIDErrorHandling::ReturnNull>;

class FunctionTypeIDPrefix : public IFunction
{
public:
    static constexpr auto name = "typeIDPrefix";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTypeIDPrefix>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"type_id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & col_string = getStringOrFixedStringColumn(arguments[0].column, name, "first");

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view value = col_string.getDataAt(row);
            std::string_view prefix;
            std::string_view suffix;
            UInt64 high_bytes = 0;
            UInt64 low_bytes = 0;

            if (!splitTypeID(value, prefix, suffix) || !decodeTypeIDSuffix(suffix, high_bytes, low_bytes))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid TypeID value ({}) in function {}", value, name);

            col_res->insertData(prefix.data(), prefix.size());
        }

        return col_res;
    }
};

class FunctionUUIDToTypeID : public IFunction
{
public:
    static constexpr auto name = "UUIDToTypeID";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUUIDToTypeID>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"uuid", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUUID), nullptr, "UUID"}};
        FunctionArgumentDescriptors optional_args{
            {"prefix", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_uuid = checkAndGetColumn<ColumnUUID>(arguments[0].column.get());
        if (!col_uuid)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}, must be UUID",
                arguments[0].column->getName(), name);

        const ColumnString * col_prefix = nullptr;
        ColumnPtr full_prefix_column;
        if (arguments.size() > 1)
        {
            full_prefix_column = arguments[1].column->convertToFullColumnIfConst();
            col_prefix = &getStringColumn(full_prefix_column, name, "second");
        }

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        char buf[TYPE_ID_MAX_PREFIX_LENGTH + 1 + TYPE_ID_SUFFIX_LENGTH];

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view prefix;
            if (col_prefix)
            {
                prefix = col_prefix->getDataAt(row);
                if (!isValidTypeIDPrefix(prefix))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid TypeID prefix ({}) in function {}: the prefix must contain at most {} characters from [a-z_] and must start and end with [a-z]",
                        prefix, name, TYPE_ID_MAX_PREFIX_LENGTH);
            }

            char * pos = buf;
            if (!prefix.empty())
            {
                memcpy(pos, prefix.data(), prefix.size());
                pos += prefix.size();
                *pos = '_';
                ++pos;
            }

            const UUID & uuid = col_uuid->getData()[row];
            encodeTypeIDSuffix(UUIDHelpers::getHighBytes(uuid), UUIDHelpers::getLowBytes(uuid), pos);
            pos += TYPE_ID_SUFFIX_LENGTH;

            col_res->insertData(buf, pos - buf);
        }

        return col_res;
    }
};

class FunctionGenerateTypeID : public IFunction
{
public:
    static constexpr auto name = "generateTypeID";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGenerateTypeID>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args;
        FunctionArgumentDescriptors optional_args{
            {"prefix", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * col_prefix = nullptr;
        ColumnPtr full_prefix_column;
        if (!arguments.empty())
        {
            full_prefix_column = arguments[0].column->convertToFullColumnIfConst();
            col_prefix = &getStringColumn(full_prefix_column, name, "first");
        }

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        if (input_rows_count)
        {
            PODArray<UUID> uuids(input_rows_count);

            /// Generate UUIDv7 values exactly like generateUUIDv7 does: fill with random
            /// bytes, then set the timestamp, version, variant and monotonic counter.
            RandImpl::execute(reinterpret_cast<char *>(uuids.data()), uuids.size() * sizeof(UUID));

            /// Note: For performance reasons, clock_gettime is called once per chunk instead of once per UUID. This reduces precision but
            /// it still complies with the UUID standard.
            uint64_t timestamp = getTimestampMillisecond();
            for (UUID & uuid : uuids)
            {
                UUIDv7Utils::Data data;
                data.generate(uuid, timestamp);
            }

            char buf[TYPE_ID_MAX_PREFIX_LENGTH + 1 + TYPE_ID_SUFFIX_LENGTH];

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                std::string_view prefix;
                if (col_prefix)
                {
                    prefix = col_prefix->getDataAt(row);
                    if (!isValidTypeIDPrefix(prefix))
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Invalid TypeID prefix ({}) in function {}: the prefix must contain at most {} characters from [a-z_] and must start and end with [a-z]",
                            prefix, name, TYPE_ID_MAX_PREFIX_LENGTH);
                }

                char * pos = buf;
                if (!prefix.empty())
                {
                    memcpy(pos, prefix.data(), prefix.size());
                    pos += prefix.size();
                    *pos = '_';
                    ++pos;
                }

                encodeTypeIDSuffix(UUIDHelpers::getHighBytes(uuids[row]), UUIDHelpers::getLowBytes(uuids[row]), pos);
                pos += TYPE_ID_SUFFIX_LENGTH;

                col_res->insertData(buf, pos - buf);
            }
        }

        return col_res;
    }

private:
    static uint64_t getTimestampMillisecond()
    {
        timespec tp{};
        clock_gettime(CLOCK_REALTIME, &tp); /// NOLINT(cert-err33-c)
        const uint64_t sec = tp.tv_sec;
        return sec * 1000 + tp.tv_nsec / 1000000;
    }
};

}

REGISTER_FUNCTION(TypeIDToUUID)
{
    FunctionDocumentation::Description description = R"(
Converts a [TypeID](https://github.com/jetify-com/typeid) string to the [UUID](../data-types/uuid.md) encoded in its suffix.
The type prefix is validated and discarded; use [`typeIDPrefix`](#typeIDPrefix) to extract it.
If the string is not a valid TypeID, an exception is thrown.
)";
    FunctionDocumentation::Syntax syntax = "typeIDToUUID(type_id)";
    FunctionDocumentation::Arguments arguments = {
        {"type_id", "TypeID string to convert.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the UUID encoded in the TypeID suffix.", {"UUID"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT typeIDToUUID('user_01h455vb4pex5vsknk084sn02q');",
        R"(
┌─typeIDToUUID⋯4sn02q')────────────────┐
│ 01890a5d-ac96-774b-bcce-b302099a8057 │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTypeIDToUUIDThrow>(documentation);
}

REGISTER_FUNCTION(TryTypeIDToUUID)
{
    FunctionDocumentation::Description description = R"(
Like [`typeIDToUUID`](#typeIDToUUID), but returns `NULL` instead of throwing an exception if the string is not a valid [TypeID](https://github.com/jetify-com/typeid).
)";
    FunctionDocumentation::Syntax syntax = "tryTypeIDToUUID(type_id)";
    FunctionDocumentation::Arguments arguments = {
        {"type_id", "TypeID string to convert.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the UUID encoded in the TypeID suffix, or NULL if the input is not a valid TypeID.", {"Nullable(UUID)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT tryTypeIDToUUID('user_01h455vb4pex5vsknk084sn02q') AS res, tryTypeIDToUUID('invalid') AS res_invalid;",
        R"(
┌─res──────────────────────────────────┬─res_invalid─┐
│ 01890a5d-ac96-774b-bcce-b302099a8057 │ ᴺᵁᴸᴸ        │
└──────────────────────────────────────┴─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTryTypeIDToUUID>(documentation);
}

REGISTER_FUNCTION(TypeIDPrefix)
{
    FunctionDocumentation::Description description = R"(
Extracts the type prefix of a [TypeID](https://github.com/jetify-com/typeid) string, e.g. `user` for `user_01h455vb4pex5vsknk084sn02q`.
Returns an empty string for a TypeID without a prefix.
If the string is not a valid TypeID, an exception is thrown.
)";
    FunctionDocumentation::Syntax syntax = "typeIDPrefix(type_id)";
    FunctionDocumentation::Arguments arguments = {
        {"type_id", "TypeID string.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the type prefix of the TypeID.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT typeIDPrefix('user_01h455vb4pex5vsknk084sn02q');",
        R"(
┌─typeIDPrefi⋯84sn02q')─┐
│ user                  │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTypeIDPrefix>(documentation);
}

REGISTER_FUNCTION(UUIDToTypeID)
{
    FunctionDocumentation::Description description = R"(
Converts a [UUID](../data-types/uuid.md) to a [TypeID](https://github.com/jetify-com/typeid) string with the given type prefix.
The prefix must contain at most 63 characters from `[a-z_]` and must start and end with `[a-z]`; it may also be empty or omitted, in which case the TypeID consists of the suffix only.
An exception is thrown for invalid prefixes.
)";
    FunctionDocumentation::Syntax syntax = "UUIDToTypeID(uuid[, prefix])";
    FunctionDocumentation::Arguments arguments = {
        {"uuid", "UUID to convert.", {"UUID"}},
        {"prefix", "Optional. Type prefix of the TypeID. Empty by default.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the TypeID string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), 'user');",
        R"(
┌─UUIDToTypeI⋯), 'user')──────────┐
│ user_01h455vb4pex5vsknk084sn02q │
└─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionUUIDToTypeID>(documentation);
}

REGISTER_FUNCTION(GenerateTypeID)
{
    FunctionDocumentation::Description description = R"(
Generates a [TypeID](https://github.com/jetify-com/typeid) with the given type prefix: the equivalent of `UUIDToTypeID(generateUUIDv7(), prefix)`.
The prefix must contain at most 63 characters from `[a-z_]` and must start and end with `[a-z]`; it may also be empty or omitted, in which case the TypeID consists of the suffix only.
)";
    FunctionDocumentation::Syntax syntax = "generateTypeID([prefix])";
    FunctionDocumentation::Arguments arguments = {
        {"prefix", "Optional. Type prefix of the TypeID. Empty by default.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a TypeID whose suffix encodes a freshly generated UUIDv7.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT generateTypeID('user');",
        R"(
┌─generateTypeID('user')──────────┐
│ user_01k0s8mgn6f5jr1w2kj0tc6cd6 │
└─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGenerateTypeID>(documentation);
}

}

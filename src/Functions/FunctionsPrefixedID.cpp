#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/StringUtils.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

/// Functions for prefixed identifiers in the style popularized by Stripe: `<prefix>_<body>`,
/// e.g. `cus_NffrFeUfNV2Hib` or `pk_test_51TpZvW`. The prefix may contain underscores
/// (multi-segment); the body is base62 ([0-9A-Za-z]), which never contains an underscore,
/// so all readers split the identifier at the last underscore.

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Split at the last underscore: everything before it is the prefix, everything after is
/// the body. Without an underscore, the prefix is empty and the whole string is the body.
/// Purely positional, never fails: `cus_` gives ("cus", "") and `_abc` gives ("", "abc").
void splitPrefixedId(std::string_view id, std::string_view & prefix, std::string_view & body)
{
    size_t separator_pos = id.rfind('_');
    if (separator_pos == std::string_view::npos)
    {
        prefix = {};
        body = id;
    }
    else
    {
        prefix = id.substr(0, separator_pos);
        body = id.substr(separator_pos + 1);
    }
}

/// Validity of a whole identifier: ^([A-Za-z][A-Za-z0-9]*(_[A-Za-z0-9]+)*_)?[0-9A-Za-z]+$
/// Equivalently: all underscore-separated segments are non-empty and alphanumeric, and when
/// at least one underscore is present (i.e. a prefix exists), the first character is a letter.
/// A bare body without a prefix is valid and may start with a digit.
bool isValidPrefixedIdImpl(std::string_view id)
{
    if (id.empty())
        return false;

    bool has_underscore = false;
    size_t segment_length = 0;
    for (char c : id)
    {
        if (c == '_')
        {
            if (segment_length == 0)
                return false;
            has_underscore = true;
            segment_length = 0;
        }
        else if (isAlphaNumericASCII(c))
            ++segment_length;
        else
            return false;
    }
    if (segment_length == 0)
        return false;

    return !has_underscore || isAlphaASCII(id.front());
}

/// Validity of a prefix passed to generatePrefixedId: non-empty, and every underscore-separated
/// segment matches [A-Za-z][A-Za-z0-9]* (stricter than isValidPrefixedId, which only requires
/// the first segment to start with a letter).
bool isValidGeneratorPrefix(std::string_view prefix)
{
    if (prefix.empty())
        return false;

    bool at_segment_start = true;
    for (char c : prefix)
    {
        if (c == '_')
        {
            if (at_segment_start)
                return false;
            at_segment_start = true;
        }
        else if (at_segment_start)
        {
            if (!isAlphaASCII(c))
                return false;
            at_segment_start = false;
        }
        else if (!isAlphaNumericASCII(c))
            return false;
    }
    return !at_segment_start;
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

class FunctionGeneratePrefixedId : public IFunction
{
public:
    static constexpr auto name = "generatePrefixedId";

    /// 22 base62 characters carry log2(62^22) ≈ 131 bits, at least the 128 bits
    /// conventionally considered collision-resistant.
    static constexpr size_t DEFAULT_BODY_LENGTH = 22;
    static constexpr size_t MAX_BODY_LENGTH = 255;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeneratePrefixedId>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"prefix", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        FunctionArgumentDescriptors optional_args{
            {"length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), nullptr, "Native unsigned integer"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        static constexpr char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        ColumnPtr full_prefix_column = arguments[0].column->convertToFullColumnIfConst();
        const ColumnString & col_prefix = getStringColumn(full_prefix_column, name, "first");

        ColumnPtr full_length_column;
        if (arguments.size() > 1)
            full_length_column = arguments[1].column->convertToFullColumnIfConst();

        auto col_res = ColumnString::create();
        ColumnString::Chars & data_to = col_res->getChars();
        ColumnString::Offsets & offsets_to = col_res->getOffsets();
        offsets_to.resize(input_rows_count);

        pcg64_fast rng(randomSeed());

        IColumn::Offset offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view prefix = col_prefix.getDataAt(row);
            if (!isValidGeneratorPrefix(prefix))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid prefix ({}) in function {}: the prefix must be non-empty and consist of underscore-separated segments matching [A-Za-z][A-Za-z0-9]*",
                    prefix, name);

            size_t body_length = DEFAULT_BODY_LENGTH;
            if (full_length_column)
            {
                body_length = full_length_column->getUInt(row);
                if (body_length < 1 || body_length > MAX_BODY_LENGTH)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid body length ({}) in function {}: must be in [1, {}]",
                        body_length, name, MAX_BODY_LENGTH);
            }

            IColumn::Offset next_offset = offset + prefix.size() + 1 + body_length;
            data_to.resize(next_offset);
            offsets_to[row] = next_offset;

            memcpy(&data_to[offset], prefix.data(), prefix.size());
            offset += prefix.size();
            data_to[offset] = '_';
            ++offset;

            /// Each 64-bit random value yields four base62 characters via the multiply-shift
            /// mapping of 16-bit halves; the bias of 62/65536 per character is negligible.
            UInt64 rand = 0;
            size_t rand_chars_left = 0;
            for (size_t i = 0; i < body_length; ++i)
            {
                if (rand_chars_left == 0)
                {
                    rand = rng();
                    rand_chars_left = 4;
                }
                data_to[offset + i] = alphabet[(static_cast<UInt16>(rand) * 62U) >> 16];
                rand >>= 16;
                --rand_chars_left;
            }
            offset += body_length;
        }

        return col_res;
    }
};

template <typename Name, bool extract_prefix>
class FunctionPrefixedIdPart : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPrefixedIdPart>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString & col_string = getStringColumn(arguments[0].column, name, "first");

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view prefix;
            std::string_view body;
            splitPrefixedId(col_string.getDataAt(row), prefix, body);

            std::string_view part = extract_prefix ? prefix : body;
            col_res->insertData(part.data(), part.size());
        }

        return col_res;
    }
};

struct NamePrefixedIdPrefix
{
    static constexpr auto name = "prefixedIdPrefix";
};

struct NamePrefixedIdBody
{
    static constexpr auto name = "prefixedIdBody";
};

using FunctionPrefixedIdPrefix = FunctionPrefixedIdPart<NamePrefixedIdPrefix, true>;
using FunctionPrefixedIdBody = FunctionPrefixedIdPart<NamePrefixedIdBody, false>;

class FunctionSplitPrefixedId : public IFunction
{
public:
    static constexpr auto name = "splitPrefixedId";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSplitPrefixedId>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()},
            Strings{"prefix", "body"});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString & col_string = getStringColumn(arguments[0].column, name, "first");

        auto col_prefix = ColumnString::create();
        auto col_body = ColumnString::create();
        col_prefix->reserve(input_rows_count);
        col_body->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view prefix;
            std::string_view body;
            splitPrefixedId(col_string.getDataAt(row), prefix, body);

            col_prefix->insertData(prefix.data(), prefix.size());
            col_body->insertData(body.data(), body.size());
        }

        Columns tuple_columns(2);
        tuple_columns[0] = std::move(col_prefix);
        tuple_columns[1] = std::move(col_body);
        return ColumnTuple::create(std::move(tuple_columns));
    }
};

class FunctionIsValidPrefixedId : public IFunction
{
public:
    static constexpr auto name = "isValidPrefixedId";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsValidPrefixedId>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        FunctionArgumentDescriptors optional_args{
            {"prefix", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString & col_string = getStringColumn(arguments[0].column, name, "first");

        const ColumnString * col_expected_prefix = nullptr;
        ColumnPtr full_prefix_column;
        if (arguments.size() > 1)
        {
            full_prefix_column = arguments[1].column->convertToFullColumnIfConst();
            col_expected_prefix = &getStringColumn(full_prefix_column, name, "second");
        }

        auto col_res = ColumnUInt8::create();
        typename ColumnUInt8::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view id = col_string.getDataAt(row);
            bool valid = isValidPrefixedIdImpl(id);

            if (col_expected_prefix)
            {
                std::string_view expected_prefix = col_expected_prefix->getDataAt(row);
                /// An empty expected prefix is ambiguous (it could mean "no prefix" or
                /// "any prefix"), so it is rejected instead of silently matching.
                if (expected_prefix.empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Empty prefix in function {}: pass a non-empty expected prefix, or use the single-argument form",
                        name);

                std::string_view prefix;
                std::string_view body;
                splitPrefixedId(id, prefix, body);
                valid = valid && prefix == expected_prefix;
            }

            vec_res[row] = valid;
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(GeneratePrefixedId)
{
    FunctionDocumentation::Description description = R"(
Generates a prefixed identifier `<prefix>_<body>` in the style popularized by Stripe, e.g. `cus_NffrFeUfNV2Hib`.
The body consists of random base62 characters (`[0-9A-Za-z]`).
The prefix must be non-empty and consist of underscore-separated segments matching `[A-Za-z][A-Za-z0-9]*` (e.g. `cus` or `pk_test`); an exception is thrown otherwise.
)";
    FunctionDocumentation::Syntax syntax = "generatePrefixedId(prefix[, length])";
    FunctionDocumentation::Arguments arguments = {
        {"prefix", "Prefix of the identifier.", {"String"}},
        {"length", "Optional. Length of the body, in [1, 255]. Default: 22, which carries about 131 bits of entropy.", {"UInt8, UInt16, UInt32, or UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a prefixed identifier with a random base62 body.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT generatePrefixedId('cus'), generatePrefixedId('pk_test', 10);",
        R"(
┌─generatePrefixedId('cus')───┬─generatePrefixedId('pk_test', 10)─┐
│ cus_h1zHkD3XlqvCpVZjBg9Ttw  │ pk_test_YZk20a9Fq3                │
└─────────────────────────────┴───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGeneratePrefixedId>(documentation);
}

REGISTER_FUNCTION(PrefixedIdPrefix)
{
    FunctionDocumentation::Description description = R"(
Returns the prefix of a prefixed identifier such as `cus_NffrFeUfNV2Hib`: the substring before the last underscore, e.g. `cus`.
Returns an empty string if the identifier contains no underscore.
The split is purely positional and never fails; use [`isValidPrefixedId`](#isValidPrefixedId) to validate the identifier.
)";
    FunctionDocumentation::Syntax syntax = "prefixedIdPrefix(id)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Prefixed identifier.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the prefix of the identifier.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT prefixedIdPrefix('pk_test_51TpZvW');",
        R"(
┌─prefixedIdP⋯_51TpZvW')─┐
│ pk_test                │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrefixedIdPrefix>(documentation);
}

REGISTER_FUNCTION(PrefixedIdBody)
{
    FunctionDocumentation::Description description = R"(
Returns the body of a prefixed identifier such as `cus_NffrFeUfNV2Hib`: the substring after the last underscore, e.g. `NffrFeUfNV2Hib`.
Returns the whole string if the identifier contains no underscore.
The split is purely positional and never fails; use [`isValidPrefixedId`](#isValidPrefixedId) to validate the identifier.
)";
    FunctionDocumentation::Syntax syntax = "prefixedIdBody(id)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Prefixed identifier.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the body of the identifier.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT prefixedIdBody('cus_NffrFeUfNV2Hib');",
        R"(
┌─prefixedIdB⋯fNV2Hib')─┐
│ NffrFeUfNV2Hib        │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrefixedIdBody>(documentation);
}

REGISTER_FUNCTION(SplitPrefixedId)
{
    FunctionDocumentation::Description description = R"(
Splits a prefixed identifier such as `cus_NffrFeUfNV2Hib` at the last underscore into a tuple of prefix and body, consistent with [`prefixedIdPrefix`](#prefixedIdPrefix) and [`prefixedIdBody`](#prefixedIdBody).
For an identifier without an underscore, the prefix is empty and the body is the whole string.
The split is purely positional and never fails; use [`isValidPrefixedId`](#isValidPrefixedId) to validate the identifier.
)";
    FunctionDocumentation::Syntax syntax = "splitPrefixedId(id)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Prefixed identifier.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple of the prefix and the body of the identifier.", {"Tuple(prefix String, body String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT splitPrefixedId('pk_test_51TpZvW');",
        R"(
┌─splitPrefix⋯_51TpZvW')─┐
│ ('pk_test','51TpZvW')  │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSplitPrefixedId>(documentation);
}

REGISTER_FUNCTION(IsValidPrefixedId)
{
    FunctionDocumentation::Description description = R"(
Checks whether a string is a valid prefixed identifier: an optional underscore-separated prefix followed by a non-empty base62 body, i.e. it matches `^([A-Za-z][A-Za-z0-9]*(_[A-Za-z0-9]+)*_)?[0-9A-Za-z]+$`.
A bare body without a prefix (e.g. `abc123`) is valid; an empty body (e.g. `cus_`) or an empty prefix segment (e.g. `_abc`) is not.
With the optional second argument, additionally requires the prefix of the identifier (the part before the last underscore) to equal `prefix` exactly.
An empty `prefix` argument is ambiguous and throws an exception.
)";
    FunctionDocumentation::Syntax syntax = "isValidPrefixedId(id[, prefix])";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Identifier to check.", {"String"}},
        {"prefix", "Optional. Expected prefix; must be non-empty.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1 if the string is a valid prefixed identifier (with the expected prefix, if given), 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT isValidPrefixedId('cus_NffrFeUfNV2Hib') AS valid, isValidPrefixedId('cus_NffrFeUfNV2Hib', 'pk') AS wrong_prefix, isValidPrefixedId('cus_') AS empty_body;",
        R"(
┌─valid─┬─wrong_prefix─┬─empty_body─┐
│     1 │            0 │          0 │
└───────┴──────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsValidPrefixedId>(documentation);
}

}

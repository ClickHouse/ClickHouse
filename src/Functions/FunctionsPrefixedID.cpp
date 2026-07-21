#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/ErrnoException.h>
#include <Common/StringUtils.h>

#include <pcg_random.hpp>

#if defined(OS_DARWIN) || defined(OS_FREEBSD)
#    include <sys/random.h>
#else
#    include <unistd.h>
#    if defined(__GLIBC__) && !__GLIBC_PREREQ(2, 25)
/// Old glibc sysroots (e.g. powerpc64le) have neither <sys/random.h> nor a declaration of
/// getentropy in <unistd.h>; the symbol itself is provided by base/glibc-compatibility.
extern "C" int getentropy(void * buffer, size_t length);
#    endif
#endif

/// Functions for prefixed identifiers in the style popularized by Stripe: `<prefix>_<body>`,
/// e.g. `user_NffrFeUfNV2Hib` or `ch_test_51TpZvW`. The prefix may contain underscores
/// (multi-segment); the body is base62 ([0-9A-Za-z]), which never contains an underscore,
/// so all readers split the identifier at the last underscore.

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int SYSTEM_ERROR;
}

namespace
{

/// Split at the last underscore: everything before it is the prefix, everything after is
/// the body. Without an underscore, the prefix is empty and the whole string is the body.
/// Purely positional, never fails: `user_` gives ("user", "") and `_abc` gives ("", "abc").
void splitPrefixedID(std::string_view id, std::string_view & prefix, std::string_view & body)
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
bool isValidPrefixedIDImpl(std::string_view id)
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

/// Validity of a prefix passed to generatePrefixedID: non-empty, and every underscore-separated
/// segment matches [A-Za-z][A-Za-z0-9]* (stricter than isValidPrefixedID, which only requires
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

/// Prefixed identifiers often have a fixed width (e.g. generatePrefixedID('user') always
/// returns 27 characters), so the identifier inputs accept both String and FixedString.
/// Both column types return the row value from getDataAt, so the callers can read rows
/// generically.
const IColumn & getStringOrFixedStringColumn(const ColumnPtr & column, const char * function_name, const char * argument_name)
{
    if (!checkAndGetColumn<ColumnString>(column.get()) && !checkAndGetColumn<ColumnFixedString>(column.get()))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of {} argument of function {}, must be String or FixedString",
            column->getName(), argument_name, function_name);
    return *column;
}

class FunctionGeneratePrefixedID : public IFunction
{
public:
    static constexpr auto name = "generatePrefixedID";

    /// 22 base62 characters span a space of 62^22 ≈ 2^131 values, comparable to the
    /// 2^128 UUID space. The bodies are pseudo-random (a PRNG stream seeded from the OS
    /// entropy source per block), so this is a size of the value space, not a guarantee
    /// of 131 bits of entropy; the identifiers are not suitable as security tokens.
    static constexpr size_t DEFAULT_BODY_LENGTH = 22;
    static constexpr size_t MAX_BODY_LENGTH = 255;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeneratePrefixedID>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    /// The ignored `expr` argument must not affect the result, so a NULL expr must not turn the
    /// result into NULL (same as generateUUIDv7 and generateSnowflakeID). The semantic arguments
    /// then reject Nullable types through their validators instead of silently returning NULL.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"prefix", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        FunctionArgumentDescriptors optional_args{
            {"length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), nullptr, "Native unsigned integer"},
            {"expr", nullptr, nullptr, "Arbitrary expression"}};
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

        /// Seed from the OS entropy source rather than randomSeed(): the latter is derived
        /// from the time and the thread id, which is too predictable for identifiers whose
        /// point is to be unique and opaque.
        UInt64 entropy[2];
        if (getentropy(entropy, sizeof(entropy)) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot get entropy from the operating system in function {}", name);
        using UInt128Raw = unsigned __int128;
        pcg64_fast rng((static_cast<UInt128Raw>(entropy[0]) << 64) | entropy[1]);

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
class FunctionPrefixedIDPart : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPrefixedIDPart>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
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
            std::string_view prefix;
            std::string_view body;
            splitPrefixedID(col_string.getDataAt(row), prefix, body);

            std::string_view part = extract_prefix ? prefix : body;
            col_res->insertData(part.data(), part.size());
        }

        return col_res;
    }
};

struct NamePrefixedIDPrefix
{
    static constexpr auto name = "prefixedIDPrefix";
};

struct NamePrefixedIDBody
{
    static constexpr auto name = "prefixedIDBody";
};

using FunctionPrefixedIDPrefix = FunctionPrefixedIDPart<NamePrefixedIDPrefix, true>;
using FunctionPrefixedIDBody = FunctionPrefixedIDPart<NamePrefixedIDBody, false>;

class FunctionSplitPrefixedID : public IFunction
{
public:
    static constexpr auto name = "splitPrefixedID";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSplitPrefixedID>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()},
            Strings{"prefix", "body"});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & col_string = getStringOrFixedStringColumn(arguments[0].column, name, "first");

        auto col_prefix = ColumnString::create();
        auto col_body = ColumnString::create();
        col_prefix->reserve(input_rows_count);
        col_body->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view prefix;
            std::string_view body;
            splitPrefixedID(col_string.getDataAt(row), prefix, body);

            col_prefix->insertData(prefix.data(), prefix.size());
            col_body->insertData(body.data(), body.size());
        }

        Columns tuple_columns(2);
        tuple_columns[0] = std::move(col_prefix);
        tuple_columns[1] = std::move(col_body);
        return ColumnTuple::create(std::move(tuple_columns));
    }
};

class FunctionIsValidPrefixedID : public IFunction
{
public:
    static constexpr auto name = "isValidPrefixedID";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsValidPrefixedID>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
        FunctionArgumentDescriptors optional_args{
            {"prefix", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & col_string = getStringOrFixedStringColumn(arguments[0].column, name, "first");

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
            bool valid = isValidPrefixedIDImpl(id);

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
                splitPrefixedID(id, prefix, body);
                valid = valid && prefix == expected_prefix;
            }

            vec_res[row] = valid;
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(GeneratePrefixedID)
{
    FunctionDocumentation::Description description = R"(
Generates a prefixed identifier `<prefix>_<body>` in the style popularized by Stripe, e.g. `user_NffrFeUfNV2Hib`.
The body consists of pseudo-random base62 characters (`[0-9A-Za-z]`) drawn from a generator seeded from the OS entropy source.
The identifiers are meant to be unique and opaque, but they are not cryptographic tokens.
The prefix must be non-empty and consist of underscore-separated segments matching `[A-Za-z][A-Za-z0-9]*` (e.g. `user` or `ch_test`); an exception is thrown otherwise.
)";
    FunctionDocumentation::Syntax syntax = "generatePrefixedID(prefix[, length[, expr]])";
    FunctionDocumentation::Arguments arguments = {
        {"prefix", "Prefix of the identifier.", {"String"}},
        {"length", "Optional. Length of the body, in [1, 255]. Default: 22, which gives a body space of 62^22 ≈ 2^131 values, comparable to a UUID.", {"UInt8, UInt16, UInt32, or UInt64"}},
        {"expr", "Optional. An arbitrary expression used to bypass [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) if the function is called multiple times in a query. The value of the expression has no effect on the returned identifier.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a prefixed identifier with a random base62 body.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT generatePrefixedID('user'), generatePrefixedID('ch_test', 10);",
        R"(
┌─generatePrefixedID('user')──┬─generatePref⋯_test', 10)─┐
│ user_DvJvYzpK9RiWDO8Yw5fRpI │ ch_test_ga43BKOCwR       │
└─────────────────────────────┴──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGeneratePrefixedID>(documentation);
}

REGISTER_FUNCTION(PrefixedIDPrefix)
{
    FunctionDocumentation::Description description = R"(
Returns the prefix of a prefixed identifier such as `user_NffrFeUfNV2Hib`: the substring before the last underscore, e.g. `user`.
Returns an empty string if the identifier contains no underscore.
The split is purely positional and never fails; use [`isValidPrefixedID`](#isValidPrefixedID) to validate the identifier.
)";
    FunctionDocumentation::Syntax syntax = "prefixedIDPrefix(id)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Prefixed identifier.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the prefix of the identifier.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT prefixedIDPrefix('ch_test_51TpZvW');",
        R"(
┌─prefixedIDP⋯_51TpZvW')─┐
│ ch_test                │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrefixedIDPrefix>(documentation);
}

REGISTER_FUNCTION(PrefixedIDBody)
{
    FunctionDocumentation::Description description = R"(
Returns the body of a prefixed identifier such as `user_NffrFeUfNV2Hib`: the substring after the last underscore, e.g. `NffrFeUfNV2Hib`.
Returns the whole string if the identifier contains no underscore.
The split is purely positional and never fails; use [`isValidPrefixedID`](#isValidPrefixedID) to validate the identifier.
)";
    FunctionDocumentation::Syntax syntax = "prefixedIDBody(id)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Prefixed identifier.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the body of the identifier.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT prefixedIDBody('user_NffrFeUfNV2Hib');",
        R"(
┌─prefixedIDB⋯fNV2Hib')─┐
│ NffrFeUfNV2Hib        │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrefixedIDBody>(documentation);
}

REGISTER_FUNCTION(SplitPrefixedID)
{
    FunctionDocumentation::Description description = R"(
Splits a prefixed identifier such as `user_NffrFeUfNV2Hib` at the last underscore into a tuple of prefix and body, consistent with [`prefixedIDPrefix`](#prefixedIDPrefix) and [`prefixedIDBody`](#prefixedIDBody).
For an identifier without an underscore, the prefix is empty and the body is the whole string.
The split is purely positional and never fails; use [`isValidPrefixedID`](#isValidPrefixedID) to validate the identifier.
)";
    FunctionDocumentation::Syntax syntax = "splitPrefixedID(id)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Prefixed identifier.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple of the prefix and the body of the identifier.", {"Tuple(prefix String, body String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT splitPrefixedID('ch_test_51TpZvW');",
        R"(
┌─splitPrefix⋯_51TpZvW')─┐
│ ('ch_test','51TpZvW')  │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSplitPrefixedID>(documentation);
}

REGISTER_FUNCTION(IsValidPrefixedID)
{
    FunctionDocumentation::Description description = R"(
Checks whether a string is a valid prefixed identifier: an optional underscore-separated prefix followed by a non-empty base62 body, i.e. it matches `^([A-Za-z][A-Za-z0-9]*(_[A-Za-z0-9]+)*_)?[0-9A-Za-z]+$`.
A bare body without a prefix (e.g. `abc123`) is valid; an empty body (e.g. `user_`) or an empty prefix segment (e.g. `_abc`) is not.
With the optional second argument, additionally requires the prefix of the identifier (the part before the last underscore) to equal `prefix` exactly.
An empty `prefix` argument is ambiguous and throws an exception.
)";
    FunctionDocumentation::Syntax syntax = "isValidPrefixedID(id[, prefix])";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Identifier to check.", {"String", "FixedString"}},
        {"prefix", "Optional. Expected prefix; must be non-empty.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1 if the string is a valid prefixed identifier (with the expected prefix, if given), 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT isValidPrefixedID('user_NffrFeUfNV2Hib') AS valid, isValidPrefixedID('user_NffrFeUfNV2Hib', 'ch') AS wrong_prefix, isValidPrefixedID('user_') AS empty_body;",
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

    factory.registerFunction<FunctionIsValidPrefixedID>(documentation);
}

}

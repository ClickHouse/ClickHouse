#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FunctionDocumentation.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>

#include <mutex>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Each matcher checks one token against the needle. The text index runs the same check on its dictionary.

struct TokenPrefixMatcher
{
    static constexpr auto name = "hasTokenPrefix";

    explicit TokenPrefixMatcher(const String & prefix_) : prefix(prefix_) {}
    bool operator()(std::string_view token) const { return token.starts_with(prefix); }

    String prefix;
};

struct TokenLikeMatcher
{
    static constexpr auto name = "hasTokenLike";

    explicit TokenLikeMatcher(const String & pattern) : regexp(Regexps::createRegexp</*like*/ true, /*no_capture*/ true, /*case_insensitive*/ false>(pattern)) {}
    bool operator()(std::string_view token) const { return regexp.match(token.data(), token.size()); }

    OptimizedRegularExpression regexp;
};

struct TokenMatchMatcher
{
    static constexpr auto name = "hasTokenMatch";

    explicit TokenMatchMatcher(const String & pattern) : regexp(Regexps::createRegexp</*like*/ false, /*no_capture*/ true, /*case_insensitive*/ false>(pattern)) {}
    bool operator()(std::string_view token) const { return regexp.match(token.data(), token.size()); }

    OptimizedRegularExpression regexp;
};

bool isStringOrFixedStringOrArrayOfStringOrFixedString(const IDataType & type)
{
    if (isStringOrFixedString(type))
        return true;

    if (const auto * array_type = checkAndGetDataType<DataTypeArray>(&type))
        return isStringOrFixedString(removeNullable(array_type->getNestedType()));

    return false;
}

template <typename Matcher>
class FunctionHasTokenPattern : public IFunction
{
public:
    static constexpr auto name = Matcher::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionHasTokenPattern>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args
        {
            {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedStringOrArrayOfStringOrFixedString), nullptr, "String, FixedString, Array(String) or Array(FixedString)"},
            {"needle", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };

        FunctionArgumentDescriptors optional_args
        {
            {"tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// The needle and the tokenizer are parsed once.
        std::call_once(init_flag, [&]
        {
            const String tokenizer_name = arguments.size() < 3
                ? String(SplitByNonAlphaTokenizer::getExternalName())
                : String(arguments[2].column->getDataAt(0));
            shared_tokenizer = TokenizerFactory::instance().get(tokenizer_name);
            matcher.emplace(String(arguments[1].column->getDataAt(0)));
        });

        /// A stateful tokenizer is not thread-safe, so each call gets its own copy.
        const auto cloned_tokenizer = shared_tokenizer->isStateful() ? shared_tokenizer->clone() : nullptr;
        const ITokenizer & tokenizer = cloned_tokenizer ? *cloned_tokenizer : *shared_tokenizer;

        auto has_matching_token = [&](std::string_view value)
        {
            bool found = false;
            forEachToken(tokenizer, value.data(), value.size(), [&](const char * token, size_t length)
            {
                found = (*matcher)(std::string_view(token, length));
                return found;
            });
            return found;
        };

        auto col_res = ColumnUInt8::create(input_rows_count);
        auto & res = col_res->getData();

        const IColumn & col_input = *arguments[0].column;

        if (isColumnStringOrFixedString(col_input))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                res[i] = has_matching_token(col_input.getDataAt(i));
        }
        else if (const auto * col_array = checkAndGetColumn<ColumnArray>(&col_input))
        {
            const auto & offsets = col_array->getOffsets();
            const auto * col_nullable = checkAndGetColumn<ColumnNullable>(&col_array->getData());
            const IColumn & col_elements = col_nullable ? col_nullable->getNestedColumn() : col_array->getData();

            size_t current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                res[i] = false;
                for (size_t j = current_offset; j < offsets[i] && !res[i]; ++j)
                {
                    if (col_nullable && col_nullable->isNullAt(j))
                        continue;
                    res[i] = has_matching_token(col_elements.getDataAt(j));
                }
                current_offset = offsets[i];
            }
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of the first argument of function {}", col_input.getName(), getName());
        }

        return col_res;
    }

private:
    static bool isColumnStringOrFixedString(const IColumn & column)
    {
        return checkAndGetColumn<ColumnString>(&column) || checkAndGetColumn<ColumnFixedString>(&column);
    }

    mutable std::once_flag init_flag;
    mutable std::unique_ptr<ITokenizer> shared_tokenizer;
    mutable std::optional<Matcher> matcher;
};

using FunctionHasTokenPrefix = FunctionHasTokenPattern<TokenPrefixMatcher>;
using FunctionHasTokenLike = FunctionHasTokenPattern<TokenLikeMatcher>;
using FunctionHasTokenMatch = FunctionHasTokenPattern<TokenMatchMatcher>;

constexpr auto text_index_note = R"(
<Note>
Column `input` should have a [text index](/reference/engines/table-engines/mergetree-family/textindexes) defined for optimal performance.
The function then finds the matching tokens in the index dictionary and reads only their posting lists instead of tokenizing every row.
The index is not used if it has a [preprocessor](/reference/engines/table-engines/mergetree-family/textindexes#preprocessor-argument-optional)
or a [postprocessor](/reference/engines/table-engines/mergetree-family/textindexes#postprocessor-argument-optional).
In these cases the function is evaluated on the raw `input` values, still with the tokenizer of the index.
</Note>
)";

constexpr auto tokenizer_description = R"(
Prior to searching, the function tokenizes `input` using the tokenizer specified for the text index on `input`, and the `splitByNonAlpha` tokenizer if `input` has no text index.
As for [`hasAnyTokens`](#hasAnyTokens), whether the tokenizer of the index is used depends on the query plan: it is used in filters and projections directly over the table, including conditions pushed down to it such as `HAVING` on a grouping key,
but not in expressions after `GROUP BY` or `JOIN`, in mutations such as `ALTER TABLE ... DELETE`, or in conditions that are not pushed down, where the result can differ.
The optional `tokenizer` argument sets the tokenizer explicitly, which gives the same result in every query, and then the text index is used only if it has the same tokenizer.
If several text indexes on `input` would give the function different tokenizers, it throws an exception, and the `tokenizer` argument selects among them.
)";

FunctionDocumentation::Arguments commonArguments(const char * needle_name, const char * needle_description)
{
    return {
        {"input", "The input column.", {"String", "FixedString", "Nullable(String)", "Nullable(FixedString)", "Array(String)", "Array(FixedString)", "Array(Nullable(String))", "Array(Nullable(FixedString))"}},
        {needle_name, needle_description, {"const String"}},
        {"tokenizer", "The tokenizer to use. Valid arguments are the same as for [`tokens`](/reference/functions/regular-functions/splitting-merging-functions#tokens). Optional, if not set explicitly, defaults to the tokenizer of the text index or to `splitByNonAlpha`.", {"const String"}},
    };
}

}

REGISTER_FUNCTION(HasTokenPrefix)
{
    FunctionDocumentation::Description description = String(R"(
Returns 1 if at least one token of `input` starts with `prefix`, and 0 otherwise.

The comparison is case-sensitive. An empty `prefix` matches every token, so the function returns 1 if `input` has at least one token.

If `input` has no text index, `hasTokenPrefix(input, prefix)` is equivalent to `arrayExists(t -> startsWith(t, prefix), tokens(input))`.
)") + tokenizer_description + text_index_note;
    FunctionDocumentation::Syntax syntax = "hasTokenPrefix(input, prefix[, tokenizer])";
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if some token starts with `prefix`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT hasTokenPrefix('Payment charged twice', 'charg')",
        R"(
┌─hasTokenPrefix('Payment charged twice', 'charg')─┐
│                                                1 │
└──────────────────────────────────────────────────┘
        )"
    },
    {
        "The prefix must be at the start of a token",
        "SELECT hasTokenPrefix('recharge', 'charg')",
        R"(
┌─hasTokenPrefix('recharge', 'charg')─┐
│                                   0 │
└─────────────────────────────────────┘
        )"
    },
    {
        "Custom tokenizer",
        R"(SELECT hasTokenPrefix('key=value;flag', 'val', 'splitByString([\'=\', \';\'])'))",
        R"(
┌─hasTokenPrefix('key=value;flag', 'val', 'splitByString([\'=\', \';\'])')─┐
│                                                                        1 │
└──────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, commonArguments("prefix", "The token prefix to search for."), {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasTokenPrefix>(documentation);
}

REGISTER_FUNCTION(HasTokenLike)
{
    FunctionDocumentation::Description description = String(R"(
Returns 1 if at least one token of `input` matches the [`LIKE`](#like) pattern `pattern`, and 0 otherwise.

The pattern is applied to each token separately and must match the whole token: `%` matches any sequence of bytes, `_` matches one character, and `\` escapes them.

If `input` has no text index, `hasTokenLike(input, pattern)` is equivalent to `arrayExists(t -> like(t, pattern), tokens(input))`.
)") + tokenizer_description + text_index_note;
    FunctionDocumentation::Syntax syntax = "hasTokenLike(input, pattern[, tokenizer])";
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if some token matches `pattern`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT hasTokenLike('Payment charged twice', 'ch%ed')",
        R"(
┌─hasTokenLike('Payment charged twice', 'ch%ed')─┐
│                                              1 │
└────────────────────────────────────────────────┘
        )"
    },
    {
        "The pattern must match a whole token",
        "SELECT hasTokenLike('Payment charged twice', 'charge')",
        R"(
┌─hasTokenLike('Payment charged twice', 'charge')─┐
│                                               0 │
└─────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, commonArguments("pattern", "The `LIKE` pattern each token is matched against."), {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasTokenLike>(documentation);
}

REGISTER_FUNCTION(HasTokenMatch)
{
    FunctionDocumentation::Description description = String(R"(
Returns 1 if at least one token of `input` matches the regular expression `regexp`, and 0 otherwise.

The regular expression uses the [re2 syntax](https://github.com/google/re2/wiki/Syntax) and is applied to each token separately, like function [`match`](#match):
it may match any part of the token, and the anchors `^` and `$` refer to the start and the end of the token.

If `input` has no text index, `hasTokenMatch(input, regexp)` is equivalent to `arrayExists(t -> match(t, regexp), tokens(input))`.
)") + tokenizer_description + text_index_note;
    FunctionDocumentation::Syntax syntax = "hasTokenMatch(input, regexp[, tokenizer])";
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if some token matches `regexp`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT hasTokenMatch('order 12345 shipped', '^[0-9]{5}$')",
        R"(
┌─hasTokenMatch('order 12345 shipped', '^[0-9]{5}$')─┐
│                                                  1 │
└────────────────────────────────────────────────────┘
        )"
    },
    {
        "Anchors refer to token boundaries",
        "SELECT hasTokenMatch('order 123456 shipped', '^[0-9]{5}$')",
        R"(
┌─hasTokenMatch('order 123456 shipped', '^[0-9]{5}$')─┐
│                                                   0 │
└─────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, commonArguments("regexp", "The regular expression each token is matched against."), {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasTokenMatch>(documentation);
}

}

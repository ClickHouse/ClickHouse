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

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Each matcher checks one token against one pattern. The text index runs the same check on its dictionary.

struct TokenPrefixMatcher
{
    explicit TokenPrefixMatcher(const String & prefix_) : prefix(prefix_) {}
    bool operator()(std::string_view token) const { return token.starts_with(prefix); }

    String prefix;
};

struct TokenLikeMatcher
{
    explicit TokenLikeMatcher(const String & pattern) : regexp(Regexps::createRegexp</*like*/ true, /*no_capture*/ true, /*case_insensitive*/ false>(pattern)) {}
    bool operator()(std::string_view token) const { return regexp.match(token.data(), token.size()); }

    OptimizedRegularExpression regexp;
};

struct TokenRegexpMatcher
{
    explicit TokenRegexpMatcher(const String & pattern) : regexp(Regexps::createRegexp</*like*/ false, /*no_capture*/ true, /*case_insensitive*/ false>(pattern)) {}
    bool operator()(std::string_view token) const { return regexp.match(token.data(), token.size()); }

    OptimizedRegularExpression regexp;
};

struct HasAnyTokenPrefixTraits
{
    static constexpr auto name = "hasAnyTokenPrefix";
    using Matcher = TokenPrefixMatcher;
    static constexpr bool match_all = false;
};

struct HasAnyTokenLikeTraits
{
    static constexpr auto name = "hasAnyTokenLike";
    using Matcher = TokenLikeMatcher;
    static constexpr bool match_all = false;
};

struct HasAllTokenLikeTraits
{
    static constexpr auto name = "hasAllTokenLike";
    using Matcher = TokenLikeMatcher;
    static constexpr bool match_all = true;
};

struct HasTokenMatchTraits
{
    static constexpr auto name = "hasTokenMatch";
    using Matcher = TokenRegexpMatcher;
    static constexpr bool match_all = false;
};

bool isStringOrFixedStringOrArrayOfStringOrFixedString(const IDataType & type)
{
    if (isStringOrFixedString(type))
        return true;

    if (const auto * array_type = checkAndGetDataType<DataTypeArray>(&type))
        return isStringOrFixedString(removeNullable(array_type->getNestedType()));

    return false;
}

bool isStringOrArrayOfStringType(const IDataType & type)
{
    if (isString(type))
        return true;

    if (const auto * array_type = checkAndGetDataType<DataTypeArray>(&type))
        return isString(array_type->getNestedType()) || isNothing(array_type->getNestedType());

    return false;
}

template <typename Traits>
class FunctionHasTokenPattern : public IFunction
{
public:
    static constexpr auto name = Traits::name;

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
            {"patterns", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrArrayOfStringType), isColumnConst, "const String or const Array(String)"},
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
        /// The patterns and the tokenizer are parsed once.
        std::call_once(init_flag, [&]
        {
            const String tokenizer_name = arguments.size() < 3
                ? String(SplitByNonAlphaTokenizer::getExternalName())
                : String(arguments[2].column->getDataAt(0));
            shared_tokenizer = TokenizerFactory::instance().get(tokenizer_name);

            /// A String is one pattern, it is not split into tokens.
            const Field patterns = (*arguments[1].column)[0];
            if (patterns.getType() == Field::Types::String)
            {
                matchers.emplace_back(patterns.safeGet<String>());
            }
            else
            {
                for (const auto & pattern : patterns.safeGet<Array>())
                    matchers.emplace_back(pattern.safeGet<String>());
            }
        });

        auto col_res = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
        if (matchers.empty())
            return col_res;

        /// A stateful tokenizer is not thread-safe, so each call gets its own copy.
        const auto cloned_tokenizer = shared_tokenizer->isStateful() ? shared_tokenizer->clone() : nullptr;
        const ITokenizer & tokenizer = cloned_tokenizer ? *cloned_tokenizer : *shared_tokenizer;

        /// For `match_all`: the patterns already matched by a token of the current row.
        std::vector<UInt8> matched(matchers.size());
        size_t num_matched = 0;

        auto start_row = [&]
        {
            std::fill(matched.begin(), matched.end(), 0);
            num_matched = 0;
        };

        /// Adds the tokens of one value to the current row, returns true once the row matches.
        auto add_value = [&](std::string_view value)
        {
            bool row_matches = false;
            forEachToken(tokenizer, value.data(), value.size(), [&](const char * token_data, size_t length)
            {
                const std::string_view token(token_data, length);
                for (size_t i = 0; i < matchers.size(); ++i)
                {
                    if constexpr (Traits::match_all)
                    {
                        if (!matched[i] && matchers[i](token))
                        {
                            matched[i] = 1;
                            ++num_matched;
                        }
                    }
                    else if (matchers[i](token))
                    {
                        row_matches = true;
                        break;
                    }
                }

                if constexpr (Traits::match_all)
                    row_matches = num_matched == matchers.size();
                return row_matches;
            });
            return row_matches;
        };

        auto & res = col_res->getData();
        const IColumn & col_input = *arguments[0].column;

        if (isColumnStringOrFixedString(col_input))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                start_row();
                res[i] = add_value(col_input.getDataAt(i));
            }
        }
        else if (const auto * col_array = checkAndGetColumn<ColumnArray>(&col_input))
        {
            const auto & offsets = col_array->getOffsets();
            const auto * col_nullable = checkAndGetColumn<ColumnNullable>(&col_array->getData());
            const IColumn & col_elements = col_nullable ? col_nullable->getNestedColumn() : col_array->getData();

            size_t current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                start_row();
                for (size_t j = current_offset; j < offsets[i] && !res[i]; ++j)
                {
                    if (col_nullable && col_nullable->isNullAt(j))
                        continue;
                    res[i] = add_value(col_elements.getDataAt(j));
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
    mutable std::vector<typename Traits::Matcher> matchers;
};

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

FunctionDocumentation::Arguments commonArguments(const char * patterns_name, const char * patterns_description)
{
    return {
        {"input", "The input column.", {"String", "FixedString", "Nullable(String)", "Nullable(FixedString)", "Array(String)", "Array(FixedString)", "Array(Nullable(String))", "Array(Nullable(FixedString))"}},
        {patterns_name, patterns_description, {"const String", "const Array(String)"}},
        {"tokenizer", "The tokenizer to use. Valid arguments are the same as for [`tokens`](/reference/functions/regular-functions/splitting-merging-functions#tokens). Optional, if not set explicitly, defaults to the tokenizer of the text index or to `splitByNonAlpha`.", {"const String"}},
    };
}

}

REGISTER_FUNCTION(HasAnyTokenPrefix)
{
    FunctionDocumentation::Description description = String(R"(
Returns 1 if at least one token of `input` starts with one of `prefixes`, and 0 otherwise.

`prefixes` is one prefix (`String`, not split into tokens) or several (`Array(String)`).
The prefix is matched literally and case-sensitively. An empty prefix matches every token; an empty array matches nothing.

If `input` has no text index, `hasAnyTokenPrefix(input, prefixes)` with an array `prefixes` is equivalent to `arrayExists(t -> arrayExists(p -> startsWith(t, p), prefixes), tokens(input))`, and a single prefix `p` is the same as `[p]`.
A prefix without the characters `%`, `_` and `\` gives the same result as the pattern `prefix%` of [`hasAnyTokenLike`](#hasAnyTokenLike).
)") + tokenizer_description + text_index_note;
    FunctionDocumentation::Syntax syntax = "hasAnyTokenPrefix(input, prefixes[, tokenizer])";
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if some token starts with one of `prefixes`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT hasAnyTokenPrefix('Payment charged twice', 'charg')",
        R"(
┌─hasAnyTokenPrefix('Payment charged twice', 'charg')─┐
│                                                   1 │
└─────────────────────────────────────────────────────┘
        )"
    },
    {
        "Several prefixes",
        "SELECT hasAnyTokenPrefix('Payment refunded', ['charg', 'refund'])",
        R"(
┌─hasAnyTokenPrefix('Payment refunded', ['charg', 'refund'])─┐
│                                                          1 │
└────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "The prefix must be at the start of a token",
        "SELECT hasAnyTokenPrefix('recharge', 'charg')",
        R"(
┌─hasAnyTokenPrefix('recharge', 'charg')─┐
│                                      0 │
└────────────────────────────────────────┘
        )"
    },
    {
        "Custom tokenizer",
        R"(SELECT hasAnyTokenPrefix('key=value;flag', 'val', 'splitByString([\'=\', \';\'])'))",
        R"(
┌─hasAnyTokenPrefix('key=value;flag', 'val', 'splitByString([\'=\', \';\'])')─┐
│                                                                           1 │
└─────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, commonArguments("prefixes", "The token prefix, or an array of token prefixes, to search for."), {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasTokenPattern<HasAnyTokenPrefixTraits>>(documentation);
}

REGISTER_FUNCTION(HasAnyTokenLike)
{
    FunctionDocumentation::Description description = String(R"(
Returns 1 if at least one token of `input` matches one of the [`LIKE`](#like) patterns `patterns`, and 0 otherwise.

`patterns` is one pattern (`String`, not split into tokens) or several (`Array(String)`).
Each pattern is applied to each token separately and must match the whole token: `%` matches any sequence of bytes, `_` matches one character, and `\` escapes them.
An empty array matches nothing.

If `input` has no text index, `hasAnyTokenLike(input, patterns)` with an array `patterns` is equivalent to `arrayExists(t -> arrayExists(p -> like(t, p), patterns), tokens(input))`, and a single pattern `p` is the same as `[p]`.
)") + tokenizer_description + text_index_note;
    FunctionDocumentation::Syntax syntax = "hasAnyTokenLike(input, patterns[, tokenizer])";
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if some token matches one of `patterns`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT hasAnyTokenLike('Payment charged twice', 'ch%ed')",
        R"(
┌─hasAnyTokenLike('Payment charged twice', 'ch%ed')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
        )"
    },
    {
        "Several patterns",
        "SELECT hasAnyTokenLike('Payment charged twice', ['%ing', 'tw_ce'])",
        R"(
┌─hasAnyTokenLike('Payment charged twice', ['%ing', 'tw_ce'])─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "The pattern must match a whole token",
        "SELECT hasAnyTokenLike('Payment charged twice', 'charge')",
        R"(
┌─hasAnyTokenLike('Payment charged twice', 'charge')─┐
│                                                  0 │
└────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, commonArguments("patterns", "The `LIKE` pattern, or an array of `LIKE` patterns, each token is matched against."), {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasTokenPattern<HasAnyTokenLikeTraits>>(documentation);
}

REGISTER_FUNCTION(HasAllTokenLike)
{
    FunctionDocumentation::Description description = String(R"(
Like [`hasAnyTokenLike`](#hasAnyTokenLike), but returns 1 only if each pattern matches at least one token of `input` (different patterns may match different tokens or the same one), and 0 otherwise.
An empty array returns 0.

If `input` has no text index, `hasAllTokenLike(input, patterns)` with an array `patterns` is equivalent to `notEmpty(patterns) AND arrayAll(p -> arrayExists(t -> like(t, p), tokens(input)), patterns)`, and a single pattern `p` is the same as `[p]`.

With several patterns, the text index selects the rows where some pattern matches a token and the function checks them;
`hasAnyTokenLike(input, p1) AND hasAnyTokenLike(input, p2)` can be answered from the index alone.
)") + tokenizer_description + text_index_note;
    FunctionDocumentation::Syntax syntax = "hasAllTokenLike(input, patterns[, tokenizer])";
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if each of `patterns` matches some token, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT hasAllTokenLike('Payment charged twice', ['ch%', 'tw%'])",
        R"(
┌─hasAllTokenLike('Payment charged twice', ['ch%', 'tw%'])─┐
│                                                        1 │
└──────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Every pattern must match",
        "SELECT hasAllTokenLike('Payment charged twice', ['ch%', 'refund%'])",
        R"(
┌─hasAllTokenLike('Payment charged twice', ['ch%', 'refund%'])─┐
│                                                            0 │
└──────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Patterns may match the same token",
        "SELECT hasAllTokenLike('charged', ['ch%', '%ed'])",
        R"(
┌─hasAllTokenLike('charged', ['ch%', '%ed'])─┐
│                                          1 │
└────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, commonArguments("patterns", "The `LIKE` pattern, or an array of `LIKE` patterns, that must each match a token."), {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasTokenPattern<HasAllTokenLikeTraits>>(documentation);
}

REGISTER_FUNCTION(HasTokenMatch)
{
    FunctionDocumentation::Description description = String(R"(
Returns 1 if at least one token of `input` matches one of the regular expressions `patterns`, and 0 otherwise.

`patterns` is one regular expression (`String`) or several (`Array(String)`).
The regular expressions use the [re2 syntax](https://github.com/google/re2/wiki/Syntax) and are applied to each token separately, like function [`match`](#match):
a regular expression may match any part of the token, and the anchors `^` and `$` refer to the start and the end of the token.
So `hasTokenMatch(input, 'err')` finds a token that contains `err`, while `hasAnyTokenLike(input, 'err')` finds a token equal to `err`.
An empty regular expression matches every token; an empty array matches nothing.

If `input` has no text index, `hasTokenMatch(input, patterns)` with an array `patterns` is equivalent to `arrayExists(t -> arrayExists(p -> match(t, p), patterns), tokens(input))`, and a single regular expression `p` is the same as `[p]`.

With a text index, a regular expression of the form `^literal` reads only the matching range of the dictionary, and one that contains a literal checks only the tokens holding it;
any other one is checked against every token in the dictionary of each part.
)") + tokenizer_description + text_index_note;
    FunctionDocumentation::Syntax syntax = "hasTokenMatch(input, patterns[, tokenizer])";
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if some token matches one of `patterns`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT hasTokenMatch('order 12345 shipped', '^[0-9]{5}$')",
        R"(
┌─hasTokenMatch('order 12345 shipped', '^[0-9]{5}$')─┐
│                                                      1 │
└────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Anchors refer to token boundaries",
        "SELECT hasTokenMatch('order 123456 shipped', '^[0-9]{5}$')",
        R"(
┌─hasTokenMatch('order 123456 shipped', '^[0-9]{5}$')─┐
│                                                       0 │
└─────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Several regular expressions",
        "SELECT hasTokenMatch('order 123456 shipped', ['^[0-9]{5}$', '^ship'])",
        R"(
┌─hasTokenMatch('order 123456 shipped', ['^[0-9]{5}$', '^ship'])─┐
│                                                                  1 │
└────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, commonArguments("patterns", "The regular expression, or an array of regular expressions, each token is matched against."), {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasTokenPattern<HasTokenMatchTraits>>(documentation);
}

}

#include <Functions/hasAnyAllTokens.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNothing.h>
#include <Common/FunctionDocumentation.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>

#include <absl/container/flat_hash_map.h>
#include <boost/dynamic_bitset.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr size_t arg_input = 0;
constexpr size_t arg_needles = 1;
constexpr size_t arg_tokenizer = 2;

TokensWithPosition initializeSearchTokens(const ColumnsWithTypeAndName & arguments, const ITokenizer & tokenizer, std::string_view function_name)
{
    if (arguments.size() < 2)
        return {};

    auto column_needles = arguments[arg_needles].column;
    if (!column_needles || column_needles->empty())
        return {};

    Field needles_field = (*column_needles)[0];
    if (needles_field.isNull())
        return {};

    TokensWithPosition search_tokens;
    std::vector<String> tokens_array;

    if (needles_field.getType() == Field::Types::String)
    {
        auto tokens_str = needles_field.safeGet<String>();
        tokenizer.stringToTokens(tokens_str.data(), tokens_str.size(), tokens_array);
        tokens_array = tokenizer.compactTokens(tokens_array);
    }
    else if (needles_field.getType() == Field::Types::Array)
    {
        Array array_data = needles_field.safeGet<Array>();

        for (const auto & element : array_data)
        {
            if (element.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Needles argument for function '{}' has unsupported type of column '{}'", function_name, column_needles->getName());

            tokens_array.emplace_back(element.safeGet<String>());
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Needles argument for function '{}' has unsupported type of column '{}'", function_name, column_needles->getName());
    }

    size_t pos = 0;
    for (const auto & token : tokens_array)
    {
        if (search_tokens.emplace(token, pos).second)
            ++pos;
    }
    return search_tokens;
}

/// Function input accept string, fixed string, array of string or array of fixed strings.
bool isStringOrFixedStringOrArrayOfStringOrFixedString(const IDataType & type)
{
    if (isStringOrFixedString(type))
        return true;

    if (const auto * array_type = checkAndGetDataType<DataTypeArray>(&type); array_type)
    {
        const DataTypePtr & nested_type = array_type->getNestedType();
        return isStringOrFixedString(nested_type);
    }

    return false;
}

/// Functions accept needles string (will be tokenized) or array of string needles/tokens (used as-is)
/// Also accepts Array(Nothing) which is the type of Array([])
bool isStringOrArrayOfStringType(const IDataType & type)
{
    if (isString(type))
        return true;

    if (const auto * array_type = checkAndGetDataType<DataTypeArray>(&type); array_type)
    {
        const DataTypePtr & nested_type = array_type->getNestedType();
        return isString(nested_type) || isNothing(nested_type);
    }

    return false;
}
}

template <class HasTokensTraits>
FunctionHasAnyAllTokensOverloadResolver<HasTokensTraits>::FunctionHasAnyAllTokensOverloadResolver(ContextPtr)
{
}

template <class HasTokensTraits>
DataTypePtr FunctionHasAnyAllTokensOverloadResolver<HasTokensTraits>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors mandatory_args
    {
        {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedStringOrArrayOfStringOrFixedString), nullptr, "String, FixedString, Array(String) or Array(FixedString)"},
        {"needles", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrArrayOfStringType), isColumnConst, "const String or const Array(String)"}
    };

    FunctionArgumentDescriptors optional_args
    {
        {"tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
    };

    validateFunctionArguments(name, arguments, mandatory_args, optional_args);
    return std::make_shared<DataTypeNumber<UInt8>>();
}

template <class HasTokensTraits>
FunctionBasePtr FunctionHasAnyAllTokensOverloadResolver<HasTokensTraits>::buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const
{
    const auto tokenizer_name = arguments.size() < 3 || !arguments[arg_tokenizer].column
        ? SplitByNonAlphaTokenizer::getExternalName()
        : arguments[arg_tokenizer].column->getDataAt(0);

    auto tokenizer = TokenizerFactory::instance().get(tokenizer_name);
    auto search_tokens = initializeSearchTokens(arguments, *tokenizer, getName());
    DataTypes argument_types{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })};
    return std::make_shared<FunctionBaseHasAnyAllTokens<HasTokensTraits>>(std::move(tokenizer), std::move(search_tokens), std::move(argument_types), return_type);
}

template <class HasTokensTraits>
ExecutableFunctionPtr FunctionBaseHasAnyAllTokens<HasTokensTraits>::prepare(const ColumnsWithTypeAndName &) const
{
    return std::make_unique<ExecutableFunctionHasAnyAllTokens<HasTokensTraits>>(tokenizer, search_tokens);
}

namespace
{

struct HasAnyTokensMatcher
{
    explicit HasAnyTokensMatcher(const TokensWithPosition & tokens_)
        : tokens(tokens_)
    {
    }

    template <typename OnMatchCallback>
    auto operator()(OnMatchCallback && onMatchCallback)
    {
        return [&](const char * token_start, size_t token_len)
        {
            if (tokens.contains(std::string_view(token_start, token_len)))
            {
                onMatchCallback();
                return true;
            }

            return false;
        };
    }

    void reset() { /* nothing to reset */ }

private:
    const TokensWithPosition & tokens;
};

struct HasAllTokensMatcher
{
    explicit HasAllTokensMatcher(const TokensWithPosition & tokens_)
        : tokens(tokens_)
        , mask(tokens.size())
        , num_set_bits(0)
    {
    }

    template <typename OnMatchCallback>
    auto operator()(OnMatchCallback && onMatchCallback)
    {
        return [&](const char * token_start, size_t token_len)
        {
            if (auto it = tokens.find(std::string_view(token_start, token_len)); it != tokens.end())
            {
                num_set_bits += !mask.test_set(it->second);

                if (num_set_bits == tokens.size())
                {
                    onMatchCallback();
                    return true;
                }
            }

            return false;
        };
    }

    void reset()
    {
        mask.reset();
        num_set_bits = 0;
    }

private:
    const TokensWithPosition & tokens;
    boost::dynamic_bitset<> mask;
    UInt64 num_set_bits;
};

template <typename T>
concept StringColumnType = std::same_as<T, ColumnString> || std::same_as<T, ColumnFixedString>;
using ArrayOffset = ColumnArray::Offset;

template <typename Matcher>
concept MatcherType = std::same_as<Matcher, HasAnyTokensMatcher> || std::same_as<Matcher, HasAllTokensMatcher>;

/// Execute on Array(String) or Array(FixedString) column
void searchOnArray(
    const ColumnArray::Offsets & offsets,
    const StringColumnType auto & input_string,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer * tokenizer,
    MatcherType auto matcher)
{
    ArrayOffset current_offset = 0;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const ArrayOffset array_size = offsets[i] - current_offset;
        col_result[i] = false;
        matcher.reset();

        for (size_t j = 0; j < array_size; ++j)
        {
            std::string_view input = input_string.getDataAt(current_offset + j);

            forEachTokenPadded(*tokenizer, input.data(), input.size(), matcher([&] { col_result[i] = true; }));

            if (col_result[i])
                break;
        }

        current_offset = offsets[i];
    }
}

/// Execute on String column
void searchOnString(
    const StringColumnType auto & col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer * tokenizer,
    MatcherType auto matcher)
{
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::string_view input = col_input.getDataAt(i);
        col_result[i] = false;
        matcher.reset();

        forEachTokenPadded(*tokenizer, input.data(), input.size(), matcher([&] { col_result[i] = true; }));
    }
}

template <class HasTokensTraits>
void executeString(
    const StringColumnType auto & col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer * tokenizer,
    const TokensWithPosition & tokens)
{
    if (tokens.empty())
    {
        /// if no search tokens we explicitly return no matches to avoid potential undefined behavior in HasAllTokensMatcher
        col_result.assign(input_rows_count, UInt8(0));
        return;
    }

    col_result.resize(input_rows_count);

    if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::Any)
        searchOnString(col_input, col_result, input_rows_count, tokenizer, HasAnyTokensMatcher(tokens));
    else if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::All)
        searchOnString(col_input, col_result, input_rows_count, tokenizer, HasAllTokensMatcher(tokens));
    else
        static_assert(false, "Unknown search mode value detected");
}

template <class HasTokensTraits>
void executeArray(
    const ColumnArray * array,
    const StringColumnType auto & input_string,
    PaddedPODArray<UInt8> & col_result,
    const ITokenizer * tokenizer,
    const TokensWithPosition & tokens)
{
    const auto & offsets = array->getOffsets();
    const size_t input_size = offsets.size();

    if (tokens.empty())
    {
        /// if no search tokens we explicitly return no matches to avoid potential undefined behavior in HasAllTokensMatcher
        col_result.assign(input_size, UInt8(0));
        return;
    }

    col_result.resize(input_size);

    if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::Any)
        searchOnArray(offsets, input_string, col_result, input_size, tokenizer, HasAnyTokensMatcher(tokens));
    else if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::All)
        searchOnArray(offsets, input_string, col_result, input_size, tokenizer, HasAllTokensMatcher(tokens));
    else
        static_assert(false, "Unknown search mode value detected");
}

template <class HasTokensTraits>
void executeStringOrArray(
    ColumnPtr col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer * tokenizer,
    const TokensWithPosition & tokens)
{
    if (const auto * col_input_string = checkAndGetColumn<ColumnString>(col_input.get()))
        executeString<HasTokensTraits>(*col_input_string, col_result, input_rows_count, tokenizer, tokens);
    else if (const auto * col_input_fixedstring = checkAndGetColumn<ColumnFixedString>(col_input.get()))
        executeString<HasTokensTraits>(*col_input_fixedstring, col_result, input_rows_count, tokenizer, tokens);
    else if (const auto * col_input_array = checkAndGetColumn<ColumnArray>(col_input.get()))
    {
        if (const auto * input_string = checkAndGetColumn<ColumnString>(&col_input_array->getData()))
            executeArray<HasTokensTraits>(col_input_array, *input_string, col_result, tokenizer, tokens);
        else if (const auto * input_fixedstring = checkAndGetColumn<ColumnFixedString>(&col_input_array->getData()))
            executeArray<HasTokensTraits>(col_input_array, *input_fixedstring, col_result, tokenizer, tokens);
    }
}

}

template <class HasTokensTraits>
ColumnPtr ExecutableFunctionHasAnyAllTokens<HasTokensTraits>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    if (input_rows_count == 0)
        return ColumnVector<UInt8>::create();

    auto col_result = ColumnVector<UInt8>::create();

    if (search_tokens.empty())
    {
        col_result->getData().assign(input_rows_count, UInt8(0));
        return col_result;
    }

    ColumnPtr col_input = arguments[arg_input].column;

    if (tokenizer->getType() == ITokenizer::Type::SparseGrams)
    {
        /// The sparse gram token extractor stores an internal state which modified during the execution.
        /// This leads to an error while executing this function multi-threaded because that state is not protected.
        /// To avoid this case, a clone of the sparse gram token extractor will be used.
        auto sparse_grams_tokenizer = tokenizer->clone();
        executeStringOrArray<HasTokensTraits>(col_input, col_result->getData(), input_rows_count, sparse_grams_tokenizer.get(), search_tokens);
    }
    else
    {
        executeStringOrArray<HasTokensTraits>(col_input, col_result->getData(), input_rows_count, tokenizer.get(), search_tokens);
    }

    return col_result;
}

template class ExecutableFunctionHasAnyAllTokens<HasAnyTokensTraits>;
template class ExecutableFunctionHasAnyAllTokens<HasAllTokensTraits>;

template class FunctionBaseHasAnyAllTokens<HasAnyTokensTraits>;
template class FunctionBaseHasAnyAllTokens<HasAllTokensTraits>;

REGISTER_FUNCTION(HasAnyTokens)
{
    FunctionDocumentation::Description description_hasAnyTokens = R"(
Returns 1, if at least one token in the `needle` string or array matches the `input` string, and 0 otherwise. If `input` is a column, returns all rows that satisfy this condition.

:::note
Column `input` should have a [text index](../../engines/table-engines/mergetree-family/textindexes) defined for optimal performance.
If no text index is defined, the function performs a brute-force column scan which is orders of magnitude slower than an index lookup.
:::

Prior to searching, the function tokenizes
- the `input` argument (always), and
- the `needle` argument (if given as a [String](../../sql-reference/data-types/string.md))
using the tokenizer specified for the text index.
If the column has no text index defined, the `splitByNonAlpha` tokenizer is used instead.
If the `needle` argument is of type [Array(String)](../../sql-reference/data-types/array.md), each array element is treated as a token — no additional tokenization takes place.

Duplicate tokens are ignored.
For example, ['ClickHouse', 'ClickHouse'] is treated the same as ['ClickHouse'].
    )";
    FunctionDocumentation::Syntax syntax_hasAnyTokens = R"(
hasAnyTokens(input, needles)
)";
    FunctionDocumentation::Arguments arguments_hasAnyTokens = {
        {"input", "The input column.", {"String", "FixedString", "Array(String)", "Array(FixedString)"}},
        {"needles", "Tokens to be searched.", {"String", "Array(String)"}},
        {"tokenizer", "The tokenizer to use. Valid arguments are `splitByNonAlpha`, `ngrams`, `splitByString`, `array`, and `sparseGrams`. Optional, if not set explicitly, defaults to `splitByNonAlpha`.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_hasAnyTokens = {"Returns `1`, if there was at least one match. `0`, otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_hasAnyTokens = {
    {
        "Basic usage with a string needle",
        R"(
CREATE TABLE table (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByString(['()', '\\']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table VALUES (1, '()a,\\bc()d'), (2, '()\\a()bc\\d'), (3, ',()a\\,bc,(),d,');

SELECT count() FROM table WHERE hasAnyTokens(msg, 'a\\d()');
        )",
        R"(
┌─count()─┐
│       3 │
└─────────┘
        )"
    },
    {
        "Specify needles to be searched for AS-IS (no tokenization) in an array",
        R"(
SELECT count() FROM table WHERE hasAnyTokens(msg, ['a', 'd']);
        )",
        R"(
┌─count()─┐
│       3 │
└─────────┘
        )"
    },
    {
        "Generate needles using the `tokens` function",
        R"(
SELECT count() FROM table WHERE hasAnyTokens(msg, tokens('a()d', 'splitByString', ['()', '\\']));
        )",
        R"(
┌─count()─┐
│       3 │
└─────────┘
        )"
    },
    {
        "Usage examples for array and map columns",
        R"(
CREATE TABLE log (
    id UInt32,
    tags Array(String),
    attributes Map(String, String),
    INDEX idx_tags (tags) TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_attributes_keys mapKeys(attributes) TYPE text(tokenizer = array),
    INDEX idx_attributes_vals mapValues(attributes) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO log VALUES
    (1, ['clickhouse', 'clickhouse cloud'], {'address': '192.0.0.1', 'log_level': 'INFO'}),
    (2, ['chdb'], {'embedded': 'true', 'log_level': 'DEBUG'});
        )",
        ""
    },
    {
        "Example with an array column",
        R"(
SELECT count() FROM log WHERE hasAnyTokens(tags, 'clickhouse');
        )",
        R"(
┌─count()─┐
│       1 │
└─────────┘
        )"
    },
    {
        "Example with mapKeys",
        R"(
SELECT count() FROM log WHERE hasAnyTokens(mapKeys(attributes), ['address', 'log_level']);
        )",
        R"(
┌─count()─┐
│       2 │
└─────────┘
        )"
    },
    {
        "Example with mapValues",
        R"(
SELECT count() FROM log WHERE hasAnyTokens(mapValues(attributes), ['192.0.0.1', 'DEBUG']);
        )",
        R"(
┌─count()─┐
│       2 │
└─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_hasAnyTokens = {25, 10};
    FunctionDocumentation::Category category_hasAnyTokens = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_hasAnyTokens = {description_hasAnyTokens, syntax_hasAnyTokens, arguments_hasAnyTokens, {}, returned_value_hasAnyTokens, examples_hasAnyTokens, introduced_in_hasAnyTokens, category_hasAnyTokens};

    factory.registerFunction<FunctionHasAnyAllTokensOverloadResolver<HasAnyTokensTraits>>(documentation_hasAnyTokens);
    factory.registerAlias("hasAnyToken", HasAnyTokensTraits::name);
}

REGISTER_FUNCTION(HasAllTokens)
{
    FunctionDocumentation::Description description_hasAllTokens = R"(
Like [`hasAnyTokens`](#hasAnyTokens), but returns 1, if all tokens in the `needle` string or array match the `input` string, and 0 otherwise. If `input` is a column, returns all rows that satisfy this condition.

:::note
Column `input` should have a [text index](../../engines/table-engines/mergetree-family/textindexes) defined for optimal performance.
If no text index is defined, the function performs a brute-force column scan which is orders of magnitude slower than an index lookup.
:::

Prior to searching, the function tokenizes
- the `input` argument (always), and
- the `needle` argument (if given as a [String](../../sql-reference/data-types/string.md))
using the tokenizer specified for the text index.
If the column has no text index defined, the `splitByNonAlpha` tokenizer is used instead.
If the `needle` argument is of type [Array(String)](../../sql-reference/data-types/array.md), each array element is treated as a token — no additional tokenization takes place.

Duplicate tokens are ignored.
For example, needles = ['ClickHouse', 'ClickHouse'] is treated the same as ['ClickHouse'].
    )";
    FunctionDocumentation::Syntax syntax_hasAllTokens = R"(
hasAllTokens(input, needles)
)";
    FunctionDocumentation::Arguments arguments_hasAllTokens = {
        {"input", "The input column.", {"String", "FixedString", "Array(String)", "Array(FixedString)"}},
        {"needles", "Tokens to be searched.", {"String", "Array(String)"}},
        {"tokenizer", "The tokenizer to use. Valid arguments are `splitByNonAlpha`, `ngrams`, `splitByString`, `array`, and `sparseGrams`. Optional, if not set explicitly, defaults to `splitByNonAlpha`.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_hasAllTokens = {"Returns 1, if all needles match. 0, otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_hasAllTokens = {
    {
        "Basic usage with a string needle",
        R"(
CREATE TABLE table (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByString(['()', '\\']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table VALUES (1, '()a,\\bc()d'), (2, '()\\a()bc\\d'), (3, ',()a\\,bc,(),d,');

SELECT count() FROM table WHERE hasAllTokens(msg, 'a\\d()');
        )",
        R"(
┌─count()─┐
│       1 │
└─────────┘
        )"
    },
    {
        "Specify needles to be searched for AS-IS (no tokenization) in an array",
        R"(
SELECT count() FROM table WHERE hasAllTokens(msg, ['a', 'd']);
        )",
        R"(
┌─count()─┐
│       1 │
└─────────┘
        )"
    },
    {
        "Generate needles using the `tokens` function",
        R"(
SELECT count() FROM table WHERE hasAllTokens(msg, tokens('a()d', 'splitByString', ['()', '\\']));
        )",
        R"(
┌─count()─┐
│       1 │
└─────────┘
        )"
    },
    {
        "Use a custom tokenizer via the 3rd argument",
        R"(
SELECT hasAllTokens('abcdef', 'abc', 'ngrams(3)');
        )",
        R"(
┌─hasAllTokens('abcdef', 'abc', 'ngrams(3)')─┐
│                                            1 │
└──────────────────────────────────────────────┘
        )"
    },
    {
        "Usage examples for array and map columns",
        R"(
CREATE TABLE log (
    id UInt32,
    tags Array(String),
    attributes Map(String, String),
    INDEX idx_tags (tags) TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_attributes_keys mapKeys(attributes) TYPE text(tokenizer = array),
    INDEX idx_attributes_vals mapValues(attributes) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO log VALUES
    (1, ['clickhouse', 'clickhouse cloud'], {'address': '192.0.0.1', 'log_level': 'INFO'}),
    (2, ['chdb'], {'embedded': 'true', 'log_level': 'DEBUG'});
        )",
        ""
    },
    {
        "Example with an array column",
        R"(
SELECT count() FROM log WHERE hasAllTokens(tags, 'clickhouse');
        )",
        R"(
┌─count()─┐
│       1 │
└─────────┘
        )"
    },
    {
        "Example with mapKeys",
        R"(
SELECT count() FROM log WHERE hasAllTokens(mapKeys(attributes), ['address', 'log_level']);
        )",
        R"(
┌─count()─┐
│       1 │
└─────────┘
        )"
    },
    {
        "Example with mapValues",
        R"(
SELECT count() FROM log WHERE hasAllTokens(mapValues(attributes), ['192.0.0.1', 'DEBUG']);
        )",
        R"(
┌─count()─┐
│       0 │
└─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_hasAllTokens = {25, 10};
    FunctionDocumentation::Category category_hasAllTokens = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_hasAllTokens = {description_hasAllTokens, syntax_hasAllTokens, arguments_hasAllTokens, {}, returned_value_hasAllTokens, examples_hasAllTokens, introduced_in_hasAllTokens, category_hasAllTokens};

    factory.registerFunction<FunctionHasAnyAllTokensOverloadResolver<HasAllTokensTraits>>(documentation_hasAllTokens);
    factory.registerAlias("hasAllToken", HasAllTokensTraits::name);
}
}

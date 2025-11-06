#include <Functions/hasAnyAllTokens.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/FunctionDocumentation.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_full_text_index;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

template <class HasTokensTraits>
FunctionPtr FunctionHasAnyAllTokens<HasTokensTraits>::create(ContextPtr context)
{
    return std::make_shared<FunctionHasAnyAllTokens>(context);
}

template <class HasTokensTraits>
FunctionHasAnyAllTokens<HasTokensTraits>::FunctionHasAnyAllTokens(ContextPtr context)
    : allow_experimental_full_text_index(context->getSettingsRef()[Setting::allow_experimental_full_text_index])
{
}

template <class HasTokensTraits>
void FunctionHasAnyAllTokens<HasTokensTraits>::setTokenExtractor(std::unique_ptr<ITokenExtractor> new_token_extractor)
{
    /// Index parameters can be set multiple times.
    /// This happens exactly in a case that same hasAnyTokens/hasAllTokens query is used again.
    /// This is fine because the parameters would be same.
    if (token_extractor != nullptr)
        return;

    token_extractor = std::move(new_token_extractor);
}

template <class HasTokensTraits>
void FunctionHasAnyAllTokens<HasTokensTraits>::setSearchTokens(const std::vector<String> & new_search_tokens)
{
    static constexpr size_t max_number_of_tokens = 64;

    if (search_tokens.has_value())
        return;

    search_tokens = TokensWithPosition();
    for (UInt64 pos = 0; const auto & new_search_token : new_search_tokens)
        if (auto [_, inserted] = search_tokens->emplace(new_search_token, pos); inserted)
            ++pos;

    if (search_tokens->size() > max_number_of_tokens)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' supports a max of {} search tokens", name, max_number_of_tokens);
}

namespace
{

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


TokensWithPosition extractTokensFromString(std::string_view value)
{
    DefaultTokenExtractor default_token_extractor;

    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;
    size_t length = value.size();
    size_t pos = 0;

    TokensWithPosition tokens;
    while (cur < length && default_token_extractor.nextInStringPadded(static_cast<const char *>(value.data()), length, &cur, &token_start, &token_len))
    {
        tokens.emplace(std::string{value.data() + token_start, token_len}, pos);
        ++pos;
    }
    return tokens;
}

}

template <class HasTokensTraits>
DataTypePtr FunctionHasAnyAllTokens<HasTokensTraits>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (!allow_experimental_full_text_index)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Enable the setting 'allow_experimental_full_text_index' to use function {}", getName());

    FunctionArgumentDescriptors mandatory_args{
        {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedStringOrArrayOfStringOrFixedString), nullptr, "String, FixedString, Array(String) or Array(FixedString)"},
        {"needles",
         static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrArrayOfStringType),
         isColumnConst,
         "const String or const Array(String)"}};

    validateFunctionArguments(*this, arguments, mandatory_args);

    return std::make_shared<DataTypeNumber<UInt8>>();
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
    {
        const size_t ns = tokens.size();
        /// It is equivalent to ((2 ^ ns) - 1), but avoids overflow in case of ns = 64.
        expected_mask = ((1ULL << (ns - 1)) + ((1ULL << (ns - 1)) - 1));
    }

    template <typename OnMatchCallback>
    auto operator()(OnMatchCallback && onMatchCallback)
    {
        return [&](const char * token_start, size_t token_len)
        {
            if (auto it = tokens.find(std::string_view(token_start, token_len)); it != tokens.end())
                mask |= (1ULL << it->second);

            if (mask == expected_mask)
            {
                onMatchCallback();
                return true;
            }

            return false;
        };
    }

    void reset() { mask = 0; }

private:
    const TokensWithPosition & tokens;
    UInt64 expected_mask;
    UInt64 mask = 0;
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
    const ITokenExtractor * token_extractor,
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
            std::string_view input = input_string.getDataAt(current_offset + j).toView();

            forEachTokenPadded(*token_extractor, input.data(), input.size(), matcher([&] { col_result[i] = true; }));

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
    const ITokenExtractor * token_extractor,
    MatcherType auto matcher)
{
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::string_view input = col_input.getDataAt(i).toView();
        col_result[i] = false;
        matcher.reset();

        forEachTokenPadded(*token_extractor, input.data(), input.size(), matcher([&] { col_result[i] = true; }));
    }
}

template <class HasTokensTraits>
void executeString(
    const StringColumnType auto & col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenExtractor * token_extractor,
    const TokensWithPosition & tokens)
{
    if (tokens.empty())
    {
        /// No needles mean we don't filter and all rows pass
        col_result.assign(input_rows_count, UInt8(1));
        return;
    }

    col_result.resize(input_rows_count);

    if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::Any)
        searchOnString(col_input, col_result, input_rows_count, token_extractor, HasAnyTokensMatcher(tokens));
    else if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::All)
        searchOnString(col_input, col_result, input_rows_count, token_extractor, HasAllTokensMatcher(tokens));
    else
        static_assert(false, "Unknown search mode value detected");
}

template <class HasTokensTraits>
void executeArray(
    const ColumnArray * array,
    const StringColumnType auto & input_string,
    PaddedPODArray<UInt8> & col_result,
    const ITokenExtractor * token_extractor,
    const TokensWithPosition & tokens)
{
    const auto & offsets = array->getOffsets();
    const size_t input_size = offsets.size();

    if (tokens.empty())
    {
        /// No needles mean we don't filter and all rows pass
        col_result.assign(input_size, UInt8(1));
        return;
    }

    col_result.resize(input_size);

    if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::Any)
        searchOnArray(offsets, input_string, col_result, input_size, token_extractor, HasAnyTokensMatcher(tokens));
    else if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::All)
        searchOnArray(offsets, input_string, col_result, input_size, token_extractor, HasAllTokensMatcher(tokens));
    else
        static_assert(false, "Unknown search mode value detected");
}

template <class HasTokensTraits>
void execute(
    ColumnPtr col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenExtractor * token_extractor,
    const TokensWithPosition & tokens)
{
    /// String
    if (const auto * col_input_string = checkAndGetColumn<ColumnString>(col_input.get()))
        executeString<HasTokensTraits>(*col_input_string, col_result, input_rows_count, token_extractor, tokens);
    /// FixedString
    else if (const auto * col_input_fixedstring = checkAndGetColumn<ColumnFixedString>(col_input.get()))
        executeString<HasTokensTraits>(*col_input_fixedstring, col_result, input_rows_count, token_extractor, tokens);
    /// Array(String) or Array(FixedString)
    else if (const auto * col_input_array = checkAndGetColumn<ColumnArray>(col_input.get()))
    {
        /// String
        if (const auto * input_string = checkAndGetColumn<ColumnString>(&col_input_array->getData()))
            executeArray<HasTokensTraits>(col_input_array, *input_string, col_result, token_extractor, tokens);
        /// FixedString
        else if (const auto * input_fixedstring = checkAndGetColumn<ColumnFixedString>(&col_input_array->getData()))
            executeArray<HasTokensTraits>(col_input_array, *input_fixedstring, col_result, token_extractor, tokens);
    }
}
}

template <class HasTokensTraits>
ColumnPtr FunctionHasAnyAllTokens<HasTokensTraits>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    constexpr size_t arg_input = 0;
    constexpr size_t arg_needles = 1;

    if (input_rows_count == 0)
        return ColumnVector<UInt8>::create();

    ColumnPtr col_input = arguments[arg_input].column;
    auto col_result = ColumnVector<UInt8>::create();

    if (token_extractor == nullptr)
    {
        /// If token_extractor == nullptr, we do a full-table scan on a column without index
        /// By default, the default tokenizer will be used to tokenize the input column.
        /// Additionally, the search tokens need to be extract from the needle argument.
        chassert(!search_tokens.has_value());

        /// Populate needles from function arguments
        TokensWithPosition search_tokens_from_args;
        const ColumnPtr col_needles = arguments[arg_needles].column;

        if (const ColumnConst * col_needles_str_const = checkAndGetColumnConst<ColumnString>(col_needles.get()))
        {
            search_tokens_from_args = extractTokensFromString(col_needles_str_const->getDataAt(0).toView());
        }
        else if (const ColumnString * col_needles_str = checkAndGetColumn<ColumnString>(col_needles.get()))
        {
            search_tokens_from_args = extractTokensFromString(col_needles_str->getDataAt(0).toView());
        }
        else if (const ColumnConst * col_needles_array_const = checkAndGetColumnConst<ColumnArray>(col_needles.get()))
        {
            const Array & array = col_needles_array_const->getValue<Array>();

            for (size_t i = 0; i < array.size(); ++i)
                search_tokens_from_args.emplace(array.at(i).safeGet<String>(), i);
        }
        else if (const ColumnArray * col_needles_array = checkAndGetColumn<ColumnArray>(col_needles.get()))
        {
            const IColumn & array_data = col_needles_array->getData();
            const ColumnArray::Offsets & array_offsets = col_needles_array->getOffsets();

            const ColumnString & needles_data_string = checkAndGetColumn<ColumnString>(array_data);

            for (size_t i = 0; i < array_offsets[0]; ++i)
                search_tokens_from_args.emplace(needles_data_string.getDataAt(i).toView(), i);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Needles argument for function '{}' has unsupported type", getName());

        static DefaultTokenExtractor default_token_extractor;

        execute<HasTokensTraits>(col_input, col_result->getData(), input_rows_count, &default_token_extractor, search_tokens_from_args);
    }
    else
    {
        /// If token_extractor != nullptr, a text index exists and we are doing text index lookups
        if (token_extractor->getType() == ITokenExtractor::Type::SparseGram)
        {
            /// The sparse gram token extractor stores an internal state which modified during the execution.
            /// This leads to an error while executing this function multi-threaded because that state is not protected.
            /// To avoid this case, a clone of the sparse gram token extractor will be used.
            auto sparse_gram_extractor = token_extractor->clone();
            execute<HasTokensTraits>(col_input, col_result->getData(), input_rows_count, sparse_gram_extractor.get(), search_tokens.value());
        }
        else
        {
            execute<HasTokensTraits>(col_input, col_result->getData(), input_rows_count, token_extractor.get(), search_tokens.value());
        }
    }

    return col_result;
}

template class FunctionHasAnyAllTokens<traits::HasAnyTokensTraits>;
template class FunctionHasAnyAllTokens<traits::HasAllTokensTraits>;

REGISTER_FUNCTION(HasAnyTokens)
{
    FunctionDocumentation::Description description_hasAnyTokens = R"(
Returns 1, if at least one token in the `needle` string or array matches the `input` string, and 0 otherwise. If `input` is a column, returns all rows that satisfy this condition.

:::note
Column `input` should have a [text index](../../engines/table-engines/mergetree-family/invertedindexes) defined for optimal performance.
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
        {"needles", "Tokens to be searched. Supports at most 64 tokens.", {"String", "Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hasAnyTokens = {"Returns `1`, if there was at least one match. `0`, otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_hasAnyTokens = {
    {
        "Usage example for a string column",
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
    FunctionDocumentation::IntroducedIn introduced_in_hasAnyTokens = {25, 7};
    FunctionDocumentation::Category category_hasAnyTokens = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_hasAnyTokens = {description_hasAnyTokens, syntax_hasAnyTokens, arguments_hasAnyTokens, returned_value_hasAnyTokens, examples_hasAnyTokens, introduced_in_hasAnyTokens, category_hasAnyTokens};

    factory.registerFunction<FunctionHasAnyAllTokens<traits::HasAnyTokensTraits>>(documentation_hasAnyTokens);
    factory.registerAlias("hasAnyToken", traits::HasAnyTokensTraits::name);
}

REGISTER_FUNCTION(HasAllTokens)
{
    FunctionDocumentation::Description description_hasAllTokens = R"(
Like [`hasAnyTokens`](#hasAnyTokens), but returns 1, if all tokens in the `needle` string or array match the `input` string, and 0 otherwise. If `input` is a column, returns all rows that satisfy this condition.

:::note
Column `input` should have a [text index](../../engines/table-engines/mergetree-family/invertedindexes) defined for optimal performance.
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
        {"needles", "Tokens to be searched. Supports at most 64 tokens.", {"String", "Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hasAllTokens = {"Returns 1, if all needles match. 0, otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_hasAllTokens = {
    {
        "Usage example for a string column",
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
    FunctionDocumentation::IntroducedIn introduced_in_hasAllTokens = {25, 7};
    FunctionDocumentation::Category category_hasAllTokens = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_hasAllTokens = {description_hasAllTokens, syntax_hasAllTokens, arguments_hasAllTokens, returned_value_hasAllTokens, examples_hasAllTokens, introduced_in_hasAllTokens, category_hasAllTokens};

    factory.registerFunction<FunctionHasAnyAllTokens<traits::HasAllTokensTraits>>(documentation_hasAllTokens);
    factory.registerAlias("hasAllToken", traits::HasAllTokensTraits::name);
}
}

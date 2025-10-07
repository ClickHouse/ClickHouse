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
void FunctionHasAnyAllTokens<HasTokensTraits>::setTokenExtractor(std::unique_ptr<ITokenExtractor> new_token_extractor_)
{
    /// Index parameters can be set multiple times.
    /// This happens exactly in a case that same hasAnyTokens/hasAllTokens query is used again.
    /// This is fine because the parameters would be same.
    if (token_extractor != nullptr)
        return;

    token_extractor = std::move(new_token_extractor_);
}

template <class HasTokensTraits>
void FunctionHasAnyAllTokens<HasTokensTraits>::setSearchTokens(const std::vector<String> & tokens)
{
    static constexpr size_t supported_number_of_needles = 64;

    if (needles.has_value())
        return;

    needles = Needles();
    for (UInt64 pos = 0; const auto & token : tokens)
        if (auto [_, inserted] = needles->emplace(token, pos); inserted)
            ++pos;

    if (needles->size() > supported_number_of_needles)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' supports a max of {} needles", name, supported_number_of_needles);
}

namespace
{

/// Also accepts Array(Nothing) which is the type of Array([])
bool isArrayOfStringType(const IDataType & type)
{
    const auto * array_type = checkAndGetDataType<DataTypeArray>(&type);
    if (!array_type)
        return false;

    const DataTypePtr & nested_type = array_type->getNestedType();
    return isString(nested_type) || isFixedString(nested_type) || isNothing(nested_type);
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
        {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
        {"needles", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArrayOfStringType), isColumnConst, "const Array(String)"}};

    validateFunctionArguments(*this, arguments, mandatory_args);

    return std::make_shared<DataTypeNumber<UInt8>>();
}

namespace
{
template <typename T>
concept StringColumnType = std::same_as<T, ColumnString> || std::same_as<T, ColumnFixedString>;

void executeHasAnyTokens(
    const ITokenExtractor * token_extractor,
    const StringColumnType auto & col_input,
    size_t input_rows_count,
    const Needles & needles,
    PaddedPODArray<UInt8> & col_result)
{
    std::vector<std::string_view> tokens;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const std::string_view value = col_input.getDataAt(i).toView();
        col_result[i] = false;

        tokens = token_extractor->getTokensView(value.data(), value.size());
        for (const auto & token : tokens)
        {
            if (needles.contains(token))
            {
                col_result[i] = true;
                break;
            }
        }
    }
}

void executeHasAllTokens(
    const ITokenExtractor * token_extractor,
    const StringColumnType auto & col_input,
    size_t input_rows_count,
    const Needles & needles,
    PaddedPODArray<UInt8> & col_result)
{
    const size_t ns = needles.size();
    /// It is equivalent to ((2 ^ ns) - 1), but avoids overflow in case of ns = 64.
    const UInt64 expected_mask = ((1ULL << (ns - 1)) + ((1ULL << (ns - 1)) - 1));

    UInt64 mask;
    std::vector<std::string_view> tokens;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const std::string_view value = col_input.getDataAt(i).toView();
        col_result[i] = false;

        mask = 0;
        tokens = token_extractor->getTokensView(value.data(), value.size());
        for (const auto & token : tokens)
        {
            if (auto it = needles.find(token); it != needles.end())
                mask |= (1ULL << it->second);

            if (mask == expected_mask)
            {
                col_result[i] = true;
                break;
            }
        }
    }
}

template <class HasTokensTraits>
void execute(
    const ITokenExtractor * token_extractor,
    const StringColumnType auto & col_input,
    size_t input_rows_count,
    const Needles & needles,
    PaddedPODArray<UInt8> & col_result)
{
    if (needles.empty())
    {
        /// No needles mean we don't filter and all rows pass
        for (size_t i = 0; i < input_rows_count; ++i)
            col_result[i] = true;
        return;
    }

    if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::Any)
        executeHasAnyTokens(token_extractor, col_input, input_rows_count, needles, col_result);
    else if constexpr (HasTokensTraits::mode == HasAnyAllTokensMode::All)
        executeHasAllTokens(token_extractor, col_input, input_rows_count, needles, col_result);
    else
        static_assert(false, "Unknown search mode value detected");
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

    col_result->getData().resize(input_rows_count);

    /// If token_extractor == nullptr, we do a brute force scan on a column without index
    if (token_extractor == nullptr)
    {
        chassert(!needles.has_value());

        /// Populate needles from function arguments
        Needles needles_tmp;
        const ColumnPtr col_needles = arguments[arg_needles].column;

        if (const ColumnConst * col_needles_const = checkAndGetColumnConst<ColumnArray>(col_needles.get()))
        {
            const Array & array = col_needles_const->getValue<Array>();

            for (size_t i = 0; i < array.size(); ++i)
                needles_tmp.emplace(array.at(i).safeGet<String>(), i);

        }
        else if (const ColumnArray * col_needles_vector = checkAndGetColumn<ColumnArray>(col_needles.get()))
        {
            const IColumn & needles_data = col_needles_vector->getData();
            const ColumnArray::Offsets & needles_offsets = col_needles_vector->getOffsets();

            const ColumnString & needles_data_string = checkAndGetColumn<ColumnString>(needles_data);

            for (size_t i = 0; i < needles_offsets[0]; ++i)
                needles_tmp.emplace(needles_data_string.getDataAt(i).toView(), i);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Needles argument for function '{}' has unsupported type", getName());

        DefaultTokenExtractor default_token_extractor;

        if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
            execute<HasTokensTraits>(&default_token_extractor, *column_string, input_rows_count, needles_tmp, col_result->getData());
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            execute<HasTokensTraits>(&default_token_extractor, *column_fixed_string, input_rows_count, needles_tmp, col_result->getData());

    }
    else
    {
        /// If token_extractor != nullptr, we are doing text index lookups
        if (!needles.has_value())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function '{}' must be used with the index column, but got column '{}'",
                getName(),
                arguments[arg_input].name);

        if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
            execute<HasTokensTraits>(token_extractor.get(), *column_string, input_rows_count, needles.value(), col_result->getData());
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            execute<HasTokensTraits>(token_extractor.get(), *column_fixed_string, input_rows_count, needles.value(), col_result->getData());
    }

    return col_result;
}

template class FunctionHasAnyAllTokens<traits::HasAnyTokensTraits>;
template class FunctionHasAnyAllTokens<traits::HasAllTokensTraits>;

REGISTER_FUNCTION(HasAnyTokens)
{
    FunctionDocumentation::Description description_hasAnyTokens = R"(
Returns 1, if at least one string needle_i matches the `input` column and 0 otherwise.

The `input` column should have a text index defined for optimal performance.
Otherwise, the function will perform a brute-force column scan which is expected to be orders of magnitude slower.

When searching, the `input` string is tokenized according to the tokenizer specified in the index definition.
If the column lacks a text index, the `splitByNonAlpha` tokenizer is used instead.
Each element in the `needle` array is treated as a complete, individual token — no additional tokenization is performed on the needle elements themselves.

**Example**

To search for "ClickHouse" in a column with an ngram tokenizer (`tokenizer = ngrams(5)`), you would provide an array of all the 5-character ngrams:

```sql
['Click', 'lickH', 'ickHo', 'ckHou', 'kHous', 'House']
```

:::note
Duplicate tokens in the needle array are automatically ignored.
For example, ['ClickHouse', 'ClickHouse'] is treated the same as ['ClickHouse'].
:::
    )";
    FunctionDocumentation::Syntax syntax_hasAnyTokens = "hasAnyTokens(input, ['needle1', 'needle2', ..., 'needleN'])";
    FunctionDocumentation::Arguments arguments_hasAnyTokens = {
        {"input", "The input column.", {"String", "FixedString"}},
        {"needles", "Tokens to be searched. Supports at most 64 tokens.", {"Array"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hasAnyTokens = {"Returns `1`, if there was at least one match. `0`, otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_hasAnyTokens = {
    {
        "Usage example",
        R"(
CREATE TABLE table (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByString(['()', '\\']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table VALUES (1, '()a,\\bc()d'), (2, '()\\a()bc\\d'), (3, ',()a\\,bc,(),d,');

SELECT count() FROM table WHERE hasAnyTokens(msg, ['a', 'd']
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
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_hasAnyTokens = {25, 7};
    FunctionDocumentation::Category category_hasAnyTokens = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_hasAnyTokens = {description_hasAnyTokens, syntax_hasAnyTokens, arguments_hasAnyTokens, returned_value_hasAnyTokens, examples_hasAnyTokens, introduced_in_hasAnyTokens, category_hasAnyTokens};

    factory.registerFunction<FunctionHasAnyAllTokens<traits::HasAnyTokensTraits>>(documentation_hasAnyTokens);
}

REGISTER_FUNCTION(HasAllTokens)
{
    FunctionDocumentation::Description description_hasAllTokens = R"(
Like [`hasAnyTokens`](#hasanytokens), but returns 1 only if all strings `needle_i` matche the `input` column and 0 otherwise.

The `input` column should have a text index defined for optimal performance.
Otherwise the function will perform a brute-force column scan which is expected to be orders of magnitude slower.

When searching, the `input` string is tokenized according to the tokenizer specified in the index definition.
If the column lacks a text index, the `splitByNonAlpha` tokenizer is used instead.
Each element in the `needle` array is treated as a complete, individual token — no additional tokenization is performed on the needle elements themselves.

**Example**

To search for "ClickHouse" in a column with an ngram tokenizer (`tokenizer = ngrams(5)`), you would provide an array of all the 5-character ngrams:

```sql
['Click', 'lickH', 'ickHo', 'ckHou', 'kHous', 'House']
```

:::note
Duplicate tokens in the needle array are automatically ignored.
For example, ['ClickHouse', 'ClickHouse'] is treated the same as ['ClickHouse'].
:::
    )";
    FunctionDocumentation::Syntax syntax_hasAllTokens = "hasAllTokens(input, ['needle1', 'needle2', ..., 'needleN'])";
    FunctionDocumentation::Arguments arguments_hasAllTokens = {
        {"input", "The input column.", {"String", "FixedString"}},
        {"needles", "Tokens to be searched. Supports at most 64 tokens.", {"Array"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hasAllTokens = {"Returns 1, if all needles match. 0, otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_hasAllTokens = {
    {
        "Usage example",
        R"(
CREATE TABLE table (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByString(['()', '\\']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table VALUES (1, '()a,\\bc()d'), (2, '()\\a()bc\\d'), (3, ',()a\\,bc,(),d,');

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
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_hasAllTokens = {25, 7};
    FunctionDocumentation::Category category_hasAllTokens = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_hasAllTokens = {description_hasAllTokens, syntax_hasAllTokens, arguments_hasAllTokens, returned_value_hasAllTokens, examples_hasAllTokens, introduced_in_hasAllTokens, category_hasAllTokens};

    factory.registerFunction<FunctionHasAnyAllTokens<traits::HasAllTokensTraits>>(documentation_hasAllTokens);
}
}

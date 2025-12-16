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

/// Functions accept needles string (will be tokenized) or array of string needles/tokens (used as-is)
/// Also accepts Array(Nothing) which is the type of Array([])
bool isStringOrArrayOfStringType(const IDataType & type)
{
    const auto * string_type = checkAndGetDataType<DataTypeString>(&type);
    if (string_type)
        return true;

    const auto * array_type = checkAndGetDataType<DataTypeArray>(&type);
    if (array_type)
    {
        const DataTypePtr & nested_type = array_type->getNestedType();
        return isString(nested_type) || isNothing(nested_type);
    }

    return false;
}


Needles extractNeedlesFromString(std::string_view needle_str)
{
    DefaultTokenExtractor default_token_extractor;

    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;
    size_t length = needle_str.size();
    size_t pos = 0;

    Needles needles;
    while (cur < length && default_token_extractor.nextInStringPadded(static_cast<const char *>(needle_str.data()), length, &cur, &token_start, &token_len))
    {
        needles.emplace(std::string{needle_str.data() + token_start, token_len}, pos);
        ++pos;
    }
    return needles;
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
        {"needles",
         static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrArrayOfStringType),
         isColumnConst,
         "const String or const Array(String)"}};

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
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::string_view input = col_input.getDataAt(i).toView();
        col_result[i] = false;

        forEachTokenPadded(*token_extractor, input.data(), input.size(), [&](const char * token_start, size_t token_len)
        {
            if (needles.contains(std::string_view(token_start, token_len)))
            {
                col_result[i] = true;
                return true;
            }

            return false;
        });
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
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::string_view input = col_input.getDataAt(i).toView();
        col_result[i] = false;
        mask = 0;

        forEachTokenPadded(*token_extractor, input.data(), input.size(), [&](const char * token_start, size_t token_len)
        {
            if (auto it = needles.find(std::string_view(token_start, token_len)); it != needles.end())
            {
                mask |= (1ULL << it->second);
            }

            if (mask == expected_mask)
            {
                col_result[i] = true;
                return true;
            }

            return false;
        });
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

        if (const ColumnConst * col_needles_str_const = checkAndGetColumnConst<ColumnString>(col_needles.get()))
        {
            needles_tmp = extractNeedlesFromString(col_needles_str_const->getDataAt(0).toView());
        }
        else if (const ColumnString * col_needles_str = checkAndGetColumn<ColumnString>(col_needles.get()))
        {
            needles_tmp = extractNeedlesFromString(col_needles_str->getDataAt(0).toView());
        }
        else if (const ColumnConst * col_needles_array_const = checkAndGetColumnConst<ColumnArray>(col_needles.get()))
        {
            const Array & array = col_needles_array_const->getValue<Array>();

            for (size_t i = 0; i < array.size(); ++i)
                needles_tmp.emplace(array.at(i).safeGet<String>(), i);
        }
        else if (const ColumnArray * col_needles_array = checkAndGetColumn<ColumnArray>(col_needles.get()))
        {
            const IColumn & array_data = col_needles_array->getData();
            const ColumnArray::Offsets & array_offsets = col_needles_array->getOffsets();

            const ColumnString & needles_data_string = checkAndGetColumn<ColumnString>(array_data);

            for (size_t i = 0; i < array_offsets[0]; ++i)
                needles_tmp.emplace(needles_data_string.getDataAt(i).toView(), i);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Needles argument for function '{}' has unsupported type", getName());

        DefaultTokenExtractor default_token_extractor;

        if (const auto * col_input_string = checkAndGetColumn<ColumnString>(col_input.get()))
            execute<HasTokensTraits>(&default_token_extractor, *col_input_string, input_rows_count, needles_tmp, col_result->getData());
        else if (const auto * col_input_fixedstring = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            execute<HasTokensTraits>(&default_token_extractor, *col_input_fixedstring, input_rows_count, needles_tmp, col_result->getData());
    }
    else
    {
        /// If token_extractor != nullptr, a text index exists and we are doing text index lookups
        /// This path is only entered for parts that have no materialized text index
        if (const auto * col_input_string = checkAndGetColumn<ColumnString>(col_input.get()))
            execute<HasTokensTraits>(token_extractor.get(), *col_input_string, input_rows_count, needles.value(), col_result->getData());
        else if (const auto * col_input_fixedstring = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            execute<HasTokensTraits>(token_extractor.get(), *col_input_fixedstring, input_rows_count, needles.value(), col_result->getData());
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
        {"input", "The input column.", {"String", "FixedString"}},
        {"needles", "Tokens to be searched. Supports at most 64 tokens.", {"String", "Array(String)"}}
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

SELECT count() FROM table WHERE hasAnyTokens(msg, 'a\\d()'
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
Like [`hasAnyTokens`](#hasanytokens), but returns 1, if all tokens in the `needle` string or array match the `input` string, and 0 otherwise. If `input` is a column, returns all rows that satisfy this condition.

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
        {"input", "The input column.", {"String", "FixedString"}},
        {"needles", "Tokens to be searched. Supports at most 64 tokens.", {"String", "Array(String)"}}
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
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_hasAllTokens = {25, 7};
    FunctionDocumentation::Category category_hasAllTokens = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_hasAllTokens = {description_hasAllTokens, syntax_hasAllTokens, arguments_hasAllTokens, returned_value_hasAllTokens, examples_hasAllTokens, introduced_in_hasAllTokens, category_hasAllTokens};

    factory.registerFunction<FunctionHasAnyAllTokens<traits::HasAllTokensTraits>>(documentation_hasAllTokens);
    factory.registerAlias("hasAllToken", traits::HasAllTokensTraits::name);
}
}

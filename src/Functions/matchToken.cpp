#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FunctionDocumentation.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/VersionNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>

#include <concepts>
#include <string_view>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

constexpr size_t arg_haystack  = 0;
constexpr size_t arg_pattern   = 1;
constexpr size_t arg_tokenizer = 2;

/// --- Haystack column dispatch helpers (inlined from tokenHaystackUtils.h) ---

template <typename T>
concept StringColumnType = std::same_as<T, ColumnString> || std::same_as<T, ColumnFixedString>;

/// Accept scalar string-like haystacks and array haystacks whose elements are string-like.
///
///   String / FixedString                    -> supported
///   Array(String) / Array(FixedString)      -> supported
///   Array(Nullable(String|FixedString))     -> supported
///
/// Top-level Nullable is intentionally not accepted here because the token-pattern
/// functions using this helper currently return plain UInt8 and do not implement
/// nullable propagation semantics.
bool isStringOrFixedStringOrArrayOfStringOrFixedString(const IDataType & type)
{
    if (isStringOrFixedString(type))
        return true;

    const auto * array_type = checkAndGetDataType<DataTypeArray>(&type);
    if (!array_type)
        return false;

    const IDataType * element_type = array_type->getNestedType().get();
    if (const auto * nullable_elem = typeid_cast<const DataTypeNullable *>(element_type))
        element_type = nullable_elem->getNestedType().get();

    return isStringOrFixedString(*element_type);
}

template <typename Matcher>
void executeOnStringHaystack(
    const StringColumnType auto & column,
    PaddedPODArray<UInt8> & result,
    size_t input_rows_count,
    Matcher && matcher)
{
    result.resize(input_rows_count);
    for (size_t row = 0; row < input_rows_count; ++row)
        result[row] = matcher(column.getDataAt(row));
}

template <typename Matcher>
void executeOnArrayHaystack(
    const ColumnArray & array,
    const StringColumnType auto & data,
    const ColumnNullable * nullable_data,
    PaddedPODArray<UInt8> & result,
    size_t input_rows_count,
    Matcher && matcher)
{
    const auto & offsets = array.getOffsets();
    result.resize(input_rows_count);

    size_t current_offset = 0;
    for (size_t row = 0; row < input_rows_count; ++row)
    {
        result[row] = false;
        const size_t row_end = offsets[row];

        for (size_t element = current_offset; element < row_end; ++element)
        {
            if (nullable_data && nullable_data->isNullAt(element))
                continue;

            if (matcher(data.getDataAt(element)))
            {
                result[row] = true;
                break;
            }
        }

        current_offset = row_end;
    }
}

template <typename Matcher>
void executeOnStringOrArrayHaystack(
    const ColumnPtr & haystack,
    std::string_view function_name,
    PaddedPODArray<UInt8> & result,
    size_t input_rows_count,
    Matcher && matcher)
{
    if (const auto * string_column = checkAndGetColumn<ColumnString>(haystack.get()))
        return executeOnStringHaystack(*string_column, result, input_rows_count, std::forward<Matcher>(matcher));

    if (const auto * fixed_column = checkAndGetColumn<ColumnFixedString>(haystack.get()))
        return executeOnStringHaystack(*fixed_column, result, input_rows_count, std::forward<Matcher>(matcher));

    const auto * array = checkAndGetColumn<ColumnArray>(haystack.get());
    if (!array)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Function '{}' requires String, FixedString, Array(String) or Array(FixedString) haystack, got '{}'",
            function_name,
            haystack->getName());

    const IColumn * raw_data = &array->getData();
    const ColumnNullable * nullable_data = checkAndGetColumn<ColumnNullable>(raw_data);
    const IColumn * nested_data = nullable_data ? &nullable_data->getNestedColumn() : raw_data;

    if (const auto * string_data = checkAndGetColumn<ColumnString>(nested_data))
        return executeOnArrayHaystack(*array, *string_data, nullable_data, result, input_rows_count, std::forward<Matcher>(matcher));

    if (const auto * fixed_data = checkAndGetColumn<ColumnFixedString>(nested_data))
        return executeOnArrayHaystack(*array, *fixed_data, nullable_data, result, input_rows_count, std::forward<Matcher>(matcher));

    throw Exception(
        ErrorCodes::ILLEGAL_COLUMN,
        "Function '{}' requires String, FixedString, Array(String) or Array(FixedString) haystack, got '{}'",
        function_name,
        haystack->getName());
}

/// Executable layer: uses forEachToken(*tokenizer, ...) for correct token boundaries.
/// The pattern is compiled once (at build time) and matched against each token.
class ExecutableFunctionMatchToken : public IExecutableFunction
{
public:
    ExecutableFunctionMatchToken(
        std::shared_ptr<const ITokenizer> tokenizer_,
        std::shared_ptr<const OptimizedRegularExpression> re_)
        : tokenizer(std::move(tokenizer_))
        , re(std::move(re_))
    {
    }

    String getName() const override { return "matchToken"; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result = ColumnUInt8::create(input_rows_count, UInt8(0));
        auto & result_data = result->getData();

        const auto matches_regexp = [&](std::string_view input)
        {
            bool found = false;
            forEachToken(*tokenizer, input.data(), input.size(),
                [&](const char * token_data, size_t token_len) -> bool
                {
                    std::string token(token_data, token_len);
                    if (re->match(token))
                    {
                        found = true;
                        return true; /// stop iteration
                    }
                    return false;
                });

            return found;
        };

        executeOnStringOrArrayHaystack(
            arguments[arg_haystack].column,
            getName(),
            result_data,
            input_rows_count,
            matches_regexp);

        return result;
    }

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    std::shared_ptr<const OptimizedRegularExpression> re;
};

/// FunctionBase layer.
class FunctionBaseMatchToken : public IFunctionBase
{
public:
    FunctionBaseMatchToken(
        std::shared_ptr<const ITokenizer> tokenizer_,
        std::shared_ptr<const OptimizedRegularExpression> re_,
        DataTypes argument_types_,
        DataTypePtr result_type_)
        : tokenizer(std::move(tokenizer_))
        , re(std::move(re_))
        , argument_types(std::move(argument_types_))
        , result_type(std::move(result_type_))
    {
    }

    String getName() const override { return "matchToken"; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionMatchToken>(tokenizer, re);
    }

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    std::shared_ptr<const OptimizedRegularExpression> re;
    DataTypes argument_types;
    DataTypePtr result_type;
};

/// OverloadResolver layer.
///
///   matchToken(haystack, pattern [, tokenizer])
///
/// The pattern is a re2-compatible regular expression. Matching is UNANCHORED
/// (substring match within each token), consistent with the ClickHouse `match` function.
/// To match the full token use explicit `^` and `$` anchors.
class FunctionMatchTokenOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "matchToken";

    static FunctionOverloadResolverPtr create(ContextPtr)
    {
        return std::make_unique<FunctionMatchTokenOverloadResolver>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {arg_pattern, arg_tokenizer}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args
        {
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedStringOrArrayOfStringOrFixedString), nullptr, "String, FixedString, Array(String) or Array(FixedString)"},
            {"pattern",  static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };
        FunctionArgumentDescriptors optional_args
        {
            {"tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };
        validateFunctionArguments(name, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeUInt8>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        /// Resolve the tokenizer: explicit tokenizer argument > default splitByNonAlpha.
        std::shared_ptr<const ITokenizer> tokenizer;
        if (arguments.size() > arg_tokenizer && arguments[arg_tokenizer].column)
        {
            std::string_view tokenizer_name = arguments[arg_tokenizer].column->getDataAt(0);
            tokenizer = TokenizerFactory::instance().get(tokenizer_name);
        }
        else
            tokenizer = TokenizerFactory::instance().get(SplitByNonAlphaTokenizer::getExternalName());

        if (!arguments[arg_pattern].column || arguments[arg_pattern].column->empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function 'matchToken': pattern argument must be a non-empty constant");

        String pattern = String(arguments[arg_pattern].column->getDataAt(0));
        auto re = std::make_shared<OptimizedRegularExpression>(pattern);

        DataTypes arg_types;
        arg_types.reserve(arguments.size());
        for (const auto & arg : arguments)
            arg_types.push_back(arg.type);

        return std::make_shared<FunctionBaseMatchToken>(
            std::move(tokenizer), std::move(re), std::move(arg_types), result_type);
    }
};

} /// anonymous namespace

REGISTER_FUNCTION(MatchToken)
{
    FunctionDocumentation::Description description = R"(
Checks if any token in the haystack matches the given regular expression.

The tokenizer used to split the haystack is controlled by the optional third argument (default: `splitByNonAlpha`).
The pattern is a re2-compatible regular expression. Matching is UNANCHORED (substring match within each token),
consistent with the ClickHouse `match` function. To match the full token use explicit `^` and `$` anchors.
When used via the Elasticsearch DSL (regexp query), the query planner automatically adds `^` and `$` anchors.
    )";
    FunctionDocumentation::Syntax syntax = "matchToken(haystack, pattern[, tokenizer])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack",  "The input column to be searched. For array inputs, returns `1` when any non-null element has a matching token.", {"String", "FixedString", "Nullable(String)", "Nullable(FixedString)", "Array(String)", "Array(FixedString)", "Array(Nullable(String))", "Array(Nullable(FixedString))"}},
        {"pattern",   "Regular expression pattern to match tokens against.",  {"const String"}},
        {"tokenizer", "Tokenizer name (optional, default splitByNonAlpha).",  {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if any token matches the pattern, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Token regexp search",
        "SELECT matchToken('clickhouse test', 'click.*')",
        R"(
┌─matchToken('clickhouse test', 'click.*')─┐
│                                         1 │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, VersionNumber{}, category};

    factory.registerFunction<FunctionMatchTokenOverloadResolver>(documentation);
}

}

#include <Storages/MergeTree/MergeTreeTextIndexAnalyzer.h>

#include <Common/Exception.h>
#include <Core/Field.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/IndicesDescription.h>

#include <fmt/ranges.h>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

const String ARGUMENT_TOKENIZER = "tokenizer";
const String ARGUMENT_PREPROCESSOR = "preprocessor";
const String ARGUMENT_DICTIONARY_BLOCK_SIZE = "dictionary_block_size";
const String ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION = "dictionary_block_frontcoding_compression";
const String ARGUMENT_POSTING_LIST_BLOCK_SIZE = "posting_list_block_size";
const String ARGUMENT_POSTING_LIST_CODEC = "posting_list_codec";

constexpr UInt64 DEFAULT_DICTIONARY_BLOCK_SIZE = 512;
constexpr UInt64 DEFAULT_DICTIONARY_BLOCK_USE_FRONTCODING = 1;
constexpr UInt64 DEFAULT_POSTING_LIST_BLOCK_SIZE = 1024 * 1024;
const String DEFAULT_POSTING_LIST_CODEC = "none";

template <typename Type>
Type castAs(const Field & field, std::string_view argument_name)
{
    auto expected_type = Field::TypeToEnum<Type>::value;
    if (expected_type != field.getType())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Text index argument '{}' expected to be {}, but got {}",
            argument_name, fieldTypeToString(Field::TypeToEnum<Type>::value), field.getTypeName());
    }
    return field.safeGet<Type>();
}

template <typename Type>
std::optional<Type> extractFieldOption(std::unordered_map<String, ASTPtr> & options, const String & option)
{
    auto it = options.find(option);
    if (it == options.end())
        return {};

    Field value = getFieldFromIndexArgumentAST(it->second);
    value = castAs<Type>(value, option);

    options.erase(it);
    return value.safeGet<Type>();
}

ASTPtr extractASTOption(std::unordered_map<String, ASTPtr> & options, const String & option, bool is_required)
{
    auto it = options.find(option);

    if (it != options.end())
    {
        ASTPtr ast = it->second;
        options.erase(it);
        return ast;
    }

    if (is_required)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' is required", option);

    return nullptr;
}

std::pair<String, ASTPtr> parseNamedArgument(const ASTFunction * ast_equal_function)
{
    if (!ast_equal_function
        || ast_equal_function->name != "equals"
        || ast_equal_function->arguments->children.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot mix key-value pair and single argument as text index arguments");

    const auto & arguments = ast_equal_function->arguments;
    const auto * key_identifier = arguments->children[0]->as<ASTIdentifier>();

    if (!key_identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument must be a key-value pair. got {}", ast_equal_function->formatForErrorMessage());

    return {key_identifier->name(), arguments->children[1]};
}

}

std::unordered_map<String, ASTPtr> textIndexConvertArgumentsToOptionsMap(const ASTPtr & arguments)
{
    std::unordered_map<String, ASTPtr> options;
    if (!arguments)
        return options;

    for (const auto & child : arguments->children)
    {
        const auto * ast_equal_function = child->as<ASTFunction>();
        auto [key, ast] = parseNamedArgument(ast_equal_function);

        if (!options.emplace(key, ast).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index '{}' argument is specified more than once", key);
    }
    return options;
}

std::unordered_map<String, ASTPtr> textIndexParseConfigString(const String & config)
{
    ParserExpressionList parser(false);
    ASTPtr ast = parseQuery(
        parser,
        config,
        "text index config",
        DBMS_DEFAULT_MAX_QUERY_SIZE,
        DBMS_DEFAULT_MAX_PARSER_DEPTH,
        DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    return textIndexConvertArgumentsToOptionsMap(ast);
}

MergeTreeIndexTextParams textIndexValidateOptions(std::unordered_map<String, ASTPtr> & options)
{
    MergeTreeIndexTextParams params;

    params.tokenizer = extractASTOption(options, ARGUMENT_TOKENIZER, true);
    params.preprocessor = extractASTOption(options, ARGUMENT_PREPROCESSOR, false);

    params.dictionary_block_size = extractFieldOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE).value_or(DEFAULT_DICTIONARY_BLOCK_SIZE);
    if (params.dictionary_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be greater than 0, but got {}",
            ARGUMENT_DICTIONARY_BLOCK_SIZE, params.dictionary_block_size);

    params.dictionary_block_frontcoding_compression = extractFieldOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION).value_or(DEFAULT_DICTIONARY_BLOCK_USE_FRONTCODING);
    if (params.dictionary_block_frontcoding_compression > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be 0 or 1, but got {}",
            ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION, params.dictionary_block_frontcoding_compression);

    params.posting_list_block_size = extractFieldOption<UInt64>(options, ARGUMENT_POSTING_LIST_BLOCK_SIZE).value_or(DEFAULT_POSTING_LIST_BLOCK_SIZE);
    if (params.posting_list_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be greater than 0, but got {}",
            ARGUMENT_POSTING_LIST_BLOCK_SIZE, params.posting_list_block_size);

    params.posting_list_codec = extractFieldOption<String>(options, ARGUMENT_POSTING_LIST_CODEC).value_or(DEFAULT_POSTING_LIST_CODEC);

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected text index arguments: {}", fmt::join(std::views::keys(options), ", "));

    return params;
}

}

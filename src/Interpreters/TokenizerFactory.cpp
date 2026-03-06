#include <Interpreters/TokenizerFactory.h>

#include <Common/Exception.h>
#include <Interpreters/ITokenizer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/IndicesDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename Type>
Type castAs(const Field & field, std::string_view argument_name)
{
    auto expected_type = Field::TypeToEnum<Type>::value;
    if (expected_type != field.getType())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Tokenizer argument '{}' expected to be of type {}, but got type: {}",
            argument_name, fieldTypeToString(expected_type), field.getTypeName());
    }
    return field.safeGet<Type>();
}

void assertParamsCount(size_t params_count, size_t max_count, std::string_view tokenizer)
{
    if (params_count > max_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "'{}' tokenizer accepts at most {} parameters, but got {}",
            tokenizer, max_count, params_count);
    }
}

}

std::unordered_map<String, ITokenizer::Type> TokenizerFactory::getAllTokenizers() const
{
    std::unordered_map<String, ITokenizer::Type> result;
    for (const auto & tokenizer : tokenizers)
        result[tokenizer.first] = tokenizer.second.type;
    return result;
}

static void registerTokenizers(TokenizerFactory & factory);

static constexpr UInt64 MIN_NGRAM_SIZE = 1;
static constexpr UInt64 DEFAULT_NGRAM_SIZE = 3;
static constexpr UInt64 DEFAULT_SPARSE_GRAMS_MIN_LENGTH = 3;
static constexpr UInt64 DEFAULT_SPARSE_GRAMS_MAX_LENGTH = 100;

TokenizerFactory & TokenizerFactory::instance()
{
    static TokenizerFactory factory;
    return factory;
}

TokenizerFactory::TokenizerFactory()
{
    registerTokenizers(*this);
}

void TokenizerFactory::registerTokenizer(const String & name, ITokenizer::Type type, Creator creator)
{
    if (!tokenizers.emplace(name, Entry{type, creator}).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TokenizerFactory: tokenizer '{}' is already registered", name);
}

std::unique_ptr<ITokenizer> TokenizerFactory::get(std::string_view full_name) const
{
    auto try_parse = [&](auto & parser) -> ASTPtr
    {
        std::string out_err;
        const char * start = full_name.data();
        return tryParseQuery(
            parser,
            start,
            start + full_name.size(),
            out_err,
            /*hilite=*/ false,
            "tokenizer",
            /*allow_multi_statements=*/ false,
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH,
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
            /*skip_insignificant=*/ true);
    };

    ParserFunction parser_function;
    if (auto ast = try_parse(parser_function))
        return get(ast);

    ParserIdentifier parser_identifier;
    if (auto ast = try_parse(parser_identifier))
        return get(ast);

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid tokenizer definition: '{}'", full_name);
}

std::unique_ptr<ITokenizer> TokenizerFactory::get(const ASTPtr & ast) const
{
    FieldVector args;
    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        return get(identifier->name(), args);
    }

    if (const auto * literal = ast->as<ASTLiteral>())
    {
        if (literal->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tokenizer name must be a string, got: '{}'", literal->value.getTypeName());

        return get(literal->value.safeGet<String>(), args);
    }

    if (const auto * function = ast->as<ASTFunction>())
    {
        args = getFieldsFromIndexArgumentsAST(function->arguments);
        return get(function->name, args);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create tokenizer from AST: '{}'", ast->formatForErrorMessage());
}

std::unique_ptr<ITokenizer> TokenizerFactory::get(
    std::string_view name,
    const FieldVector & args,
    const std::set<ITokenizer::Type> & allowed) const
{
    auto it = tokenizers.find(String(name));
    if (it == tokenizers.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown tokenizer: '{}'", name);

    if (!allowed.empty() && !allowed.contains(it->second.type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tokenizer '{}' is not allowed in this context", name);

    return it->second.creator(args);
}

static void registerTokenizers(TokenizerFactory & factory)
{
    auto ngrams_creator = [](const FieldVector & args) -> std::unique_ptr<ITokenizer>
    {
        assertParamsCount(args.size(), 1, NgramsTokenizer::getExternalName());
        auto ngram_size = args.empty() ? DEFAULT_NGRAM_SIZE : castAs<UInt64>(args[0], "ngram_size");

        if (ngram_size < MIN_NGRAM_SIZE)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Incorrect parameter of tokenizer '{}': ngram length must be at least {}, but got {}",
                NgramsTokenizer::getExternalName(),
                MIN_NGRAM_SIZE,
                ngram_size);

        return std::make_unique<NgramsTokenizer>(ngram_size);
    };

    factory.registerTokenizer(NgramsTokenizer::getName(), ITokenizer::Type::Ngrams, ngrams_creator);
    factory.registerTokenizer(NgramsTokenizer::getExternalName(), ITokenizer::Type::Ngrams, ngrams_creator);

    auto split_by_non_alpha_creator = [](const FieldVector & args) -> std::unique_ptr<ITokenizer>
    {
        assertParamsCount(args.size(), 0, SplitByNonAlphaTokenizer::getExternalName());
        return std::make_unique<SplitByNonAlphaTokenizer>();
    };

    factory.registerTokenizer(SplitByNonAlphaTokenizer::getName(), ITokenizer::Type::SplitByNonAlpha, split_by_non_alpha_creator);
    factory.registerTokenizer(SplitByNonAlphaTokenizer::getExternalName(), ITokenizer::Type::SplitByNonAlpha, split_by_non_alpha_creator);

    auto split_by_string_creator = [](const FieldVector & args) -> std::unique_ptr<ITokenizer>
    {
        assertParamsCount(args.size(), 1, SplitByStringTokenizer::getExternalName());
        if (args.empty())
            return std::make_unique<SplitByStringTokenizer>(std::vector<String>{" "});

        auto array = castAs<Array>(args[0], "separators");
        std::vector<String> values;
        for (const auto & value : array)
            values.emplace_back(castAs<String>(value, "separator"));

        if (values.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Incorrect parameter of tokenizer '{}': separators cannot be empty",
                SplitByStringTokenizer::getExternalName());

        return std::make_unique<SplitByStringTokenizer>(values);
    };

    factory.registerTokenizer(SplitByStringTokenizer::getName(), ITokenizer::Type::SplitByString, split_by_string_creator);

    auto array_creator = [](const FieldVector & args) -> std::unique_ptr<ITokenizer>
    {
        assertParamsCount(args.size(), 0, ArrayTokenizer::getExternalName());
        return std::make_unique<ArrayTokenizer>();
    };

    factory.registerTokenizer(ArrayTokenizer::getName(), ITokenizer::Type::Array, array_creator);

    auto sparse_grams_creator = [](const FieldVector & args) -> std::unique_ptr<ITokenizer>
    {
        const auto * tokenizer_name = SparseGramsTokenizer::getExternalName();
        assertParamsCount(args.size(), 3, tokenizer_name);

        UInt64 min_length = DEFAULT_SPARSE_GRAMS_MIN_LENGTH;
        UInt64 max_length = DEFAULT_SPARSE_GRAMS_MAX_LENGTH;
        std::optional<UInt64> min_cutoff_length;

        if (!args.empty())
            min_length = castAs<UInt64>(args[0], "min_length");

        if (args.size() > 1)
            max_length = castAs<UInt64>(args[1], "max_length");

        if (args.size() > 2)
            min_cutoff_length = castAs<UInt64>(args[2], "min_cutoff_length");

        if (min_length < DEFAULT_SPARSE_GRAMS_MIN_LENGTH)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected parameter of tokenizer '{}': minimal length must be at least {}, but got {}",
                tokenizer_name, DEFAULT_SPARSE_GRAMS_MIN_LENGTH, min_length);

        if (max_length > DEFAULT_SPARSE_GRAMS_MAX_LENGTH)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected parameter of tokenizer '{}': maximal length must be at most {}, but got {}",
                tokenizer_name, DEFAULT_SPARSE_GRAMS_MAX_LENGTH, max_length);

        if (min_length > max_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected parameter of tokenizer '{}': minimal length {} cannot be larger than maximal length {}",
                tokenizer_name, min_length, max_length);

        if (min_cutoff_length.has_value() && min_cutoff_length.value() < min_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected parameter of tokenizer '{}': minimal cutoff length {} cannot be smaller than minimal length {}",
                tokenizer_name, min_cutoff_length.value(), min_length);

        if (min_cutoff_length.has_value() && min_cutoff_length.value() > max_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected parameter of tokenizer '{}': minimal cutoff length {} cannot be larger than maximal length {}",
                tokenizer_name, min_cutoff_length.value(), max_length);

        return std::make_unique<SparseGramsTokenizer>(min_length, max_length, min_cutoff_length);
    };

    factory.registerTokenizer(SparseGramsTokenizer::getName(), ITokenizer::Type::SparseGrams, sparse_grams_creator);
    factory.registerTokenizer(SparseGramsTokenizer::getBloomFilterIndexName(), ITokenizer::Type::SparseGrams, sparse_grams_creator);
}

}

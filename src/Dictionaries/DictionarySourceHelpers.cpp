#include <Dictionaries/DictionarySourceHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Databases/removeWhereConditionPlaceholder.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/SettingsChanges.h>
#include <Common/quoteString.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int INCORRECT_QUERY;
}

namespace
{

ASTPtr parseDictionaryQueryAsSelect(const String & query, const String & description, const char * error_message)
{
    const char * query_begin = query.data();
    const char * query_end = query.data() + query.size();
    ParserQuery parser(query_end);
    ASTPtr ast = parseQuery(parser, query_begin, query_end, description, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    if (!ast || ast->getQueryKind() != IAST::QueryKind::Select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);

    return ast;
}

bool hasSettings(const IAST & ast)
{
    if (ast.as<ASTSetQuery>())
        return true;
    for (const auto & child : ast.children)
        if (hasSettings(*child))
            return true;
    return false;
}


/// The number of aliases of each name in the query.
void countAliases(const IAST & ast, UnorderedMapWithMemoryTracking<String, size_t> & aliases)
{
    if (const String alias = ast.tryGetAlias(); !alias.empty())
        ++aliases[alias];
    for (const auto & child : ast.children)
        countAliases(*child, aliases);
}

}

void checkDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message)
{
    parseDictionaryQueryAsSelect(query, description, error_message);
}

void checkExternalDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message, bool is_odbc)
{
    /// For ODBC, how many times each name is written after `AS`, see below.
    UnorderedMapWithMemoryTracking<String, size_t> names_written_after_as;
    std::string_view previous_bare_word;

    Lexer lexer(query.data(), query.data() + query.size());
    for (Token token = lexer.nextToken(); !token.isEnd() && !token.isError(); token = lexer.nextToken())
    {
        const std::string_view text(token.begin, token.size());
        const bool is_quoted = token.type == TokenType::StringLiteral || token.type == TokenType::QuotedIdentifier;
        const bool is_ascii_quoted = is_quoted && (text.starts_with('\'') || text.starts_with('"') || text.starts_with('`'));
        if (token.type == TokenType::Comment || token.type == TokenType::HereDoc || (is_quoted && (!is_ascii_quoted || text.contains('\\'))))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} cannot contain comments, heredocs, typographic quotes or backslashes, because the database may read them differently: {}",
                description,
                text);

        if (!is_odbc)
            continue;

        /// The dialect of an ODBC database is unknown. SQL Server reads `[']` as an identifier in brackets, while ClickHouse
        /// reads an array of a string, so `SELECT ['] = 1; DROP TABLE t; SELECT ['] = 1` is one statement for ClickHouse only.
        if (token.type == TokenType::OpeningSquareBracket || token.type == TokenType::ClosingSquareBracket)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} cannot contain square brackets, because the database may read them as quoted identifiers",
                description);

        if (token.isSignificant())
        {
            if (boost::iequals(previous_bare_word, "AS"))
            {
                if (token.type == TokenType::BareWord)
                    ++names_written_after_as[String(text)];
                else if (token.type == TokenType::QuotedIdentifier && text.size() >= 2)
                    ++names_written_after_as[String(text.substr(1, text.size() - 2))];
            }
            previous_bare_word = token.type == TokenType::BareWord ? text : std::string_view{};
        }
    }

    ASTPtr ast = parseDictionaryQueryAsSelect(query, description, error_message);
    const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());
    if (hasSettings(*ast) || (query_with_output && (query_with_output->out_file || query_with_output->format_ast)))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);

    /// ClickHouse reads a word after an expression as its alias, SQL Server can read it as another statement:
    /// `SELECT 1 SHUTDOWN` is `SELECT 1` and `SHUTDOWN`. So for ODBC every alias must be written with `AS`.
    /// It is a best effort, counted on the tokens: `AS` in `CAST(x AS T)` counts too.
    if (is_odbc)
    {
        UnorderedMapWithMemoryTracking<String, size_t> aliases;
        countAliases(*ast, aliases);
        for (const auto & [alias, count] : aliases)
        {
            const auto it = names_written_after_as.find(alias);
            if (it == names_written_after_as.end() || it->second < count)
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "{} must write the alias {} with AS, because the database may read it as another statement",
                    description,
                    backQuote(alias));
        }
    }
}

void checkExternalDictionaryLoadQueryIsSelect(const String & query, const String & load_all_query, const String & source_name)
{
    const String description = "Query for " + source_name + " dictionary";

    Lexer lexer(query.data(), query.data() + query.size());
    for (Token token = lexer.nextToken(); !token.isEnd() && !token.isError(); token = lexer.nextToken())
    {
        if ((token.type == TokenType::StringLiteral || token.type == TokenType::QuotedIdentifier)
            && std::string_view(token.begin, token.size()).contains(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} cannot contain {} in a string literal or a quoted identifier, because it is replaced by a condition with quotes",
                description,
                CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
    }

    checkExternalDictionaryQueryIsSelect(load_all_query, description, "Only SELECT query can be used as a dictionary source", source_name == "ODBC");
}

/// For simple key

Block blockForIds(
    const DictionaryStructure & dict_struct,
    const VectorWithMemoryTracking<UInt64> & ids)
{
    auto column = ColumnUInt64::create(ids.size());
    memcpy(column->getData().data(), ids.data(), ids.size() * sizeof(ids.front()));

    Block block{{std::move(column), std::make_shared<DataTypeUInt64>(), (*dict_struct.id).name}};

    return block;
}

/// For composite key

Block blockForKeys(
    const DictionaryStructure & dict_struct,
    const Columns & key_columns,
    const VectorWithMemoryTracking<size_t> & requested_rows)
{
    Block block;

    for (size_t i = 0, size = key_columns.size(); i < size; ++i)
    {
        const ColumnPtr & source_column = key_columns[i];
        size_t column_rows_size = source_column->size();

        PaddedPODArray<UInt8> filter(column_rows_size, false);

        for (size_t idx : requested_rows)
            filter[idx] = true;

        auto filtered_column = source_column->filter(filter, requested_rows.size());

        block.insert({filtered_column, (*dict_struct.key)[i].type, (*dict_struct.key)[i].name});
    }

    return block;
}


SettingsChanges readSettingsFromDictionaryConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (!config.has(config_prefix + ".settings"))
        return {};

    const auto prefix = config_prefix + ".settings";

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(prefix, config_keys);

    SettingsChanges changes;

    for (const std::string & key : config_keys)
    {
        const auto value = config.getString(prefix + "." + key);
        changes.emplace_back(key, value);
    }

    return changes;
}


ContextMutablePtr copyContextAndApplySettingsFromDictionaryConfig(
    const ContextPtr & context, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    auto context_copy = Context::createCopy(context);
    auto changes = readSettingsFromDictionaryConfig(config, config_prefix);
    context_copy->applySettingsChanges(changes);
    return context_copy;
}

static Block transformHeader(Block header, Block block_to_add)
{
    for (Int64 i = static_cast<Int64>(block_to_add.columns() - 1); i >= 0; --i)
        header.insert(0, block_to_add.getByPosition(i).cloneEmpty());

    return header;
}

TransformWithAdditionalColumns::TransformWithAdditionalColumns(
    SharedHeader block_to_add_, SharedHeader header)
    : ISimpleTransform(header, std::make_shared<const Block>(transformHeader(*header, *block_to_add_)), true)
    , block_to_add(std::move(block_to_add_))
{
}

void TransformWithAdditionalColumns::transform(Chunk & chunk)
{
    if (chunk)
    {
        auto num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        auto cut_block = block_to_add->cloneWithCutColumns(current_range_index, num_rows);

        if (cut_block.rows() != num_rows)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "Number of rows in block to add after cut must equal to number of rows in block from inner stream");

        for (Int64 i = static_cast<Int64>(cut_block.columns() - 1); i >= 0; --i)
            columns.insert(columns.begin(), cut_block.getByPosition(i).column);

        current_range_index += num_rows;
        chunk.setColumns(std::move(columns), num_rows);
    }
}

String TransformWithAdditionalColumns::getName() const
{
    return "TransformWithAdditionalColumns";
}

}

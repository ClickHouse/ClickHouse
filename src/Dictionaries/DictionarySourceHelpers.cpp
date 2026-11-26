#include <Dictionaries/DictionarySourceHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <Core/ServerSettings.h>
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

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsBool dictionaries_external_source_queries_select_only;
}

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int INCORRECT_QUERY;
    extern const int SYNTAX_ERROR;
}

namespace
{

/// The errors of the checks below do not contain the text of the query: the query can come from a named collection, whose values
/// are hidden from a user without `SHOW NAMED COLLECTIONS SECRETS`, and the error reaches the caller of `dictGet` and
/// `system.dictionaries`.
size_t positionInQuery(const String & query, const char * pos)
{
    return pos - query.data() + 1;
}

ASTPtr parseDictionaryQueryAsSelect(const String & query, const String & description, const char * error_message)
{
    const char * pos = query.data();
    const char * query_end = query.data() + query.size();
    ParserQuery parser(query_end);
    String parser_error_message; /// Contains the text of the query from the error, so it is not shown.
    ASTPtr ast = tryParseQuery(
        parser, pos, query_end, parser_error_message, /* hilite = */ false, description, /* allow_multi_statements = */ false,
        /* max_query_size = */ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, /* skip_insignificant = */ true);
    if (!ast)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Syntax error ({}): the query is not a SELECT that the ClickHouse parser reads. "
            "The query is not shown, because it can come from a named collection",
            description);

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

}

void checkDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message)
{
    parseDictionaryQueryAsSelect(query, description, error_message);
}

void checkExternalDictionaryQueryIsSelect(const String & query, const String & source_description, const char * error_message)
{
    /// A query valid for the database can be rejected by the ClickHouse parser, so the errors say which check rejected it.
    const String description = source_description
        + " (checked as a ClickHouse SELECT query, see the server setting `dictionaries_external_source_queries_select_only`)";

    Lexer lexer(query.data(), query.data() + query.size());
    for (Token token = lexer.nextToken(); !token.isEnd() && !token.isError(); token = lexer.nextToken())
    {
        const std::string_view text(token.begin, token.size());
        const bool is_quoted = token.type == TokenType::StringLiteral || token.type == TokenType::QuotedIdentifier;
        const bool is_ascii_quoted = is_quoted && (text.starts_with('\'') || text.starts_with('"') || text.starts_with('`'));
        const char * kind = nullptr;
        if (token.type == TokenType::Comment)
            kind = "a comment";
        else if (token.type == TokenType::HereDoc)
            kind = "a heredoc";
        else if (is_quoted && !is_ascii_quoted)
            kind = "a typographic quote";
        else if (is_quoted && text.contains('\\'))
            kind = "a backslash in a quoted token";

        if (kind)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} cannot contain comments, heredocs, typographic quotes or backslashes, because the database may read them differently: "
                "{} at position {}",
                description,
                kind,
                positionInQuery(query, token.begin));
    }

    ASTPtr ast = parseDictionaryQueryAsSelect(query, description, error_message);
    const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());
    if (hasSettings(*ast) || (query_with_output && (query_with_output->out_file || query_with_output->format_ast)))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);
}

bool shouldCheckExternalDictionaryQueries(const ContextPtr & context, bool created_from_ddl)
{
    return created_from_ddl && context->getServerSettings()[ServerSetting::dictionaries_external_source_queries_select_only];
}

void checkExternalDictionaryQueryConditionPlaceholder(const String & query, const String & source_name)
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
}

void checkExternalDictionaryLoadQueryIsSelect(const String & load_all_query, const String & source_name)
{
    checkExternalDictionaryQueryIsSelect(
        load_all_query, "Query for " + source_name + " dictionary", "Only SELECT query can be used as a dictionary source");
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

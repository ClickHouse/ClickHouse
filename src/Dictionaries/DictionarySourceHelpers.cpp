#include <Dictionaries/DictionarySourceHelpers.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Databases/removeWhereConditionPlaceholder.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/SettingsChanges.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/quoteString.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int INCORRECT_QUERY;
}

ASTPtr checkDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message)
{
    const char * query_begin = query.data();
    const char * query_end = query.data() + query.size();
    ParserQuery parser(query_end);
    ASTPtr ast = parseQuery(parser, query_begin, query_end, description, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    if (!ast || ast->getQueryKind() != IAST::QueryKind::Select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);

    return ast;
}

namespace
{

/// The number of aliases of each name in the query, and whether it has `SETTINGS`.
void countAliasesAndFindSettings(const IAST & ast, UnorderedMapWithMemoryTracking<String, size_t> & aliases, bool & has_settings)
{
    if (const String alias = ast.tryGetAlias(); !alias.empty())
        ++aliases[alias];
    if (ast.as<ASTSetQuery>())
        has_settings = true;
    for (const auto & child : ast.children)
        countAliasesAndFindSettings(*child, aliases, has_settings);
}

/// The query is checked with the ClickHouse parser, so everything that the external database reads differently
/// is rejected: MySQL executes the text of `/*! ... */`, does not take `--` without a following space for a comment,
/// reads `$$...$$` and the typographic quotes that ClickHouse accepts (`‘...’`) as identifiers;
/// PostgreSQL reads a backslash in a string literally. The dialect of an ODBC database is unknown, so square brackets
/// are rejected for it too: SQL Server reads `['] = 1` as an identifier in brackets, while ClickHouse reads an array of a string.
void checkExternalDatabaseQueryIsSelect(const String & query, const String & source_type, const String & description, const char * error_message)
{
    const bool is_odbc = source_type == "odbc";

    /// For ODBC, how many times each name is written after `AS`, see below.
    UnorderedMapWithMemoryTracking<String, size_t> names_allowed_as_alias;
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
                "{} cannot contain comments, heredocs, typographic quotes or backslashes, because the external database may read them differently: {}",
                description,
                text);

        if (is_odbc && (token.type == TokenType::OpeningSquareBracket || token.type == TokenType::ClosingSquareBracket))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} cannot contain square brackets, because the database may read them as quoted identifiers",
                description);

        if (is_odbc && token.isSignificant())
        {
            if (boost::iequals(previous_bare_word, "AS"))
            {
                if (token.type == TokenType::BareWord)
                    ++names_allowed_as_alias[String(text)];
                else if (token.type == TokenType::QuotedIdentifier && text.size() >= 2)
                    ++names_allowed_as_alias[String(text.substr(1, text.size() - 2))];
            }

            previous_bare_word = token.type == TokenType::BareWord ? text : std::string_view{};
        }
    }

    ASTPtr ast = checkDictionaryQueryIsSelect(query, description, error_message);

    /// `INTO OUTFILE` is a part of a `SELECT` for ClickHouse, but an external database (MySQL) writes the file on its host.
    /// `FORMAT` and `SETTINGS` after the query are read by ClickHouse only: the database can read the words after them as another statement.
    UnorderedMapWithMemoryTracking<String, size_t> aliases;
    bool has_settings = false;
    countAliasesAndFindSettings(*ast, aliases, has_settings);
    if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());
        has_settings || (query_with_output && (query_with_output->out_file || query_with_output->format_ast)))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);

    /// ClickHouse reads a word after an expression as its alias. A database can read it as another statement instead:
    /// SQL Server reads `SELECT 1 SHUTDOWN` as `SELECT 1` and `SHUTDOWN`. So for ODBC, whose dialect is unknown,
    /// every alias must be written with `AS`. This is a best effort: the dialect cannot be checked with the ClickHouse parser.
    if (is_odbc)
    {
        for (const auto & [alias, count] : aliases)
        {
            const auto it = names_allowed_as_alias.find(alias);
            if (it == names_allowed_as_alias.end() || it->second < count)
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "{} must write the alias {} with AS, because the database may read it as another statement",
                    description,
                    backQuote(alias));
        }
    }
}

/// The source replaces the first `{condition}` in `query` with a condition on the keys, which contains quoted names and values.
/// Inside a string literal or a quoted identifier, the quotes of this condition would end the quoted token, so the condition
/// would be read as a part of the query: `SELECT '{condition}' AS id` becomes `SELECT '("id"=' AS ... ')' AS id`.
/// Comments are rejected anyway, so outside quotes the placeholder is replaced by an expression of quoted names and escaped
/// values, which stays one expression.
void checkConditionPlaceholderIsNotQuoted(const String & query, const String & description)
{
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

}

bool isExternalDatabaseDictionarySource(const String & source_type)
{
    return source_type == "mysql" || source_type == "postgresql" || source_type == "odbc";
}

void checkExternalDictionarySourceQueries(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys source_keys;
    config.keys(config_prefix + ".source", source_keys);
    /// The number of elements is checked when the source is created.
    if (source_keys.empty() || source_keys.size() > 2)
        return;

    /// The source type is chosen as in `DictionarySourceFactory::create`.
    const auto & source_type = source_keys.front() == "settings" ? source_keys.back() : source_keys.front();
    if (!isExternalDatabaseDictionarySource(source_type))
        return;

    const String source_prefix = config_prefix + ".source." + source_type;

    /// Only the queries of the definition are checked. A named collection can hold them too, but it is written by a user who
    /// can create or alter named collections and so controls the credentials anyway, as for the configuration files.
    const String query = config.getString(source_prefix + ".query", "");
    const String where = config.getString(source_prefix + ".where", "");
    const String update_field = config.getString(source_prefix + ".update_field", "");
    const String invalidate_query = config.getString(source_prefix + ".invalidate_query", "");

    /// The source composes its queries from `query` or `where`, the attribute expressions and `update_field`, which are inserted verbatim,
    /// and from identifiers, which it quotes in the style of the database. The identifiers do not change the kind of the query,
    /// so they are replaced here by a placeholder table and quoted in the ClickHouse style, which the ClickHouse parser reads correctly.
    /// The table, the database and the schema can also come from a named collection, so they are not used.
    const DictionaryStructure dict_struct(config, config_prefix);
    const ExternalQueryBuilder query_builder(
        dict_struct, /* db_ */ "", /* schema_ */ "", /* table_ */ query.empty() ? "table" : "", query, where, IdentifierQuotingStyle::Backticks);

    static constexpr auto source_error = "Only SELECT query can be used as a dictionary source";
    checkConditionPlaceholderIsNotQuoted(query, "Query for " + source_type + " dictionary");
    checkExternalDatabaseQueryIsSelect(query_builder.composeLoadAllQuery(), source_type, "Query for " + source_type + " dictionary", source_error);

    if (!update_field.empty())
        checkExternalDatabaseQueryIsSelect(
            query_builder.composeUpdateQuery(update_field, "1970-01-01 00:00:00"),
            source_type,
            "Update query for " + source_type + " dictionary",
            source_error);

    /// The lookups of a `direct` or a `cache` layout replace `{condition}` with a condition on the requested keys instead.
    /// The keys are values of the lookup, so one key with default values stands for them: it is quoted and escaped the same way.
    /// A `query` without `{condition}` is put in a subquery, which is checked as the load query already.
    const bool lookup_inserts_condition = query.empty() || query.contains(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
    String lookup_query;
    if (lookup_inserts_condition && dict_struct.id)
    {
        lookup_query = query_builder.composeLoadIdsQuery({0});
    }
    else if (lookup_inserts_condition && dict_struct.key)
    {
        Columns key_columns;
        for (const auto & key : *dict_struct.key)
            key_columns.push_back(key.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst());
        lookup_query = query_builder.composeLoadKeysQuery(key_columns, {0}, ExternalQueryBuilder::AND_OR_CHAIN);
    }
    if (!lookup_query.empty())
        checkExternalDatabaseQueryIsSelect(lookup_query, source_type, "Lookup query for " + source_type + " dictionary", source_error);

    if (!invalidate_query.empty())
        checkExternalDatabaseQueryIsSelect(
            invalidate_query,
            source_type,
            "Invalidate query for " + source_type + " dictionary",
            "Only SELECT query can be used as a dictionary invalidate query");
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

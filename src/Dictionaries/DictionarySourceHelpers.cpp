#include <Dictionaries/DictionarySourceHelpers.h>
#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/SettingsChanges.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>

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

/// The query is checked with the ClickHouse parser, so everything that the external database reads differently
/// is rejected: MySQL executes the text of `/*! ... */`, does not take `--` without a following space for a comment,
/// reads `$$...$$` and the typographic quotes that ClickHouse accepts (`‘...’`) as identifiers;
/// PostgreSQL reads a backslash in a string literally. The dialect of an ODBC database is unknown, so square brackets
/// are rejected for it too: SQL Server reads `['] = 1` as an identifier in brackets, while ClickHouse reads an array of a string.
void checkExternalDatabaseQueryIsSelect(const String & query, const String & source_type, const String & description, const char * error_message)
{
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

        if (source_type == "odbc" && (token.type == TokenType::OpeningSquareBracket || token.type == TokenType::ClosingSquareBracket))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} cannot contain square brackets, because the database may read them as quoted identifiers",
                description);
    }

    ASTPtr ast = checkDictionaryQueryIsSelect(query, description, error_message);

    /// `INTO OUTFILE` is a part of a `SELECT` for ClickHouse, but an external database (MySQL) writes the file on its host.
    if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()); query_with_output && query_with_output->out_file)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);
}

}

bool isExternalDatabaseDictionarySource(const String & source_type)
{
    return source_type == "mysql" || source_type == "postgresql" || source_type == "odbc";
}

void checkExternalDictionarySourceQueries(
    const DictionaryStructure & dict_struct,
    const String & source_type,
    const String & query,
    const String & where,
    const String & update_field,
    const String & invalidate_query)
{
    /// The source composes its queries from `query` or `where`, the attribute expressions and `update_field`, which are inserted verbatim,
    /// and from identifiers, which it quotes in the style of the database. The identifiers do not change the kind of the query,
    /// so they are replaced here by a placeholder table and quoted in the ClickHouse style, which the ClickHouse parser reads correctly.
    /// The table, the database and the schema can also come from a named collection, so they are not used.
    const ExternalQueryBuilder query_builder(
        dict_struct, /* db_ */ "", /* schema_ */ "", /* table_ */ query.empty() ? "table" : "", query, where, IdentifierQuotingStyle::Backticks);

    static constexpr auto source_error = "Only SELECT query can be used as a dictionary source";
    checkExternalDatabaseQueryIsSelect(query_builder.composeLoadAllQuery(), source_type, "Query for " + source_type + " dictionary", source_error);

    if (!update_field.empty())
        checkExternalDatabaseQueryIsSelect(
            query_builder.composeUpdateQuery(update_field, "1970-01-01 00:00:00"),
            source_type,
            "Update query for " + source_type + " dictionary",
            source_error);

    if (!invalidate_query.empty())
        checkExternalDatabaseQueryIsSelect(
            invalidate_query,
            source_type,
            "Invalidate query for " + source_type + " dictionary",
            "Only SELECT query can be used as a dictionary invalidate query");
}

bool hasExternalDictionarySourceQueryFromNamedCollection(
    const Poco::Util::AbstractConfiguration & config, const String & source_config_prefix, const NamedCollection & collection)
{
    for (const auto * key : {"query", "where", "update_field", "invalidate_query"})
    {
        if (collection.has(key) && !config.has(source_config_prefix + "." + key))
            return true;
    }
    return false;
}

void checkExternalDictionarySourceQueries(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
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

    /// The MySQL and PostgreSQL sources take the queries from the named collection that they refer to, with the keys
    /// of the definition replacing the keys of the collection, so the checked queries are the ones that the source sends.
    /// The ODBC source does not use named collections. A missing collection is reported when the source is created.
    /// The privilege is checked before the collection is read, so that the errors do not show its queries to a user without it.
    NamedCollectionPtr collection;
    if (const String collection_name = config.getString(source_prefix + ".name", "");
        !collection_name.empty() && (source_type == "mysql" || source_type == "postgresql"))
    {
        context->checkAccess(AccessType::NAMED_COLLECTION, collection_name);
        NamedCollectionFactory::instance().loadIfNot();
        collection = NamedCollectionFactory::instance().tryGet(collection_name);
    }
    auto get_query_key = [&](const String & key) -> String
    {
        if (config.has(source_prefix + "." + key) || !collection)
            return config.getString(source_prefix + "." + key, "");
        return collection->getOrDefault<String>(key, "");
    };

    const String query = get_query_key("query");
    const String where = get_query_key("where");
    const String update_field = get_query_key("update_field");
    const String invalidate_query = get_query_key("invalidate_query");

    checkExternalDictionarySourceQueries(DictionaryStructure(config, config_prefix), source_type, query, where, update_field, invalidate_query);
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

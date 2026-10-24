#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Update.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/ParserMongoUpdateQuery.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Interpreters/MongoUpdateAccess.h>
#include <Common/Exception.h>

#include <bson/bson.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

namespace
{

/// Serializes a required member of the update statement.
String serializeRequiredMember(const rapidjson::Value & json, const char * name)
{
    auto it = json.FindMember(name);
    if (it == json.MemberEnd())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'update' command does not contain the '{}' field", name);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    it->value.Accept(writer);
    return buffer.GetString();
}

/// Throws if an assignment of the `ALTER TABLE ... UPDATE` that `ast` is names a column that the
/// collection does not have.
void rejectAssignmentsToMissingColumns(const ASTPtr & ast, const std::vector<String> & columns, const CollectionRef & collection)
{
    const auto * alter = ast->as<ASTAlterQuery>();
    if (!alter || !alter->command_list)
        return;

    const std::unordered_set<std::string_view> column_set(columns.begin(), columns.end());
    for (const auto & child : alter->command_list->children)
    {
        const auto * command = child->as<ASTAlterCommand>();
        if (!command || command->type != ASTAlterCommand::UPDATE || !command->update_assignments)
            continue;

        for (const auto & assignment : command->update_assignments->children)
        {
            const auto & column = assignment->as<const ASTAssignment &>().column_name;
            if (!column_set.contains(column))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The 'update' command updates the field '{}', which the collection '{}.{}' does not have: the fields of a "
                    "collection are the ones of its first inserted document, and an update cannot add a new one",
                    column,
                    collection.database,
                    collection.collection);
        }
    }
}

}

std::vector<Document> UpdateHandler::handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(sections[0].documents[0], "update");
    rejectUnorderedWriteBatch(sections[0].documents[0], "update");
    rejectUnsupportedOptions(sections[0].documents[0].getRapidJSONRepresentation(), "update", {"let"});
    rejectMaxTime(sections[0].documents[0].getRapidJSONRepresentation(), "update");

    /// The specs come either as an `updates` document sequence or as the `updates` array of the
    /// command body itself, see `getWriteBatch`.
    const auto update_specs = getWriteBatch(sections, "updates", "update");

    /// An update of a collection that does not exist matches no document, which Mongo reports as
    /// an update of zero documents rather than an error. So does an update of the placeholder of
    /// `createCollection`, which has no columns for the filter to name yet.
    const bool collection_exists = collectionHasSchema(collection, executor);

    /// All the columns, including the ones the user may not see, see `getMongoCollectionColumns`.
    std::vector<String> columns;
    if (collection_exists)
        columns = getMongoCollectionColumns(collection.database, collection.collection, executor->getQueryContext());

    /// The 'update' command carries one or more update specs, each with its own 'q', 'u',
    /// 'multi', and 'upsert'. Execute every spec; 'multi: false' (updateOne) cannot be
    /// expressed as a ClickHouse mutation over an unordered table and 'upsert' has no
    /// counterpart either, so both are rejected instead of being silently widened into
    /// updateMany or dropped.
    /// A spec is translated and executed before the next one is read, so that the writes of the
    /// earlier specs of an ordered batch - the Mongo default - survive an error raised by a later
    /// one. The translation of a spec still happens before its execution, so that a malformed
    /// update is an error whether the collection exists or not.
    /// A ClickHouse mutation is asynchronous and says nothing about the rows it will rewrite, so
    /// the documents a spec matches are counted with the very same filter, translated as a `find`,
    /// before the mutation is submitted. Without it the reply would claim that a successful
    /// `updateMany` matched nothing.
    auto translate = [&](const String & mongo_dialect_query, const IAST::FormatSettings & format_settings)
    {
        auto ast = parseMongoDialectCommand(mongo_dialect_query, collection, *executor);

        /// `$set` of a subdocument resets the leaves of the field it does not name, which only
        /// the columns of the collection tell, see `expandMongoSubtreeAssignments`.
        if (collection_exists)
            expandMongoSubtreeAssignmentsCheckingAccess(ast, columns, executor->getQueryContext());

        /// A collection gets its columns from the first inserted document and an update does not
        /// add any, so `$set` or `$inc` of a field that is not a column, or `$rename` to one, is
        /// rejected here with an error that names the field, rather than reaching the mutation.
        if (collection_exists)
            rejectAssignmentsToMissingColumns(ast, columns, collection);

        String sql_query;
        {
            WriteBufferFromString buffer(sql_query);
            ast->format(buffer, format_settings);
        }
        return sql_query;
    };

    Int64 matched = 0;
    for (const auto & update_spec : update_specs)
    {
        String serialized_filter;
        String serialized_update;
        {
            auto json_representation = update_spec.getRapidJSONRepresentation();
            rejectUnsupportedOptions(json_representation, "update", {"collation", "arrayFilters"});
            serialized_filter = serializeRequiredMember(json_representation, "q");
            serialized_update = serializeRequiredMember(json_representation, "u");

            /// A filter names a nested field either as a subdocument or as a dotted path, while a
            /// column is always the dotted path, so the filter of an `update` is normalized the
            /// same way as the one of a `find` or a `delete`.
            serialized_filter = modifyFilter(serialized_filter);

            auto multi_it = json_representation.FindMember("multi");
            if (multi_it == json_representation.MemberEnd() || !multi_it->value.IsBool() || !multi_it->value.GetBool())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The 'update' command supports only 'multi: true' (updateMany); updating a single document is not supported");

            /// A malformed `upsert` is an error of its own, rather than being read as an absent one.
            if (getBoolOption(json_representation, "upsert", "update").value_or(false))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'update' command does not support 'upsert: true'");
        }

        auto alter_settings = IAST::FormatSettings(true, IdentifierQuotingRule::WhenNecessary, IdentifierQuotingStyle::Backticks);
        const String alter_query = translate(
            fmt::format("{}.updateMany({}, {})", MONGO_DIALECT_PLACEHOLDER_NAMESPACE, serialized_filter, serialized_update), alter_settings);
        const String select_query
            = translate(fmt::format("{}.find({})", MONGO_DIALECT_PLACEHOLDER_NAMESPACE, serialized_filter), IAST::FormatSettings(true));

        if (collection_exists)
        {
            /// Mongo applies the specs one after another, so each mutation is awaited (see
            /// `getMutationSettings`) before the next spec is counted: otherwise a spec whose
            /// predicate an earlier one has already changed would still be counted against the
            /// pre-mutation rows and the reply would over-report the matches.
            matched += countMatchedRows(select_query, executor);
            executor->execute(alter_query, getMutationSettings());
        }
    }

    bson_t * bson_doc = bson_new();

    /// `n` is the number of matched documents, which is known before the mutation is submitted.
    /// `nModified` - the number of documents whose values actually change - is not: a mutation is
    /// asynchronous and rewrites a matched row whether or not the assigned value differs from the
    /// one it already holds. It is therefore omitted rather than reported as a number that would
    /// not be true; a driver reads a missing `nModified` as "the server did not say".
    if (matched <= INT32_MAX)
        BSON_APPEND_INT32(bson_doc, "n", static_cast<int32_t>(matched));
    else
        BSON_APPEND_INT64(bson_doc, "n", matched);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerUpdateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<UpdateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}

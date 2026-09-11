#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Update.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <bson/bson.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

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

}

std::vector<Document> UpdateHandler::handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(sections[0].documents[0], "update");

    /// The specs come either as an `updates` document sequence or as the `updates` array of the
    /// command body itself, see `getWriteBatch`.
    const auto update_specs = getWriteBatch(sections, "updates", "update");

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
        auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
        auto ast = Mongo::parseMongoQuery(
            parser,
            mongo_dialect_query.data(),
            mongo_dialect_query.data() + mongo_dialect_query.size(),
            "",
            10000,
            10000,
            10000,
            collection.database,
            collection.collection);

        String sql_query;
        {
            WriteBufferFromString buffer(sql_query);
            ast->format(buffer, format_settings);
        }
        return sql_query;
    };

    /// An update of a collection that does not exist matches no document, which Mongo reports as
    /// an update of zero documents rather than an error.
    const bool collection_exists = objectExists(executor, "TABLE", collection.getQualifiedName());

    Int64 matched = 0;
    for (const auto & update_spec : update_specs)
    {
        String serialized_filter;
        String serialized_update;
        {
            auto json_representation = update_spec.getRapidJSONRepresentation();
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

            auto upsert_it = json_representation.FindMember("upsert");
            if (upsert_it != json_representation.MemberEnd() && upsert_it->value.IsBool() && upsert_it->value.GetBool())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'update' command does not support 'upsert: true'");
        }

        auto alter_settings = IAST::FormatSettings(true, IdentifierQuotingRule::WhenNecessary, IdentifierQuotingStyle::Backticks);
        const String alter_query = translate(
            fmt::format("db.{}.updateMany({}, {})", collection.collection, serialized_filter, serialized_update), alter_settings);
        const String select_query
            = translate(fmt::format("db.{}.find({})", collection.collection, serialized_filter), IAST::FormatSettings(true));

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

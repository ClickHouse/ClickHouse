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
    if (sections.size() < 2 || sections[1].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'update' command does not contain any update statement");

    auto collection = getCollectionRef(sections[0].documents[0], "update");

    /// The 'update' command carries one or more update specs, each with its own 'q', 'u',
    /// 'multi', and 'upsert'. Execute every spec; 'multi: false' (updateOne) cannot be
    /// expressed as a ClickHouse mutation over an unordered table and 'upsert' has no
    /// counterpart either, so both are rejected instead of being silently widened into
    /// updateMany or dropped.
    /// Every spec is translated first, and only then executed: a malformed update has to be an
    /// error whether the collection exists or not.
    std::vector<String> alter_queries;
    for (const auto & update_spec : sections[1].documents)
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

        auto mongo_dialect_query
            = fmt::format("db.{}.updateMany({}, {})", collection.collection, serialized_filter, serialized_update);

        auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
        auto ast = Mongo::parseMongoQuery(
            parser,
            mongo_dialect_query.data(),
            mongo_dialect_query.data() + mongo_dialect_query.size(),
            "",
            10000,
            10000,
            10000,
            collection.database);

        String alter_query;
        {
            WriteBufferFromString buffer(alter_query);
            auto settings = IAST::FormatSettings(true, IdentifierQuotingRule::WhenNecessary, IdentifierQuotingStyle::Backticks);
            ast->format(buffer, settings);
        }

        alter_queries.push_back(std::move(alter_query));
    }

    /// An update of a collection that does not exist matches no document, which Mongo reports as
    /// an update of zero documents rather than an error.
    if (objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        for (const auto & alter_query : alter_queries)
            executor->execute(alter_query);
    }

    bson_t * bson_doc = bson_new();

    /// A mutation is asynchronous, so the number of rows it will write is not known here; the
    /// reply of an `update` carries both counts, and a driver reads a missing `nModified` as
    /// "the server did not say" rather than as zero.
    BSON_APPEND_INT32(bson_doc, "n", 0);
    BSON_APPEND_INT32(bson_doc, "nModified", 0);
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

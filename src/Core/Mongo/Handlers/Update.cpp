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

    String serialized_filter;
    String serialized_update;
    {
        auto json_representation = sections[1].documents[0].getRapidJSONRepresentation();
        serialized_filter = serializeRequiredMember(json_representation, "q");
        serialized_update = serializeRequiredMember(json_representation, "u");
    }

    auto mongo_dialect_query
        = fmt::format("db.{}.updateMany({}, {})", collection.collection, serialized_filter, serialized_update);

    auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
    auto ast = Mongo::parseMongoQuery(
        parser, mongo_dialect_query.data(), mongo_dialect_query.data() + mongo_dialect_query.size(), "", 10000, 10000, 10000);

    /// The Mongo dialect parses `updateMany` into a single `ALTER TABLE` command, which is not
    /// a statement on its own, so the statement around it is built here.
    String alter_command;
    {
        WriteBufferFromString buffer(alter_command);
        auto settings = IAST::FormatSettings(true, IdentifierQuotingRule::WhenNecessary, IdentifierQuotingStyle::Backticks);
        ast->format(buffer, settings);
    }

    executor->execute(fmt::format("ALTER TABLE {} {}", collection.getQualifiedName(), alter_command));

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", 0);
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

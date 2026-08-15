#include <Core/Mongo/DocumentCollectionShape.h>
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
extern const int NOT_IMPLEMENTED;
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

bool isOrderedUpdate(const Document & command)
{
    auto json = command.getRapidJSONRepresentation();
    auto ordered = json.FindMember("ordered");
    if (ordered == json.MemberEnd())
        return true;
    if (!ordered->value.IsBool())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'ordered' option of an 'update' command must be a boolean");
    return ordered->value.GetBool();
}

void appendCount(bson_t * document, const char * name, Int64 count)
{
    if (count <= INT32_MAX)
        BSON_APPEND_INT32(document, name, static_cast<Int32>(count));
    else
        BSON_APPEND_INT64(document, name, count);
}

}

std::vector<Document> UpdateHandler::handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    if (sections.size() < 2 || sections[1].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'update' command does not contain any update statement");

    auto collection = getCollectionRef(sections[0].documents[0], "update");
    const bool ordered = isOrderedUpdate(sections[0].documents[0]);

    /** An update of a collection of documents changes the paths of the document column rather than
      * the columns of a row, so it is a rewrite of the document rather than an assignment per field.
      * Until that is translated, such an update is refused: writing the fields as columns would
      * write columns the collection does not have.
      */
    if (getCollectionShape(collection, executor).stores_documents)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "An 'update' of the collection '{}.{}', which keeps whole documents, is not supported yet: it changes the paths of a "
            "document rather than the columns of a row",
            collection.database,
            collection.collection);

    /// The 'update' command carries one or more update specs, each with its own 'q', 'u',
    /// 'multi', and 'upsert'. Execute every spec; 'multi: false' (updateOne) cannot be
    /// expressed as a ClickHouse mutation over an unordered table and 'upsert' has no
    /// counterpart either, so both are rejected instead of being silently widened into
    /// updateMany or dropped.
    /// Each spec is translated before it is run, so a malformed update is still an error for a
    /// collection that does not exist.
    Int64 matched = 0;
    bson_t * bson_doc = bson_new();
    bson_t write_errors;
    bool has_write_errors = false;
    size_t error_count = 0;

    for (size_t update_index = 0; update_index < sections[1].documents.size(); ++update_index)
    {
        try
        {
            const auto & update_spec = sections[1].documents[update_index];
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

            auto mongo_dialect_query = fmt::format("db.{}.updateMany({}, {})", collection.collection, serialized_filter, serialized_update);

            const auto max_query_size = mongo_dialect_query.size();
            auto parser = Mongo::ParserMongoQuery(max_query_size, 10000, 10000);
            auto ast = Mongo::parseMongoQuery(
                parser,
                mongo_dialect_query.data(),
                mongo_dialect_query.data() + mongo_dialect_query.size(),
                "",
                max_query_size,
                10000,
                10000,
                collection.database);

            String alter_query;
            {
                WriteBufferFromString buffer(alter_query);
                auto settings = IAST::FormatSettings(true, IdentifierQuotingRule::WhenNecessary, IdentifierQuotingStyle::Backticks);
                ast->format(buffer, settings);
            }

            /// An update of a collection that does not exist matches no document, which Mongo reports as
            /// an update of zero documents rather than an error.
            if (objectExists(executor, "TABLE", collection.getQualifiedName()))
            {
                auto count_query = fmt::format("db.{}.find({})", collection.collection, serialized_filter);
                auto count_parser = Mongo::ParserMongoQuery(count_query.size(), 10000, 10000);
                auto count_ast = Mongo::parseMongoQuery(
                    count_parser,
                    count_query.data(),
                    count_query.data() + count_query.size(),
                    "",
                    count_query.size(),
                    10000,
                    10000,
                    collection.database);
                String sql_count_query;
                WriteBufferFromString count_buffer(sql_count_query);
                count_ast->format(count_buffer, IAST::FormatSettings(true));
                matched += std::stoll(executor->execute(fmt::format("SELECT count() FROM ({}) FORMAT TSV", sql_count_query)));
                executor->execute(alter_query);
            }
        }
        catch (const Exception & e)
        {
            if (!has_write_errors)
            {
                bson_append_array_begin(bson_doc, "writeErrors", -1, &write_errors);
                has_write_errors = true;
            }
            bson_t write_error;
            const auto error_key = std::to_string(error_count++);
            bson_append_document_begin(&write_errors, error_key.data(), static_cast<int>(error_key.size()), &write_error);
            BSON_APPEND_INT32(&write_error, "index", static_cast<Int32>(update_index));
            BSON_APPEND_INT32(&write_error, "code", e.code());
            BSON_APPEND_UTF8(&write_error, "errmsg", e.message().c_str());
            bson_append_document_end(&write_errors, &write_error);
            if (ordered)
                break;
        }
    }

    if (has_write_errors)
        bson_append_array_end(bson_doc, &write_errors);
    /// A mutation is asynchronous, so its modified count is not known at acknowledgement time.
    appendCount(bson_doc, "n", matched);
    if (matched == 0)
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

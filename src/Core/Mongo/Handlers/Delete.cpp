#include <Core/Mongo/Document.h>
#include <Core/Mongo/DocumentCollectionShape.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Delete.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <bson/bson.h>
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

bool isOrderedDelete(const Document & command)
{
    auto json = command.getRapidJSONRepresentation();
    auto ordered = json.FindMember("ordered");
    if (ordered == json.MemberEnd())
        return true;
    if (!ordered->value.IsBool())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'ordered' option of a 'delete' command must be a boolean");
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

std::vector<Document> DeleteHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(documents[0].documents[0], "delete");

    /// A field of the command that would change what the write does or promises - a `maxTimeMS`
    /// bound, a `let` of variables the statements would read - must be refused rather than
    /// acknowledged and ignored, the same way the read commands refuse theirs. It is checked
    /// before the statements are, so a command that both asks for an option that is not
    /// implemented and carries no statements is refused for the stronger reason.
    static const std::unordered_set<String> supported_command_fields{"deletes", "ordered"};
    rejectUnsupportedCommandFields(documents[0].documents[0].getRapidJSONRepresentation(), supported_command_fields, "delete");

    validateWriteConcern(documents[0].documents[0].getRapidJSONRepresentation(), "delete");
    const bool ordered = isOrderedDelete(documents[0].documents[0]);

    if (documents.size() < 2 || documents[1].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'delete' command does not contain any filter");

    /** A `collation` changes which documents a filter matches, so a statement that asks for one is
      * refused rather than deleting by a different comparison. A `hint` only names an index to read
      * by, which changes nothing about what is deleted. The statements are checked before any of
      * them runs: an option that is not implemented is a fault of the command rather than a write
      * error of one statement, and no part of such a command is executed.
      */
    for (const auto & delete_spec : documents[1].documents)
    {
        static const std::unordered_set<String> supported_fields{"q", "limit", "hint", "comment"};
        rejectUnsupportedFields(delete_spec.getRapidJSONRepresentation(), supported_fields, "delete statement", "delete");
    }

    /// The 'delete' command carries one or more delete specs, each with its own 'q' filter
    /// and 'limit'. Execute every spec; 'limit: 1' (deleteOne) cannot be expressed as a
    /// ClickHouse mutation over an unordered table, so it is rejected instead of being
    /// silently widened into deleteMany.
    /// Each spec is translated before it is run, so a malformed filter is still an error for a
    /// collection that does not exist.
    bson_t * bson_doc = bson_new();
    bson_t write_errors;
    bool has_write_errors = false;
    size_t error_count = 0;
    for (size_t delete_index = 0; delete_index < documents[1].documents.size(); ++delete_index)
    {
        try
        {
            const auto & delete_spec = documents[1].documents[delete_index];
            String serialized_filter;
            {
                auto json_representation = delete_spec.getRapidJSONRepresentation();
                auto filter_it = json_representation.FindMember("q");
                if (filter_it == json_representation.MemberEnd())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'delete' command does not contain the 'q' filter");

                auto limit_it = json_representation.FindMember("limit");
                if (limit_it != json_representation.MemberEnd() && !(limit_it->value.IsNumber() && limit_it->value.GetDouble() == 0))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The 'delete' command supports only 'limit: 0' (deleteMany); deleting a limited number of documents is not "
                        "supported");

                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                filter_it->value.Accept(writer);
                serialized_filter = buffer.GetString();
            }
            serialized_filter = modifyFilter(serialized_filter);

            auto mongo_dialect_query = fmt::format("db.{}.deleteMany({})", collection.collection, serialized_filter);

            const auto max_query_size = mongo_dialect_query.size();
            /// The reparse runs under the parser limits of the session, the same ones the Mongo
            /// dialect parses under, so that the wire endpoint accepts what the dialect accepts.
            const auto limits = executor->getParserLimits();
            auto parser = Mongo::ParserMongoQuery(max_query_size, limits.max_parser_depth, limits.max_parser_backtracks);
            auto ast = Mongo::parseMongoQuery(
                parser,
                mongo_dialect_query.data(),
                mongo_dialect_query.data() + mongo_dialect_query.size(),
                "",
                max_query_size,
                limits.max_parser_depth,
                limits.max_parser_backtracks,
                collection.database);

            /// A collection of documents addresses its fields as the paths of the document column.
            adaptQueryToCollectionShape(ast, collection, executor);

            String sql_query;
            {
                WriteBufferFromString sql_buffer(sql_query);
                ast->format(sql_buffer, IAST::FormatSettings(true));
            }

            /// A delete from a collection that does not exist matches no document, which Mongo reports as
            /// a delete of zero documents rather than an error.
            if (objectExists(executor, "TABLE", collection.getQualifiedName()))
            {
                executor->execute(sql_query);
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
            BSON_APPEND_INT32(&write_error, "index", static_cast<Int32>(delete_index));
            BSON_APPEND_INT32(&write_error, "code", e.code());
            BSON_APPEND_UTF8(&write_error, "errmsg", e.message().c_str());
            bson_append_document_end(&write_errors, &write_error);
            if (ordered)
                break;
        }
    }

    if (has_write_errors)
        bson_append_array_end(bson_doc, &write_errors);
    appendCount(bson_doc, "n", 0);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerDeleteHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<DeleteHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}

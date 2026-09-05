#include <Core/Mongo/Document.h>
#include <Core/Mongo/DocumentCollectionShape.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Insert.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Mongo/DocumentCollection.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/Utils.h>

#include <fmt/format.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <map>
#include <optional>
#include <unordered_set>
#include <vector>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace DB::MongoProtocol
{

namespace
{

std::optional<String> getSimpleTypeField(const rapidjson::Value & document)
{
    if (document.IsBool())
        return "bool";
    if (document.IsInt())
        return "int";
    if (document.IsInt64())
        return "Int64";
    if (document.IsFloat())
        return "float";
    if (document.IsDouble())
        return "double";
    if (document.IsString())
        return "String";
    return std::nullopt;
}

/** The Extended JSON wrapper conversion is shared with the dialect (`Parsers/Mongo/MongoConstants.h`),
  * so that a document written over the wire and the same document written by `insertOne` become
  * one and the same stored value.
  */
using Mongo::convertMongoExtendedJSONWrapper;
using Mongo::convertMongoExtendedJSONWrappersDeep;
using Mongo::isMongoExtendedJSONWrapper;

/// The widest of two decimal types when both are decimal, so that an array of `$numberDecimal`
/// values of different scales still becomes one decimal column: every value fits a scale that is
/// the maximum of the individual ones, only padded with zeros. Nothing when either type is not a
/// decimal - such a pair has no common type here.
std::optional<String> mergeDecimalTypes(const String & left, const String & right)
{
    UInt32 left_scale = 0;
    UInt32 right_scale = 0;
    ReadBufferFromMemory left_buffer(left.data(), left.size());
    ReadBufferFromMemory right_buffer(right.data(), right.size());
    if (!checkString("Decimal128(", left_buffer) || !tryReadText(left_scale, left_buffer) || !checkString(")", left_buffer)
        || !left_buffer.eof() || !checkString("Decimal128(", right_buffer) || !tryReadText(right_scale, right_buffer)
        || !checkString(")", right_buffer) || !right_buffer.eof())
        return std::nullopt;
    return fmt::format("Decimal128({})", std::max(left_scale, right_scale));
}

/** Converts the Extended JSON wrappers among the elements of an array. When every element is a
  * wrapper of one and the same type, that type is returned so the column becomes an array of it -
  * an array of `$date` values becomes `Array(DateTime64(3, 'UTC'))`; otherwise the elements keep
  * their converted serializations and the column type is inferred from them as usual.
  */
std::optional<String> convertWrappersInArray(
    const rapidjson::Value & array, const String & field_name, rapidjson::Value & out, rapidjson::Document::AllocatorType & allocator)
{
    std::optional<String> common_type;
    bool all_wrappers = true;

    for (const auto & element : array.GetArray())
    {
        if (isMongoExtendedJSONWrapper(element))
        {
            auto [type, value] = convertMongoExtendedJSONWrapper(element, field_name, allocator);
            if (!common_type)
                common_type = std::move(type);
            else if (*common_type != type)
            {
                if (auto merged = mergeDecimalTypes(*common_type, type))
                    common_type = std::move(merged);
                else
                    all_wrappers = false;
            }
            out.PushBack(value, allocator);
        }
        else
        {
            all_wrappers = false;
            rapidjson::Value converted = convertMongoExtendedJSONWrappersDeep(element, field_name, allocator);
            out.PushBack(converted, allocator);
        }
    }

    if (all_wrappers && common_type)
        return common_type;
    return std::nullopt;
}

/** Flattens a Mongo document into a JSON object whose member names are the ClickHouse column
  * names: nested documents become dot separated paths, and values of types
  * that do not map onto a column are skipped.
  *
  * Both the inferred schema and the inserted rows are derived from this flattened form, so a
  * document can never produce a value for a column that is not in the schema.
  */
void flattenDocument(
    const rapidjson::Value & document,
    const String & prefix,
    rapidjson::Value & out,
    rapidjson::Document::AllocatorType & allocator,
    std::map<String, String> & wrapper_types,
    bool drop_object_id)
{
    if (!document.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a document, got a scalar value");

    for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it)
    {
        String name = it->name.GetString();
        /// An endpoint-created collection keeps its object id in a dedicated column. An existing
        /// ClickHouse table can have an `_id` column of its own, which must receive this value.
        if (drop_object_id && prefix.empty() && name == "_id")
            continue;

        String full_name = prefix.empty() ? name : prefix + "." + name;

        if (isMongoExtendedJSONWrapper(it->value))
        {
            auto [type, value] = convertMongoExtendedJSONWrapper(it->value, full_name, allocator);
            wrapper_types[full_name] = std::move(type);
            rapidjson::Value key(full_name.c_str(), static_cast<rapidjson::SizeType>(full_name.size()), allocator);
            out.AddMember(key, value, allocator);
            continue;
        }

        if (it->value.IsObject())
        {
            flattenDocument(it->value, full_name, out, allocator, wrapper_types, drop_object_id);
            continue;
        }

        if (it->value.IsArray())
        {
            rapidjson::Value converted(rapidjson::kArrayType);
            if (auto element_type = convertWrappersInArray(it->value, full_name, converted, allocator))
                wrapper_types[full_name] = fmt::format("Array({})", *element_type);
            rapidjson::Value key(full_name.c_str(), static_cast<rapidjson::SizeType>(full_name.size()), allocator);
            out.AddMember(key, converted, allocator);
            continue;
        }

        /// An explicit `null` is a real Mongo value, not an omitted field, so it is kept and
        /// becomes a `Dynamic` column, which holds `NULL` natively.
        if (!it->value.IsNull() && !getSimpleTypeField(it->value).has_value())
            continue;

        rapidjson::Value key(full_name.c_str(), static_cast<rapidjson::SizeType>(full_name.size()), allocator);
        rapidjson::Value value;
        value.CopyFrom(it->value, allocator);
        out.AddMember(key, value, allocator);
    }
}

/** A Mongo object id, as a Mongo server assigns one to a document that arrives without it: 12 bytes
  * written as 24 hexadecimal digits, of which the first four bytes are the seconds of the epoch, so
  * that the ids of the documents of a collection grow with time and the primary key of the table
  * follows the order they were inserted in. Every driver generates one for each document it sends,
  * so this is only reached by a raw `insert` command.
  */
String generateObjectId()
{
    static std::atomic<UInt32> counter{0};

    const auto seconds = static_cast<UInt32>(time(nullptr));
    const auto random = static_cast<UInt64>(thread_local_rng());
    const auto sequence = counter.fetch_add(1, std::memory_order_relaxed);

    /// The seconds, then five bytes that only have to be unique, then a three byte counter.
    return fmt::format("{:08x}{:010x}{:06x}", seconds, random & 0xFFFFFFFFFFULL, sequence & 0xFFFFFFULL);
}

/// The object id of a document, as the text of the `_id` column: the hexadecimal digits of an
/// `ObjectId`, or the value itself when a document names its own id, which Mongo allows.
String extractObjectId(const rapidjson::Value & document)
{
    auto it = document.FindMember("_id");
    if (it == document.MemberEnd())
        return generateObjectId();

    if (it->value.IsString())
        return {it->value.GetString(), it->value.GetStringLength()};

    if (isMongoExtendedJSONWrapper(it->value))
    {
        const auto & member = *it->value.MemberBegin();
        if (std::string_view(member.name.GetString()) == "$oid" && member.value.IsString())
            return {member.value.GetString(), member.value.GetStringLength()};
    }

    if (it->value.IsBool())
        return it->value.GetBool() ? "true" : "false";

    if (it->value.IsNumber())
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        it->value.Accept(writer);
        return {buffer.GetString(), buffer.GetSize()};
    }

    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "An '_id' that is a document or an array is not supported, only a scalar and an 'ObjectId' are");
}

/// The document to store, which is the one that arrived without its object id - that is a column of
/// its own - and with the Extended JSON wrappers of the values converted the way a column holds them.
rapidjson::Value documentToStore(const rapidjson::Value & document, rapidjson::Document::AllocatorType & allocator)
{
    if (!document.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An inserted document must be a document");

    rapidjson::Value stored(rapidjson::kObjectType);
    for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it)
    {
        std::string_view name(it->name.GetString(), it->name.GetStringLength());
        if (name == "_id")
            continue;
        if (name.starts_with("$"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of an inserted document must not start with '$', got '{}'", name);

        rapidjson::Value key;
        key.CopyFrom(it->name, allocator);
        rapidjson::Value value = convertMongoExtendedJSONWrappersDeep(it->value, String(name), allocator);
        stored.AddMember(key, value, allocator);
    }
    return stored;
}

bool isOrderedInsert(const Document & command)
{
    auto json = command.getRapidJSONRepresentation();
    auto ordered = json.FindMember("ordered");
    if (ordered == json.MemberEnd())
        return true;
    if (!ordered->value.IsBool())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'ordered' option of an 'insert' command must be a boolean");
    return ordered->value.GetBool();
}

}

void InsertHandler::createDatabase(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    executor->execute(fmt::format("CREATE DATABASE IF NOT EXISTS {}", backQuoteIfNeed(collection.database)));
}

void InsertHandler::createCollection(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    /** A Mongo collection has no schema, so a document goes into one `JSON` column: a later document
      * may hold a field that no document before it had, and whether a document holds a field at all
      * is a question about the document rather than about the table. The object id is a column of
      * its own and the primary key of the table, so that a read by `_id` - which is how a driver
      * addresses a document it inserted - reads by the key.
      */
    executor->execute(
        fmt::format(
            "CREATE TABLE IF NOT EXISTS {} ({} String, {} JSON) ENGINE = MergeTree ORDER BY {} COMMENT {}",
            collection.getQualifiedName(),
            backQuoteIfNeed(String(Mongo::OBJECT_ID_COLUMN)),
            backQuoteIfNeed(String(Mongo::DOCUMENT_COLUMN)),
            backQuoteIfNeed(String(Mongo::OBJECT_ID_COLUMN)),
            quoteString(String(Mongo::DOCUMENT_COLLECTION_COMMENT))));
}

std::vector<Document> InsertHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(documents[0].documents[0], "insert");

    /// A field of the command that would change what the write does or promises - a `maxTimeMS`
    /// bound, a `commitQuorum` - must be refused rather than acknowledged and ignored, the same
    /// way the read commands refuse theirs.
    static const std::unordered_set<String> supported_fields{"documents", "ordered"};
    rejectUnsupportedCommandFields(documents[0].documents[0].getRapidJSONRepresentation(), supported_fields, "insert");

    validateWriteConcern(documents[0].documents[0].getRapidJSONRepresentation(), "insert");
    const bool ordered = isOrderedInsert(documents[0].documents[0]);

    /// The documents to insert are sent in the sections that follow the command itself.
    std::vector<const Document *> to_insert;
    for (size_t section_id = 1; section_id < documents.size(); ++section_id)
        for (const auto & doc : documents[section_id].documents)
            to_insert.push_back(&doc);

    if (to_insert.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'insert' command does not contain any document");

    /** A collection this endpoint creates keeps whole documents; a table that was created in
      * ClickHouse keeps its own columns, and a document is written into them as a row, so that an
      * application can be pointed at a table that already holds the data.
      *
      * The collection is created after the documents have been converted, so that an `insert` of a
      * document this endpoint cannot write leaves nothing behind: a collection that only the failed
      * command created would be an empty collection MongoDB never has.
      */
    bool exists = objectExists(executor, "TABLE", collection.getQualifiedName());
    CollectionShape shape;
    if (exists)
        shape = getCollectionShape(collection, executor);
    else
    {
        shape.stores_documents = true;
        shape.has_object_id = true;
    }

    /// Values never go into the query text: the rows are passed as `JSONEachRow` data, which
    /// rapidjson escapes, so no value can change the meaning of the query.
    rapidjson::Document allocator_owner;
    auto & allocator = allocator_owner.GetAllocator();

    /// The object ids of this command, which addresses one document by each of them.
    std::unordered_set<String> object_ids;
    size_t inserted = 0;
    bson_t * bson_doc = bson_new();
    bson_t write_errors;
    bool has_write_errors = false;
    size_t error_count = 0;

    for (size_t document_index = 0; document_index < to_insert.size(); ++document_index)
    {
        try
        {
            const auto & document = to_insert[document_index]->getRapidJSONRepresentation();

            rapidjson::Value row(rapidjson::kObjectType);
            String object_id;
            if (shape.stores_documents)
            {
                /// The document as it arrived, next to the object id that addresses it.
                object_id = extractObjectId(document);
                if (object_ids.contains(object_id))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The 'insert' command holds more than one document with the object id '{}', and an object id addresses one "
                        "document",
                        object_id);
                rapidjson::Value id;
                id.SetString(object_id.c_str(), static_cast<rapidjson::SizeType>(object_id.size()), allocator);
                row.AddMember(
                    rapidjson::Value(rapidjson::StringRef(Mongo::OBJECT_ID_COLUMN.data(), Mongo::OBJECT_ID_COLUMN.size())), id, allocator);
                row.AddMember(
                    rapidjson::Value(rapidjson::StringRef(Mongo::DOCUMENT_COLUMN.data(), Mongo::DOCUMENT_COLUMN.size())),
                    documentToStore(document, allocator),
                    allocator);
            }
            else
            {
                /// The columns of the table the document names. Unknown fields are rejected instead of
                /// being silently dropped, and a column a document has no field for keeps its default.
                /// The object id a client generates is one of them: it is written when the table has
                /// an `_id` column of its own and dropped when it has none, because it names nothing
                /// there - it is not a field the document was written with.
                std::map<String, String> wrapper_types;
                flattenDocument(document, "", row, allocator, wrapper_types, /* drop_object_id = */ !shape.has_object_id);
            }

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            row.Accept(writer);

            if (!exists)
            {
                createDatabase(collection, executor);
                createCollection(collection, executor);
                exists = true;
            }

            /// An ordered Mongo batch makes every successfully inserted prefix visible before a
            /// later document fails. Execute one row at a time so a ClickHouse `INSERT` rollback
            /// cannot erase that prefix.
            executor->execute(
                fmt::format(
                    "INSERT INTO {} SETTINGS input_format_skip_unknown_fields = 0 FORMAT JSONEachRow\n{}\n",
                    collection.getQualifiedName(),
                    buffer.GetString()));
            ++inserted;

            /// An object id is taken only by a document that was written: an unordered batch goes
            /// on after a document that failed, and the id of that document is free for a later
            /// one - nothing addresses a document that does not exist.
            if (shape.stores_documents)
                object_ids.insert(std::move(object_id));
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
            BSON_APPEND_INT32(&write_error, "index", static_cast<Int32>(document_index));
            BSON_APPEND_INT32(&write_error, "code", e.code());
            BSON_APPEND_UTF8(&write_error, "errmsg", e.message().c_str());
            bson_append_document_end(&write_errors, &write_error);

            if (ordered)
                break;
        }
    }

    if (has_write_errors)
        bson_append_array_end(bson_doc, &write_errors);

    BSON_APPEND_INT32(bson_doc, "n", static_cast<int32_t>(inserted));
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerInsertHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<InsertHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}

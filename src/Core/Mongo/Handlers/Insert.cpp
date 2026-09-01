#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Insert.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/Utils.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <fmt/format.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <map>
#include <optional>
#include <vector>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
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
  * names: nested documents become dot separated paths, `_id` is dropped, and values of types
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
    std::map<String, String> & wrapper_types)
{
    if (!document.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a document, got a scalar value");

    for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it)
    {
        String name = it->name.GetString();
        /// The Mongo object id is generated by the client and has no ClickHouse counterpart.
        if (prefix.empty() && name == "_id")
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
            flattenDocument(it->value, full_name, out, allocator, wrapper_types);
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

/// Infers the column definitions from an already flattened document. A field that came from an
/// Extended JSON wrapper keeps the type of the wrapper rather than the type of its serialization.
std::vector<InsertHandler::DocumentField> inferSchema(const rapidjson::Value & flattened, const std::map<String, String> & wrapper_types)
{
    std::vector<InsertHandler::DocumentField> fields;
    for (auto it = flattened.MemberBegin(); it != flattened.MemberEnd(); ++it)
    {
        if (auto wrapper_it = wrapper_types.find(it->name.GetString()); wrapper_it != wrapper_types.end())
        {
            fields.push_back(InsertHandler::DocumentField{.full_name = it->name.GetString(), .type = wrapper_it->second});
            continue;
        }

        if (auto simple_type = getSimpleTypeField(it->value))
        {
            fields.push_back(InsertHandler::DocumentField{.full_name = it->name.GetString(), .type = std::move(*simple_type)});
            continue;
        }

        /// A field whose first value is `null` tells nothing about the values to come, so the
        /// column is `Dynamic`: it accepts whatever they turn out to be, and holds the `null`
        /// itself, which a typed column would silently turn into its default.
        if (it->value.IsNull())
        {
            fields.push_back(InsertHandler::DocumentField{.full_name = it->name.GetString(), .type = "Dynamic"});
            continue;
        }

        /// The element type is inferred from all the elements, not only the first one:
        /// a homogeneous array of scalars keeps the scalar type, an array of nested
        /// documents becomes `Array(JSON)`, and everything else - an empty array or a
        /// heterogeneous one - becomes `Array(Dynamic)`, which accepts any element
        /// (`JSON` rejects scalar elements, so it cannot serve as the mixed fallback).
        const auto & array = it->value.GetArray();
        String element_type = "Dynamic";
        if (!array.Empty())
        {
            if (auto element_simple_type = getSimpleTypeField(array[0]))
            {
                bool homogeneous = true;
                for (rapidjson::SizeType i = 1; i < array.Size(); ++i)
                {
                    auto other_type = getSimpleTypeField(array[i]);
                    if (!other_type || *other_type != *element_simple_type)
                    {
                        homogeneous = false;
                        break;
                    }
                }
                if (homogeneous)
                    element_type = std::move(*element_simple_type);
            }
            else
            {
                bool all_objects = true;
                for (rapidjson::SizeType i = 0; i < array.Size(); ++i)
                {
                    if (!array[i].IsObject())
                    {
                        all_objects = false;
                        break;
                    }
                }
                if (all_objects)
                    element_type = "JSON";
            }
        }
        fields.push_back(
            InsertHandler::DocumentField{.full_name = it->name.GetString(), .type = fmt::format("Array({})", element_type)});
    }
    return fields;
}

/** Tells whether the collection is the placeholder table that `createCollection` leaves behind: a
  * single `JSON` column named `json`, because an explicitly created collection has no document to
  * infer a schema from. The first `insert` gives it the schema of the inserted document.
  *
  * That column layout is also a perfectly ordinary table a user may have created directly in
  * ClickHouse, so the layout alone does not authorize the rewrite: the table must additionally
  * carry the comment `createCollection` puts on the placeholder. A table without that marker is
  * used as it is, and an inserted document that does not fit it fails on the `INSERT` itself
  * rather than having its columns silently retyped.
  */
bool isPlaceholderCollection(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    auto answer = executor->execute(fmt::format(
        "SELECT (SELECT count() = 1 AND countIf(name = 'json' AND type = 'JSON') = 1 FROM system.columns "
        "WHERE database = {0} AND table = {1}) "
        "AND (SELECT countIf(comment = {2}) = 1 FROM system.tables WHERE database = {0} AND name = {1}) FORMAT TSV",
        quoteString(collection.database),
        quoteString(collection.collection),
        quoteString(PLACEHOLDER_COLLECTION_COMMENT)));
    return answer.starts_with('1');
}

}

void InsertHandler::createDatabase(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    executor->execute(fmt::format("CREATE DATABASE IF NOT EXISTS {}", backQuoteIfNeed(collection.database)));
}

void InsertHandler::createTable(
    const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor, const std::vector<DocumentField> & fields)
{
    if (fields.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Can not create the collection '{}.{}': the first inserted document has no fields that map onto columns",
            collection.database,
            collection.collection);

    if (isPlaceholderCollection(collection, executor))
    {
        /// The placeholder is given the schema of the first inserted document, so that a
        /// collection created explicitly ends up with the same columns as one created by the
        /// insert itself. The columns are altered rather than the table recreated: an `ALTER`
        /// of an empty table only rewrites the metadata, while a `DROP` would throw away a
        /// table that the user may have created for something else.
        auto count = executor->execute(fmt::format("SELECT count() FROM {} FORMAT TSV", collection.getQualifiedName()));
        if (!count.starts_with('0'))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The collection '{}.{}' keeps whole documents in a single 'json' column and is not empty, so a document cannot be "
                "inserted into it as a row of columns",
                collection.database,
                collection.collection);

        WriteBufferFromOwnString alter_query;
        alter_query << "ALTER TABLE " << collection.getQualifiedName() << " ";
        bool json_column_is_a_field = false;
        for (size_t i = 0; i < fields.size(); ++i)
        {
            if (i != 0)
                alter_query << ", ";
            /// A document whose own field is named `json` keeps the column and only changes its type.
            if (fields[i].full_name == "json")
            {
                json_column_is_a_field = true;
                alter_query << "MODIFY COLUMN `json` " << fields[i].type;
            }
            else
                alter_query << "ADD COLUMN " << backQuoteIfNeed(fields[i].full_name) << " " << fields[i].type;
        }
        if (!json_column_is_a_field)
            alter_query << ", DROP COLUMN `json`";

        executor->execute(alter_query.str());
        return;
    }

    WriteBufferFromOwnString query;
    query << "CREATE TABLE IF NOT EXISTS " << collection.getQualifiedName() << " (";
    for (size_t i = 0; i < fields.size(); ++i)
    {
        if (i != 0)
            query << ", ";
        query << backQuoteIfNeed(fields[i].full_name) << " " << fields[i].type;
    }
    /// A `Dynamic` or `JSON` column cannot be a sorting key, so the key is the first column of
    /// any other type, and a document with none of those gets no sorting key at all.
    const DocumentField * key_field = nullptr;
    for (const auto & field : fields)
    {
        if (!field.type.contains("Dynamic") && !field.type.contains("JSON"))
        {
            key_field = &field;
            break;
        }
    }
    query << ") ENGINE = MergeTree ORDER BY " << (key_field ? backQuoteIfNeed(key_field->full_name) : "tuple()");

    executor->execute(query.str());
}

std::vector<Document> InsertHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(documents[0].documents[0], "insert");

    /// The documents to insert are sent in the sections that follow the command itself.
    std::vector<const Document *> to_insert;
    for (size_t section_id = 1; section_id < documents.size(); ++section_id)
        for (const auto & doc : documents[section_id].documents)
            to_insert.push_back(&doc);

    if (to_insert.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'insert' command does not contain any document");

    /// Values never go into the query text: the rows are passed as `JSONEachRow` data, which
    /// rapidjson escapes, so no value can change the meaning of the query. Unknown fields are
    /// rejected instead of being silently dropped, and fields that a document does not have
    /// get the default value of their column.
    rapidjson::Document allocator_owner;
    auto & allocator = allocator_owner.GetAllocator();

    WriteBufferFromOwnString data;
    std::vector<DocumentField> schema;

    for (const auto * doc : to_insert)
    {
        rapidjson::Value flattened(rapidjson::kObjectType);
        std::map<String, String> wrapper_types;
        flattenDocument(doc->getRapidJSONRepresentation(), "", flattened, allocator, wrapper_types);

        /// The schema comes from the first document only, as in Mongo a collection has no
        /// schema of its own.
        if (schema.empty())
        {
            schema = inferSchema(flattened, wrapper_types);
            createDatabase(collection, executor);
            createTable(collection, executor, schema);
        }

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        flattened.Accept(writer);
        data << buffer.GetString() << "\n";
    }

    executor->execute(fmt::format(
        "INSERT INTO {} SETTINGS input_format_skip_unknown_fields = 0 FORMAT JSONEachRow\n{}",
        collection.getQualifiedName(),
        data.str()));

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", static_cast<int32_t>(to_insert.size()));
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

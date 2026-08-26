#include <Core/Mongo/Handler.h>

#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <base/defines.h>
#include <Core/DecimalFunctions.h>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/IsMaster.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <bson/bson.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
}

namespace DB::MongoProtocol
{

std::vector<std::string> splitByNewline(const std::string & s)
{
    std::vector<std::string> result;
    ReadBufferFromString in(s);

    while (!in.eof())
    {
        String line;
        readStringUntilNewlineInto(line, in);
        if (!in.eof())
            in.ignore();
        result.push_back(std::move(line));
    }

    return result;
}

/// Tells whether the value of a filter field is a subdocument naming nested fields, as opposed to
/// the operators applied to the field it is the value of.
static bool isSubdocument(const rapidjson::Value & value)
{
    if (!value.IsObject() || value.ObjectEmpty())
        return false;

    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
    {
        std::string_view name = it->name.GetString();
        if (name.empty() || name.front() == '$')
            return false;
    }
    return true;
}

/// Replaces a subdocument by the fields it names: a nested field is the column of its dotted path,
/// so `{"profile" : {"name" : "x"}}` has to reach the parser as `{"profile.name" : "x"}`.
static void flattenSubdocument(
    rapidjson::Value & subdocument, const String & path, rapidjson::Value & out, rapidjson::Document::AllocatorType & allocator)
{
    for (auto it = subdocument.MemberBegin(); it != subdocument.MemberEnd(); ++it)
    {
        auto result_path = path + "." + it->name.GetString();
        if (isSubdocument(it->value))
        {
            flattenSubdocument(it->value, result_path, out, allocator);
            continue;
        }

        rapidjson::Value new_key(result_path.c_str(), allocator);
        out.AddMember(new_key, it->value, allocator);
    }
}

static void AddPrefixToKeys(
    rapidjson::Value & value, rapidjson::Document::AllocatorType & allocator, const String & current_path = "", bool in_projection = false)
{
    if (value.IsObject())
    {
        rapidjson::Value new_object(rapidjson::kObjectType);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
        {
            if (!in_projection)
            {
                std::string key = it->name.GetString();
                if (key.empty() || key[0] == '$')
                {
                    new_object.AddMember(it->name, it->value, allocator);
                }
                else
                {
                    auto result_path = current_path.empty() ? key : current_path + "." + key;
                    if (isSubdocument(it->value))
                    {
                        flattenSubdocument(it->value, result_path, new_object, allocator);
                    }
                    else
                    {
                        rapidjson::Value new_key(result_path.c_str(), allocator);
                        new_object.AddMember(new_key, it->value, allocator);
                    }
                }
            }
            else if (it->value.IsString())
            {
                std::string str_value = it->value.GetString();
                if (str_value.empty() || str_value[0] == '$')
                {
                    new_object.AddMember(it->name, it->value, allocator);
                }
                else
                {
                    auto result_path = current_path.empty() ? str_value : current_path + "." + str_value;
                    rapidjson::Value new_value(result_path.c_str(), allocator);
                    new_object.AddMember(it->name, new_value, allocator);
                }
            }
            else
            {
                new_object.AddMember(it->name, it->value, allocator);
            }
        }
        value = std::move(new_object);
    }

    if (value.IsObject())
    {
        for (auto & member : value.GetObject())
        {
            String name = member.name.GetString();
            if (name == "$projection")
                AddPrefixToKeys(member.value, allocator, current_path, true);
            else if (!current_path.empty())
                AddPrefixToKeys(member.value, allocator, current_path + "." + name, in_projection);
            else
                AddPrefixToKeys(member.value, allocator, name, in_projection);
        }
    }
    else if (value.IsArray())
    {
        for (auto & element : value.GetArray())
        {
            AddPrefixToKeys(element, allocator);
        }
    }
}

namespace
{

/** Appends an unsigned integer that does not fit a signed 64-bit one, which is the widest integer
  * BSON has. A `double` would lose its low digits, so the value goes as a BSON decimal128 - the
  * type a Mongo client spells as `$numberDecimal` - which holds 34 significant digits and
  * therefore every `UInt64` exactly.
  */
void appendLargeUInt64(bson_t * document, const String & key, UInt64 value)
{
    const int key_length = static_cast<int>(key.size());
    auto text = std::to_string(value);
    bson_decimal128_t decimal;
    if (bson_decimal128_from_string(text.c_str(), &decimal))
        bson_append_decimal128(document, key.data(), key_length, &decimal);
    else
        bson_append_utf8(document, key.data(), key_length, text.data(), static_cast<int>(text.size()));
}

/// Appends a JSON value whose column type says no more than the JSON itself: the shape of the
/// value is kept, and a number becomes the narrowest of `int32`/`int64`/`double` that holds it.
void appendUntypedValue(bson_t * document, const String & key, const rapidjson::Value & value)
{
    const int key_length = static_cast<int>(key.size());

    if (value.IsNull())
    {
        bson_append_null(document, key.data(), key_length);
    }
    else if (value.IsBool())
    {
        bson_append_bool(document, key.data(), key_length, value.GetBool());
    }
    else if (value.IsInt())
    {
        bson_append_int32(document, key.data(), key_length, value.GetInt());
    }
    else if (value.IsInt64())
    {
        bson_append_int64(document, key.data(), key_length, value.GetInt64());
    }
    else if (value.IsUint64())
    {
        /// BSON has no unsigned type, so what does not fit a signed 64-bit integer goes as a
        /// decimal128, which holds it exactly.
        UInt64 unsigned_value = value.GetUint64();
        if (unsigned_value <= static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            bson_append_int64(document, key.data(), key_length, static_cast<Int64>(unsigned_value));
        else
            appendLargeUInt64(document, key, unsigned_value);
    }
    else if (value.IsNumber())
    {
        bson_append_double(document, key.data(), key_length, value.GetDouble());
    }
    else if (value.IsString())
    {
        bson_append_utf8(document, key.data(), key_length, value.GetString(), static_cast<int>(value.GetStringLength()));
    }
    else if (value.IsObject())
    {
        bson_t child;
        bson_append_document_begin(document, key.data(), key_length, &child);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
            appendUntypedValue(&child, it->name.GetString(), it->value);
        bson_append_document_end(document, &child);
    }
    else if (value.IsArray())
    {
        bson_t child;
        bson_append_array_begin(document, key.data(), key_length, &child);
        size_t index = 0;
        for (const auto & element : value.GetArray())
            appendUntypedValue(&child, std::to_string(index++), element);
        bson_append_array_end(document, &child);
    }
}

/// The milliseconds since the Unix epoch of a `DateTime64` value, which is what a BSON date holds.
Int64 dateTime64ToMilliseconds(DateTime64 value, UInt32 scale)
{
    if (scale >= 3)
        return value.value / DecimalUtils::scaleMultiplier<Int64>(scale - 3);
    return value.value * DecimalUtils::scaleMultiplier<Int64>(3 - scale);
}

}

void appendTypedValue(bson_t * document, const String & key, const rapidjson::Value & value, const DataTypePtr & type)
{
    const int key_length = static_cast<int>(key.size());

    /// `Nullable` of anything.
    if (value.IsNull())
    {
        bson_append_null(document, key.data(), key_length);
        return;
    }

    /// `Bool` is a numeric type dressed up, so it is told by the value.
    if (value.IsBool())
    {
        bson_append_bool(document, key.data(), key_length, value.GetBool());
        return;
    }

    auto unwrapped = removeLowCardinalityAndNullable(type);
    WhichDataType which(unwrapped);

    if (which.isDateTime64() && value.IsString())
    {
        const auto & datetime_type = assert_cast<const DataTypeDateTime64 &>(*unwrapped);
        DateTime64 datetime(0);
        ReadBufferFromString in(std::string_view(value.GetString(), value.GetStringLength()));
        readDateTime64Text(datetime, datetime_type.getScale(), in, datetime_type.getTimeZone());
        bson_append_date_time(document, key.data(), key_length, dateTime64ToMilliseconds(datetime, datetime_type.getScale()));
        return;
    }
    if (which.isDateTime() && value.IsString())
    {
        const auto & datetime_type = assert_cast<const DataTypeDateTime &>(*unwrapped);
        time_t datetime = 0;
        ReadBufferFromString in(std::string_view(value.GetString(), value.GetStringLength()));
        readDateTimeText(datetime, in, datetime_type.getTimeZone());
        bson_append_date_time(document, key.data(), key_length, static_cast<Int64>(datetime) * 1000);
        return;
    }
    if (which.isDateOrDate32() && value.IsString())
    {
        /// A day number does not depend on a time zone, and BSON has no date without a time,
        /// so a `Date` is the UTC midnight of that day.
        ExtendedDayNum day;
        ReadBufferFromString in(std::string_view(value.GetString(), value.GetStringLength()));
        readDateText(day, in, DateLUT::instance("UTC"));
        bson_append_date_time(document, key.data(), key_length, static_cast<Int64>(day.toUnderType()) * 86400000);
        return;
    }
    if ((which.isInt8() || which.isInt16() || which.isInt32() || which.isUInt8() || which.isUInt16()) && value.IsInt())
    {
        bson_append_int32(document, key.data(), key_length, value.GetInt());
        return;
    }
    if ((which.isInt64() || which.isUInt32()) && value.IsInt64())
    {
        bson_append_int64(document, key.data(), key_length, value.GetInt64());
        return;
    }
    if (which.isUInt64() && value.IsUint64())
    {
        /// `output_format_json_quote_64bit_integers` is pinned to `false` (see `MongoProtocol.cpp`),
        /// so a `UInt64` arrives as a JSON number of up to 20 digits. The ones that fit a signed
        /// 64-bit integer keep the type every other integer column returns as; the larger ones
        /// would lose their low digits as a double, so they go as a decimal128.
        UInt64 unsigned_value = value.GetUint64();
        if (unsigned_value <= static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            bson_append_int64(document, key.data(), key_length, static_cast<Int64>(unsigned_value));
        else
            appendLargeUInt64(document, key, unsigned_value);
        return;
    }
    if (which.isFloat() && value.IsString())
    {
        /// `output_format_json_quote_denormals` is pinned to `true` (see `MongoProtocol.cpp`), so
        /// `NaN` and the infinities of a float column arrive as strings - the finite values arrive
        /// as JSON numbers - and BSON doubles can hold them.
        std::string_view text(value.GetString(), value.GetStringLength());
        std::optional<double> denormal;
        if (text == "nan")
            denormal = std::numeric_limits<double>::quiet_NaN();
        else if (text == "-nan")
            denormal = -std::numeric_limits<double>::quiet_NaN();
        else if (text == "inf")
            denormal = std::numeric_limits<double>::infinity();
        else if (text == "-inf")
            denormal = -std::numeric_limits<double>::infinity();
        if (denormal)
        {
            bson_append_double(document, key.data(), key_length, *denormal);
            return;
        }
    }
    if (which.isDecimal() && value.IsString())
    {
        /// `output_format_json_quote_decimals` is pinned to `true` (see `MongoProtocol.cpp`), so a
        /// decimal arrives as a string with all of its digits and becomes a BSON decimal128, the
        /// type a Mongo client sends as `$numberDecimal`. A value decimal128 cannot hold exactly -
        /// a wide enough `Decimal128` or `Decimal256` - stays a string, because
        /// `bson_decimal128_from_string` would silently round it to 34 significant digits.
        std::string_view text(value.GetString(), value.GetStringLength());
        size_t digits = 0;
        bool seen_nonzero = false;
        for (char c : text)
        {
            if (c < '0' || c > '9')
                continue;
            seen_nonzero |= c != '0';
            digits += seen_nonzero;
        }
        bson_decimal128_t decimal;
        if (digits <= 34 && bson_decimal128_from_string(value.GetString(), &decimal))
        {
            bson_append_decimal128(document, key.data(), key_length, &decimal);
            return;
        }
    }
    if (which.isArray() && value.IsArray())
    {
        const auto & element_type = assert_cast<const DataTypeArray &>(*unwrapped).getNestedType();
        bson_t child;
        bson_append_array_begin(document, key.data(), key_length, &child);
        size_t index = 0;
        for (const auto & element : value.GetArray())
            appendTypedValue(&child, std::to_string(index++), element, element_type);
        bson_append_array_end(document, &child);
        return;
    }
    if (which.isTuple())
    {
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*unwrapped);
        if (tuple_type.hasExplicitNames() && value.IsObject())
        {
            const auto & names = tuple_type.getElementNames();
            const auto & types = tuple_type.getElements();
            bson_t child;
            bson_append_document_begin(document, key.data(), key_length, &child);
            for (size_t i = 0; i < names.size(); ++i)
            {
                auto it = value.FindMember(names[i].c_str());
                if (it != value.MemberEnd())
                    appendTypedValue(&child, names[i], it->value, types[i]);
            }
            bson_append_document_end(document, &child);
            return;
        }
        if (!tuple_type.hasExplicitNames() && value.IsArray() && value.Size() == tuple_type.getElements().size())
        {
            const auto & types = tuple_type.getElements();
            bson_t child;
            bson_append_array_begin(document, key.data(), key_length, &child);
            for (size_t i = 0; i < types.size(); ++i)
                appendTypedValue(&child, std::to_string(i), value[static_cast<rapidjson::SizeType>(i)], types[i]);
            bson_append_array_end(document, &child);
            return;
        }
    }
    if (which.isMap() && value.IsObject())
    {
        const auto & value_type = assert_cast<const DataTypeMap &>(*unwrapped).getValueType();
        bson_t child;
        bson_append_document_begin(document, key.data(), key_length, &child);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
            appendTypedValue(&child, it->name.GetString(), it->value, value_type);
        bson_append_document_end(document, &child);
        return;
    }

    /// Everything else either already is what the JSON says (`String`, `Enum`, `UUID`, a wide
    /// integer that only fits a double), or carries no type information at all (`JSON`, `Dynamic`).
    appendUntypedValue(document, key, value);
}

namespace
{

/** The dotted column names of a result regrouped into the tree of the documents they name:
  * the columns `profile.name` and `profile.age` become one `profile` subtree with the leaves
  * `name` and `age`. The insertion order - the order of the select list - is kept.
  */
struct FieldTree
{
    struct Entry
    {
        String name;
        /// The index of the column when the entry is a leaf, unused otherwise.
        size_t column = 0;
        std::unique_ptr<FieldTree> subtree;
    };
    std::vector<Entry> entries;

    Entry * find(std::string_view name)
    {
        for (auto & entry : entries)
            if (entry.name == name)
                return &entry;
        return nullptr;
    }
};

/// Returns false when the path cannot be added because it conflicts with an existing entry,
/// e.g. the columns `a` and `a.b` in one result.
bool insertColumnPath(FieldTree & tree, std::string_view remaining_path, size_t column)
{
    auto dot = remaining_path.find('.');
    String head(remaining_path.substr(0, dot));

    auto * entry = tree.find(head);
    if (dot == std::string_view::npos)
    {
        if (entry)
            return false;
        tree.entries.push_back({.name = std::move(head), .column = column, .subtree = nullptr});
        return true;
    }

    if (!entry)
    {
        tree.entries.push_back({.name = std::move(head), .column = 0, .subtree = std::make_unique<FieldTree>()});
        entry = &tree.entries.back();
    }
    else if (!entry->subtree)
        return false;

    return insertColumnPath(*entry->subtree, remaining_path.substr(dot + 1), column);
}

void appendFieldTree(
    bson_t * document,
    const FieldTree & tree,
    const rapidjson::Value & row,
    const std::vector<std::pair<String, DataTypePtr>> & columns)
{
    for (const auto & entry : tree.entries)
    {
        if (!entry.subtree)
        {
            const auto & [column_name, column_type] = columns[entry.column];
            auto it = row.FindMember(column_name.c_str());
            if (it != row.MemberEnd())
                appendTypedValue(document, entry.name, it->value, column_type);
        }
        else
        {
            bson_t child;
            bson_append_document_begin(document, entry.name.data(), static_cast<int>(entry.name.size()), &child);
            appendFieldTree(&child, *entry.subtree, row, columns);
            bson_append_document_end(document, &child);
        }
    }
}

}

std::vector<std::pair<String, DataTypePtr>> extractResultColumns(const rapidjson::Document & result_json)
{
    auto meta_it = result_json.FindMember("meta");
    if (meta_it == result_json.MemberEnd() || !meta_it->value.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no column list");

    std::vector<std::pair<String, DataTypePtr>> columns;
    for (const auto & column : meta_it->value.GetArray())
    {
        if (!column.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The column list of the result is malformed");
        auto name_it = column.FindMember("name");
        auto type_it = column.FindMember("type");
        if (name_it == column.MemberEnd() || !name_it->value.IsString() || type_it == column.MemberEnd() || !type_it->value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The column list of the result is malformed");
        columns.emplace_back(name_it->value.GetString(), DataTypeFactory::instance().get(type_it->value.GetString()));
    }
    return columns;
}

namespace
{

/// The full reply document around the rows of a cursor. It is also built with no rows at all to
/// measure the envelope, so the size bound of `executeSelectIntoCursor` is checked against the
/// document that is actually sent on the wire.
Document buildCursorReply(const std::vector<Document> & selected, const CollectionRef & collection)
{
    /// `bson_append_array_begin` turns `first_batch` into a writer into `cursor` and
    /// `bson_append_array_end` finishes it, so it must not be allocated separately.
    bson_t cursor;
    bson_init(&cursor);

    {
        static constexpr std::string_view key_identifier = "firstBatch";
        bson_t first_batch;
        bson_append_array_begin(&cursor, key_identifier.data(), static_cast<int>(key_identifier.size()), &first_batch);
        for (size_t i = 0; i < selected.size(); ++i)
        {
            auto key_str = std::to_string(i);
            bson_append_document(&first_batch, key_str.c_str(), static_cast<int>(key_str.size()), selected[i].getBson());
        }
        bson_append_array_end(&cursor, &first_batch);
    }
    BSON_APPEND_INT64(&cursor, "id", 0);
    String namespace_name = collection.database + "." + collection.collection;
    BSON_APPEND_UTF8(&cursor, "ns", namespace_name.c_str());

    bson_t * result_doc = bson_new();
    BSON_APPEND_DOCUMENT(result_doc, "cursor", &cursor);
    BSON_APPEND_DOUBLE(result_doc, "ok", 1.0);
    bson_destroy(&cursor);
    return Document(result_doc);
}

}

std::vector<Document>
executeSelectIntoCursor(const String & sql_query, const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    /// The reply is one BSON document holding the whole result: the cursor id is always 0,
    /// so there is no `getMore` to continue from, and a result that does not fit into the
    /// `maxBsonObjectSize` advertised by `isMaster` must be rejected rather than sent as an
    /// oversized reply the driver would refuse to read. The bound holds for the document sent
    /// on the wire: besides the row documents themselves, every row costs an element header in
    /// the `firstBatch` array - the type byte and the decimal index as a NUL-terminated key -
    /// and the envelope around the batch is measured by building the reply with no rows. The
    /// bound is checked while the rows are collected, so an oversized result is dropped before
    /// it is held whole in memory.
    size_t reply_size = buildCursorReply({}, collection).getBson()->len;

    std::vector<Document> selected;
    {
        auto output = executor->execute(sql_query);

        rapidjson::Document result_json;
        if (result_json.Parse(output.data()).HasParseError())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not parse the result of the query");

        auto columns = extractResultColumns(result_json);

        FieldTree tree;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            /// A conflict - the columns `a` and `a.b` in one result - keeps the dotted name as
            /// the literal key it always was, rather than dropping the column.
            if (!insertColumnPath(tree, columns[i].first, i))
                tree.entries.push_back({.name = columns[i].first, .column = i, .subtree = nullptr});
        }

        auto data_it = result_json.FindMember("data");
        if (data_it == result_json.MemberEnd() || !data_it->value.IsArray())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no rows");

        for (const auto & json_data : data_it->value.GetArray())
        {
            if (!json_data.IsObject())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A row of the result is not a document");

            bson_t * row_document = bson_new();
            appendFieldTree(row_document, tree, json_data, columns);
            selected.emplace_back(row_document);

            reply_size += 2 + std::to_string(selected.size() - 1).size() + selected.back().getBson()->len;
            if (reply_size > MAX_BSON_OBJECT_SIZE)
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "The result is larger than the largest reply that can be sent ({} bytes). "
                    "Ask for less at a time, with a filter, a projection, 'limit' and 'skip'",
                    MAX_BSON_OBJECT_SIZE);
        }
    }

    auto reply = buildCursorReply(selected, collection);
    chassert(reply.getBson()->len == reply_size);

    std::vector<Document> result;
    result.emplace_back(std::move(reply));
    return result;
}

std::vector<Document> makeEmptyCursorReply(const CollectionRef & collection)
{
    std::vector<Document> result;
    result.emplace_back(buildCursorReply({}, collection));
    return result;
}

String modifyFilter(const String & json)
{
    rapidjson::Document doc;
    doc.Parse(json.c_str());

    if (doc.HasParseError())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect json in filter");
    }

    AddPrefixToKeys(doc, doc.GetAllocator());

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    String result = buffer.GetString();
    return result;
}

/// Rewrites the filter of every `$match` stage of the pipeline through the same normalization as
/// the filter of a `find`, including the pipelines nested inside a `$unionWith`.
static void normalizeMatchStages(rapidjson::Value & pipeline, rapidjson::Document::AllocatorType & allocator)
{
    if (!pipeline.IsArray())
        return;

    for (auto & stage : pipeline.GetArray())
    {
        if (!stage.IsObject())
            continue;

        if (auto match_it = stage.FindMember("$match"); match_it != stage.MemberEnd() && match_it->value.IsObject())
        {
            String serialized_match;
            {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                match_it->value.Accept(writer);
                serialized_match = buffer.GetString();
            }

            rapidjson::Document modified;
            if (modified.Parse(modifyFilter(serialized_match).c_str()).HasParseError())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not parse the normalized filter of a '$match' stage");
            match_it->value.CopyFrom(modified, allocator);
        }

        if (auto union_it = stage.FindMember("$unionWith"); union_it != stage.MemberEnd() && union_it->value.IsObject())
        {
            if (auto nested_it = union_it->value.FindMember("pipeline"); nested_it != union_it->value.MemberEnd())
                normalizeMatchStages(nested_it->value, allocator);
        }
    }
}

String serializePipeline(const rapidjson::Value & pipeline)
{
    /// The pipeline is copied so that only the `$match` filters are rewritten: everywhere else in
    /// a pipeline a nested document is a value, not a set of paths.
    rapidjson::Document normalized;
    auto & allocator = normalized.GetAllocator();
    normalized.CopyFrom(pipeline, allocator);

    normalizeMatchStages(normalized, allocator);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    normalized.Accept(writer);
    return buffer.GetString();
}

std::optional<Int64> getWholeNumberOption(const rapidjson::Value & json, const char * name, const char * command)
{
    auto it = json.FindMember(name);
    if (it == json.MemberEnd() || it->value.IsNull())
        return std::nullopt;

    if (!it->value.IsNumber())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' of a '{}' command must be a number", name, command);

    if (it->value.IsDouble())
    {
        const double value = it->value.GetDouble();
        /// The bound excludes 2^63 itself: it is representable as a double but not as an `Int64`.
        if (value != std::trunc(value) || value < -9223372036854775808.0 || value >= 9223372036854775808.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' of a '{}' command must be a whole number", name, command);
        return static_cast<Int64>(value);
    }

    if (it->value.IsUint64() && it->value.GetUint64() > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' of a '{}' command is too large", name, command);

    return it->value.GetInt64();
}

String CollectionRef::getQualifiedName() const
{
    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(collection);
}

CollectionRef getCollectionRef(const Document & command, const String & command_name)
{
    auto json = command.getRapidJSONRepresentation();

    auto collection_it = json.FindMember(command_name.c_str());
    if (collection_it == json.MemberEnd() || !collection_it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command '{}' does not contain a collection name", command_name);

    /// Every command sent over `OP_MSG` carries the database it applies to in `$db`.
    auto database_it = json.FindMember("$db");
    if (database_it == json.MemberEnd() || !database_it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command '{}' does not contain the '$db' database name", command_name);

    CollectionRef result{.database = database_it->value.GetString(), .collection = collection_it->value.GetString()};

    if (result.database.empty() || result.collection.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Empty Mongo database or collection name in the command '{}': '{}.{}'",
            command_name,
            result.database,
            result.collection);

    return result;
}

bool objectExists(std::shared_ptr<QueryExecutor> executor, const String & object_kind, const String & name)
{
    /// `EXISTS TABLE` also answers `0` when the database itself is absent.
    auto output = executor->execute(fmt::format("EXISTS {} {}", object_kind, name));
    return !output.empty() && output[0] == '1';
}

Int64 countMatchedRows(const String & select_query, std::shared_ptr<QueryExecutor> executor)
{
    auto output = executor->execute(fmt::format("SELECT count() FROM ({}) FORMAT TSV", select_query));

    /// A ClickHouse table is free to hold more rows than an `int32` can count.
    return std::stoll(output);
}

Header makeResponseHeader(Header request_header, Int32 message_size, Int32 response_id)
{
    Header result;
    result.message_length = message_size;
    result.operation_code = static_cast<Int32>(OperationCode::OP_REPLY);
    result.response_to = request_header.request_id;
    result.request_id = response_id;
    return result;
}

std::vector<Document> runMessageRequest(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    if (sections.empty() || sections[0].kind != 0 || sections[0].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo message does not start with a command document");

    auto keys = sections[0].documents[0].getDocumentKeys();
    if (keys.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command document is empty");

    const auto & command = keys[0];
    auto handler = HandlerRegitstry().getHandler(command);
    if (!handler)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command {} is not supported yet.", command);

    return handler->handle(sections, executor);
}

std::vector<Document> runQueryRequst(const std::vector<Document> & documents, std::shared_ptr<QueryExecutor> executor)
{
    if (documents.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An OP_QUERY command must contain exactly one command document");

    const auto keys = documents[0].getDocumentKeys();
    if (keys.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command document is empty");

    const auto & command = keys[0];
    if (command != "isMaster" && command != "ismaster" && command != "hello")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command {} is not supported over OP_QUERY", command);

    auto handler = IsMasterHandler();
    return handler.handle({}, executor);
}


namespace
{

/// Turns the current exception into the `{"errmsg": ..., "ok": 0}` document Mongo clients expect.
std::vector<Document> makeErrorResponse()
{
    bson_t * bson_doc = bson_new();

    BSON_APPEND_UTF8(bson_doc, "errmsg", getCurrentExceptionMessage(false).c_str());
    BSON_APPEND_DOUBLE(bson_doc, "ok", 0.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

}

void handle(
    const Header & header, ReadBuffer & payload, std::shared_ptr<MessageTransport> transport, std::shared_ptr<QueryExecutor> executor)
{
    auto op_code = static_cast<OperationCode>(header.operation_code);
    switch (op_code)
    {
        case OperationCode::OP_MSG: {
            OpMessage request;
            request.deserialize(payload);

            std::vector<Document> response_doc;
            try
            {
                response_doc = runMessageRequest(request.sections, executor);
            }
            catch (...)
            {
                tryLogCurrentException("MongoProtocol", "Failed to execute an OP_MSG command");
                response_doc = makeErrorResponse();
            }
            /// The reply carries no flags of its own. Echoing the flags of the request would
            /// promise the client a checksum we do not write, or a message we do not send.
            auto response = OpMessage(/* flags_= */ 0, /* kind_= */ 0, response_doc);
            auto response_header = makeResponseHeader(header, response.size(), transport->getNextResponseId());
            response_header.operation_code = static_cast<Int32>(OperationCode::OP_MSG);
            response.header = response_header;

            transport->send(response, true);
            break;
        }
        case OperationCode::OP_QUERY: {
            OpQuery request;
            request.deserialize(payload);

            std::vector<Document> response_doc;
            try
            {
                response_doc = runQueryRequst({request.query}, executor);
            }
            catch (...)
            {
                tryLogCurrentException("MongoProtocol", "Failed to execute an OP_QUERY command");
                response_doc = makeErrorResponse();
            }
            auto response = OpQuery(std::move(response_doc[0]));
            auto response_header = makeResponseHeader(header, response.size(), transport->getNextResponseId());
            response.header = response_header;

            transport->send(response, true);
            break;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not supported operation code {}", header.operation_code);
    }
}

}

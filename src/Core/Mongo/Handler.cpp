#include <Core/Mongo/Handler.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <base/defines.h>
#include <Core/DecimalFunctions.h>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/IsMaster.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/parseMongoQuery.h>
#include <DataTypes/DataTypeArray.h>
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
#include <Common/re2.h>

namespace DB::Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
extern const SettingsUInt64 max_query_size;
}

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
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

/** The text `<digits>E<exponent>` of the decimal number `text` - an optional sign, the digits and an
  * optional point - without its leading and trailing zeros, if at most 34 digits remain, which a
  * decimal128 holds exactly; `std::nullopt` otherwise.
  */
std::optional<String> significantDecimalDigits(std::string_view text)
{
    String sign;
    if (!text.empty() && (text.front() == '-' || text.front() == '+'))
    {
        if (text.front() == '-')
            sign = "-";
        text.remove_prefix(1);
    }

    String digits;
    Int64 exponent = 0;
    bool after_point = false;
    for (char c : text)
    {
        if (c == '.' && !after_point)
            after_point = true;
        else if (c >= '0' && c <= '9')
        {
            digits.push_back(c);
            exponent -= after_point;
        }
        else
            return std::nullopt;
    }

    while (!digits.empty() && digits.back() == '0')
    {
        digits.pop_back();
        ++exponent;
    }
    size_t first_nonzero = digits.find_first_not_of('0');
    if (first_nonzero == String::npos)
        return "0";
    digits.erase(0, first_nonzero);
    if (digits.size() > 34)
        return std::nullopt;
    return fmt::format("{}{}E{}", sign, digits, exponent);
}

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

/** The text of an integer of the result that does not fit a 64-bit integer, which `parseQueryResult`
  * keeps as a string behind `wide_integer_marker`; `std::nullopt` for any other value.
  */
std::optional<std::string_view> getWideIntegerText(const rapidjson::Value & value)
{
    if (!value.IsString())
        return std::nullopt;
    std::string_view text(value.GetString(), value.GetStringLength());
    if (!text.starts_with(wide_integer_marker))
        return std::nullopt;
    text.remove_prefix(wide_integer_marker.size());
    if (text.empty())
        return std::nullopt;
    for (size_t i = text.front() == '-'; i < text.size(); ++i)
        if (!isNumericASCII(text[i]))
            return std::nullopt;
    return text;
}

/** Appends an integer that does not fit a 64-bit integer, the value of a wide integer column such as
  * `UInt128`. It goes as a BSON decimal128 when its significant digits fit one, which holds it
  * exactly; a larger one stays a string with all of its digits, as a decimal that decimal128 cannot
  * hold does (see `appendTypedValue`), rather than a double that loses its low digits.
  */
void appendWideInteger(bson_t * document, const String & key, std::string_view text)
{
    const int key_length = static_cast<int>(key.size());
    if (auto significant = significantDecimalDigits(text))
    {
        bson_decimal128_t decimal;
        if (bson_decimal128_from_string(significant->c_str(), &decimal))
        {
            bson_append_decimal128(document, key.data(), key_length, &decimal);
            return;
        }
    }
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
    else if (auto wide_integer = getWideIntegerText(value))
    {
        /** Without a column type the text may also be a float - a `Float64` of a `JSON` column is
          * written as `100000000000000000000` - which never has more than 17 significant digits, the
          * most that the shortest text of a double has. Such a text stays the double it always was;
          * a longer one is the exact value of a wide integer.
          */
        auto significant = significantDecimalDigits(*wide_integer);
        if (significant && significant->find('E') <= 17 + significant->starts_with('-'))
            bson_append_double(document, key.data(), key_length, parseFromString<Float64>(*wide_integer));
        else
            appendWideInteger(document, key, *wide_integer);
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

    /// `date_time_output_format` is pinned to `iso` (see `MongoProtocol.cpp`), so a `DateTime` or a
    /// `DateTime64` arrives as its UTC time with the `Z` designator, e.g. `2021-11-07T05:30:00Z`,
    /// whatever the time zone of its column. It names one instant, which is read in UTC rather than
    /// in the time zone of the column - a column without an explicit time zone has the one of the
    /// query, which is gone by now.
    if (which.isDateTime64() && value.IsString())
    {
        const auto & datetime_type = assert_cast<const DataTypeDateTime64 &>(*unwrapped);
        DateTime64 datetime(0);
        ReadBufferFromString in(std::string_view(value.GetString(), value.GetStringLength()));
        readDateTime64Text(datetime, datetime_type.getScale(), in, DateLUT::instance("UTC"));
        assertChar('Z', in);
        assertEOF(in);
        bson_append_date_time(document, key.data(), key_length, dateTime64ToMilliseconds(datetime, datetime_type.getScale()));
        return;
    }
    if (which.isDateTime() && value.IsString())
    {
        time_t datetime = 0;
        ReadBufferFromString in(std::string_view(value.GetString(), value.GetStringLength()));
        readDateTimeText(datetime, in, DateLUT::instance("UTC"));
        assertChar('Z', in);
        assertEOF(in);
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
    if (auto wide_integer = getWideIntegerText(value))
    {
        /// An integer out of the range of a 64-bit one, see `parseQueryResult`: a float written
        /// without an exponent, such as `100000000000000000000`, or the value of a wide integer.
        if (which.isFloat())
            bson_append_double(document, key.data(), key_length, parseFromString<Float64>(*wide_integer));
        else
            appendWideInteger(document, key, *wide_integer);
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
    if (which.isFloat() && value.IsNumber())
    {
        /// A finite float arrives as the shortest JSON number that reads back as the same double,
        /// which may have no fraction, e.g. `1` or `1000000000000000100`. It stays a BSON double:
        /// it must neither turn into an integer nor be rounded to the digits of that text.
        bson_append_double(document, key.data(), key_length, value.GetDouble());
        return;
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
        /// The trailing zeros of a value - of a sum of `Decimal256`, or the scale an average is
        /// computed with (see `parseMongoAccumulator`) - are not significant digits: such a value
        /// is held exactly as its significant digits and an exponent.
        if (auto significant = significantDecimalDigits(text);
            significant && bson_decimal128_from_string(significant->c_str(), &decimal))
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

/// The maximum nesting depth of a document in MongoDB.
constexpr size_t max_result_nesting_depth = 100;

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

namespace
{

/** The SAX events of the result, forwarded to `document` - except a number, which the reader passes
  * as its text, see `parseQueryResult`.
  */
struct QueryResultHandler
{
    rapidjson::Document & document;
    rapidjson::Reader number_reader;
    std::string buffer;

    bool Null() { return document.Null(); }
    bool Bool(bool b) { return document.Bool(b); }
    bool Int(int i) { return document.Int(i); }
    bool Uint(unsigned i) { return document.Uint(i); }
    bool Int64(int64_t i) { return document.Int64(i); }
    bool Uint64(uint64_t i) { return document.Uint64(i); }
    bool Double(double d) { return document.Double(d); }
    bool String(const char * str, rapidjson::SizeType length, bool copy) { return document.String(str, length, copy); }
    bool StartObject() { return document.StartObject(); }
    bool Key(const char * str, rapidjson::SizeType length, bool copy) { return document.Key(str, length, copy); }
    bool EndObject(rapidjson::SizeType member_count) { return document.EndObject(member_count); }
    bool StartArray() { return document.StartArray(); }
    bool EndArray(rapidjson::SizeType element_count) { return document.EndArray(element_count); }

    bool RawNumber(const char * str, rapidjson::SizeType length, bool /* copy */)
    {
        std::string_view text(str, length);
        if (isWideInteger(text))
        {
            buffer.assign(wide_integer_marker);
            buffer.append(text);
            return document.String(buffer.data(), static_cast<rapidjson::SizeType>(buffer.size()), /* copy= */ true);
        }
        /// Any other number is read as the reader would have read it.
        buffer.assign(text);
        rapidjson::StringStream stream(buffer.c_str());
        return !number_reader.Parse<rapidjson::kParseFullPrecisionFlag>(stream, document).IsError();
    }

    /// Whether `text` is an integer that does not fit an `Int64` or a `UInt64`.
    static bool isWideInteger(std::string_view text)
    {
        const bool negative = text.starts_with('-');
        std::string_view digits = text.substr(negative);
        if (digits.empty() || !std::all_of(digits.begin(), digits.end(), isNumericASCII))
            return false;
        while (digits.size() > 1 && digits.front() == '0')
            digits.remove_prefix(1);
        const std::string_view limit = negative ? "9223372036854775808" : "18446744073709551615";
        return digits.size() > limit.size() || (digits.size() == limit.size() && digits > limit);
    }
};

}

void parseQueryResult(rapidjson::Document & result, const String & output)
{
    bool parsed = false;
    /// `Populate` makes the value the handler leaves on the stack of the document its root.
    auto generator = [&](rapidjson::Document & document)
    {
        QueryResultHandler handler{.document = document, .number_reader = {}, .buffer = {}};
        rapidjson::Reader reader;
        rapidjson::StringStream stream(output.c_str());
        parsed = !reader.Parse<rapidjson::kParseNumbersAsStringsFlag | rapidjson::kParseFullPrecisionFlag>(stream, handler).IsError();
        return parsed;
    };
    result.Populate(generator);
    if (!parsed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not parse the result of the query");
}

std::vector<Document> executeSelectIntoCursor(
    const String & sql_query,
    const CollectionRef & collection,
    std::shared_ptr<QueryExecutor> executor,
    const SettingsChanges & client_settings)
{
    /// The reply is one BSON document holding the whole result: the cursor id is always 0,
    /// so there is no `getMore` to continue from, and a result that does not fit into the
    /// `maxBsonObjectSize` advertised by `isMaster` must be rejected rather than sent as an
    /// oversized reply the driver would refuse to read. The bound holds for the document sent
    /// on the wire: besides the row documents themselves, every row costs an element header in
    /// the `firstBatch` array - the type byte and the decimal index as a NUL-terminated key -
    /// and the envelope around the batch is measured by building the reply with no rows. The
    /// bound is checked while the rows are collected, so an oversized result is dropped before
    /// its reply is held whole in memory; the text output of the query itself is bounded while
    /// the query runs (see `MAX_QUERY_OUTPUT_SIZE`).
    size_t reply_size = buildCursorReply({}, collection).getBson()->len;

    std::vector<Document> selected;
    {
        auto output = executor->execute(sql_query, {}, client_settings);

        rapidjson::Document result_json;
        parseQueryResult(result_json, output);

        auto columns = extractResultColumns(result_json);

        FieldTree tree;
        std::set<std::string_view, std::less<>> column_names;
        for (const auto & column : columns)
        {
            /// Every dot of a column name is a level of nested documents, built and serialized
            /// recursively. The depth is bounded as MongoDB bounds the nesting of a document.
            if (static_cast<size_t>(std::count(column.first.begin(), column.first.end(), '.')) >= max_result_nesting_depth)
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "The column '{}' of the result names a document nested deeper than {} levels",
                    column.first.substr(0, 100), max_result_nesting_depth);
            column_names.insert(column.first);
        }

        /** A conflict - the columns `a` and `a.b` in one result, where `a` would be both a value and
          * a document - keeps the names of all the columns involved as the literal keys they are,
          * whatever the order of the columns: `{"a": 1, "a.b": 2}`. Building the tree of the column
          * that comes first would either put the key `a` twice into the document or make the key of
          * the other column depend on the order. A column whose name repeats is the same value, the
          * one the row of the result holds under that name, and is sent once.
          */
        std::set<std::string_view> conflicting;
        for (const auto & name : column_names)
        {
            for (size_t dot = name.find('.'); dot != String::npos; dot = name.find('.', dot + 1))
            {
                std::string_view ancestor = name.substr(0, dot);
                if (column_names.contains(ancestor))
                {
                    conflicting.insert(ancestor);
                    conflicting.insert(name);
                }
            }
        }

        std::set<std::string_view> added;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & name = columns[i].first;
            if (!added.insert(name).second)
                continue;
            if (conflicting.contains(name) || !insertColumnPath(tree, name, i))
                tree.entries.push_back({.name = name, .column = i, .subtree = nullptr});
        }

        auto data_it = result_json.FindMember("data");
        if (data_it == result_json.MemberEnd() || !data_it->value.IsArray())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no rows");

        for (const auto & json_data : data_it->value.GetArray())
        {
            if (!json_data.IsObject())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A row of the result is not a document");

            BsonPtr row_document_owner(bson_new());
            bson_t * row_document = row_document_owner.get();
            appendFieldTree(row_document, tree, json_data, columns);
            selected.emplace_back(std::move(row_document_owner));

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
    doc.Parse<rapidjson::kParseFullPrecisionFlag>(json.c_str());

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
            if (modified.Parse<rapidjson::kParseFullPrecisionFlag>(modifyFilter(serialized_match).c_str()).HasParseError())
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

/// A batch larger than the `maxWriteBatchSize` we advertise is refused, as Mongo does: a driver
/// never sends one, and the `insert` handler relies on the bound to write a batch as one block.
static void checkWriteBatchSize(const std::vector<Document> & batch, const char * field_name, const char * command)
{
    if (batch.size() > MAX_WRITE_BATCH_SIZE)
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "The '{}' of a '{}' command has {} elements, while at most {} are allowed",
            field_name,
            command,
            batch.size(),
            MAX_WRITE_BATCH_SIZE);
}

std::vector<Document>
getWriteBatch(const std::vector<OpMessageSection> & sections, const char * field_name, const char * command)
{
    if (sections.empty() || sections[0].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' command does not contain a command document", command);

    std::vector<Document> batch;

    /// A document sequence is identified by the name of the field it stands for, so a section
    /// naming something else must not be read as this batch.
    for (size_t i = 1; i < sections.size(); ++i)
    {
        const auto & section = sections[i];
        if (section.kind != 1)
            continue;
        if (section.identifier != field_name)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The '{}' command carries a document sequence '{}', while it expects '{}'",
                command,
                section.identifier,
                field_name);
        for (const auto & document : section.documents)
            batch.push_back(document);
    }

    if (!batch.empty())
    {
        checkWriteBatchSize(batch, field_name, command);
        return batch;
    }

    /// Otherwise the batch is an array of the command body. Its elements are taken from the BSON
    /// directly, so that the types of their values survive: a conversion to JSON and back would
    /// lose, for instance, the width of an integer.
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, sections[0].documents[0].getBson(), field_name) || !BSON_ITER_HOLDS_ARRAY(&iter))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' command does not contain the '{}' array", command, field_name);

    bson_iter_t element;
    if (!bson_iter_recurse(&iter, &element))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' command of a malformed '{}' array", command, field_name);

    while (bson_iter_next(&element))
    {
        if (!BSON_ITER_HOLDS_DOCUMENT(&element))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Every element of the '{}' array of a '{}' command must be a document", field_name, command);

        const uint8_t * data = nullptr;
        uint32_t length = 0;
        bson_iter_document(&element, &length, &data);

        bson_t * document = bson_new_from_data(data, length);
        if (!document)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "A malformed document in the '{}' array of a '{}' command", field_name, command);
        batch.emplace_back(document);
    }

    if (batch.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' array of a '{}' command is empty", field_name, command);

    checkWriteBatchSize(batch, field_name, command);
    return batch;
}

void rejectUnorderedWriteBatch(const Document & command, const char * command_name)
{
    auto json = command.getRapidJSONRepresentation();
    if (!getBoolOption(json, "ordered", command_name).value_or(true))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The '{}' command supports only ordered batches; 'ordered: false' is not supported",
            command_name);
}

void rejectUnsupportedOptions(const rapidjson::Value & json, const char * command, std::initializer_list<std::string_view> options)
{
    for (const auto option : options)
    {
        auto it = json.FindMember(rapidjson::StringRef(option.data(), option.size()));
        if (it == json.MemberEnd() || it->value.IsNull())
            continue;

        if (option == "collation" && it->value.IsObject() && it->value.MemberCount() == 1)
        {
            auto locale_it = it->value.FindMember("locale");
            if (locale_it != it->value.MemberEnd() && locale_it->value.IsString()
                && std::string_view(locale_it->value.GetString()) == "simple")
                continue;
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The '{}' option of a '{}' command is not supported", option, command);
    }
}

namespace
{

void rejectUnsupportedWriteConcern(const rapidjson::Value & write_concern, const String & command)
{
    if (!write_concern.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'writeConcern' of a '{}' command must be a document", command);

    for (auto it = write_concern.MemberBegin(); it != write_concern.MemberEnd(); ++it)
    {
        const std::string_view name(it->name.GetString(), it->name.GetStringLength());
        const auto & value = it->value;
        if (value.IsNull())
            continue;

        if (name == "w")
        {
            if (value.IsNumber() && (value.GetDouble() == 0 || value.GetDouble() == 1))
                continue;
            if (value.IsString() && std::string_view(value.GetString(), value.GetStringLength()) == "majority")
                continue;
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The 'writeConcern' of a '{}' command supports only 'w' equal to 0, 1 or 'majority'",
                command);
        }

        if (name == "j" || name == "fsync")
        {
            /// A legacy driver may send the flag as a number.
            if ((value.IsBool() && !value.GetBool()) || (value.IsNumber() && value.GetDouble() == 0))
                continue;
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "The option '{}' of the 'writeConcern' of a '{}' command is not supported", name, command);
        }

        /// The time to wait for the acknowledgement of other members, of which there are none.
        if (name == "wtimeout" || name == "wtimeoutMS")
            continue;

        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "The option '{}' of the 'writeConcern' of a '{}' command is not supported", name, command);
    }
}

void rejectUnsupportedReadConcern(const rapidjson::Value & read_concern, const String & command)
{
    if (!read_concern.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'readConcern' of a '{}' command must be a document", command);

    for (auto it = read_concern.MemberBegin(); it != read_concern.MemberEnd(); ++it)
    {
        const std::string_view name(it->name.GetString(), it->name.GetStringLength());
        const auto & value = it->value;
        if (value.IsNull())
            continue;

        if (name == "level")
        {
            if (value.IsString())
            {
                const std::string_view level(value.GetString(), value.GetStringLength());
                if (level == "local" || level == "available" || level == "majority")
                    continue;
            }
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The 'readConcern' of a '{}' command supports only the levels 'local', 'available' and 'majority'",
                command);
        }

        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "The option '{}' of the 'readConcern' of a '{}' command is not supported", name, command);
    }
}

}

void rejectUnsupportedConcerns(const rapidjson::Value & json, const String & command)
{
    if (auto it = json.FindMember("writeConcern"); it != json.MemberEnd() && !it->value.IsNull())
        rejectUnsupportedWriteConcern(it->value, command);
    if (auto it = json.FindMember("readConcern"); it != json.MemberEnd() && !it->value.IsNull())
        rejectUnsupportedReadConcern(it->value, command);
}

void rejectTransaction(const rapidjson::Value & json, const String & command)
{
    /// Every command of a multi-document transaction carries `autocommit: false`, and the first one
    /// also `startTransaction: true`. Such a command must not run: its writes would be applied at
    /// once rather than on `commitTransaction`, and no `abortTransaction` could undo them. A
    /// retryable write outside of a transaction carries `lsid` and `txnNumber` alone, which ask for
    /// nothing a single server that applies a write before it replies does not do, so they are
    /// accepted.
    for (const char * field : {"startTransaction", "autocommit"})
    {
        if (json.HasMember(field))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The '{}' command carries '{}', i.e. it is a part of a multi-document transaction, and transactions are not supported",
                command,
                field);
    }
}

ASTPtr parseMongoDialectCommand(const String & mongo_dialect_query, const CollectionRef & collection, const QueryExecutor & executor)
{
    /// The limits of the authenticated user's profile, as for a query of the `clickhouse` dialect.
    const auto query_context = executor.getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    const size_t max_query_size = settings[Setting::max_query_size];
    const size_t max_parser_depth = settings[Setting::max_parser_depth];
    const size_t max_parser_backtracks = settings[Setting::max_parser_backtracks];

    Mongo::ParserMongoQuery parser(max_query_size, max_parser_depth, max_parser_backtracks);
    return Mongo::parseMongoQuery(
        parser,
        mongo_dialect_query.data(),
        mongo_dialect_query.data() + mongo_dialect_query.size(),
        "",
        max_query_size,
        max_parser_depth,
        max_parser_backtracks,
        collection.database,
        collection.collection);
}

String CollectionRef::getQualifiedName() const
{
    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(collection);
}

bool isValidMongoDatabaseName(const String & database)
{
    if (database.empty())
        return false;
    for (char symbol : database)
        if (!isWordCharASCII(symbol) && symbol != '-')
            return false;
    return true;
}

void validateMongoDatabaseName(const String & database, const String & command_name)
{
    if (database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty Mongo database name in the command '{}'", command_name);
    if (!isValidMongoDatabaseName(database))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Mongo database name '{}' in the command '{}'", database, command_name);
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

    /// The names keep their length: a BSON string may hold a null byte, and a name cut at it would
    /// address another, existing collection or database.
    CollectionRef result{
        .database = String(database_it->value.GetString(), database_it->value.GetStringLength()),
        .collection = String(collection_it->value.GetString(), collection_it->value.GetStringLength())};

    if (result.database.empty() || result.collection.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Empty Mongo database or collection name in the command '{}': '{}.{}'",
            command_name,
            result.database,
            result.collection);

    validateMongoDatabaseName(result.database, command_name);

    if (result.collection.contains('\0'))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Invalid Mongo collection name in the command '{}': it contains a null byte", command_name);

    return result;
}

bool objectExists(std::shared_ptr<QueryExecutor> executor, const String & object_kind, const String & name)
{
    /// `EXISTS TABLE` also answers `0` when the database itself is absent.
    auto output = executor->execute(fmt::format("EXISTS {} {}", object_kind, name));
    return !output.empty() && output[0] == '1';
}

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

bool collectionHasSchema(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    return objectExists(executor, "TABLE", collection.getQualifiedName()) && !isPlaceholderCollection(collection, executor);
}

std::optional<bool> getBoolOption(const rapidjson::Value & json, const char * name, const char * command)
{
    auto it = json.FindMember(name);
    if (it == json.MemberEnd() || it->value.IsNull())
        return std::nullopt;
    if (!it->value.IsBool())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '{}' of a '{}' command must be a boolean", name, command);
    return it->value.GetBool();
}

SettingsChanges getMaxTimeSettings(const rapidjson::Value & json, const char * command)
{
    auto max_time_ms = getWholeNumberOption(json, "maxTimeMS", command);
    if (!max_time_ms || *max_time_ms == 0)
        return {};
    if (*max_time_ms < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'maxTimeMS' of a '{}' command must not be negative", command);

    SettingsChanges settings;
    settings.emplace_back("max_execution_time", Field(static_cast<Float64>(*max_time_ms) / 1000));
    settings.emplace_back("timeout_overflow_mode", Field("throw"));
    return settings;
}

void rejectMaxTime(const rapidjson::Value & json, const char * command)
{
    if (!getMaxTimeSettings(json, command).empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The option 'maxTimeMS' of a '{}' command is not supported", command);
}

std::function<bool(const String &)> getNameFilter(const rapidjson::Value & command, const char * command_name)
{
    auto filter_it = command.FindMember("filter");
    if (filter_it == command.MemberEnd() || filter_it->value.IsNull())
        return [](const String &) { return true; };

    const auto & filter = filter_it->value;
    if (!filter.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'filter' of a '{}' command must be a document", command_name);
    if (filter.ObjectEmpty())
        return [](const String &) { return true; };

    /// The names are the only thing this server knows about the objects it lists, so `name` is
    /// the one field a filter can ask about; a filter on any other field would have to be
    /// answered with the full listing, i.e. wrongly.
    if (filter.MemberCount() != 1 || std::string_view(filter.MemberBegin()->name.GetString()) != "name")
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The 'filter' of a '{}' command supports only the 'name' field, got '{}'",
            command_name,
            filter.MemberBegin()->name.GetString());

    const auto & condition = filter.MemberBegin()->value;
    if (condition.IsString())
    {
        String expected = condition.GetString();
        return [expected](const String & name) { return name == expected; };
    }
    if (!condition.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'name' of the 'filter' of a '{}' command must be a string or a document", command_name);

    if (auto regex = Mongo::tryParseMongoRegularExpression(condition))
    {
        /// The regular expression comes with its `$options` at most; another operator next to it
        /// would be silently ignored otherwise.
        for (auto it = condition.MemberBegin(); it != condition.MemberEnd(); ++it)
        {
            std::string_view key = it->name.GetString();
            if (key != "$regex" && key != "$options" && key != "$regularExpression")
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "The 'name' of the 'filter' of a '{}' command cannot combine a regular expression with '{}'",
                    command_name,
                    key);
        }
        auto pattern = std::make_shared<re2::RE2>(*regex, re2::RE2::Quiet);
        if (!pattern->ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The regular expression of the 'filter' of a '{}' command is invalid: {}",
                command_name,
                pattern->error());
        return [pattern](const String & name) { return re2::RE2::PartialMatch(name, *pattern); };
    }

    if (condition.MemberCount() != 1)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The 'name' of the 'filter' of a '{}' command supports one of '$eq', '$in' or a regular expression",
            command_name);

    const auto & member = *condition.MemberBegin();
    std::string_view op = member.name.GetString();
    if (op == "$eq")
    {
        if (!member.value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '$eq' of the 'name' filter of a '{}' command must be a string", command_name);
        String expected = member.value.GetString();
        return [expected](const String & name) { return name == expected; };
    }
    if (op == "$in")
    {
        if (!member.value.IsArray())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '$in' of the 'name' filter of a '{}' command must be an array", command_name);
        std::set<String> expected;
        for (const auto & element : member.value.GetArray())
        {
            if (!element.IsString())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "The '$in' of the 'name' filter of a '{}' command must hold strings", command_name);
            expected.insert(element.GetString());
        }
        return [expected](const String & name) { return expected.contains(name); };
    }

    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The 'name' of the 'filter' of a '{}' command supports one of '$eq', '$in' or a regular expression, got '{}'",
        command_name,
        op);
}

Int64 countMatchedRows(const String & select_query, std::shared_ptr<QueryExecutor> executor)
{
    auto output = executor->execute(fmt::format("SELECT count() FROM ({}) FORMAT TSV", select_query));

    /// A ClickHouse table is free to hold more rows than an `int32` can count.
    return std::stoll(output);
}

SettingsChanges getMutationSettings()
{
    SettingsChanges changes;
    /// Waiting on this server is enough: the count of the next spec is taken by this same
    /// connection, i.e. on this server.
    changes.setSetting("mutations_sync", Field(UInt64(1)));
    /// A generated `DELETE FROM` lowered into an `ALTER TABLE ... UPDATE _row_exists = 0`
    /// replaces `mutations_sync` with `lightweight_deletes_sync`, so a profile that sets the
    /// latter to zero would otherwise make the delete asynchronous.
    changes.setSetting("lightweight_deletes_sync", Field(UInt64(1)));
    return changes;
}

const char * const PLACEHOLDER_COLLECTION_COMMENT = "Mongo collection created by `createCollection`, awaiting the schema of the first inserted document";

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

    /// Checked here rather than by each handler, so that no command, read or write, runs with a
    /// concern it does not honour, nor as a part of a transaction.
    const auto command_json = sections[0].documents[0].getRapidJSONRepresentation();
    rejectUnsupportedConcerns(command_json, command);
    rejectTransaction(command_json, command);

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
    BsonPtr bson_doc_owner(bson_new());
    bson_t * bson_doc = bson_doc_owner.get();

    BSON_APPEND_UTF8(bson_doc, "errmsg", getCurrentExceptionMessage(false).c_str());
    BSON_APPEND_DOUBLE(bson_doc, "ok", 0.0);

    std::vector<Document> result;
    result.emplace_back(std::move(bson_doc_owner));
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

            /// With `moreToCome` the client does not wait for a reply and may send the next
            /// message right away, so a reply would be read as the answer to a later request.
            if (request.flags & OpMessage::MORE_TO_COME)
                break;

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

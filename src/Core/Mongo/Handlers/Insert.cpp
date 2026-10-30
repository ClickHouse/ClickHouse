#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Insert.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/Utils.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <base/wide_integer_to_string.h>
#include <fmt/format.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <map>
#include <limits>
#include <optional>
#include <set>
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
    /// A BSON double is a `Float64`. `IsFloat` only says that the value is within the range of a
    /// `Float32`, not that a `Float32` holds it exactly, so it must not narrow the column.
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

/// The scale of the `Decimal128(<scale>)` type a `$numberDecimal` wrapper converts into, or nothing
/// for any other type.
std::optional<UInt32> getWrapperDecimalScale(const String & type)
{
    UInt32 scale = 0;
    ReadBufferFromMemory buffer(type.data(), type.size());
    if (!checkString("Decimal128(", buffer) || !tryReadText(scale, buffer) || !checkString(")", buffer) || !buffer.eof())
        return std::nullopt;
    return scale;
}

/// The widest of two decimal types when both are decimal, so that an array of `$numberDecimal`
/// values of different scales still becomes one decimal column: every value fits a scale that is
/// the maximum of the individual ones, only padded with zeros. Nothing when either type is not a
/// decimal - such a pair has no common type here.
std::optional<String> mergeDecimalTypes(const String & left, const String & right)
{
    auto left_scale = getWrapperDecimalScale(left);
    auto right_scale = getWrapperDecimalScale(right);
    if (!left_scale || !right_scale)
        return std::nullopt;
    return fmt::format("Decimal128({})", std::max(*left_scale, *right_scale));
}

/// Records that the field `name` holds a decimal of the scale of `type`, see `checkValuesFitColumns`.
void recordDecimalScale(const String & name, const String & type, std::map<String, UInt32> & decimal_scales)
{
    if (auto scale = getWrapperDecimalScale(type))
    {
        auto [it, inserted] = decimal_scales.emplace(name, *scale);
        if (!inserted)
            it->second = std::max(it->second, *scale);
    }
}

/** Converts the Extended JSON wrappers among the elements of an array. When every element is a
  * wrapper of one and the same type, that type is returned so the column becomes an array of it -
  * an array of `$date` values becomes `Array(DateTime64(3, 'UTC'))`; otherwise the elements keep
  * their converted serializations and the column type is inferred from them as usual.
  */
std::optional<String> convertWrappersInArray(
    const rapidjson::Value & array,
    const String & field_name,
    rapidjson::Value & out,
    rapidjson::Document::AllocatorType & allocator,
    std::map<String, UInt32> & decimal_scales)
{
    std::optional<String> common_type;
    bool all_wrappers = true;

    for (const auto & element : array.GetArray())
    {
        if (isMongoExtendedJSONWrapper(element))
        {
            auto [type, value] = convertMongoExtendedJSONWrapper(element, field_name, allocator);
            recordDecimalScale(field_name, type, decimal_scales);
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
  *
  * `decimal_scales` collects the largest scale of the `$numberDecimal` values of every field,
  * including the elements of an array, see `checkValuesFitColumns`.
  */
void flattenDocument(
    const rapidjson::Value & document,
    const String & prefix,
    rapidjson::Value & out,
    rapidjson::Document::AllocatorType & allocator,
    std::map<String, String> & wrapper_types,
    std::map<String, UInt32> & decimal_scales)
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
            recordDecimalScale(full_name, type, decimal_scales);
            wrapper_types[full_name] = std::move(type);
            rapidjson::Value key(full_name.c_str(), static_cast<rapidjson::SizeType>(full_name.size()), allocator);
            out.AddMember(key, value, allocator);
            continue;
        }

        if (it->value.IsObject())
        {
            /// A nested document is stored as a column per field, so an empty one has no column
            /// to go to. Flattening it into nothing would store the document as if the field were
            /// absent, so it is rejected before anything is written.
            if (it->value.MemberCount() == 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The field '{}' is an empty embedded document, which is not supported: a nested document is stored as one column "
                    "per field, so an empty one names no column",
                    full_name);
            flattenDocument(it->value, full_name, out, allocator, wrapper_types, decimal_scales);
            continue;
        }

        if (it->value.IsArray())
        {
            rapidjson::Value converted(rapidjson::kArrayType);
            if (auto element_type = convertWrappersInArray(it->value, full_name, converted, allocator, decimal_scales))
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

/** The smallest and the largest integer among the values of one field, see `checkValuesFitColumns`,
  * and the first of its string values that is not the text of an integer, if any.
  */
struct IntegerRange
{
    std::optional<Int128> min;
    std::optional<Int128> max;
    std::optional<String> non_integer;
};

/** The integer a string is the text of: the `JSONEachRow` reader reads a quoted number into a number
  * column, and the serialization of a `$numberDecimal` is such a string. Its magnitude is saturated
  * at `10^30`, which is out of the range of every integer type checked here anyway.
  */
std::optional<Int128> parseIntegerText(std::string_view text)
{
    bool negative = false;
    if (!text.empty() && (text.front() == '-' || text.front() == '+'))
    {
        negative = text.front() == '-';
        text.remove_prefix(1);
    }
    if (text.empty())
        return std::nullopt;

    static constexpr Int128 saturation = Int128(1'000'000'000'000'000ULL) * Int128(1'000'000'000'000'000ULL);
    Int128 number = 0;
    for (char c : text)
    {
        if (!isNumericASCII(c))
            return std::nullopt;
        if (number < saturation)
            number = number * 10 + (c - '0');
    }
    number = std::min(number, saturation);
    return negative ? -number : number;
}

/** The scale a decimal column needs to hold the number that `text` is exactly, `max` when no
  * `Decimal128` holds it, nothing when the text is not a number at all. Unlike the scale of a
  * `$numberDecimal` (see `recordDecimalScale`), trailing fractional zeros do not count: the value
  * is a plain number, so `1.50` is stored exactly in a column of scale 1.
  */
std::optional<UInt32> decimalScaleOfNumberText(std::string_view text)
{
    String normalized(text);
    if (auto point = normalized.find('.'); point != String::npos)
    {
        size_t fractional_end = point + 1;
        while (fractional_end < normalized.size() && isNumericASCII(normalized[fractional_end]))
            ++fractional_end;
        size_t significant_end = fractional_end;
        while (significant_end > point + 1 && normalized[significant_end - 1] == '0')
            --significant_end;
        normalized.erase(significant_end, fractional_end - significant_end);
    }
    if (auto scale = Mongo::decimalScaleOfNumberDecimal(normalized))
        return scale;
    /// A number, but one that no `Decimal128` holds exactly.
    ReadBufferFromMemory buffer(normalized.data(), normalized.size());
    Float64 number = 0;
    if (tryReadText(number, buffer) && buffer.eof())
        return std::numeric_limits<UInt32>::max();
    return std::nullopt;
}

void recordDecimalTextScale(const String & name, std::string_view text, std::map<String, UInt32> & decimal_scales)
{
    if (auto scale = decimalScaleOfNumberText(text))
    {
        auto [it, inserted] = decimal_scales.emplace(name, *scale);
        if (!inserted)
            it->second = std::max(it->second, *scale);
    }
}

/** Records the integers of a value into `integer_ranges` and the scale every other number of it
  * needs into `decimal_scales`, see `checkValuesFitColumns`. A double reaches the reader as the
  * text rapidjson writes for it, and a string as itself, so the scale is the one of that text.
  */
void recordNumbers(
    const String & name,
    const rapidjson::Value & value,
    std::map<String, IntegerRange> & integer_ranges,
    std::map<String, UInt32> & decimal_scales)
{
    if (value.IsArray())
    {
        /// The elements of an array of arrays go into the same column, `Array(Array(...))`.
        for (const auto & element : value.GetArray())
            recordNumbers(name, element, integer_ranges, decimal_scales);
        return;
    }

    std::optional<Int128> number;
    if (value.IsInt64())
        number = value.GetInt64();
    else if (value.IsUint64())
        number = value.GetUint64();
    else if (value.IsDouble())
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        value.Accept(writer);
        recordDecimalTextScale(name, std::string_view(buffer.GetString(), buffer.GetSize()), decimal_scales);
        return;
    }
    else if (value.IsString())
    {
        std::string_view text(value.GetString(), value.GetStringLength());
        number = parseIntegerText(text);
        if (!number)
        {
            recordDecimalTextScale(name, text, decimal_scales);
            auto & range = integer_ranges[name];
            if (!range.non_integer)
                range.non_integer = String(text);
            return;
        }
    }
    else
        return;

    auto & range = integer_ranges[name];
    range.min = range.min ? std::min(*range.min, *number) : *number;
    range.max = range.max ? std::max(*range.max, *number) : *number;
}

/** Records the range of the integers of every field of an already flattened document, including
  * the integer elements of its arrays, and of the strings that are the text of an integer, which is
  * what a `$numberDecimal` becomes; and the scale of the other numbers - doubles and the strings
  * that are the text of a number - which a decimal column has to hold exactly. A nested document
  * of an array is a value of a `JSON` element, which has no fixed type to check, so it is not
  * descended into.
  */
void recordNumberRanges(
    const rapidjson::Value & flattened, std::map<String, IntegerRange> & integer_ranges, std::map<String, UInt32> & decimal_scales)
{
    for (auto it = flattened.MemberBegin(); it != flattened.MemberEnd(); ++it)
        recordNumbers(it->name.GetString(), it->value, integer_ranges, decimal_scales);
}

/// The range of the values of an integer type of at most 64 bits, nothing for any other type.
std::optional<std::pair<Int128, Int128>> getIntegerTypeRange(const DataTypePtr & type)
{
    /// `Bool` is a `UInt8` that holds only `0` and `1`.
    if (isBool(type))
        return std::pair<Int128, Int128>(0, 1);

    WhichDataType which(type);
    if (which.isInt8())
        return std::pair<Int128, Int128>(std::numeric_limits<Int8>::min(), std::numeric_limits<Int8>::max());
    if (which.isInt16())
        return std::pair<Int128, Int128>(std::numeric_limits<Int16>::min(), std::numeric_limits<Int16>::max());
    if (which.isInt32())
        return std::pair<Int128, Int128>(std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max());
    if (which.isInt64())
        return std::pair<Int128, Int128>(std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max());
    if (which.isUInt8())
        return std::pair<Int128, Int128>(0, std::numeric_limits<UInt8>::max());
    if (which.isUInt16())
        return std::pair<Int128, Int128>(0, std::numeric_limits<UInt16>::max());
    if (which.isUInt32())
        return std::pair<Int128, Int128>(0, std::numeric_limits<UInt32>::max());
    if (which.isUInt64())
        return std::pair<Int128, Int128>(0, std::numeric_limits<UInt64>::max());
    return std::nullopt;
}

/** A decimal column keeps a fixed number of fractional digits and reading a value with more of
  * them silently drops the rest, while a `$numberDecimal` - or any other number, a double or the
  * text of a number - is supposed to be stored exactly. The column of a decimal field is the one
  * of the first document that had the field - in this batch or in an earlier insert - so a later
  * value of a larger scale is rejected here, before anything is written, rather than rounded. The types are read from `system.columns`, which shows only the
  * columns the user may see; a decimal field without a visible column cannot be checked, so it is
  * an error as well.
  *
  * An integer column has a fixed range, and the `JSONEachRow` reader does not check it: a value
  * outside of it is stored wrapped around, e.g. `2147483648` in the `Int32` column inferred from
  * a small first value becomes `-2147483648`. So the integers of every field - the values and the
  * elements of the arrays - are checked against the range of the integer type of its column the
  * same way, before anything is written, and so are the integers of a later insert.
  */
void checkValuesFitColumns(
    const CollectionRef & collection,
    std::shared_ptr<QueryExecutor> executor,
    const std::map<String, UInt32> & decimal_scales,
    const std::map<String, IntegerRange> & integer_ranges)
{
    if (decimal_scales.empty() && integer_ranges.empty())
        return;

    std::set<String> field_names;
    for (const auto & [name, _] : decimal_scales)
        field_names.insert(name);
    for (const auto & [name, _] : integer_ranges)
        field_names.insert(name);

    WriteBufferFromOwnString names;
    for (auto it = field_names.begin(); it != field_names.end(); ++it)
        names << (it == field_names.begin() ? "" : ", ") << quoteString(*it);

    auto output = executor->execute(fmt::format(
        "SELECT name, type FROM system.columns WHERE database = {} AND table = {} AND name IN ({}) FORMAT JSONCompactEachRow",
        quoteString(collection.database),
        quoteString(collection.collection),
        names.str()));

    std::map<String, String> column_types;
    size_t line_begin = 0;
    while (line_begin < output.size())
    {
        size_t line_end = output.find('\n', line_begin);
        if (line_end == String::npos)
            line_end = output.size();
        if (line_end > line_begin)
        {
            rapidjson::Document row;
            row.Parse(output.data() + line_begin, line_end - line_begin);
            if (row.HasParseError() || !row.IsArray() || row.Size() != 2 || !row[0].IsString() || !row[1].IsString())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Unexpected row in the list of the columns of '{}'", collection.getQualifiedName());
            column_types.emplace(
                String(row[0].GetString(), row[0].GetStringLength()), String(row[1].GetString(), row[1].GetStringLength()));
        }
        line_begin = line_end + 1;
    }

    /// The type of the values of a column inside the wrappers that do not change them.
    auto get_value_type = [](const String & column_type)
    {
        DataTypePtr type = DataTypeFactory::instance().get(column_type);
        while (true)
        {
            type = removeLowCardinalityAndNullable(type);
            if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
                type = array_type->getNestedType();
            else
                break;
        }
        return type;
    };

    for (const auto & [name, range] : integer_ranges)
    {
        auto it = column_types.find(name);
        if (it == column_types.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The field '{}' holds an integer value or a string, but the collection '{}.{}' has no visible column of that name to "
                "check the value against",
                name,
                collection.database,
                collection.collection);

        auto type_range = getIntegerTypeRange(get_value_type(it->second));
        if (!type_range)
            continue;
        const auto [type_min, type_max] = *type_range;

        /// The reader would read the leading digits of a decimal such as `1.5` or fail on other text,
        /// and neither is the value.
        if (range.non_integer)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The field '{}' holds the value '{}', which is not an integer, while its column is of the type {}: the value cannot "
                "be stored",
                name,
                *range.non_integer,
                it->second);

        if (*range.min < type_min || *range.max > type_max)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The field '{}' holds the integer value {}, which is out of the range of its column of the type {}: the value cannot "
                "be stored",
                name,
                *range.min < type_min ? *range.min : *range.max,
                it->second);
    }

    for (const auto & [name, scale] : decimal_scales)
    {
        auto it = column_types.find(name);
        if (it == column_types.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The field '{}' holds a fractional number, but the collection '{}.{}' has no visible column of that name to check its "
                "scale against",
                name,
                collection.database,
                collection.collection);

        DataTypePtr type = get_value_type(it->second);

        if (isDecimal(type) && scale == std::numeric_limits<UInt32>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The field '{}' holds a number that no decimal holds exactly, while its column is of the type {}: the value cannot be "
                "stored exactly",
                name,
                it->second);

        if (isDecimal(type) && getDecimalScale(*type) < scale)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The field '{}' holds a number with {} fractional digits, more than the scale {} of its column of the type {}: "
                "the value cannot be stored exactly",
                name,
                scale,
                getDecimalScale(*type),
                it->second);
    }
}

}

void InsertHandler::createDatabase(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    /// The access rights of a `CREATE ... IF NOT EXISTS` are checked before it finds out that there
    /// is nothing to create, so it is only issued for a database that is missing: an `insert` into
    /// an existing collection must need no more than the `INSERT` grant.
    if (objectExists(executor, "DATABASE", backQuoteIfNeed(collection.database)))
        return;
    executor->execute(fmt::format("CREATE DATABASE IF NOT EXISTS {}", backQuoteIfNeed(collection.database)));
}

void InsertHandler::createTable(
    const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor, const std::vector<DocumentField> & fields)
{
    /// A collection that already has its columns needs nothing created, and no DDL is issued for
    /// it, see `createDatabase`. An empty document is a valid one there too: it is a row in which
    /// every field is absent, i.e. has the default value of its column, the same as any field a
    /// document does not have.
    const bool table_exists = objectExists(executor, "TABLE", collection.getQualifiedName());
    const bool is_placeholder = table_exists && isPlaceholderCollection(collection, executor);
    if (table_exists && !is_placeholder)
        return;

    if (fields.empty())
    {
        /// Only a new collection (or the placeholder of `createCollection`) cannot get its columns
        /// from an empty document.
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Can not create the collection '{}.{}': its columns are inferred from the first inserted document, which has no fields "
            "that map onto columns",
            collection.database,
            collection.collection);
    }

    if (is_placeholder)
    {
        /// The placeholder is given the schema of the first inserted document, so that a
        /// collection created explicitly ends up with the same columns as one created by the
        /// insert itself. The columns are altered rather than the table recreated: an `ALTER`
        /// of an empty table only rewrites the metadata, while a `DROP` would throw away a
        /// table that the user may have created for something else.
        /// Whether it is empty is read from the metadata rather than from the rows, so that the
        /// first insert needs only the `INSERT` and `ALTER` grants, not `SELECT`. An unknown number
        /// of rows (`NULL`) counts as not empty.
        auto is_empty = executor->execute(fmt::format(
            "SELECT total_rows = 0 FROM system.tables WHERE database = {} AND name = {} FORMAT TSV",
            quoteString(collection.database),
            quoteString(collection.collection)));
        if (!is_empty.starts_with('1'))
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
    /// No column is a sorting key: `ALTER TABLE ... UPDATE`, which implements `update`, cannot
    /// change a key column, and every field of a Mongo document must stay updatable, renamable
    /// and removable, whatever its position in the first inserted document.
    query << ") ENGINE = MergeTree ORDER BY tuple()";

    executor->execute(query.str());
}

std::vector<Document> InsertHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(documents[0].documents[0], "insert");
    rejectUnorderedWriteBatch(documents[0].documents[0], "insert");

    /// The documents to insert come either as a `documents` document sequence or as the
    /// `documents` array of the command body itself, see `getWriteBatch`.
    const auto to_insert = getWriteBatch(documents, "documents", "insert");

    /// Values never go into the query text: the rows are passed as `JSONEachRow` data, which
    /// rapidjson escapes, so no value can change the meaning of the query. Unknown fields are
    /// rejected instead of being silently dropped, and fields that a document does not have
    /// get the default value of their column.
    rapidjson::Document allocator_owner;
    auto & allocator = allocator_owner.GetAllocator();

    WriteBufferFromOwnString data;
    bool table_prepared = false;
    std::map<String, UInt32> decimal_scales;
    std::map<String, IntegerRange> integer_ranges;

    for (const auto & doc : to_insert)
    {
        rapidjson::Value flattened(rapidjson::kObjectType);
        std::map<String, String> wrapper_types;
        flattenDocument(doc.getRapidJSONRepresentation(), "", flattened, allocator, wrapper_types, decimal_scales);
        recordNumberRanges(flattened, integer_ranges, decimal_scales);

        /// The schema comes from the first document only, as in Mongo a collection has no
        /// schema of its own.
        if (!table_prepared)
        {
            createDatabase(collection, executor);
            createTable(collection, executor, inferSchema(flattened, wrapper_types));
            table_prepared = true;
        }

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        flattened.Accept(writer);
        data << buffer.GetString() << "\n";
    }

    checkValuesFitColumns(collection, executor, decimal_scales, integer_ranges);

    /** The whole batch is one `INSERT`, and it is written all or nothing. Mongo writes the documents
      * of an ordered batch one by one and keeps the ones before a failing document; here a document
      * that does not fit - an unknown field, a value of a wrong type - fails the whole command
      * before anything is written, so a client that retries the batch does not duplicate a prefix
      * of it. That holds because every document is parsed before the block is written: the batch
      * is at most `MAX_WRITE_BATCH_SIZE` documents (see `getWriteBatch`), and the settings below
      * make the reader of the input return all of them as one block, and keep that block whole.
      * Every condition that makes the reader return a block early is pinned, rather than only
      * the ones a profile sets by default: the row limit is the size of the largest batch, the
      * byte limits - `max_insert_block_size_bytes` and `input_format_max_block_size_bytes`, which
      * the reader takes when the former is zero - are zero, i.e. unlimited, and so are the minimums
      * `min_insert_block_size_rows` and `min_insert_block_size_bytes`, as the reader returns a block
      * as soon as one of the minimums is reached; the same minimums and maximums bound the
      * squashing of the blocks before they are written. A block is not returned on a timer either
      * (`input_format_max_block_wait_ms`, `input_format_connection_handling`), the input is not
      * split between parallel parsers, and the insert is not deferred by an asynchronous insert
      * whose error the reply would not see. A profile that tolerates malformed rows must not apply
      * either: the skipped row would be acknowledged as written.
      */
    SettingsChanges insert_settings;
    insert_settings.setSetting("max_insert_block_size", Field(UInt64(MAX_WRITE_BATCH_SIZE)));
    insert_settings.setSetting("max_insert_block_size_bytes", Field(UInt64(0)));
    insert_settings.setSetting("input_format_max_block_size_bytes", Field(UInt64(0)));
    insert_settings.setSetting("min_insert_block_size_rows", Field(UInt64(0)));
    insert_settings.setSetting("min_insert_block_size_bytes", Field(UInt64(0)));
    insert_settings.setSetting("input_format_max_block_wait_ms", Field(UInt64(0)));
    insert_settings.setSetting("input_format_connection_handling", Field(false));
    insert_settings.setSetting("input_format_parallel_parsing", Field(false));
    insert_settings.setSetting("async_insert", Field(false));
    insert_settings.setSetting("input_format_allow_errors_num", Field(UInt64(0)));
    insert_settings.setSetting("input_format_allow_errors_ratio", Field(0.0));
    /// A BSON date is written as the text of an instant in UTC with the `Z` suffix (see
    /// `convertMongoExtendedJSONWrapper`), which only this reader understands. The basic one would
    /// reject it, and the text without the suffix would be read in the time zone of the column.
    insert_settings.setSetting("date_time_input_format", Field(String("best_effort")));

    /// The data is `JSONEachRow` whatever the profile says: its `input_format` or `format` would
    /// otherwise win over the `FORMAT` of the query, which `QueryExecutor::execute` prevents.
    executor->execute(
        fmt::format(
            "INSERT INTO {} SETTINGS input_format_skip_unknown_fields = 0 FORMAT JSONEachRow\n{}",
            collection.getQualifiedName(),
            data.str()),
        insert_settings);

    BsonPtr bson_doc_owner(bson_new());
    bson_t * bson_doc = bson_doc_owner.get();

    BSON_APPEND_INT32(bson_doc, "n", static_cast<int32_t>(to_insert.size()));
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(std::move(bson_doc_owner));
    return result;
}

void registerInsertHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<InsertHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}

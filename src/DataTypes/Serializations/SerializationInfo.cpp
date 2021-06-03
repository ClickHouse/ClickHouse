#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

SerializationInfoBuilder::SerializationInfoBuilder(
    double ratio_for_sparse_serialization_,
    size_t default_rows_search_step_)
    : ratio_for_sparse_serialization(ratio_for_sparse_serialization_)
    , default_rows_search_step(default_rows_search_step_)
    , info(std::make_shared<SerializationInfo>())
{
}

void SerializationInfoBuilder::add(const Block & block)
{
    info->number_of_rows += block.rows();
    for (const auto & elem : block)
    {
        /// Just skip column and always return default serialization.
        if (!elem.type->supportsSparseSerialization())
            continue;

        /// Multiply by step to restore approximate number of default values.
        info->columns[elem.name].num_defaults += elem.column->getNumberOfDefaultRows(default_rows_search_step) * default_rows_search_step;
        for (const auto & subcolumn_name : elem.type->getSubcolumnNames())
        {
            auto subcolumn_type = elem.type->getSubcolumnType(subcolumn_name);
            if (!subcolumn_type->supportsSparseSerialization())
                continue;

            auto parent_subcolumn_name = Nested::splitName(subcolumn_name, /*reverse=*/ true).first;
            if (!parent_subcolumn_name.empty())
            {
                auto parent_subcolumn_type = elem.type->tryGetSubcolumnType(parent_subcolumn_name);
                if (parent_subcolumn_type && !parent_subcolumn_type->supportsSparseSerialization())
                    continue;
            }

            auto subcolumn = elem.type->getSubcolumn(subcolumn_name, *elem.column);
            auto full_name = Nested::concatenateName(elem.name, subcolumn_name);
            info->columns[full_name].num_defaults += subcolumn->getNumberOfDefaultRows(default_rows_search_step) * default_rows_search_step;
        }
    }
}

void SerializationInfoBuilder::add(const SerializationInfo & other)
{
    info->number_of_rows += other.number_of_rows;
    for (const auto & [name, column_info] : other.columns)
        info->columns[name].num_defaults += column_info.num_defaults;
}

SerializationInfoPtr SerializationInfoBuilder::build()
{
    size_t total_rows = info->number_of_rows;
    for (auto & [_, column_info] : info->columns)
    {
        double ratio = total_rows ? std::min(static_cast<double>(column_info.num_defaults) / total_rows, 1.0) : 0.0;
        if (ratio > ratio_for_sparse_serialization)
            column_info.kind = ISerialization::Kind::SPARSE;
    }

    return info;
}

SerializationInfoPtr SerializationInfoBuilder::buildFrom(const SerializationInfo & other)
{
    for (auto & [name, column_info] : other.columns)
    {
        auto it = info->columns.find(name);
        if (it == info->columns.end())
            info->columns[name] = column_info;
        else
            it->second.kind = column_info.kind;
    }

    return info;
}

SerializationInfo::SerializationInfo(size_t number_of_rows_, const NameToKind & kinds)
    : number_of_rows(number_of_rows_)
{
    for (const auto & [name, kind] : kinds)
        columns[name].kind = kind;
}

ISerialization::Kind SerializationInfo::getKind(const String & column_name) const
{
    auto it = columns.find(column_name);
    if (it == columns.end())
        return ISerialization::Kind::DEFAULT;

    return it->second.kind;
}

size_t SerializationInfo::getNumberOfDefaultRows(const String & column_name) const
{
    auto it = columns.find(column_name);
    if (it == columns.end())
        return 0;

    return it->second.num_defaults;
}

namespace
{

constexpr auto KEY_VERSION = "version";
constexpr auto KEY_NUMBER_OF_ROWS = "number_of_rows";
constexpr auto KEY_COLUMNS = "columns";
constexpr auto KEY_NUM_DEFAULTS = "num_defaults";
constexpr auto KEY_KIND = "kind";
constexpr auto KEY_NAME = "name";

}

void SerializationInfo::fromJSON(const String & json_str)
{
    Poco::JSON::Parser parser;
    auto object = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();

    if (object->has(KEY_NUMBER_OF_ROWS))
        number_of_rows = object->getValue<size_t>(KEY_NUMBER_OF_ROWS);

    if (object->has(KEY_COLUMNS))
    {
        auto array = object->getArray(KEY_COLUMNS);
        for (const auto & elem : *array)
        {
            auto elem_object = elem.extract<Poco::JSON::Object::Ptr>();
            if (!elem_object->has(KEY_NAME) || !elem_object->has(KEY_NUM_DEFAULTS) || !elem_object->has(KEY_KIND))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Missed field '{}' or '{}' or '{}' in SerializationInfo of columns",
                    KEY_NAME, KEY_NUM_DEFAULTS, KEY_KIND);

            auto name = elem_object->getValue<String>(KEY_NAME);
            auto kind = elem_object->getValue<String>(KEY_KIND);
            auto num_defaults = elem_object->getValue<size_t>(KEY_NUM_DEFAULTS);
            columns[name] = {ISerialization::stringToKind(kind), num_defaults};
        }
    }
}

String SerializationInfo::toJSON() const
{
    Poco::JSON::Object info;
    info.set(KEY_VERSION, version);
    info.set(KEY_NUMBER_OF_ROWS, number_of_rows);

    Poco::JSON::Array column_infos;
    for (const auto & [name, column_info] : columns)
    {
        Poco::JSON::Object column_info_json;
        column_info_json.set(KEY_NAME, name);
        column_info_json.set(KEY_KIND, ISerialization::kindToString(column_info.kind));
        column_info_json.set(KEY_NUM_DEFAULTS, column_info.num_defaults);
        column_infos.add(std::move(column_info_json));
    }

    info.set(KEY_COLUMNS, std::move(column_infos));

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(info, oss);

    return oss.str();
}

void SerializationInfo::readText(ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);
    fromJSON(json_str);
}

void SerializationInfo::writeText(WriteBuffer & out) const
{
    writeString(toJSON(), out);
}

SerializationInfo::NameToKind SerializationInfo::getKinds(const Block & block)
{
    NameToKind kinds;
    for (const auto & elem : block)
    {
        kinds[elem.name] = ISerialization::getKind(*elem.column);
        for (const auto & subcolumn_name : elem.type->getSubcolumnNames())
        {
            auto full_name = Nested::concatenateName(elem.name, subcolumn_name);
            auto subcolumn = elem.type->getSubcolumn(subcolumn_name, *elem.column);
            kinds[full_name] = ISerialization::getKind(*subcolumn);
        }
    }

    return kinds;
}

SerializationInfo::NameToKind SerializationInfo::readKindsBinary(ReadBuffer & in)
{
    size_t size = 0;
    readVarUInt(size, in);

    NameToKind kinds;
    kinds.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        String name;
        UInt8 kind;

        readBinary(name, in);
        readBinary(kind, in);
        if (!kinds.emplace(name, static_cast<ISerialization::Kind>(kind)).second)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicated name '{}' found in serialization kinds", name);
    }

    return kinds;
}

void SerializationInfo::writeKindsBinary(const NameToKind & kinds, WriteBuffer & out)
{
    writeVarUInt(kinds.size(), out);
    for (const auto & [name, kind] : kinds)
    {
        writeBinary(name, out);
        writeBinary(static_cast<UInt8>(kind), out);
    }
}

}

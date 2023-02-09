#include <DataTypes/Serializations/SerializationInfo.h>
#include <Columns/ColumnSparse.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <Core/Block.h>
#include <base/EnumReflection.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

constexpr auto KEY_VERSION = "version";
constexpr auto KEY_NUM_ROWS = "num_rows";
constexpr auto KEY_COLUMNS = "columns";
constexpr auto KEY_NUM_DEFAULTS = "num_defaults";
constexpr auto KEY_KIND = "kind";
constexpr auto KEY_NAME = "name";

}

void SerializationInfo::Data::add(const IColumn & column)
{
    size_t rows = column.size();
    double ratio = column.getRatioOfDefaultRows(ColumnSparse::DEFAULT_ROWS_SEARCH_SAMPLE_RATIO);

    num_rows += rows;
    num_defaults += static_cast<size_t>(ratio * rows);
}

void SerializationInfo::Data::add(const Data & other)
{
    num_rows += other.num_rows;
    num_defaults += other.num_defaults;
}

void SerializationInfo::Data::addDefaults(size_t length)
{
    num_rows += length;
    num_defaults += length;
}

SerializationInfo::SerializationInfo(ISerialization::Kind kind_, const Settings & settings_)
    : settings(settings_)
    , kind(kind_)
{
}

SerializationInfo::SerializationInfo(ISerialization::Kind kind_, const Settings & settings_, const Data & data_)
    : settings(settings_)
    , kind(kind_)
    , data(data_)
{
}

void SerializationInfo::add(const IColumn & column)
{
    data.add(column);
    if (settings.choose_kind)
        kind = chooseKind(data, settings);
}

void SerializationInfo::add(const SerializationInfo & other)
{
    data.add(other.data);
    if (settings.choose_kind)
        kind = chooseKind(data, settings);
}

void SerializationInfo::addDefaults(size_t length)
{
    data.addDefaults(length);
    if (settings.choose_kind)
        kind = chooseKind(data, settings);
}

void SerializationInfo::replaceData(const SerializationInfo & other)
{
    data = other.data;
}

MutableSerializationInfoPtr SerializationInfo::clone() const
{
    return std::make_shared<SerializationInfo>(kind, settings, data);
}

void SerializationInfo::serialializeKindBinary(WriteBuffer & out) const
{
    writeBinary(static_cast<UInt8>(kind), out);
}

void SerializationInfo::deserializeFromKindsBinary(ReadBuffer & in)
{
    UInt8 kind_num;
    readBinary(kind_num, in);
    auto maybe_kind = magic_enum::enum_cast<ISerialization::Kind>(kind_num);
    if (!maybe_kind)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown serialization kind " + std::to_string(kind_num));

    kind = *maybe_kind;
}

Poco::JSON::Object SerializationInfo::toJSON() const
{
    Poco::JSON::Object object;
    object.set(KEY_KIND, ISerialization::kindToString(kind));
    object.set(KEY_NUM_DEFAULTS, data.num_defaults);
    object.set(KEY_NUM_ROWS, data.num_rows);
    return object;
}

void SerializationInfo::fromJSON(const Poco::JSON::Object & object)
{
    if (!object.has(KEY_KIND) || !object.has(KEY_NUM_DEFAULTS) || !object.has(KEY_NUM_ROWS))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Missed field '{}' or '{}' or '{}' in SerializationInfo of columns",
            KEY_KIND, KEY_NUM_DEFAULTS, KEY_NUM_ROWS);

    data.num_rows = object.getValue<size_t>(KEY_NUM_ROWS);
    data.num_defaults = object.getValue<size_t>(KEY_NUM_DEFAULTS);
    kind = ISerialization::stringToKind(object.getValue<String>(KEY_KIND));
}

ISerialization::Kind SerializationInfo::chooseKind(const Data & data, const Settings & settings)
{
    double ratio = data.num_rows ? std::min(static_cast<double>(data.num_defaults) / data.num_rows, 1.0) : 0.0;
    return ratio > settings.ratio_of_defaults_for_sparse ? ISerialization::Kind::SPARSE : ISerialization::Kind::DEFAULT;
}

SerializationInfoByName::SerializationInfoByName(
    const NamesAndTypesList & columns,
    const SerializationInfo::Settings & settings)
{
    if (settings.isAlwaysDefault())
        return;

    for (const auto & column : columns)
        if (column.type->supportsSparseSerialization())
            emplace(column.name, column.type->createSerializationInfo(settings));
}

void SerializationInfoByName::add(const Block & block)
{
    for (const auto & column : block)
    {
        auto it = find(column.name);
        if (it == end())
            continue;

        it->second->add(*column.column);
    }
}

void SerializationInfoByName::add(const SerializationInfoByName & other)
{
    for (const auto & [name, info] : other)
    {
        auto it = find(name);
        if (it == end())
            continue;

        it->second->add(*info);
    }
}

void SerializationInfoByName::replaceData(const SerializationInfoByName & other)
{
    for (const auto & [name, new_info] : other)
    {
        auto & old_info = (*this)[name];

        if (old_info)
            old_info->replaceData(*new_info);
        else
            old_info = new_info->clone();
    }
}

void SerializationInfoByName::writeJSON(WriteBuffer & out) const
{
    Poco::JSON::Object object;
    object.set(KEY_VERSION, SERIALIZATION_INFO_VERSION);

    Poco::JSON::Array column_infos;
    for (const auto & [name, info] : *this)
    {
        auto info_json = info->toJSON();
        info_json.set(KEY_NAME, name);
        column_infos.add(std::move(info_json)); /// NOLINT
    }

    object.set(KEY_COLUMNS, std::move(column_infos)); /// NOLINT

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(object, oss);

    return writeString(oss.str(), out);
}

void SerializationInfoByName::readJSON(ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);

    Poco::JSON::Parser parser;
    auto object = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();

    if (!object->has(KEY_VERSION))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Missed version of serialization infos");

    if (object->getValue<size_t>(KEY_VERSION) > SERIALIZATION_INFO_VERSION)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Unknown version of serialization infos ({}). Should be less or equal than {}",
            object->getValue<size_t>(KEY_VERSION), SERIALIZATION_INFO_VERSION);

    if (object->has(KEY_COLUMNS))
    {
        auto array = object->getArray(KEY_COLUMNS);
        for (const auto & elem : *array)
        {
            auto elem_object = elem.extract<Poco::JSON::Object::Ptr>();

            if (!elem_object->has(KEY_NAME))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Missed field '{}' in SerializationInfo of columns", KEY_NAME);

            auto name = elem_object->getValue<String>(KEY_NAME);
            if (auto it = find(name); it != end())
                it->second->fromJSON(*elem_object);
        }
    }
}

}

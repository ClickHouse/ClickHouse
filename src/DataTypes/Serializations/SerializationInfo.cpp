#include <DataTypes/Serializations/SerializationInfo.h>

#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
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

constexpr auto KEY_TYPES_SERIALIZATION_VERSIONS = "types_serialization_versions";
constexpr auto KEY_STRING_SERIALIZATION_VERSION = "string";

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

void SerializationInfo::Data::remove(const Data & other)
{
    num_rows -= other.num_rows;
    num_defaults -= other.num_defaults;
}

void SerializationInfo::Data::addDefaults(size_t length)
{
    num_rows += length;
    num_defaults += length;
}

SerializationInfo::SerializationInfo(ISerialization::KindStack kind_stack_, const Settings & settings_)
    : settings(settings_)
    , kind_stack(kind_stack_)
{
}

SerializationInfo::SerializationInfo(ISerialization::KindStack kind_stack_, const Settings & settings_, const Data & data_)
    : settings(settings_)
    , kind_stack(kind_stack_)
    , data(data_)
{
}

void SerializationInfo::add(const IColumn & column)
{
    data.add(column);
    if (settings.choose_kind)
        kind_stack = chooseKindStack(data, settings);
}

void SerializationInfo::add(const SerializationInfo & other)
{
    data.add(other.data);
    if (settings.choose_kind)
        kind_stack = chooseKindStack(data, settings);
}

void SerializationInfo::remove(const SerializationInfo & other)
{
    data.remove(other.data);
    if (settings.choose_kind)
        kind_stack = chooseKindStack(data, settings);
}


void SerializationInfo::addDefaults(size_t length)
{
    data.addDefaults(length);
    if (settings.choose_kind)
        kind_stack = chooseKindStack(data, settings);
}

void SerializationInfo::replaceData(const SerializationInfo & other)
{
    data = other.data;
}

MutableSerializationInfoPtr SerializationInfo::clone() const
{
    return std::make_shared<SerializationInfo>(kind_stack, settings, data);
}

/// Returns true if all rows with default values of type 'lhs'
/// are mapped to default values of type 'rhs' after conversion.
static bool preserveDefaultsAfterConversion(const IDataType & lhs, const IDataType & rhs)
{
    if (lhs.equals(rhs))
        return true;

    bool lhs_is_columned_as_numeric = isColumnedAsNumber(lhs) || isColumnedAsDecimal(lhs);
    bool rhs_is_columned_as_numeric = isColumnedAsNumber(rhs) || isColumnedAsDecimal(rhs);

    if (lhs_is_columned_as_numeric && rhs_is_columned_as_numeric)
        return true;

    if (isStringOrFixedString(lhs) && isStringOrFixedString(rhs))
        return true;

    return false;
}

std::shared_ptr<SerializationInfo> SerializationInfo::createWithType(
    const IDataType & old_type,
    const IDataType & new_type,
    const Settings & new_settings) const
{
    ISerialization::KindStack new_kind_stack;
    for (auto kind : kind_stack)
    {
        if (kind == ISerialization::Kind::SPARSE && (!new_type.supportsSparseSerialization() || !preserveDefaultsAfterConversion(old_type, new_type)))
            continue;
        new_kind_stack.push_back(kind);
    }

    auto new_info = new_type.createSerializationInfo(new_settings);
    new_info->kind_stack = new_kind_stack;
    return new_info;
}

namespace
{

enum class KindStackBinarySerializationType : UInt8
{
    /// First 4 added for compatibility with old versions where we didn't have kind stack but a single kind.
    DEFAULT = 0,
    SPARSE = 1, /// stack: {Default, Sparse}
    DETACHED = 2,  /// stack: {Default, Detached}
    DETACHED_OVER_SPARSE = 3,  /// stack: {Default, Sparse, Detached}
    REPLICATED = 4,  /// stack: {Default, Replicated}

    COMBINATION = 5, /// other stacks, serialized as number of kinds and all kinds one after another.
};

}

void SerializationInfo::serialializeKindStackBinary(WriteBuffer & out) const
{
    if (kind_stack == ISerialization::KindStack{ISerialization::Kind::DEFAULT})
    {
        writeBinary(static_cast<UInt8>(KindStackBinarySerializationType::DEFAULT), out);
    }
    else if (kind_stack == ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE})
    {
        writeBinary(static_cast<UInt8>(KindStackBinarySerializationType::SPARSE), out);
    }
    else if (kind_stack == ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::DETACHED})
    {
        writeBinary(static_cast<UInt8>(KindStackBinarySerializationType::DETACHED), out);
    }
    else if (kind_stack == ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE, ISerialization::Kind::DETACHED})
    {
        writeBinary(static_cast<UInt8>(KindStackBinarySerializationType::DETACHED_OVER_SPARSE), out);
    }
    else if (kind_stack == ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::REPLICATED})
    {
        writeBinary(static_cast<UInt8>(KindStackBinarySerializationType::REPLICATED), out);
    }
    else
    {
        writeBinary(static_cast<UInt8>(KindStackBinarySerializationType::COMBINATION), out);
        writeVarUInt(kind_stack.size(), out);
        for (auto kind : kind_stack)
            writeBinary(static_cast<UInt8>(kind), out);
    }
}

void SerializationInfo::deserializeFromKindsBinary(ReadBuffer & in)
{
    UInt8 type;
    readBinary(type, in);
    auto maybe_type = magic_enum::enum_cast<KindStackBinarySerializationType>(type);
    if (!maybe_type)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown serialization kind type {}", UInt32(type));

    switch (*maybe_type)
    {
        case KindStackBinarySerializationType::DEFAULT:
            kind_stack = {ISerialization::Kind::DEFAULT};
            break;
        case KindStackBinarySerializationType::SPARSE:
            kind_stack = {ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE};
            break;
        case KindStackBinarySerializationType::DETACHED:
            kind_stack = {ISerialization::Kind::DEFAULT, ISerialization::Kind::DETACHED};
            break;
        case KindStackBinarySerializationType::DETACHED_OVER_SPARSE:
            kind_stack = {ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE, ISerialization::Kind::DETACHED};
            break;
        case KindStackBinarySerializationType::REPLICATED:
            kind_stack = {ISerialization::Kind::DEFAULT, ISerialization::Kind::REPLICATED};
            break;
        case KindStackBinarySerializationType::COMBINATION:
        {
            size_t num_kinds;
            readVarUInt(num_kinds, in);
            for (size_t i = 0; i != num_kinds; ++i)
            {
                UInt8 kind;
                readBinary(kind, in);
                auto maybe_kind = magic_enum::enum_cast<ISerialization::Kind>(kind);
                if (!maybe_kind)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown serialization kind {}", UInt32(kind));
                kind_stack.push_back(*maybe_kind);
            }

            break;
        }
    }
}

void SerializationInfo::toJSON(Poco::JSON::Object & object) const
{
    object.set(KEY_KIND, ISerialization::kindStackToString(kind_stack));
    object.set(KEY_NUM_DEFAULTS, data.num_defaults);
    object.set(KEY_NUM_ROWS, data.num_rows);
}

void SerializationInfo::fromJSON(const Poco::JSON::Object & object)
{
    if (!object.has(KEY_KIND) || !object.has(KEY_NUM_DEFAULTS) || !object.has(KEY_NUM_ROWS))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Missed field '{}' or '{}' or '{}' in SerializationInfo of columns",
            KEY_KIND, KEY_NUM_DEFAULTS, KEY_NUM_ROWS);

    data.num_rows = object.getValue<size_t>(KEY_NUM_ROWS);
    data.num_defaults = object.getValue<size_t>(KEY_NUM_DEFAULTS);
    kind_stack = ISerialization::stringToKindStack(object.getValue<String>(KEY_KIND));
}

ISerialization::KindStack SerializationInfo::chooseKindStack(const Data & data, const Settings & settings)
{
    ISerialization::KindStack kind_stack = {ISerialization::Kind::DEFAULT};
    double ratio = data.num_rows ? std::min(static_cast<double>(data.num_defaults) / data.num_rows, 1.0) : 0.0;
    if (ratio > settings.ratio_of_defaults_for_sparse)
        kind_stack.push_back(ISerialization::Kind::SPARSE);
    return kind_stack;
}

SerializationInfoByName::SerializationInfoByName(const SerializationInfo::Settings & settings_)
    : settings(settings_)
{
    /// Downgrade to BASIC version if `string_serialization_version` is SINGLE_STREAM.
    ///
    /// Rationale:
    /// - `BASIC` means old serialization format (no per-type specialization).
    /// - `WITH_TYPES` means new format that supports per-type serialization versions,
    ///   where `string_serialization_version` is currently the only one specialization.
    ///
    /// If `string_serialization_version` is SINGLE_STREAM, there is no effective type specialization
    /// in use, so writing `WITH_TYPES` would add no benefit but reduce compatibility.
    /// Falling back to `BASIC` keeps the output fully compatible with older servers.
    if (settings.string_serialization_version == MergeTreeStringSerializationVersion::SINGLE_STREAM)
        settings.version = MergeTreeSerializationInfoVersion::BASIC;
}

SerializationInfoByName::SerializationInfoByName(const NamesAndTypesList & columns, const SerializationInfo::Settings & settings_)
    : SerializationInfoByName(settings_)
{
    if (settings.isAlwaysDefault())
        return;

    for (const auto & column : columns)
    {
        if (column.type->supportsSparseSerialization())
            emplace(column.name, column.type->createSerializationInfo(settings));
    }
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
        add(name, *info);
}

void SerializationInfoByName::add(const String & name, const SerializationInfo & info)
{
    if (auto it = find(name); it != end())
        it->second->add(info);
}

void SerializationInfoByName::remove(const SerializationInfoByName & other)
{
    for (const auto & [name, info] : other)
        remove(name, *info);
}

void SerializationInfoByName::remove(const String & name, const SerializationInfo & info)
{
    if (auto it = find(name); it != end())
        it->second->remove(info);
}

SerializationInfoPtr SerializationInfoByName::tryGet(const String & name) const
{
    auto it = find(name);
    return it == end() ? nullptr : it->second;
}

MutableSerializationInfoPtr SerializationInfoByName::tryGet(const String & name)
{
    auto it = find(name);
    return it == end() ? nullptr : it->second;
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

ISerialization::KindStack SerializationInfoByName::getKindStack(const String & column_name) const
{
    auto it = find(column_name);
    return it != end() ? it->second->getKindStack() : ISerialization::KindStack{ISerialization::Kind::DEFAULT};
}

MergeTreeSerializationInfoVersion SerializationInfoByName::getVersion() const
{
    return settings.version;
}

bool SerializationInfoByName::needsPersistence() const
{
    return !empty() || getVersion() > MergeTreeSerializationInfoVersion::BASIC;
}

void SerializationInfoByName::writeJSON(WriteBuffer & out) const
{
    Poco::JSON::Object object;
    Poco::JSON::Array column_infos;

    for (const auto & [name, info] : *this)
    {
        Poco::JSON::Object info_json;
        info->toJSON(info_json);
        info_json.set(KEY_NAME, name);
        column_infos.add(std::move(info_json)); /// NOLINT
    }

    auto version = getVersion();
    object.set(KEY_VERSION, static_cast<uint8_t>(version));
    object.set(KEY_COLUMNS, std::move(column_infos)); /// NOLINT

    if (version >= MergeTreeSerializationInfoVersion::WITH_TYPES)
    {
        Poco::JSON::Object type_versions_obj;
        type_versions_obj.set(KEY_STRING_SERIALIZATION_VERSION, static_cast<size_t>(settings.string_serialization_version));
        object.set(KEY_TYPES_SERIALIZATION_VERSIONS, type_versions_obj);
    }

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(object, oss);

    writeString(oss.str(), out);
}

SerializationInfoByName SerializationInfoByName::clone() const
{
    SerializationInfoByName res(settings);
    for (const auto & [name, info] : *this)
        res.emplace(name, info->clone());
    return res;
}

SerializationInfoByName SerializationInfoByName::readJSONFromString(const NamesAndTypesList & columns, const std::string & json_str)
{
    Poco::JSON::Parser parser;
    auto object = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();

    if (!object->has(KEY_VERSION))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Missed version of serialization infos");

    MergeTreeSerializationInfoVersion version = MergeTreeSerializationInfoVersion::BASIC;
    {
        size_t version_value = object->getValue<size_t>(KEY_VERSION);
        auto maybe_enum = magic_enum::enum_cast<MergeTreeSerializationInfoVersion>(version_value);
        if (!maybe_enum)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown version of serialization infos ({})", version_value);
        version = *maybe_enum;
    }

    Poco::JSON::Array::Ptr columns_array;
    Poco::JSON::Object::Ptr type_versions_obj;
    for (const auto & [key, value] : *object)
    {
        if (key == KEY_VERSION)
        {
            continue;
        }
        else if (key == KEY_COLUMNS)
        {
            columns_array = value.extract<Poco::JSON::Array::Ptr>();
        }
        else if (version >= MergeTreeSerializationInfoVersion::WITH_TYPES && key == KEY_TYPES_SERIALIZATION_VERSIONS)
        {
            type_versions_obj = value.extract<Poco::JSON::Object::Ptr>();
        }
        else
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected field '{}' in MergeTreeSerializationInfo JSON", key);
        }
    }

    MergeTreeStringSerializationVersion string_serialization_version = MergeTreeStringSerializationVersion::SINGLE_STREAM;
    if (version >= MergeTreeSerializationInfoVersion::WITH_TYPES)
    {
        /// types_serialization_versions is mandatory in WITH_TYPES mode
        if (!type_versions_obj)
        {
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Missing mandatory field 'types_serialization_versions' in MergeTreeSerializationInfo (version WITH_TYPES)");
        }

        for (const auto & [type_name, value] : *type_versions_obj)
        {
            size_t version_value = value.convert<size_t>();
            if (type_name == KEY_STRING_SERIALIZATION_VERSION)
            {
                auto maybe_enum = magic_enum::enum_cast<MergeTreeStringSerializationVersion>(version_value);
                if (!maybe_enum.has_value())
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid version {} for type '{}'", version_value, type_name);
                string_serialization_version = *maybe_enum;
            }
            else
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown field '{}' in types_serialization_versions", type_name);
            }
        }
    }

    SerializationInfoSettings settings(
        1.0 /* Doesn't matter when constructing from JSON */,
        false /* Cannot choose kind when constructing from JSON */,
        version,
        string_serialization_version);

    SerializationInfoByName infos(settings);
    if (columns_array)
    {
        std::unordered_map<std::string_view, const IDataType *> column_type_by_name;
        for (const auto & [name, type] : columns)
            column_type_by_name.emplace(name, type.get());

        for (const auto & elem : *columns_array)
        {
            const auto & elem_object = elem.extract<Poco::JSON::Object::Ptr>();

            if (!elem_object->has(KEY_NAME))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Missed field '{}' in serialization infos", KEY_NAME);

            auto name = elem_object->getValue<String>(KEY_NAME);
            auto it = column_type_by_name.find(name);

            if (it == column_type_by_name.end())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Found unexpected column '{}' in serialization infos", name);

            auto info = it->second->createSerializationInfo(infos.settings);
            info->fromJSON(*elem_object);
            infos.emplace(name, std::move(info));
        }
    }

    return infos;
}

SerializationInfoByName SerializationInfoByName::readJSON(const NamesAndTypesList & columns, ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);
    return readJSONFromString(columns, json_str);
}

}

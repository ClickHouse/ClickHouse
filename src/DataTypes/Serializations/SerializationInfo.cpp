#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationInfo.h>

#include <Columns/ColumnSparse.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics/Statistics.h>
#include <base/EnumReflection.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>
#include <Common/logger_useful.h>


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

SerializationInfo::SerializationInfo(ISerialization::Kind kind_, const Settings & settings_)
    : settings(settings_), kind(kind_)
{
}

MutableSerializationInfoPtr SerializationInfo::clone() const
{
    return std::make_shared<SerializationInfo>(kind, settings);
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
    auto new_kind = kind;
    if (new_kind == ISerialization::Kind::SPARSE)
    {
        if (!new_type.supportsSparseSerialization()
            || !preserveDefaultsAfterConversion(old_type, new_type))
            new_kind = ISerialization::Kind::DEFAULT;
    }

    return std::make_shared<SerializationInfo>(new_kind, new_settings);
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
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown serialization kind {}", std::to_string(kind_num));

    kind = *maybe_kind;
}

void SerializationInfo::toJSON(Poco::JSON::Object & object) const
{
    object.set(KEY_KIND, ISerialization::kindToString(kind));
}

void SerializationInfo::toJSONWithStats(Poco::JSON::Object & object, const StatisticsInfo & stats) const
{
    object.set(KEY_KIND, ISerialization::kindToString(kind));
    object.set(KEY_NUM_DEFAULTS, stats.estimated_defaults.value_or(0));
    object.set(KEY_NUM_ROWS, stats.rows_count);
}

void SerializationInfo::fromJSON(const Poco::JSON::Object & object)
{
    if (!object.has(KEY_KIND) || !object.has(KEY_NUM_DEFAULTS) || !object.has(KEY_NUM_ROWS))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Missed field '{}' or '{}' or '{}' in SerializationInfo of columns",
            KEY_KIND, KEY_NUM_DEFAULTS, KEY_NUM_ROWS);

    // data.num_rows = object.getValue<size_t>(KEY_NUM_ROWS);
    // data.num_defaults = object.getValue<size_t>(KEY_NUM_DEFAULTS);
    kind = ISerialization::stringToKind(object.getValue<String>(KEY_KIND));
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

ISerialization::Kind SerializationInfoByName::getKind(const String & column_name) const
{
    auto it = find(column_name);
    return it != end() ? it->second->getKind() : ISerialization::Kind::DEFAULT;
}

template <typename ElementWriter>
void SerializationInfoByName::writeJSONImpl(size_t version, WriteBuffer & out, ElementWriter && write_element) const
{
    Poco::JSON::Object object;
    object.set(KEY_VERSION, version);

    Poco::JSON::Array column_infos;
    for (const auto & [name, info] : *this)
    {
        Poco::JSON::Object info_json;
        info_json.set(KEY_NAME, name);
        write_element(name, *info, info_json);
        column_infos.add(std::move(info_json)); /// NOLINT
    }

    object.set(KEY_COLUMNS, std::move(column_infos)); /// NOLINT

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(object, oss);

    writeString(oss.str(), out);
}

void SerializationInfoByName::writeJSON(WriteBuffer & out) const
{
    writeJSONImpl(SERIALIZATION_INFO_VERSION_WITHOUT_STATS, out,
        [&](const String &, const SerializationInfo & info, Poco::JSON::Object & info_json)
        {
            info.toJSON(info_json);
        });
}

void SerializationInfoByName::writeJSONWithStats(WriteBuffer & out, const StatisticsInfos & stats) const
{
    writeJSONImpl(SERIALIZATION_INFO_VERSION_WITH_STATS, out,
        [&](const String & name, const SerializationInfo & info, Poco::JSON::Object & info_json)
        {
            auto it = stats.find(name);
            if (it == stats.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Missed statistics for column {}", name);

            info.toJSONWithStats(info_json, it->second);
        });
}

SerializationInfoByName SerializationInfoByName::readJSONFromString(
    const NamesAndTypesList & columns, const Settings & settings, const std::string & json_str)
{
    Poco::JSON::Parser parser;
    auto object = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();

    if (!object->has(KEY_VERSION))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Missed version of serialization infos");

    if (object->getValue<size_t>(KEY_VERSION) > SERIALIZATION_INFO_VERSION_WITH_STATS)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Unknown version of serialization infos ({}). Should be less or equal than {}",
            object->getValue<size_t>(KEY_VERSION), SERIALIZATION_INFO_VERSION_WITH_STATS);

    SerializationInfoByName infos;
    if (object->has(KEY_COLUMNS))
    {
        std::unordered_map<std::string_view, const IDataType *> column_type_by_name;
        for (const auto & [name, type] : columns)
            column_type_by_name.emplace(name, type.get());

        auto array = object->getArray(KEY_COLUMNS);
        for (const auto & elem : *array)
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

            auto info = it->second->createSerializationInfo(settings);
            info->fromJSON(*elem_object);
            infos.emplace(name, std::move(info));
        }
    }

    return infos;
}


SerializationInfoByName SerializationInfoByName::readJSON(
    const NamesAndTypesList & columns, const Settings & settings, ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);
    return readJSONFromString(columns, settings, json_str);
}

SerializationInfoByName SerializationInfoByName::fromStatistics(const ColumnsStatistics & statistics, const Settings & settings)
{
    SerializationInfoByName infos;

    for (const auto & [column_name, column_stats] : statistics)
    {
        size_t num_rows = column_stats->rowCount();

        const auto & desc = column_stats->getDescription();
        const auto & stats = column_stats->getStats();
        auto non_nullable_type = removeNullable(desc.data_type);

        if (desc.data_type->supportsSparseSerialization() && stats.contains(StatisticsType::Defaults))
        {
            size_t num_defaults = stats.at(StatisticsType::Defaults)->estimateDefaults();
            double ratio = static_cast<double>(num_defaults) / num_rows;

            if (ratio > settings.ratio_of_defaults_for_sparse)
            {
                infos.emplace(column_name, std::make_shared<SerializationInfo>(ISerialization::Kind::SPARSE, settings));
                continue;
            }
        }

        if (isStringOrFixedString(non_nullable_type) && stats.contains(StatisticsType::Uniq))
        {
            size_t cardinality = stats.at(StatisticsType::Uniq)->estimateCardinality();

            if (cardinality < settings.number_of_uniq_for_low_cardinality)
            {
                infos.emplace(column_name, std::make_shared<SerializationInfo>(ISerialization::Kind::LOW_CARDINALITY, settings));
                continue;
            }
        }

        infos.emplace(column_name, std::make_shared<SerializationInfo>(ISerialization::Kind::DEFAULT, settings));
    }

    return infos;
}

ColumnsStatistics getImplicitStatisticsForSparseSerialization(const Block & block, const SerializationInfoSettings & settings)
{
    if (settings.ratio_of_defaults_for_sparse >= 1.0)
        return {};

    ColumnsStatistics statistics;
    for (const auto & column : block)
    {
        if (!column.type->supportsSparseSerialization())
            continue;

        ColumnStatisticsDescription desc;
        SingleStatisticsDescription stat_desc(StatisticsType::Defaults, nullptr);

        desc.data_type = column.type;
        desc.types_to_desc.emplace(StatisticsType::Defaults, std::move(stat_desc));

        statistics.emplace(column.name, MergeTreeStatisticsFactory::instance().get(desc));
    }

    return statistics;
}

}

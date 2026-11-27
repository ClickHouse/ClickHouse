#include <Storages/ObjectStorage/DataLakes/DeltaLake/LogColumnStatistics.h>

#if USE_DELTA_KERNEL_RS

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/Statistics/Statistics.h>

#if USE_SIMDJSON
#include <Common/JSONParsers/SimdJSONParser.h>
#elif USE_RAPIDJSON
#include <Common/JSONParsers/RapidJSONParser.h>
#else
#include <Common/JSONParsers/DummyJSONParser.h>
#endif
#include <IO/WriteHelpers.h>

namespace DeltaLake
{

namespace
{

#if USE_SIMDJSON
using JSONParser = DB::SimdJSONParser;
#elif USE_RAPIDJSON
using JSONParser = DB::RapidJSONParser;
#else
using JSONParser = DB::DummyJSONParser;
#endif

/// The text of a number, string or boolean field of `object`; nullopt if the field is missing or of another kind.
std::optional<String> getScalarText(const std::optional<JSONParser::Object> & object, const String & name)
{
    JSONParser::Element value;
    if (!object || !object->find(name, value))
        return std::nullopt;
    /// Also an integer wider than 64 bits, as a decimal bound can be.
    if (value.isString())
        return String(value.getString());
    if (value.isInt64())
        return DB::toString(value.getInt64());
    if (value.isUInt64())
        return DB::toString(value.getUInt64());
    if (value.isDouble())
        return DB::toString(value.getDouble());
    if (value.isBool())
        return String(value.getBool() ? "true" : "false");
    return std::nullopt;
}

/// A non-negative integer field of `object`; nullopt if it is missing or is not one.
std::optional<UInt64> getCount(const std::optional<JSONParser::Object> & object, const String & name)
{
    JSONParser::Element value;
    if (!object || !object->find(name, value))
        return std::nullopt;
    if (value.isUInt64())
        return value.getUInt64();
    if (value.isInt64() && value.getInt64() >= 0)
        return static_cast<UInt64>(value.getInt64());
    return std::nullopt;
}

/// A bound in `minValues` or `maxValues` as a value of `type`; Null if it is missing or does not convert.
DB::Field getBound(
    const std::optional<JSONParser::Object> & bounds, const String & name, const DB::IDataType & type, const DB::FormatSettings & format_settings)
{
    const auto text = getScalarText(bounds, name);
    if (!text)
        return {};
    /// As text, so that a number, a date and a timestamp are all parsed by the type itself.
    return DB::tryConvertFieldToType(DB::Field(*text), type, nullptr, format_settings);
}

/// The object field `name` of `object`, if it has one.
std::optional<JSONParser::Object> getObject(const JSONParser::Object & object, const String & name)
{
    JSONParser::Element value;
    if (!object.find(name, value) || !value.isObject())
        return std::nullopt;
    return value.getObject();
}

}

struct LogColumnStatistics::Parser
{
    JSONParser parser;
};

LogColumnStatistics::~LogColumnStatistics() = default;

LogColumnStatistics::LogColumnStatistics(
    const DB::Names & column_names,
    const DB::ColumnsDescription & columns,
    const DB::Names & partition_columns,
    const DB::NameToNameMap & physical_names_map,
    DB::ContextPtr context)
    : statistics(std::move(context))
    , parser(std::make_unique<Parser>())
{
    format_settings.date_time_input_format = DB::FormatSettings::DateTimeInputFormat::BestEffort;

    for (const auto & name : column_names)
    {
        auto column = columns.tryGetPhysical(name);
        if (!column)
            continue;

        auto nested_type = DB::removeLowCardinalityAndNullable(column->type);
        if (!DB::DataLakeColumnStatistics::canBeTarget(*nested_type))
            continue;

        auto physical_name = tryGetPhysicalName(appendToLogicalPath({}, name), physical_names_map);
        if (!physical_name)
            continue;

        TargetSource source{std::move(*physical_name), std::nullopt, DB::canStatisticsTrackMinMax(column->type), nested_type};
        if (const auto * date_time = typeid_cast<const DB::DataTypeDateTime64 *>(nested_type.get()))
            source.bound_type = std::make_shared<DB::DataTypeDateTime64>(date_time->getScale(), "UTC");
        if (const auto it = std::ranges::find(partition_columns, name); it != partition_columns.end())
            source.partition_index = it - partition_columns.begin();

        statistics.addTarget({name, column->type, std::move(nested_type), std::nullopt});
        sources.push_back(std::move(source));
    }
}

bool LogColumnStatistics::addFile(const TableSnapshot::DataFileStatistics & file)
{
    if (!file.stats)
        return false;

    /// The stats are optional columns over a required row count; a document that does not parse gives neither.
    JSONParser::Element document;
    if (!parser->parser.parse(*file.stats, document) || !document.isObject())
        return false;
    const std::optional<JSONParser::Object> stats = document.getObject();
    const auto num_records = getCount(stats, "numRecords");
    if (!num_records)
        return false;
    const UInt64 rows = *num_records;

    if (file.deleted_rows)
    {
        if (*file.deleted_rows < 0 || static_cast<UInt64>(*file.deleted_rows) > rows)
            return false;
        deleted_rows += static_cast<UInt64>(*file.deleted_rows);
        has_deletion_vectors = true;
    }

    if (rows == 0)
        return true;

    const auto & targets = statistics.getTargets();
    if (targets.empty())
    {
        statistics.addFile(rows, {});
        return true;
    }

    const auto min_values = getObject(*stats, "minValues");
    const auto max_values = getObject(*stats, "maxValues");
    const auto null_counts = getObject(*stats, "nullCount");

    std::vector<DB::DataLakeColumnStatistics::ColumnMetrics> file_metrics(targets.size());
    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & source = sources[i];
        const auto & nested_type = *targets[i].nested_type;
        auto & metrics = file_metrics[i];

        if (source.partition_index)
        {
            if (*source.partition_index >= file.partition_values.size())
                continue;
            const auto & partition_value = file.partition_values[*source.partition_index];
            if (partition_value.isNull())
                metrics.setIdentityPartitionValue({}, rows, source.tracks_min_max);
            else if (auto value = DB::tryConvertFieldToType(partition_value, nested_type); !value.isNull())
                metrics.setIdentityPartitionValue(std::move(value), rows, source.tracks_min_max);
            continue;
        }

        if (source.tracks_min_max)
        {
            auto lower = getBound(min_values, source.physical_name, *source.bound_type, format_settings);
            auto upper = getBound(max_values, source.physical_name, *source.bound_type, format_settings);
            if (!lower.isNull() && !upper.isNull())
            {
                metrics.min_value = std::move(lower);
                metrics.max_value = std::move(upper);
            }
        }

        if (const auto nulls = getCount(null_counts, source.physical_name); nulls && *nulls <= rows)
            metrics.nulls = *nulls;
    }
    statistics.addFile(rows, file_metrics);
    return true;
}

void LogColumnStatistics::finalize(DB::DataLakeReadEstimate & estimate)
{
    statistics.subtractDeletedRows(deleted_rows);
    statistics.finalize(estimate);
}

}

#endif

#include <Storages/ObjectStorage/DataLakes/DeltaLake/LogColumnStatistics.h>

#if USE_DELTA_KERNEL_RS

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/Statistics/Statistics.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DeltaLake
{

namespace
{

/// A bound in `minValues` or `maxValues` as a value of `type`; Null if it is missing or does not convert.
DB::Field getBound(
    const Poco::JSON::Object::Ptr & bounds, const String & name, const DB::IDataType & type, const DB::FormatSettings & format_settings)
{
    if (!bounds || !bounds->has(name) || bounds->isNull(name))
        return {};
    const auto value = bounds->get(name);
    if (!value.isString() && !value.isNumeric() && !value.isBoolean())
        return {};
    /// As text, so that a number, a date and a timestamp are all parsed by the type itself.
    return DB::tryConvertFieldToType(DB::Field(value.convert<String>()), type, nullptr, format_settings);
}

}

LogColumnStatistics::LogColumnStatistics(
    const DB::Names & column_names,
    const DB::ColumnsDescription & columns,
    const DB::Names & partition_columns,
    const DB::NameToNameMap & physical_names_map,
    DB::ContextPtr context)
    : statistics(std::move(context))
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

        TargetSource source{std::move(*physical_name), std::nullopt, DB::canStatisticsTrackMinMax(column->type)};
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

    Poco::JSON::Object::Ptr stats;
    try
    {
        stats = Poco::JSON::Parser().parse(String(*file.stats)).extract<Poco::JSON::Object::Ptr>();
    }
    catch (const Poco::Exception &)
    {
        return false;
    }

    if (!stats || !stats->has("numRecords") || !stats->get("numRecords").isInteger())
        return false;
    const auto num_records = stats->getValue<Int64>("numRecords");
    if (num_records < 0)
        return false;
    const auto rows = static_cast<UInt64>(num_records);

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

    const auto min_values = stats->getObject("minValues");
    const auto max_values = stats->getObject("maxValues");
    const auto null_counts = stats->getObject("nullCount");

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
            auto lower = getBound(min_values, source.physical_name, nested_type, format_settings);
            auto upper = getBound(max_values, source.physical_name, nested_type, format_settings);
            if (!lower.isNull() && !upper.isNull())
            {
                metrics.min_value = std::move(lower);
                metrics.max_value = std::move(upper);
            }
        }

        if (null_counts && null_counts->has(source.physical_name) && null_counts->get(source.physical_name).isInteger())
        {
            const auto nulls = null_counts->getValue<Int64>(source.physical_name);
            if (nulls >= 0 && static_cast<UInt64>(nulls) <= rows)
                metrics.nulls = static_cast<UInt64>(nulls);
        }
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

#include <Storages/TimeSeries/TimeSeriesColumnsValidator.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int THERE_IS_NO_COLUMN;
}


TimeSeriesColumnsValidator::TimeSeriesColumnsValidator(StorageID time_series_storage_id_,
                                                       std::reference_wrapper<const TimeSeriesSettings> time_series_settings_)
    : time_series_storage_id(std::move(time_series_storage_id_))
    , time_series_settings(time_series_settings_)
{
}


void TimeSeriesColumnsValidator::validateColumns(const ColumnsDescription & columns) const
{
    try
    {
        validateColumnsImpl(columns);
    }
    catch (Exception & e)
    {
        e.addMessage("While checking columns of TimeSeries table {}", time_series_storage_id.getNameForLogs());
        throw;
    }
}


void TimeSeriesColumnsValidator::validateColumnsImpl(const ColumnsDescription & columns) const
{

    auto get_column_description = [&](const String & column_name) -> const ColumnDescription &
    {
        const auto * column = columns.tryGet(column_name);
        if (!column)
        {
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Column {} is required for the TimeSeries table engine", column_name);
        }
        return *column;
    };

    /// Validate columns for the "data" table.
    validateColumnForID(get_column_description(TimeSeriesColumnNames::ID));
    validateColumnForTimestamp(get_column_description(TimeSeriesColumnNames::Timestamp));
    validateColumnForValue(get_column_description(TimeSeriesColumnNames::Value));

    /// Validate columns for the "tags" table.
    validateColumnForMetricName(get_column_description(TimeSeriesColumnNames::MetricName));

    const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        validateColumnForTagValue(get_column_description(column_name));
    }

    validateColumnForTagsMap(get_column_description(TimeSeriesColumnNames::Tags));
    validateColumnForTagsMap(get_column_description(TimeSeriesColumnNames::AllTags));

    /// Validate columns for the "metrics" table.
    validateColumnForMetricFamilyName(get_column_description(TimeSeriesColumnNames::MetricFamilyName));
    validateColumnForType(get_column_description(TimeSeriesColumnNames::Type));
    validateColumnForUnit(get_column_description(TimeSeriesColumnNames::Unit));
    validateColumnForHelp(get_column_description(TimeSeriesColumnNames::Help));
}


void TimeSeriesColumnsValidator::validateTargetColumns(ViewTarget::Kind target_kind, const StorageID & target_table_id, const ColumnsDescription & target_columns) const
{
    try
    {
        validateTargetColumnsImpl(target_kind, target_columns);
    }
    catch (Exception & e)
    {
        e.addMessage("While checking columns of table {} which is the {} target of TimeSeries table {}", target_table_id.getNameForLogs(),
                     toString(target_kind), time_series_storage_id.getNameForLogs());
        throw;
    }
}


void TimeSeriesColumnsValidator::validateTargetColumnsImpl(ViewTarget::Kind target_kind, const ColumnsDescription & target_columns) const
{
    auto get_column_description = [&](const String & column_name) -> const ColumnDescription &
    {
        const auto * column = target_columns.tryGet(column_name);
        if (!column)
        {
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Column {} is required for the TimeSeries table engine", column_name);
        }
        return *column;
    };

    switch (target_kind)
    {
        case ViewTarget::Data:
        {
            /// Here "check_default = false" because it's ok for the "id" column in the target table not to contain
            /// an expression for calculating the identifier of a time series.
            validateColumnForID(get_column_description(TimeSeriesColumnNames::ID), /* check_default= */ false);

            validateColumnForTimestamp(get_column_description(TimeSeriesColumnNames::Timestamp));
            validateColumnForValue(get_column_description(TimeSeriesColumnNames::Value));

            break;
        }

        case ViewTarget::Tags:
        {
            validateColumnForMetricName(get_column_description(TimeSeriesColumnNames::MetricName));

            const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
            for (const auto & tag_name_and_column_name : tags_to_columns)
            {
                const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                const auto & column_name = tuple.at(1).safeGet<String>();
                validateColumnForTagValue(get_column_description(column_name));
            }

            validateColumnForTagsMap(get_column_description(TimeSeriesColumnNames::Tags));

            break;
        }

        case ViewTarget::Metrics:
        {
            validateColumnForMetricFamilyName(get_column_description(TimeSeriesColumnNames::MetricFamilyName));
            validateColumnForType(get_column_description(TimeSeriesColumnNames::Type));
            validateColumnForUnit(get_column_description(TimeSeriesColumnNames::Unit));
            validateColumnForHelp(get_column_description(TimeSeriesColumnNames::Help));
            break;
        }

        default:
            UNREACHABLE();
    }
}


void TimeSeriesColumnsValidator::validateColumnForID(const ColumnDescription & column, bool check_default) const
{
    if (check_default && !column.default_desc.expression)
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "The DEFAULT expression for column {} must contain an expression "
                        "which will be used to calculate the identifier of each time series: {} {} DEFAULT ...",
                        column.name, column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTimestamp(const ColumnDescription & column) const
{
    if (!isDateTime64(removeNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Column {} has illegal data type {}, expected DateTime64",
                        column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTimestamp(const ColumnDescription & column, UInt32 & out_scale) const
{
    auto maybe_datetime64_type = removeNullable(column.type);
    if (!isDateTime64(maybe_datetime64_type))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Column {} has illegal data type {}, expected DateTime64",
                        column.name, column.type->getName());
    }
    const auto & datetime64_type = typeid_cast<const DataTypeDateTime64 &>(*maybe_datetime64_type);
    out_scale = datetime64_type.getScale();
}

void TimeSeriesColumnsValidator::validateColumnForValue(const ColumnDescription & column) const
{
    if (!isFloat(removeNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Column {} has illegal data type {}, expected Float32 or Float64",
                        column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForMetricName(const ColumnDescription & column) const
{
    validateColumnForTagValue(column);
}

void TimeSeriesColumnsValidator::validateColumnForMetricName(const ColumnWithTypeAndName & column) const
{
    validateColumnForTagValue(column);
}

void TimeSeriesColumnsValidator::validateColumnForTagValue(const ColumnDescription & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTagValue(const ColumnWithTypeAndName & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTagsMap(const ColumnDescription & column) const
{
    if (!isMap(column.type)
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getKeyType()))
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getValueType())))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Column {} has illegal data type {}, expected Map(String, String) or Map(LowCardinality(String), String)",
                        column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTagsMap(const ColumnWithTypeAndName & column) const
{
    if (!isMap(column.type)
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getKeyType()))
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getValueType())))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column {} has illegal data type {}, expected Map(String, String) or Map(LowCardinality(String), String)",
                        column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForMetricFamilyName(const ColumnDescription & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForType(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

void TimeSeriesColumnsValidator::validateColumnForUnit(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

void TimeSeriesColumnsValidator::validateColumnForHelp(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

}

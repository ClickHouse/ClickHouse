#include <Storages/System/StorageSystemQuotaUsage.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Access/QuotaUsage.h>
#include <Access/Common/AccessFlags.h>
#include <base/range.h>


namespace DB
{

namespace
{
    void addValue(IColumn & out_column, NullMap & out_column_null_map, QuotaValue value, const QuotaTypeInfo & type_info)
    {
        out_column_null_map.push_back(false);
        if (type_info.output_as_float)
            static_cast<ColumnFloat64 &>(out_column).getData().push_back(double(value) / type_info.output_denominator);
        else
            static_cast<ColumnUInt64 &>(out_column).getData().push_back(value / type_info.output_denominator);
    }

    void addValue(IColumn & out_column, NullMap & out_column_null_map, std::optional<QuotaValue> value, const QuotaTypeInfo & type_info)
    {
        if (value)
            addValue(out_column, out_column_null_map, *value, type_info);
        else
        {
            out_column_null_map.push_back(true);
            out_column.insertDefault();
        }
    }
}


NamesAndTypesList StorageSystemQuotaUsage::getNamesAndTypes()
{
    return getNamesAndTypesImpl(/* add_column_is_current = */ false);
}

NamesAndTypesList StorageSystemQuotaUsage::getNamesAndTypesImpl(bool add_column_is_current)
{
    NamesAndTypesList names_and_types{
        {"quota_name", std::make_shared<DataTypeString>()},
        {"quota_key", std::make_shared<DataTypeString>()}
    };

    if (add_column_is_current)
        names_and_types.push_back({"is_current", std::make_shared<DataTypeUInt8>()});

    names_and_types.push_back({"start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())});
    names_and_types.push_back({"end_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())});
    names_and_types.push_back({"duration", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())});

    for (auto quota_type : collections::range(QuotaType::MAX))
    {
        const auto & type_info = QuotaTypeInfo::get(quota_type);
        String column_name = type_info.name;
        DataTypePtr data_type;
        if (type_info.output_as_float)
            data_type = std::make_shared<DataTypeFloat64>();
        else
            data_type = std::make_shared<DataTypeUInt64>();
        names_and_types.push_back({column_name, std::make_shared<DataTypeNullable>(data_type)});
        names_and_types.push_back({String("max_") + column_name, std::make_shared<DataTypeNullable>(data_type)});
    }

    return names_and_types;
}


void StorageSystemQuotaUsage::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_QUOTAS);

    auto usage = context->getQuotaUsage();
    if (!usage)
        return;

    fillDataImpl(res_columns, context, /* add_column_is_current = */ false, {std::move(usage).value()});
}


void StorageSystemQuotaUsage::fillDataImpl(
    MutableColumns & res_columns,
    ContextPtr context,
    bool add_column_is_current,
    const std::vector<QuotaUsage> & quotas_usage)
{
    size_t column_index = 0;
    auto & column_quota_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_quota_key = assert_cast<ColumnString &>(*res_columns[column_index++]);

    ColumnUInt8::Container * column_is_current = nullptr;
    if (add_column_is_current)
        column_is_current = &assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto & column_start_time = assert_cast<ColumnUInt32 &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_start_time_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_end_time = assert_cast<ColumnUInt32 &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_end_time_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_duration = assert_cast<ColumnUInt32 &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_duration_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();

    IColumn * column_usage[static_cast<size_t>(QuotaType::MAX)];
    NullMap * column_usage_null_map[static_cast<size_t>(QuotaType::MAX)];
    IColumn * column_max[static_cast<size_t>(QuotaType::MAX)];
    NullMap * column_max_null_map[static_cast<size_t>(QuotaType::MAX)];
    for (auto quota_type : collections::range(QuotaType::MAX))
    {
        auto quota_type_i = static_cast<size_t>(quota_type);
        column_usage[quota_type_i] = &assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn();
        column_usage_null_map[quota_type_i] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
        column_max[quota_type_i] = &assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn();
        column_max_null_map[quota_type_i] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    }

    std::optional<UUID> current_quota_id;
    if (add_column_is_current)
    {
        if (auto current_usage = context->getQuotaUsage())
            current_quota_id = current_usage->quota_id;
    }

    auto add_row = [&](const String & quota_name, const UUID & quota_id, const String & quota_key, const QuotaUsage::Interval * interval)
    {
        column_quota_name.insertData(quota_name.data(), quota_name.length());
        column_quota_key.insertData(quota_key.data(), quota_key.length());

        if (add_column_is_current)
            column_is_current->push_back(quota_id == current_quota_id);

        if (!interval)
        {
            column_start_time.insertDefault();
            column_start_time_null_map.push_back(true);
            column_end_time.insertDefault();
            column_end_time_null_map.push_back(true);
            column_duration.insertDefault();
            column_duration_null_map.push_back(true);
            for (auto quota_type : collections::range(QuotaType::MAX))
            {
                auto quota_type_i = static_cast<size_t>(quota_type);
                column_usage[quota_type_i]->insertDefault();
                column_usage_null_map[quota_type_i]->push_back(true);
                column_max[quota_type_i]->insertDefault();
                column_max_null_map[quota_type_i]->push_back(true);
            }
            return;
        }

        time_t end_time = std::chrono::system_clock::to_time_t(interval->end_of_interval);
        UInt32 duration = static_cast<UInt32>(std::chrono::duration_cast<std::chrono::seconds>(interval->duration).count());
        time_t start_time = end_time - duration;
        column_start_time.getData().push_back(start_time);
        column_end_time.getData().push_back(end_time);
        column_duration.getData().push_back(duration);
        column_start_time_null_map.push_back(false);
        column_end_time_null_map.push_back(false);
        column_duration_null_map.push_back(false);

        for (auto quota_type : collections::range(QuotaType::MAX))
        {
            auto quota_type_i = static_cast<size_t>(quota_type);
            const auto & type_info = QuotaTypeInfo::get(quota_type);
            addValue(*column_max[quota_type_i], *column_max_null_map[quota_type_i], interval->max[quota_type_i], type_info);
            addValue(*column_usage[quota_type_i], *column_usage_null_map[quota_type_i], interval->used[quota_type_i], type_info);
        }
    };

    auto add_rows = [&](const String & quota_name, const UUID & quota_id, const String & quota_key, const std::vector<QuotaUsage::Interval> & intervals)
    {
        if (intervals.empty())
        {
            add_row(quota_name, quota_id, quota_key, nullptr);
            return;
        }

        for (const auto & interval : intervals)
            add_row(quota_name, quota_id, quota_key, &interval);
    };

    for (const auto & usage : quotas_usage)
        add_rows(usage.quota_name, usage.quota_id, usage.quota_key, usage.intervals);
}
}

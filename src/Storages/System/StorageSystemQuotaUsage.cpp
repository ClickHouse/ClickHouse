#include <Storages/System/StorageSystemQuotaUsage.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/QuotaUsage.h>
#include <Access/AccessFlags.h>
#include <ext/range.h>


namespace DB
{
using ResourceAmount = Quota::ResourceAmount;
using ResourceType = Quota::ResourceType;
using ResourceTypeInfo = Quota::ResourceTypeInfo;
constexpr auto MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;


namespace
{
    void addValue(IColumn & out_column, NullMap & out_column_null_map, ResourceAmount amount, const ResourceTypeInfo & type_info)
    {
        out_column_null_map.push_back(false);
        if (type_info.output_as_float)
            static_cast<ColumnFloat64 &>(out_column).getData().push_back(double(amount) / type_info.output_denominator);
        else
            static_cast<ColumnUInt64 &>(out_column).getData().push_back(amount / type_info.output_denominator);
    }

    void addValue(IColumn & out_column, NullMap & out_column_null_map, std::optional<ResourceAmount> amount, const ResourceTypeInfo & type_info)
    {
        if (amount)
            addValue(out_column, out_column_null_map, *amount, type_info);
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

    for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
    {
        const auto & type_info = ResourceTypeInfo::get(resource_type);
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


void StorageSystemQuotaUsage::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_QUOTAS);
    auto usage = context.getQuotaUsage();
    if (!usage)
        return;

    fillDataImpl(res_columns, context, /* add_column_is_current = */ false, {std::move(usage).value()});
}


void StorageSystemQuotaUsage::fillDataImpl(
    MutableColumns & res_columns,
    const Context & context,
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

    IColumn * column_usage[MAX_RESOURCE_TYPE];
    NullMap * column_usage_null_map[MAX_RESOURCE_TYPE];
    IColumn * column_max[MAX_RESOURCE_TYPE];
    NullMap * column_max_null_map[MAX_RESOURCE_TYPE];
    for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
    {
        column_usage[resource_type] = &assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn();
        column_usage_null_map[resource_type] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
        column_max[resource_type] = &assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn();
        column_max_null_map[resource_type] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    }

    std::optional<UUID> current_quota_id;
    if (add_column_is_current)
    {
        if (auto current_usage = context.getQuotaUsage())
            current_quota_id = current_usage->quota_id;
    }

    auto add_row = [&](const String & quota_name, const UUID & quota_id, const String & quota_key, const QuotaUsage::Interval * interval)
    {
        column_quota_name.insertData(quota_name.data(), quota_name.length());
        column_quota_key.insertData(quota_key.data(), quota_key.length());

        if (!interval)
        {
            column_start_time.insertDefault();
            column_start_time_null_map.push_back(true);
            column_end_time.insertDefault();
            column_end_time_null_map.push_back(true);
            column_duration.insertDefault();
            column_duration_null_map.push_back(true);
            for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
            {
                column_usage[resource_type]->insertDefault();
                column_usage_null_map[resource_type]->push_back(true);
                column_max[resource_type]->insertDefault();
                column_max_null_map[resource_type]->push_back(true);
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

        for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
        {
            const auto & type_info = ResourceTypeInfo::get(resource_type);
            addValue(*column_max[resource_type], *column_max_null_map[resource_type], interval->max[resource_type], type_info);
            addValue(*column_usage[resource_type], *column_usage_null_map[resource_type], interval->used[resource_type], type_info);
        }

        if (add_column_is_current)
            column_is_current->push_back(quota_id == current_quota_id);
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

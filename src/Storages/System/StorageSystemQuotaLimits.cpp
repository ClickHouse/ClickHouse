#include <Storages/System/StorageSystemQuotaLimits.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Access/Quota.h>
#include <Access/Common/AccessFlags.h>
#include <base/range.h>
#include <boost/range/algorithm_ext/push_back.hpp>


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


NamesAndTypesList StorageSystemQuotaLimits::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"quota_name", std::make_shared<DataTypeString>()},
        {"duration", std::make_shared<DataTypeUInt32>()},
        {"is_randomized_interval", std::make_shared<DataTypeUInt8>()},
    };

    for (auto resource_type : collections::range(MAX_RESOURCE_TYPE))
    {
        const auto & type_info = ResourceTypeInfo::get(resource_type);
        String column_name = "max_" + type_info.name;
        DataTypePtr data_type;
        if (type_info.output_as_float)
            data_type = std::make_shared<DataTypeFloat64>();
        else
            data_type = std::make_shared<DataTypeUInt64>();
        names_and_types.push_back({column_name, std::make_shared<DataTypeNullable>(data_type)});
    }

    return names_and_types;
}


void StorageSystemQuotaLimits::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_QUOTAS);
    const auto & access_control = context->getAccessControl();
    std::vector<UUID> ids = access_control.findAll<Quota>();

    size_t column_index = 0;
    auto & column_quota_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_duration = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();
    auto & column_is_randomized_interval = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    IColumn * column_max[MAX_RESOURCE_TYPE];
    NullMap * column_max_null_map[MAX_RESOURCE_TYPE];
    for (auto resource_type : collections::range(MAX_RESOURCE_TYPE))
    {
        column_max[resource_type] = &assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn();
        column_max_null_map[resource_type] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    }

    auto add_row = [&](const String & quota_name, const Quota::Limits & limits)
    {
        column_quota_name.insertData(quota_name.data(), quota_name.length());
        column_duration.push_back(limits.duration.count());
        column_is_randomized_interval.push_back(limits.randomize_interval);

        for (auto resource_type : collections::range(MAX_RESOURCE_TYPE))
        {
            const auto & type_info = ResourceTypeInfo::get(resource_type);
            addValue(*column_max[resource_type], *column_max_null_map[resource_type], limits.max[resource_type], type_info);
        }
    };

    auto add_rows = [&](const String & quota_name, const std::vector<Quota::Limits> & all_limits)
    {
        for (const auto & limits : all_limits)
            add_row(quota_name, limits);
    };

    for (const auto & id : ids)
    {
        auto quota = access_control.tryRead<Quota>(id);
        if (!quota)
            continue;

        add_rows(quota->getName(), quota->all_limits);
    }
}
}

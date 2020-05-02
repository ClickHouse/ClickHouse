#include <Storages/System/StorageSystemQuotaUsage.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsageInfo.h>
#include <Access/AccessFlags.h>
#include <ext/range.h>


namespace DB
{
NamesAndTypesList StorageSystemQuotaUsage::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"key", std::make_shared<DataTypeString>()},
        {"duration", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"end_of_interval", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())}};

    for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
    {
        DataTypePtr data_type;
        if (resource_type == Quota::EXECUTION_TIME)
            data_type = std::make_shared<DataTypeFloat64>();
        else
            data_type = std::make_shared<DataTypeUInt64>();

        String column_name = Quota::resourceTypeToColumnName(resource_type);
        names_and_types.push_back({column_name, std::make_shared<DataTypeNullable>(data_type)});
        names_and_types.push_back({String("max_") + column_name, std::make_shared<DataTypeNullable>(data_type)});
    }
    return names_and_types;
}


void StorageSystemQuotaUsage::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_QUOTAS);
    const auto & access_control = context.getAccessControlManager();

    for (const auto & info : access_control.getQuotaUsageInfo())
    {
        for (const auto & interval : info.intervals)
        {
            size_t i = 0;
            res_columns[i++]->insert(info.quota_name);
            res_columns[i++]->insert(info.quota_id);
            res_columns[i++]->insert(info.quota_key);
            res_columns[i++]->insert(std::chrono::seconds{interval.duration}.count());
            res_columns[i++]->insert(std::chrono::system_clock::to_time_t(interval.end_of_interval));
            for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
            {
                if (resource_type == Quota::EXECUTION_TIME)
                {
                    res_columns[i++]->insert(Quota::executionTimeToSeconds(interval.used[resource_type]));
                    res_columns[i++]->insert(Quota::executionTimeToSeconds(interval.max[resource_type]));
                }
                else
                {
                    res_columns[i++]->insert(interval.used[resource_type]);
                    res_columns[i++]->insert(interval.max[resource_type]);
                }
            }
        }

        if (info.intervals.empty())
        {
            size_t i = 0;
            res_columns[i++]->insert(info.quota_name);
            res_columns[i++]->insert(info.quota_id);
            res_columns[i++]->insert(info.quota_key);
            for (size_t j = 0; j != Quota::MAX_RESOURCE_TYPE * 2 + 2; ++j)
                res_columns[i++]->insertDefault();
        }
    }
}
}

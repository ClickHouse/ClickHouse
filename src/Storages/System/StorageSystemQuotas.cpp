#include <Storages/System/StorageSystemQuotas.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/Quota.h>
#include <Access/AccessFlags.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    DataTypeEnum8::Values getKeyTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto key_type : ext::range(Quota::KeyType::MAX))
            enum_values.push_back({Quota::getNameOfKeyType(key_type), static_cast<UInt8>(key_type)});
        return enum_values;
    }
}


NamesAndTypesList StorageSystemQuotas::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"source", std::make_shared<DataTypeString>()},
        {"key_type", std::make_shared<DataTypeEnum8>(getKeyTypeEnumValues())},
        {"roles", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"intervals.duration", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"intervals.randomize_interval", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>())}};

    for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
    {
        DataTypePtr data_type;
        if (resource_type == Quota::EXECUTION_TIME)
            data_type = std::make_shared<DataTypeFloat64>();
        else
            data_type = std::make_shared<DataTypeUInt64>();

        String column_name = String("intervals.max_") + Quota::resourceTypeToColumnName(resource_type);
        names_and_types.push_back({column_name, std::make_shared<DataTypeArray>(data_type)});
    }
    return names_and_types;
}


void StorageSystemQuotas::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_QUOTAS);

    size_t i = 0;
    auto & name_column = *res_columns[i++];
    auto & id_column = *res_columns[i++];
    auto & storage_name_column = *res_columns[i++];
    auto & key_type_column = *res_columns[i++];
    auto & roles_data = assert_cast<ColumnArray &>(*res_columns[i]).getData();
    auto & roles_offsets = assert_cast<ColumnArray &>(*res_columns[i++]).getOffsets();
    auto & durations_data = assert_cast<ColumnArray &>(*res_columns[i]).getData();
    auto & durations_offsets = assert_cast<ColumnArray &>(*res_columns[i++]).getOffsets();
    auto & randomize_intervals_data = assert_cast<ColumnArray &>(*res_columns[i]).getData();
    auto & randomize_intervals_offsets = assert_cast<ColumnArray &>(*res_columns[i++]).getOffsets();
    IColumn * limits_data[Quota::MAX_RESOURCE_TYPE];
    ColumnArray::Offsets * limits_offsets[Quota::MAX_RESOURCE_TYPE];
    for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
    {
        limits_data[resource_type] = &assert_cast<ColumnArray &>(*res_columns[i]).getData();
        limits_offsets[resource_type] = &assert_cast<ColumnArray &>(*res_columns[i++]).getOffsets();
    }

    const auto & access_control = context.getAccessControlManager();
    for (const auto & id : access_control.findAll<Quota>())
    {
        auto quota = access_control.tryRead<Quota>(id);
        if (!quota)
            continue;
        const auto * storage = access_control.findStorage(id);
        String storage_name = storage ? storage->getStorageName() : "";

        name_column.insert(quota->getName());
        id_column.insert(id);
        storage_name_column.insert(storage_name);
        key_type_column.insert(static_cast<UInt8>(quota->key_type));

        for (const String & role : quota->to_roles.toStringsWithNames(access_control))
            roles_data.insert(role);
        roles_offsets.push_back(roles_data.size());

        for (const auto & limits : quota->all_limits)
        {
            durations_data.insert(std::chrono::seconds{limits.duration}.count());
            randomize_intervals_data.insert(static_cast<UInt8>(limits.randomize_interval));
            for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
            {
                if (resource_type == Quota::EXECUTION_TIME)
                    limits_data[resource_type]->insert(Quota::executionTimeToSeconds(limits.max[resource_type]));
                else
                    limits_data[resource_type]->insert(limits.max[resource_type]);
            }
        }

        durations_offsets.push_back(durations_data.size());
        randomize_intervals_offsets.push_back(randomize_intervals_data.size());
        for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
            limits_offsets[resource_type]->push_back(limits_data[resource_type]->size());
    }
}
}

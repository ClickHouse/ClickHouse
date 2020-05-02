#include <Storages/System/StorageSystemRowPolicies.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/RowPolicy.h>
#include <Access/AccessFlags.h>
#include <ext/range.h>


namespace DB
{
NamesAndTypesList StorageSystemRowPolicies::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"short_name", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"source", std::make_shared<DataTypeString>()},
        {"restrictive", std::make_shared<DataTypeUInt8>()},
    };

    for (auto index : ext::range(RowPolicy::MAX_CONDITION_TYPE))
        names_and_types.push_back({RowPolicy::conditionTypeToColumnName(index), std::make_shared<DataTypeString>()});

    return names_and_types;
}


void StorageSystemRowPolicies::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_ROW_POLICIES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<RowPolicy>();

    for (const auto & id : ids)
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy)
            continue;
        const auto * storage = access_control.findStorage(id);

        size_t i = 0;
        res_columns[i++]->insert(policy->getDatabase());
        res_columns[i++]->insert(policy->getTableName());
        res_columns[i++]->insert(policy->getShortName());
        res_columns[i++]->insert(policy->getName());
        res_columns[i++]->insert(id);
        res_columns[i++]->insert(storage ? storage->getStorageName() : "");
        res_columns[i++]->insert(policy->isRestrictive());

        for (auto index : ext::range(RowPolicy::MAX_CONDITION_TYPE))
            res_columns[i++]->insert(policy->conditions[index]);
    }
}
}

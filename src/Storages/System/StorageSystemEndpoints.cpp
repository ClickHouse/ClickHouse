#include <Storages/System/StorageSystemEndpoints.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterMetadataManager.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool format_display_secrets_in_show_and_select;
}

ColumnsDescription StorageSystemEndpoints::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the SQL endpoint (`CREATE ENDPOINT`)."},
        {"host", std::make_shared<DataTypeString>(), "Remote host of the endpoint."},
        {"port", std::make_shared<DataTypeUInt16>(), "TCP port of the endpoint."},
        {"user", std::make_shared<DataTypeString>(), "User for the endpoint connection."},
        {"password", std::make_shared<DataTypeString>(), "Password for the endpoint connection (masked without secret access)."},
        {"secure", std::make_shared<DataTypeUInt8>(), "Whether SSL/TLS is enabled for the endpoint."},
        {"compression", std::make_shared<DataTypeUInt8>(), "Whether compression is enabled for the endpoint."},
        {"priority", std::make_shared<DataTypeInt64>(), "Load-balancing priority of the endpoint."},
        {"bind_host", std::make_shared<DataTypeString>(), "Optional source bind host for the endpoint."},
        {"default_database", std::make_shared<DataTypeString>(), "Optional default database for the endpoint."},
        {
            "bound_shards",
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "SQL shard names that reference this endpoint.",
        },
    };
}

void StorageSystemEndpoints::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemEndpoints::fillData");

    const auto & access = context->getAccess();
    const bool show_secrets = access->isGranted(AccessType::displaySecretsInShowAndSelect)
        && context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select]
        && context->displaySecretsInShowAndSelect();

    for (const auto & endpoint_row : ClusterMetadataManager::instance().listEndpointsForSystemTable())
    {
        const auto & endpoint = endpoint_row.endpoint;
        res_columns[0]->insert(endpoint_row.name);
        res_columns[1]->insert(endpoint.host);
        res_columns[2]->insert(endpoint.port);
        res_columns[3]->insert(endpoint.user);
        res_columns[4]->insert(show_secrets ? endpoint.password : String{"[HIDDEN]"});
        res_columns[5]->insert(static_cast<UInt8>(endpoint.secure ? 1 : 0));
        res_columns[6]->insert(static_cast<UInt8>(endpoint.compression ? 1 : 0));
        res_columns[7]->insert(endpoint.priority);
        res_columns[8]->insert(endpoint.bind_host);
        res_columns[9]->insert(endpoint.default_database);

        Array bound_shards;
        bound_shards.reserve(endpoint_row.bound_shards.size());
        for (const auto & shard_name : endpoint_row.bound_shards)
            bound_shards.push_back(shard_name);
        res_columns[10]->insert(bound_shards);
    }
}

}

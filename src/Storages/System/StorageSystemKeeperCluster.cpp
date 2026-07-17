#include <Storages/System/StorageSystemKeeperCluster.h>

#include "config.h"

#if USE_NURAFT

#include <Coordination/KeeperDispatcher.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>

namespace DB
{

ColumnsDescription StorageSystemKeeperCluster::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"server_id", std::make_shared<DataTypeInt32>(), "Raft server id of this cluster member."},
        {"host", std::make_shared<DataTypeString>(), "Host parsed from the endpoint (prefix before the last colon)."},
        {"endpoint", std::make_shared<DataTypeString>(), "Raw Raft endpoint as configured (host:port)."},
        {"is_observer", DataTypeFactory::instance().get("Bool"), "True if this member is a non-voting observer."},
        {"priority", std::make_shared<DataTypeInt32>(), "Raft priority of this member; higher values are preferred during leader election."},
        {"is_leader", DataTypeFactory::instance().get("Bool"), "True if this member is the current Raft leader."},
        {"is_self", DataTypeFactory::instance().get("Bool"), "True if this row describes the local Keeper node."},
        {"last_log_index", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Highest log index in the local Raft log store on this node. Populated only on the row matching the current node (is_self = true), NULL for other rows."},
    };
}

namespace
{

String extractHost(const String & endpoint)
{
    auto pos = endpoint.rfind(':');
    if (pos == String::npos)
        return endpoint;
    return endpoint.substr(0, pos);
}

}

void StorageSystemKeeperCluster::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto dispatcher = context->tryGetKeeperDispatcher();
    if (!dispatcher)
        return;

    auto members = dispatcher->getClusterMembersInfo();
    for (const auto & m : members)
    {
        size_t i = 0;
        res_columns[i++]->insert(m.server_id);
        res_columns[i++]->insert(extractHost(m.endpoint));
        res_columns[i++]->insert(m.endpoint);
        res_columns[i++]->insert(static_cast<UInt8>(m.is_observer));
        res_columns[i++]->insert(m.priority);
        res_columns[i++]->insert(static_cast<UInt8>(m.is_leader));
        res_columns[i++]->insert(static_cast<UInt8>(m.is_self));
        if (m.last_log_index.has_value())
            res_columns[i++]->insert(*m.last_log_index);
        else
            res_columns[i++]->insertDefault();
    }
}

}

#endif

#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/System/StorageSystemZooKeeperConnection.h>

namespace DB
{
struct ContextSharedPart;

NamesAndTypesList StorageSystemZooKeeperConnection::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"host", std::make_shared<DataTypeString>()},
        {"port", std::make_shared<DataTypeString>()},
        {"index", std::make_shared<DataTypeUInt8>()},
        {"connected_time", std::make_shared<DataTypeDateTime>()},
        {"is_expired", std::make_shared<DataTypeUInt8>()},
        {"keeper_api_version", std::make_shared<DataTypeUInt8>()},
        {"client_id", std::make_shared<DataTypeInt64>()}
    };
}

void StorageSystemZooKeeperConnection::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    res_columns[0]->insert("default_zookeeper");
    res_columns[1]->insert(context->getZooKeeper()->getConnectedZooKeeperHost());
    res_columns[2]->insert(context->getZooKeeper()->getConnectedZooKeeperPort());
    res_columns[3]->insert(context->getZooKeeper()->getConnectedZooKeeperIndex());
    res_columns[4]->insert(context->getZooKeeperSessionUptime());
    res_columns[5]->insert(context->getZooKeeper()->expired());
    res_columns[6]->insert(context->getZooKeeper()->getApiVersion());
    res_columns[7]->insert(context->getZooKeeper()->getClientID());

    std::map<String, zkutil::ZooKeeperPtr>::iterator iter;

    for (iter = context->getAuxiliaryZooKeepers().begin(); iter !=
         context->getAuxiliaryZooKeepers().end(); iter ++)
    {
        res_columns[0]->insert(iter->first);
        res_columns[1]->insert(iter->second->getConnectedZooKeeperHost());
        res_columns[1]->insert(iter->second->getConnectedZooKeeperHost());
        res_columns[2]->insert(iter->second->getConnectedZooKeeperPort());
        res_columns[3]->insert(iter->second->getConnectedZooKeeperIndex());
        res_columns[4]->insert(iter->second->getSessionUptime());
        res_columns[5]->insert(iter->second->expired());
        res_columns[6]->insert(iter->second->getApiVersion());
        res_columns[7]->insert(iter->second->getClientID());
    }

}

}


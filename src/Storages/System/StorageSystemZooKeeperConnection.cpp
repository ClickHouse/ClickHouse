#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/System/StorageSystemZooKeeperConnection.h>

namespace DB
{

NamesAndTypesList StorageSystemZooKeeperConnection::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"host", std::make_shared<DataTypeString>()},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"index", std::make_shared<DataTypeUInt8>()},
        {"connected_time", std::make_shared<DataTypeDateTime>()},
        {"is_expired", std::make_shared<DataTypeUInt8>()},
        {"keeper_api_version", std::make_shared<DataTypeUInt8>()},
        {"client_id", std::make_shared<DataTypeInt64>()}
    };
}

void StorageSystemZooKeeperConnection::fillData(MutableColumns & res_columns, ContextPtr context,
    const SelectQueryInfo &) const
{
    res_columns[0]->insert("default");
    res_columns[1]->insert(context->getZooKeeper()->getConnectedZooKeeperHost());
    res_columns[2]->insert(context->getZooKeeper()->getConnectedZooKeeperPort());
    res_columns[3]->insert(context->getZooKeeper()->getConnectedZooKeeperIndex());
    res_columns[4]->insert(context->getZooKeeperSessionUptime());
    res_columns[5]->insert(context->getZooKeeper()->expired());
    res_columns[6]->insert(static_cast<uint8_t>(KeeperApiVersion::WITH_MULTI_READ));
    res_columns[7]->insert(context->getZooKeeper()->getClientID());

    for (const auto & elem : context->getAuxiliaryZooKeepers())
    {
        res_columns[0]->insert(elem.first);
        res_columns[1]->insert(elem.second->getConnectedZooKeeperHost());
        res_columns[2]->insert(elem.second->getConnectedZooKeeperPort());
        res_columns[3]->insert(elem.second->getConnectedZooKeeperIndex());
        res_columns[4]->insert(elem.second->getSessionUptime());
        res_columns[5]->insert(elem.second->expired());
        res_columns[6]->insert(static_cast<uint8_t>(KeeperApiVersion::WITH_MULTI_READ));
        res_columns[7]->insert(elem.second->getClientID());
    }

}

}

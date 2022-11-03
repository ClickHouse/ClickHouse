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
        {"zookeeper_name", std::make_shared<DataTypeString>()},
        {"zookeeper_address", std::make_shared<DataTypeString>()},
        {"zookeeper_index", std::make_shared<DataTypeUInt8>()},
        {"zookeeper_connected_time", std::make_shared<DataTypeDateTime>()},
    };
}

void StorageSystemZooKeeperConnection::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->fillZkConnectionInfo(res_columns);
}

}


#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemZooKeeperWatches.h>
#include <magic_enum.hpp>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

namespace
{

using namespace DB;

DataTypePtr getOpNumEnumType()
{
    DataTypeEnum16::Values values;
    values.reserve(magic_enum::enum_count<Coordination::OpNum>());
    for (const auto & [op, name] : magic_enum::enum_entries<Coordination::OpNum>())
        values.emplace_back(std::string{name}, static_cast<Int16>(op));
    return std::make_shared<DataTypeEnum16>(std::move(values));
}

DataTypePtr getWatchTypeEnumType()
{
    return std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Children", 0},
        {"Exists", 1},
        {"Data", 2},
    });
}

enum class WatchType : UInt8
{
    Children = 0,
    Exists = 1,
    Data = 2,
};

constexpr inline WatchType toWatchType(Coordination::OpNum op) noexcept
{
    using O = Coordination::OpNum;
    switch (op)
    {
        case O::Exists: return WatchType::Exists; // Exists
        case O::Get: return WatchType::Data; // Data
        case O::List:
        case O::SimpleList:
        case O::FilteredList: return WatchType::Children; // Children
        // Note: MultiRead handled at sub-request level; default to Data if unknown
        default: return WatchType::Data;
    }
}

}

namespace DB
{

ColumnsDescription StorageSystemZooKeeperWatches::getColumnsDescription()
{
    auto opnum_enum = getOpNumEnumType();
    auto watch_type_enum = getWatchTypeEnumType();

    return ColumnsDescription{
        /* 0 */ {"zookeeper_name", std::make_shared<DataTypeString>(), "Name of current zookeeper connection (default by default)"},
        /* 1 */ {"create_time", std::make_shared<DataTypeDateTime>(), "When watch was created"},
        /* 2 */ {"path", std::make_shared<DataTypeString>(), "Observed path"},
        /* 3 */ {"session_id", std::make_shared<DataTypeInt64>(), "Session which created watch"},
        /* 4 */ {"request_xid", std::make_shared<DataTypeInt64>(), "Request_xid which created watch"},
        /* 5 */ {"op_num", opnum_enum, "Operation which created watch"},
        /* 6 */ {"watch_type", watch_type_enum, "Type of watch"},
    };
}

void StorageSystemZooKeeperWatches::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto fill_data = [&](const std::string & name, const zkutil::ZooKeeperPtr & zookeeper)
    {
        for (const auto & snapshot : zookeeper->getWatchesSnapshot())
        {
            res_columns[0]->insert(name);
            res_columns[1]->insert(snapshot.create_time);
            res_columns[2]->insert(snapshot.path);
            res_columns[3]->insert(snapshot.session_id);
            res_columns[4]->insert(snapshot.request_xid);
            res_columns[5]->insert(static_cast<Int16>(snapshot.op_num));
            res_columns[6]->insert(static_cast<UInt8>(toWatchType(snapshot.op_num)));
        }
    };

    fill_data(std::string(zkutil::DEFAULT_ZOOKEEPER_NAME), context->getZooKeeper());
    for (const auto & [name, zk] : context->getAuxiliaryZooKeepers())
        fill_data(name, zk);
}

}

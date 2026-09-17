#include <chrono>
#include <Storages/System/StorageSystemZooKeeperWatches.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/SystemTablesDataTypes.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>


namespace DB
{

ColumnsDescription StorageSystemZooKeeperWatches::getColumnsDescription()
{
    auto watch_type = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Children", static_cast<Int8>(1)},
        {"Exists", static_cast<Int8>(2)},
        {"Data", static_cast<Int8>(3)},
        {"Unexpected", static_cast<Int8>(-1)},
    });

    return ColumnsDescription{
        {"zookeeper_name",
         std::make_shared<DataTypeString>(),
         "Name of the ZooKeeper connection (\"default\" for the main connection, or the auxiliary name)."},
        {"create_time", std::make_shared<DataTypeDateTime>(), "Time when the watch was created."},
        {"create_time_microseconds",
         std::make_shared<DataTypeDateTime64>(6),
         "Time when the watch was created with microsecond precision."},
        {"path", std::make_shared<DataTypeString>(), "ZooKeeper path being watched."},
        {"session_id", std::make_shared<DataTypeInt64>(), "Session ID of the connection that registered the watch."},
        {"request_xid", std::make_shared<DataTypeInt64>(), "XID of the request that created the watch."},
        {"op_num", Coordination::SystemTablesDataTypes::operationEnum(), "Operation that created the watch."},
        {"watch_type", std::move(watch_type), "Watch type: Children (list), Exists, or Data (get)."},
    };
}

static Int8 watchTypeFromOpNum(Coordination::OpNum op_num)
{
    switch (op_num)
    {
        case Coordination::OpNum::List:
        case Coordination::OpNum::SimpleList:
        case Coordination::OpNum::FilteredList:
        case Coordination::OpNum::FilteredListWithStatsAndData:
            return 1; /// Children
        case Coordination::OpNum::Exists:
            return 2; /// Exists
        case Coordination::OpNum::Get:
            return 3; /// Data
        default:
            return -1;
    }
}

void StorageSystemZooKeeperWatches::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto fill = [&](const std::string_view name, const zkutil::ZooKeeperPtr & zookeeper)
    {
        using namespace std::chrono;

        auto zk_args = zookeeper->getArgs();

        for (const auto & [path, watches] : zookeeper->getWatchesSnapshot())
        {
            auto full_path = path;
            if (!zk_args.chroot.empty())
                full_path = path == "/" ? zk_args.chroot : zk_args.chroot + path;

            for (const auto & watch_info : watches)
            {
                auto create_ts = watch_info.create_time.time_since_epoch();
                auto create_ts_sec = duration_cast<seconds>(create_ts).count();
                auto create_ts_mcsec = Decimal64(duration_cast<microseconds>(create_ts).count());

                res_columns[0]->insert(name);
                res_columns[1]->insert(create_ts_sec);
                res_columns[2]->insert(create_ts_mcsec);
                res_columns[3]->insert(full_path);
                res_columns[4]->insert(zookeeper->getClientID());
                res_columns[5]->insert(watch_info.request_xid);
                res_columns[6]->insert(static_cast<Int16>(watch_info.op_num));
                res_columns[7]->insert(watchTypeFromOpNum(watch_info.op_num));
            }
        }
    };

    fill("default", context->getZooKeeper());

    for (const auto & [aux_name, aux_zk] : context->getAuxiliaryZooKeepers())
        fill(aux_name, aux_zk);
}

}

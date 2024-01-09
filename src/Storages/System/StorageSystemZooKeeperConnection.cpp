#include <Interpreters/Context.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Coordination/KeeperFeatureFlags.h>
#include <Storages/System/StorageSystemZooKeeperConnection.h>

namespace DB
{

NamesAndTypesList StorageSystemZooKeeperConnection::getNamesAndTypes()
{
    DataTypeEnum16::Values feature_flags_enum_values;
    feature_flags_enum_values.reserve(magic_enum::enum_count<KeeperFeatureFlag>());
    for (const auto & [feature_flag, feature_flag_string] : magic_enum::enum_entries<KeeperFeatureFlag>())
        feature_flags_enum_values.push_back(std::pair{std::string{feature_flag_string}, static_cast<Int16>(feature_flag)});

    auto feature_flags_enum = std::make_shared<DataTypeEnum16>(std::move(feature_flags_enum_values));

    return {
        /* 0 */ {"name", std::make_shared<DataTypeString>()},
        /* 1 */ {"host", std::make_shared<DataTypeString>()},
        /* 2 */ {"port", std::make_shared<DataTypeUInt16>()},
        /* 3 */ {"index", std::make_shared<DataTypeUInt8>()},
        /* 4 */ {"connected_time", std::make_shared<DataTypeDateTime>()},
        /* 5 */ {"session_uptime_elapsed_seconds", std::make_shared<DataTypeUInt64>()},
        /* 6 */ {"is_expired", std::make_shared<DataTypeUInt8>()},
        /* 7 */ {"keeper_api_version", std::make_shared<DataTypeUInt8>()},
        /* 8 */ {"client_id", std::make_shared<DataTypeInt64>()},
        /* 9 */ {"xid", std::make_shared<DataTypeInt32>()},
        /* 10*/ {"enabled_feature_flags", std::make_shared<DataTypeArray>(std::move(feature_flags_enum))}
    };
}

void StorageSystemZooKeeperConnection::fillData(MutableColumns & res_columns, ContextPtr context,
    const SelectQueryInfo &) const
{
    const auto add_enabled_feature_flags = [&](const auto & zookeeper)
    {
        Array enabled_feature_flags;
        const auto * feature_flags = zookeeper->getKeeperFeatureFlags();
        if (feature_flags)
        {
            for (const auto & feature_flag : magic_enum::enum_values<KeeperFeatureFlag>())
            {
                if (feature_flags->isEnabled(feature_flag))
                {
                    enabled_feature_flags.push_back(feature_flag);
                }
            }
        }
        res_columns[10]->insert(std::move(enabled_feature_flags));
    };

    /// For read-only snapshot type functionality, it's acceptable even though 'getZooKeeper' may cause data inconsistency.
    auto fill_data = [&](const String & name, const zkutil::ZooKeeperPtr zookeeper, MutableColumns & columns)
    {
        Int8 index = zookeeper->getConnectedHostIdx();
        String host_port = zookeeper->getConnectedHostPort();
        if (index != -1 && !host_port.empty())
        {
            size_t offset = host_port.find_last_of(':');
            String host = host_port.substr(0, offset);
            UInt16 port = static_cast<UInt16>(Poco::NumberParser::parseUnsigned(host_port.substr(offset + 1)));

            UInt32 uptime = zookeeper->getSessionUptime();
            time_t connected_time = time(nullptr) - uptime;

            columns[0]->insert(name);
            columns[1]->insert(host);
            columns[2]->insert(port);
            columns[3]->insert(index);
            columns[4]->insert(connected_time);
            columns[5]->insert(uptime);
            columns[6]->insert(zookeeper->expired());
            columns[7]->insert(0);
            columns[8]->insert(zookeeper->getClientID());
            columns[9]->insert(zookeeper->getConnectionXid());
            add_enabled_feature_flags(zookeeper);
        }
    };

    /// default zookeeper.
    fill_data("default", context->getZooKeeper(), res_columns);

    /// auxiliary zookeepers.
    for (const auto & elem : context->getAuxiliaryZooKeepers())
    {
        fill_data(elem.first, elem.second, res_columns);
    }
}

}

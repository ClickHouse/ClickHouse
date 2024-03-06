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
        {"name", std::make_shared<DataTypeString>()},
        {"host", std::make_shared<DataTypeString>()},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"index", std::make_shared<DataTypeUInt8>()},
        {"connected_time", std::make_shared<DataTypeDateTime>()},
        {"session_uptime_elapsed_seconds", std::make_shared<DataTypeUInt64>()},
        {"is_expired", std::make_shared<DataTypeUInt8>()},
        {"keeper_api_version", std::make_shared<DataTypeUInt8>()},
        {"client_id", std::make_shared<DataTypeInt64>()},
        {"enabled_feature_flags", std::make_shared<DataTypeArray>(std::move(feature_flags_enum))}
    };
}

void StorageSystemZooKeeperConnection::fillData(MutableColumns & res_columns, ContextPtr context,
    const SelectQueryInfo &) const
{
    res_columns[0]->insert("default");
    res_columns[1]->insert(context->getZooKeeper()->getConnectedZooKeeperHost());
    res_columns[2]->insert(context->getZooKeeper()->getConnectedZooKeeperPort());
    res_columns[3]->insert(context->getZooKeeper()->getConnectedZooKeeperIndex());
    res_columns[4]->insert(context->getZooKeeper()->getConnectedTime());
    res_columns[5]->insert(context->getZooKeeperSessionUptime());
    res_columns[6]->insert(context->getZooKeeper()->expired());
    res_columns[7]->insert(0);
    res_columns[8]->insert(context->getZooKeeper()->getClientID());

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
        res_columns[9]->insert(std::move(enabled_feature_flags));
    };

    add_enabled_feature_flags(context->getZooKeeper());

    for (const auto & elem : context->getAuxiliaryZooKeepers())
    {
        res_columns[0]->insert(elem.first);
        res_columns[1]->insert(elem.second->getConnectedZooKeeperHost());
        res_columns[2]->insert(elem.second->getConnectedZooKeeperPort());
        res_columns[3]->insert(elem.second->getConnectedZooKeeperIndex());
        res_columns[4]->insert(elem.second->getConnectedTime());
        res_columns[5]->insert(elem.second->getSessionUptime());
        res_columns[6]->insert(elem.second->expired());
        res_columns[7]->insert(0);
        res_columns[8]->insert(elem.second->getClientID());
        add_enabled_feature_flags(elem.second);
    }

}

}

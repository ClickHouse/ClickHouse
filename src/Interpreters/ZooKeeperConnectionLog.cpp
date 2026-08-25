#include <Interpreters/ZooKeeperConnectionLog.h>
#include <Common/SystemTableDocumentation.h>

#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/NumberParser.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUTImpl.h>
#include <Common/ZooKeeper/KeeperFeatureFlags.h>


namespace DB
{

ColumnsDescription ZooKeeperConnectionLogElement::getColumnsDescription()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Connected",         static_cast<Int8>(EventType::Connected)},
            {"Disconnected",    static_cast<Int8>(EventType::Disconnected)},
        });

    DataTypeEnum16::Values feature_flags_enum_values;
    feature_flags_enum_values.reserve(magic_enum::enum_count<KeeperFeatureFlag>());
    for (const auto & [feature_flag, feature_flag_string] : magic_enum::enum_entries<KeeperFeatureFlag>())
        feature_flags_enum_values.push_back(std::pair{std::string{feature_flag_string}, static_cast<Int16>(feature_flag)});

    auto feature_flags_enum = std::make_shared<DataTypeEnum16>(std::move(feature_flags_enum_values));

    return ColumnsDescription{
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server which is connected to or disconnected from ZooKeeper."},
        {"type", std::move(type_enum), "The type of the event. Possible values: Connected, Disconnected."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time of the entry"},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Time of the entry with microseconds precision."},
        {"name", std::make_shared<DataTypeString>(), "ZooKeeper cluster's name."},
        {"host", std::make_shared<DataTypeString>(), "The hostname/IP of the ZooKeeper node that ClickHouse connected to or disconnected from."},
        {"port", std::make_shared<DataTypeUInt16>(), "The port of the ZooKeeper node that ClickHouse connected to or disconnected from."},
        {"index", std::make_shared<DataTypeUInt8>(), "The index of the ZooKeeper node that ClickHouse connected to or disconnected from. The index is from ZooKeeper config."},
        {"client_id", std::make_shared<DataTypeInt64>(), "Session id of the connection."},
        {"keeper_api_version", std::make_shared<DataTypeUInt8>(), "Keeper API version."},
        {"enabled_feature_flags", std::make_shared<DataTypeArray>(std::move(feature_flags_enum)), "Feature flags which are enabled. Only applicable to ClickHouse Keeper."},
        {"availability_zone", std::make_shared<DataTypeString>(), "Availability zone"},
        {"reason", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Reason for the connection or disconnection."}, // Updated field
    };
}

void ZooKeeperConnectionLog::addConnected(const std::string_view name, const zkutil::ZooKeeper & zookeeper, const std::string_view reason)
{
    addWithEventType(ZooKeeperConnectionLogElement::EventType::Connected, name, zookeeper, reason);
}

void ZooKeeperConnectionLog::addDisconnected(
    const std::string_view name, const zkutil::ZooKeeper & zookeeper, const std::string_view reason)
{
    addWithEventType(ZooKeeperConnectionLogElement::EventType::Disconnected, name, zookeeper, reason);
}

Array ZooKeeperConnectionLog::getEnabledFeatureFlags(const zkutil::ZooKeeper& zookeeper)
{
    Array enabled_feature_flags;
    const auto * feature_flags = zookeeper.getKeeperFeatureFlags();
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
    return enabled_feature_flags;
}

void ZooKeeperConnectionLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(event_type);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(cluster_name);
    columns[i++]->insert(host);
    columns[i++]->insert(port);

    if (index)
        columns[i++]->insert(index);
    else
        columns[i++]->insertDefault();

    columns[i++]->insert(client_id);
    columns[i++]->insert(keeper_api_version);
    columns[i++]->insert(enabled_feature_flags);
    columns[i++]->insert(availability_zone);
    columns[i++]->insert(reason);
}

void ZooKeeperConnectionLog::addWithEventType(
    ZooKeeperConnectionLogElement::EventType type,
    const std::string_view name,
    const zkutil::ZooKeeper & zookeeper,
    const std::string_view reason)
{
    add([&](ZooKeeperConnectionLogElement & element)
    {
    element.event_type = type;

    std::chrono::time_point<std::chrono::system_clock> current_time = std::chrono::system_clock::now();
    element.event_time = timeInSeconds(current_time);
    element.event_time_microseconds = timeInMicroseconds(current_time);

    element.cluster_name = name;

    const auto host_port = zookeeper.getConnectedHostPort();
    if (!host_port.empty())
    {
        const auto offset = host_port.find_last_of(':');
        element.host = host_port.substr(0, offset);
        element.port = static_cast<UInt16>(Poco::NumberParser::parseUnsigned(host_port.substr(offset + 1)));
    }
    const auto maybe_index = zookeeper.getConnectedHostIdx();
    chassert(maybe_index.has_value(), "Already connected ZooKeeper host index is not set");
    element.index = *maybe_index;
    element.keeper_api_version = 0;
    element.client_id = zookeeper.getClientID();
    element.enabled_feature_flags = getEnabledFeatureFlags(zookeeper);
    element.availability_zone = zookeeper.getConnectedHostAvailabilityZone();
    element.reason = reason;
    });
}
}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "zookeeper_connection_log",
    .description = R"DOCS_MD(
The 'system.zookeeper_connection_log' table shows the history of ZooKeeper connections (including auxiliary ZooKeepers). Each row shows information about one event regarding connections.

<Note>
The table doesn't contain events for disconnections caused by server shutdown.
</Note>
)DOCS_MD",
    .get_columns = ZooKeeperConnectionLogElement::getColumnsDescription,
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.zookeeper_connection_log;
```

```text
    ┌─hostname─┬─type─────────┬─event_date─┬──────────event_time─┬────event_time_microseconds─┬─name───────────────┬─host─┬─port─┬─index─┬─client_id─┬─keeper_api_version─┬─enabled_feature_flags───────────────────────────────────────────────────────────────────────┬─availability_zone─┬─reason──────────────┐
 1. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:35 │ 2025-05-12 19:49:35.713067 │ zk_conn_log_test_4 │ zoo2 │ 2181 │     0 │        10 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 2. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:23 │ 2025-05-12 19:49:23.981570 │ default            │ zoo1 │ 2181 │     0 │         4 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 3. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:28 │ 2025-05-12 19:49:28.104021 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 4. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.459251 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 5. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.574312 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 6. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909890 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 7. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909895 │ default            │ zoo2 │ 2181 │     0 │         8 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 8. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912010 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 9. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912014 │ zk_conn_log_test_2 │ zoo3 │ 2181 │     0 │         9 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
10. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912061 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Removed from config │
    └──────────┴──────────────┴────────────┴─────────────────────┴────────────────────────────┴────────────────────┴──────┴──────┴───────┴───────────┴────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────┴─────────────────────┘
```
)DOCS_MD")

}

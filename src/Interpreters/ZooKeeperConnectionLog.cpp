#include <Interpreters/ZooKeeperConnectionLog.h>

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
    ZooKeeperConnectionLogElement element;
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

    add(std::move(element));
}
}

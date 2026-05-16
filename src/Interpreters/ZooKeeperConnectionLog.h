#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct ZooKeeperConnectionLogElement
{
    enum class EventType : int8_t
    {
        Connected = 0,
        Disconnected = 1,
    };

    EventType event_type;

    time_t event_time{};
    Decimal64 event_time_microseconds = 0;

    String cluster_name;
    String host;
    UInt16 port;
    UInt8 index;
    Int64 client_id;
    UInt8 keeper_api_version;
    Array enabled_feature_flags;
    String availability_zone;
    String reason;

    static std::string name() { return "ZooKeeperConnectionLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ZooKeeperConnectionLog : public SystemLog<ZooKeeperConnectionLogElement>
{
public:
    using SystemLog<ZooKeeperConnectionLogElement>::SystemLog;

    void addConnected(std::string_view name, const zkutil::ZooKeeper& zookeeper, std::string_view reason);
    void addDisconnected(std::string_view name, const zkutil::ZooKeeper& zookeeper, std::string_view reason);

    static Array getEnabledFeatureFlags(const zkutil::ZooKeeper& zookeeper);
    constexpr static std::string_view default_zookeeper_name = "default";
    constexpr static std::string_view keeper_init_reason = "Initialization";
    constexpr static std::string_view keeper_removed_from_config = "Removed from config";
    constexpr static std::string_view keeper_expired_reason = "Session expired";

private:
    void addWithEventType(ZooKeeperConnectionLogElement::EventType type, std::string_view name, const zkutil::ZooKeeper& zookeeper, std::string_view reason);
};

}

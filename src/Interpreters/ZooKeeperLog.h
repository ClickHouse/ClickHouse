#pragma once

#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientInfo.h>
#include <Common/ZooKeeper/IKeeper.h>


namespace DB
{

struct ZooKeeperLogElement
{
    enum Type
    {
        UNKNOWN = 0,
        SEND = 1,
        RECEIVE = 2,
        FINALIZE = 3
    };

    Type type = UNKNOWN;
    Decimal64 event_time = 0;
    Poco::Net::SocketAddress address;
    Int64 session_id = 0;

    /// Common request info
    Int32 xid = 0;
    bool has_watch = false;
    Int32 op_num = 0;
    String path;

    /// create, set
    String data;

    /// create
    bool is_ephemeral = false;
    bool is_sequential = false;

    /// remove, check, set
    std::optional<Int32> version = 0;

    /// multi
    UInt32 requests_size = 0;
    UInt32 request_idx = 0;

    /// Common response info
    Int64 zxid = 0;
    std::optional<Int32> error;

    /// watch
    Int32 watch_type = 0;
    Int32 watch_state = 0;

    /// create
    String path_created;

    /// exists, get, set, list
    Coordination::Stat stat = {};

    /// list
    Strings children;


    static std::string name() { return "ZooKeeperLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ZooKeeperLog : public SystemLog<ZooKeeperLogElement>
{
    using SystemLog<ZooKeeperLogElement>::SystemLog;
};

}

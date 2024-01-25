#pragma once

#include <Core/NamesAndTypes.h>
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
        REQUEST = 1,
        RESPONSE = 2,
        FINALIZE = 3
    };

    Type type = UNKNOWN;
    Decimal64 event_time = 0;
    UInt64 thread_id = 0;
    String query_id;
    Poco::Net::SocketAddress address;
    Int64 session_id = 0;

    UInt64 duration_ms = 0;

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
    std::optional<Int32> version;

    /// multi
    UInt32 requests_size = 0;
    UInt32 request_idx = 0;

    /// Common response info
    Int64 zxid = 0;
    std::optional<Int32> error;

    /// watch
    std::optional<Int32> watch_type;
    std::optional<Int32> watch_state;

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
    static const char * getCustomColumnList() { return nullptr; }
};

class ZooKeeperLog : public SystemLog<ZooKeeperLogElement>
{
    using SystemLog<ZooKeeperLogElement>::SystemLog;
};

DataTypePtr getCoordinationErrorCodesEnumType();

}

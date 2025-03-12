#pragma once

#include <string>
#include <cstdint>
#include <magic_enum.hpp>


namespace Coordination
{

using XID = int64_t;

static constexpr XID WATCH_XID = -1;
static constexpr XID PING_XID  = -2;
static constexpr XID AUTH_XID  = -4;
static constexpr XID CLOSE_XID = std::numeric_limits<int32_t>::max();
static constexpr XID CLOSE_XID_64 = std::numeric_limits<int64_t>::max();

enum class OpNum : int32_t
{
    Close = -11,
    Error = -1,
    Create = 1,
    Remove = 2,
    Exists = 3,
    Get = 4,
    Set = 5,
    GetACL = 6,
    SetACL = 7,
    SimpleList = 8,
    Sync = 9,
    Heartbeat = 11,
    List = 12,
    Check = 13,
    Multi = 14,
    Reconfig = 16,
    MultiRead = 22,
    Auth = 100,

    // CH Keeper specific operations
    FilteredList = 500,
    CheckNotExists = 501,
    CreateIfNotExists = 502,
    RemoveRecursive = 503,

    SessionID = 997, /// Special internal request
};

OpNum getOpNum(int32_t raw_op_num);

static constexpr int32_t ZOOKEEPER_PROTOCOL_VERSION = 0;
static constexpr int32_t ZOOKEEPER_PROTOCOL_VERSION_WITH_COMPRESSION = 10;
static constexpr int32_t ZOOKEEPER_PROTOCOL_VERSION_WITH_XID_64 = 11;
static constexpr int32_t KEEPER_PROTOCOL_VERSION_CONNECTION_REJECT = 42;
static constexpr int32_t CLIENT_HANDSHAKE_LENGTH = 44;
static constexpr int32_t CLIENT_HANDSHAKE_LENGTH_WITH_READONLY = 45;
static constexpr int32_t SERVER_HANDSHAKE_LENGTH = 36;
static constexpr int32_t SERVER_HANDSHAKE_LENGTH_WITH_READONLY = 37;
static constexpr int32_t PASSWORD_LENGTH = 16;

/// ZooKeeper has 1 MB node size and serialization limit by default,
/// but it can be raised up, so we have a slightly larger limit on our side.
static constexpr int32_t MAX_STRING_OR_ARRAY_SIZE = 1 << 28;  /// 256 MiB
static constexpr int32_t DEFAULT_SESSION_TIMEOUT_MS = 30000;
static constexpr int32_t DEFAULT_MIN_SESSION_TIMEOUT_MS = 10000;
static constexpr int32_t DEFAULT_MAX_SESSION_TIMEOUT_MS = 100000;
static constexpr int32_t DEFAULT_OPERATION_TIMEOUT_MS = 10000;
static constexpr int32_t DEFAULT_CONNECTION_TIMEOUT_MS = 1000;

}

/// This is used by fmt::format to print OpNum as strings.
/// All OpNum values should be in range [min, max] to be printed.
template <>
struct magic_enum::customize::enum_range<Coordination::OpNum>
{
    static constexpr int min = -100;
    static constexpr int max = 1000;
};

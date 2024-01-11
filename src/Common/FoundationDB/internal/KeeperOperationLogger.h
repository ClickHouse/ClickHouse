#pragma once

#include <atomic>
#include <optional>
#include <Interpreters/ZooKeeperLog.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include "KeeperSession.h"

namespace DB
{
class ZooKeeperLog;

namespace FoundationDB
{

struct KeeperResponseLogger;

class KeeperOperationLogger : private boost::noncopyable
{
public:
    using XID = Coordination::XID;
    using OptionalXID = std::optional<XID>;

    explicit KeeperOperationLogger(std::shared_ptr<ZooKeeperLog> zk_log_, KeeperSession & session_);

    void setZooKeeperLog(std::shared_ptr<ZooKeeperLog> zk_log_);

    KeeperResponseLogger
    createRequest(const String & path, const String & data, bool is_ephemeral, bool is_sequential, OptionalXID force_xid = {});

    KeeperResponseLogger removeRequest(const String & path, int32_t version, OptionalXID force_xid = {});

    KeeperResponseLogger getRequest(const String & path, bool has_watch, OptionalXID force_xid = {});

    KeeperResponseLogger existsRequest(const String & path, bool has_watch, OptionalXID force_xid = {});

    KeeperResponseLogger setRequest(const String & path, const String & data, int32_t version, OptionalXID force_xid = {});

    KeeperResponseLogger listRequest(const String & path, bool has_watch, OptionalXID force_xid = {});

    KeeperResponseLogger simpleListRequest(const String & path, bool has_watch, OptionalXID force_xid = {});

    KeeperResponseLogger filteredListRequest(const String & path, bool has_watch, OptionalXID force_xid = {});

    KeeperResponseLogger checkRequest(const String & path, int32_t version, OptionalXID force_xid = {});

    KeeperResponseLogger multiRequest(const Coordination::Requests & requests);

private:
    std::shared_ptr<ZooKeeperLog> zk_log;

    /// FDBKeeper doesnt need xid. This xid just for making zk_log happy.
    std::atomic<XID> xid_cnt{1};

    /// Get current session id
    KeeperSession & session;

    XID next_xid(const OptionalXID & force_xid) { return force_xid.has_value() ? force_xid.value() : xid_cnt.fetch_add(1); }
};

struct KeeperResponseLogger
{
    std::shared_ptr<ZooKeeperLog> zk_log;
    int64_t request_session;
    Coordination::XID xid;

    void response(const Coordination::Response & resp, size_t elapsed_ms) const;
};
}
}

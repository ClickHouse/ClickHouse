#pragma once

#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Interpreters/ZooKeeperLog.h>

namespace DB
{
class ZooKeeperLog;
}

namespace Coordination
{

class KeeperOperationLogger
{
public:
    explicit KeeperOperationLogger(std::shared_ptr<ZooKeeperLog> zk_log_);

    void setZooKeeperLog(std::shared_ptr<ZooKeeperLog> zk_log_);

    void createRequest(const String & path, const String & data, bool is_ephemeral, bool is_sequential, bool has_watch, int64_t session_id) const;

    void removeRequest(const String & path, int32_t version, bool has_watch, int64_t session_id) const;

    void getRequest(const String & path, bool has_watch, int64_t session_id) const;

    void existsRequest(const String & path, bool has_watch, int64_t session_id) const;

    void setRequest(const String & path, const String & data, int32_t version, bool has_watch, int64_t session_id) const;

    void listRequest(const String & path, bool has_watch, int64_t session_id) const;

    void simpleListRequest(const String & path, bool has_watch, int64_t session_id) const;

    void filteredListRequest(const String & path, bool has_watch, int64_t session_id) const;

    void checkRequest(const String & path, int32_t version, bool has_watch, int64_t session_id) const;

    void multiRequest(const Requests & requests, bool has_watch, int64_t session_id) const;

    void response(const Response & resp, size_t elapsed_ms, int64_t session_id) const;

private:
    std::shared_ptr<ZooKeeperLog> zk_log;
};

}

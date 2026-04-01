#pragma once

#include "config.h"

#if USE_NURAFT

#include <atomic>
#include <mutex>
#include <unordered_map>

#include <Poco/Timespan.h>

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/KeeperDispatcher.h>

namespace Coordination
{

class KeeperOverDispatcher final : public IKeeper
{
public:
    explicit KeeperOverDispatcher(
        const std::shared_ptr<KeeperDispatcher> & keeper_dispatcher_,
        const Poco::Timespan & session_timeout_);
    ~KeeperOverDispatcher() override;

    bool isExpired() const override { return expired; }
    std::optional<int8_t> getConnectedNodeIdx() const override { return 0; }
    String getConnectedHostPort() const override { return "KeeperOverDispatcher:0000"; }
    int64_t getConnectionXid() const override { return 0; }
    int64_t getSessionID() const override { return session_id; }
    int64_t getLastZXIDSeen() const override { return 0; }

    using ResponseCallback = std::function<void(const ZooKeeperResponsePtr &)>;

    void create(
        const String & path,
        const String & data,
        bool is_ephemeral,
        bool is_sequential,
        const ACLs & acls,
        CreateCallback callback) override;

    void remove(
        const String & path,
        int32_t version,
        RemoveCallback callback) override;

    void removeRecursive(
        const String & path,
        uint32_t remove_nodes_limit,
        RemoveRecursiveCallback callback) override;

    void exists(
        const String & path,
        ExistsCallback callback,
        WatchCallbackPtrOrEventPtr watch) override;

    void get(
        const String & path,
        GetCallback callback,
        WatchCallbackPtrOrEventPtr watch) override;

    void set(
        const String & path,
        const String & data,
        int32_t version,
        SetCallback callback) override;

    void list(
        const String & path,
        ListRequestType list_request_type,
        ListCallback callback,
        WatchCallbackPtrOrEventPtr watch,
        bool with_stat,
        bool with_data) override;

    void check(
        const String & path,
        int32_t version,
        CheckCallback callback) override;

    void sync(
        const String & path,
        SyncCallback callback) override;

    void reconfig(
        std::string_view joining,
        std::string_view leaving,
        std::string_view new_members,
        int32_t version,
        ReconfigCallback callback) override;

    void multi(
        std::span<const RequestPtr> requests,
        MultiCallback callback) override;

    void multi(
        const Requests & requests,
        MultiCallback callback) override;

    void finalize(const String & reason) override;

    bool isFeatureEnabled(DB::KeeperFeatureFlag) const override { return false; }

    void getACL(const String & path, GetACLCallback  callback) override;

private:
    void pushRequest(ZooKeeperRequestPtr request, ResponseCallback callback);

    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
    Poco::Timespan session_timeout;
    int64_t session_id;
    std::atomic<bool> expired{false};

    std::mutex callbacks_mutex;
    std::unordered_map<XID, ResponseCallback> callbacks;
    std::atomic<XID> next_xid{1};
};

}

#endif

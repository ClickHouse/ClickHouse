#pragma once
#include <exception>
#include <memory>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/fdb_c_fwd.h>
#include <Common/FoundationDB/internal/AsyncTrxTracker.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include "Coordination/KeeperConstants.h"

namespace DB
{
class BackgroundSchedulePool;

namespace ErrorCodes
{
    extern const int OK;
}

}

namespace DB::FoundationDB
{
class KeeperKeys;
class KeeperSession;
class KeeperCleaner;
}

namespace Coordination
{
class FDBKeeper : public IKeeper
{
public:
    explicit FDBKeeper(const zkutil::ZooKeeperArgs & args);
    ~FDBKeeper() override;

    /// If expired, you can only destroy the object. All other methods will throw exception.
    bool isExpired() const override;

    /// Useful to check owner of ephemeral node.
    int64_t getSessionID() const override;

    Int8 getConnectedNodeIdx() const override { return 0; }
    String getConnectedHostPort() const override { return "FDBKeeper:0000"; }
    int32_t getConnectionXid() const override { return 0; }

    /// If the method will throw an exception, callbacks won't be called.
    ///
    /// After the method is executed successfully, you must wait for callbacks
    ///  (don't destroy callback data before it will be called).
    /// TODO: The above line is the description of an error-prone interface. It's better
    ///  to replace callbacks with std::future results, so the caller shouldn't think about
    ///  lifetime of the callback data.
    ///
    /// All callbacks are executed sequentially (the execution of callbacks is serialized).
    ///
    /// If an exception is thrown inside the callback, the session will expire,
    ///  and all other callbacks will be called with "Session expired" error.

    void create(const String & path, const String & data, bool is_ephemeral, bool is_sequential, const ACLs & acls, CreateCallback callback)
        override;

    void remove(const String & path, int32_t version, RemoveCallback callback) override;

    void exists(const String & path, ExistsCallback callback, WatchCallbackPtr watch) override;

    void get(const String & path, GetCallback callback, WatchCallbackPtr watch) override;

    void set(const String & path, const String & data, int32_t version, SetCallback callback) override;

    void list(const String & path, ListRequestType list_request_type, ListCallback callback, WatchCallbackPtr watch) override;

    void check(const String & path, int32_t version, CheckCallback callback) override;

    void multi(const Requests & requests, MultiCallback callback) override;

    void sync(const String & path, SyncCallback callback) override;

    /// FoundationDB offers elasticity, allowing machines to be provisioned or deprovisioned
    /// in a running cluster with automated load balancing and data distribution.
    bool hasReachedDeadline() const override { return false; }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
    void reconfig(
        std::string_view joining,
        std::string_view leaving,
        std::string_view new_members,
        int32_t version,
        ReconfigCallback callback) override
    {
        throw DB::Exception(
            DB::ErrorCodes::OK,
            "FoundationDB allows machines to be provisioned or deprovisioned in a running cluster with automated load balancing and data "
            "distribution.");
    }
#pragma clang diagnostic pop

    bool isFeatureEnabled(DB::KeeperFeatureFlag feature_flag) const override { return keeper_feature.isEnabled(feature_flag); }

    const DB::KeeperFeatureFlags * getKeeperFeatureFlags() const override { return &keeper_feature; }

    /// Expire session and finish all pending requests
    void finalize(const String & reason) override;

    // Force clean specific session. Only for test.
    std::future<void> cleanSession(int64_t session_id);

private:
    Poco::Logger * log;
    std::unique_ptr<DB::BackgroundSchedulePool> bg_pool;

    FoundationDBNetwork::Holder fdb_network;

    FDBDatabase * db;

    /// Track running transactions.
    /// trx_tracker is used by finalize() and onExipred() callback in KeeperSession.
    /// NOTE: trx_tracker must be placed before session.
    FoundationDB::AsyncTrxTracker trx_tracker;

    std::unique_ptr<DB::FoundationDB::KeeperKeys> keys;
    std::unique_ptr<DB::FoundationDB::KeeperSession> session;
    std::unique_ptr<DB::FoundationDB::KeeperCleaner> cleaner;

    FDBTransaction * newTrx();

    String chroot;
    DB::KeeperFeatureFlags keeper_feature;
};
}

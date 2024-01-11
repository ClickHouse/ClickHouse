#pragma once

#include <Common/ZooKeeper/IKeeper.h>

#include "AsyncTrx.h"
#include "KeeperCommon.h"

namespace DB::FoundationDB
{
/// Operators of one znode
class ZNodeLayer
{
public:
    ZNodeLayer(AsyncTrxBuilder & trxb_, const String & path_, const KeeperKeys & keys_)
        : trxb(trxb_), path(path_), keys(keys_), log(&Poco::Logger::get("FDBZNodeLayer"))
    {
    }

    enum StatSkip : char
    {
        SKIP_NONE = 0,
        SKIP_MZXID = KeeperKeys::Stat::mzxid,
        SKIP_MPZXID = KeeperKeys::Stat::pzxid,
        SKIP_MPCZXID = KeeperKeys::Stat::czxid,
    };

    /// Retrieve znode stat into `resp.stat`. Assert exists.
    template <typename StatResponse>
    void stat(AsyncTrxVar<StatResponse> var_resp, bool throw_on_non_exists = true, StatSkip skip = StatSkip::SKIP_NONE);

    /// Retrieve znode stat and data. Implicitly assert exists.
    void get(AsyncTrxVar<Coordination::GetResponse> var_resp, bool throw_on_non_exists = true);

    /// Check znode version is updated. Implicitly assert exists.
    void check(int32_t version);

    /// Check znode not exists or has different version.
    void checkNotExists(int32_t version);

    /// Create znode if not exists
    void create(const String & data, bool is_sequential, AsyncTrxVar<Coordination::CreateResponse> var_resp, bool ignore_exists = false);

    /// Register ephemeral node without assert exist.
    /// registerEphemeralUnsafe should be called after create().
    void registerEphemeralUnsafe(AsyncTrxVar<int64_t> var_session, AsyncTrxVar<Coordination::CreateResponse> var_resp);

    /// Set data without checking exists and version.
    void setUnsafe(const String & data);

    /// Remove znode. Implicitly assert exists.
    void remove(int32_t version);

    /// Remove znode without checking exists and version.
    /// removeUnsafeTrx() does not unregister ephemeral key.
    static void removeUnsafeTrx(FDBTransaction * tr, const KeeperKeys & keys, const String & path);

    /// List children. Implicitly assert exists.
    void list(
        AsyncTrxVar<Coordination::ListResponse> var_resp,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    /// Watch data change
    void watch(Coordination::WatchCallbackPtr cb, const String & request_path);

    /// Watch add or remove child
    void watchChildren(Coordination::WatchCallbackPtr cb, const String & request_path);

    /// Watch znode exists. watchExists() determine whether the fdb watch event
    /// represents create or delete based on the ExistsResponse.
    void
    watchExists(AsyncTrxVar<Coordination::ExistsResponse> var_exists_resp, Coordination::WatchCallbackPtr cb, const String & request_path);

    void assertExists();

private:
    AsyncTrxBuilder & trxb;
    const String & path;
    const KeeperKeys & keys;
    Poco::Logger * log;

    void assertExists(const String & path, bool exists, Coordination::Error error);
    static bool isRootPath(const String & path) { return path.empty() || path == "/"; }

    template <Coordination::Event resp_event>
    static void onWatch(FDBFuture *, void *) noexcept;

    struct WatchPayload
    {
        String path;
        Coordination::WatchCallbackPtr callback;
        std::unique_ptr<AsyncTrxCancelToken> trackerToken;
    };
};

}

#pragma once

#include <Common/ZooKeeper/IKeeper.h>

#include "Coroutine.h"
#include "KeeperCommon.h"

namespace DB::FoundationDB
{
/// Operators of znode
class ZNodeLayer
{
public:
    ZNodeLayer(FDBTransaction & trx_, const String & path_, const KeeperKeys & keys_)
        : trx(trx_), path(path_), keys(keys_), log(&Poco::Logger::get("FDBZNodeLayer"))
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
    Coroutine::Task<void> stat(StatResponse & resp, bool throw_on_non_exists = true, StatSkip skip = StatSkip::SKIP_NONE);

    /// Retrieve znode stat and data. Implicitly assert exists.
    Coroutine::Task<void> get(Coordination::GetResponse & resp, bool throw_on_non_exists = true);

    /// Check znode version is updated. Implicitly assert exists.
    Coroutine::Task<void> check(int32_t version);

    /// Check znode not exists or has different version.
    Coroutine::Task<void> checkNotExists(int32_t version);

    /// Create znode if not exists
    Coroutine::Task<void> create(const String & data, bool is_sequential, Coordination::CreateResponse & resp, bool ignore_exists = false);

    /// Register ephemeral node without assert exist.
    /// registerEphemeralUnsafe should be called after create().
    void registerEphemeralUnsafe(int64_t session, const Coordination::CreateResponse & resp);

    /// Set data without checking exists and version.
    void setUnsafe(const String & data);

    /// Remove znode. Implicitly assert exists.
    Coroutine::Task<void> remove(int32_t version);

    /// Remove znode without checking exists and version.
    /// removeUnsafeTrx() does not unregister ephemeral key.
    static void removeUnsafeTrx(FDBTransaction * tr, const KeeperKeys & keys, const String & path);

    /// List children. Implicitly assert exists.
    Coroutine::Task<void>
    list(Coordination::ListResponse & resp, Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    /// Watch data change
    Coroutine::Task<void> watch(Coordination::WatchCallbackPtr cb, const String & request_path);

    /// Watch add or remove child
    Coroutine::Task<void> watchChildren(Coordination::WatchCallbackPtr cb, const String & request_path);

    /// Watch znode exists. watchExists() determine whether the fdb watch event
    /// represents create or delete based on the ExistsResponse.
    Coroutine::Task<void>
    watchExists(const Coordination::ExistsResponse & exists_resp, Coordination::WatchCallbackPtr cb, const String & request_path);

    Coroutine::Task<void> assertExists();

    static bool isRootPath(const String & path) { return path.empty() || path == "/"; }

private:
    FDBTransaction & trx;
    const String & path;
    const KeeperKeys & keys;
    Poco::Logger * log;

    Coroutine::Task<void> assertExists(const String & path, bool exists, Coordination::Error error);

    template <Coordination::Event resp_event>
    static void onWatch(FDBFuture *, void *) noexcept;

    struct WatchPayload
    {
        String path;
        Coordination::WatchCallbackPtr callback;
        std::unique_ptr<AsyncTrxCancelToken> tracker_token;
    };
};
}

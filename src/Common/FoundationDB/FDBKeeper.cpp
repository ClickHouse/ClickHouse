#include <chrono>
#include <exception>
#include <filesystem>
#include <type_traits>
#include <unistd.h>
#include <Coordination/KeeperConstants.h>
#include <Core/BackgroundSchedulePool.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FDBKeeper.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/fdb_error_definitions.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include "internal/KeeperCleaner.h"
#include "internal/KeeperCommon.h"
#include "internal/KeeperOperationLogger.h"
#include "internal/KeeperSession.h"
#include "internal/ZNodeLayer.h"
#include "internal/ZNodeLocker.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}
}

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric BackgroundCommonPoolTask;
extern const Metric BackgroundCommonPoolSize;
extern const Metric FDBClientBusyness;
}

namespace ProfileEvents
{
extern const Event ZooKeeperInit;
extern const Event ZooKeeperTransactions;
extern const Event ZooKeeperCreate;
extern const Event ZooKeeperRemove;
extern const Event ZooKeeperExists;
extern const Event ZooKeeperMulti;
extern const Event ZooKeeperGet;
extern const Event ZooKeeperSet;
extern const Event ZooKeeperList;
extern const Event ZooKeeperCheck;
extern const Event ZooKeeperSync;
extern const Event ZooKeeperClose;
extern const Event ZooKeeperWaitMicroseconds;
}

namespace Coordination
{
using namespace DB::FoundationDB;

template <typename... Args>
std::tuple<std::remove_cvref_t<Args>...> packArgs(Args &&... args)
{
    return {std::forward<Args>(args)...};
}

// NOLINTBEGIN(bugprone-macro-parentheses)
#define COROUTINE_INHERIT_SELF(X) auto & X = self->X
// NOLINTEND(bugprone-macro-parentheses)

// clang-format off
#define COROUTINE_BEGIN(...) \
    { \
        auto _args_pack = packArgs(__VA_ARGS__); \
        _Pragma("clang diagnostic push") \
        _Pragma("clang diagnostic ignored \"-Wunused-variable\"") \
        _Pragma("clang diagnostic ignored \"-Wshadow\"") \
        []( \
            decltype(this) self, \
            decltype(_args_pack) _args \
        ) -> Coroutine::Task<void> \
        { \
            const auto & [__VA_ARGS__] = _args; \
            COROUTINE_INHERIT_SELF(log); \
            COROUTINE_INHERIT_SELF(keeper_logger); \
            COROUTINE_INHERIT_SELF(session); \
            COROUTINE_INHERIT_SELF(chroot); \
            COROUTINE_INHERIT_SELF(keys); \
        _Pragma("clang diagnostic pop") \

#define COROUTINE_END \
        co_return; \
        } \
        (this, std::move(_args_pack)).start({}, trx_tracker.newToken()); \
    }
// clang-format on

template <typename Response, typename TrxFn>
Coroutine::Task<void> runTrx(
    FDBTransaction * tr,
    Response & resp,
    const String & error_message,
#ifdef ZOOKEEPER_LOG
    const KeeperResponseLogger & resp_logger,
#endif
    const TrxFn & fn) noexcept
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    auto request_time = std::chrono::steady_clock::now();
    auto trx = fdb_manage_object(tr);
    try
    {
        while (true)
        {
            fdb_error_t fdb_error = FDBErrorCode::success;
            try
            {
                co_await fn(*trx);
            }
            catch (FoundationDBException & e)
            {
                fdb_error = e.code;
            }
            if (fdb_error == FDBErrorCode::success)
                break;
            co_await fdb_transaction_on_error(trx.get(), fdb_error);
        }
    }
    catch (...)
    {
        resp.error = getKeeperErrorFromException(std::current_exception(), error_message);
    }

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - request_time).count();
    ProfileEvents::increment(ProfileEvents::ZooKeeperWaitMicroseconds, elapsed_ms);

#ifdef ZOOKEEPER_LOG
    resp_logger.response(resp, elapsed_ms);
#endif
}

static String chroot_path(const String & root, const String & path)
{
    assert(!root.ends_with('/'));

    if (root.empty())
        return path;
    if (path.empty() || path == "/")
        return root;

    // root + '/' + path
    auto combined = root;
    if (path.front() != '/')
        combined.push_back('/');
    combined.append(path);

    return combined;
}

FDBKeeper::FDBKeeper(const zkutil::ZooKeeperArgs & args, std::shared_ptr<ZooKeeperLog> zk_log)
    : log(getLogger("FDBKeeper"))
    , bg_pool(std::make_unique<DB::BackgroundSchedulePool>(
          1, CurrentMetrics::BackgroundCommonPoolTask, CurrentMetrics::BackgroundCommonPoolSize, "FDBKeeper"))
{
    const auto cluster_file_path = args.fdb_cluster;
    if (!fs::exists(cluster_file_path))
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Cluster file `{}` not found", cluster_file_path);
    LOG_DEBUG(log, "Using cluster file {}", cluster_file_path);

    auto key_prefix = args.fdb_prefix;
    if (key_prefix.empty())
        key_prefix = "fdbkeeper/";
    keys = std::make_unique<FoundationDB::KeeperKeys>(key_prefix);
    LOG_DEBUG(log, "Using key prefix: {}", fdb_print_key(key_prefix));

    if (args.chroot.empty() || args.chroot.front() != '/')
        chroot = '/';
    chroot.append(args.chroot);
    if (chroot.back() == '/')
        chroot.pop_back();
    LOG_DEBUG(log, "Using chroot: {}", chroot);

    /// Mark support feature
    keeper_feature.enableFeatureFlag(KeeperFeatureFlag::CHECK_NOT_EXISTS);
    keeper_feature.enableFeatureFlag(KeeperFeatureFlag::FILTERED_LIST);
    keeper_feature.enableFeatureFlag(KeeperFeatureFlag::CREATE_IF_NOT_EXISTS);
    keeper_feature.enableFeatureFlag(KeeperFeatureFlag::MULTI_READ);

    /// Init fdb database
    throwIfFDBError(fdb_create_database(cluster_file_path.c_str(), &db));
    static int64_t timeout_ms = 5000;
    throwIfFDBError(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_TIMEOUT, FDB_VALUE_FROM_POD(timeout_ms)));

    /// Setup fdbkeeper threads
    session = std::make_unique<FoundationDB::KeeperSession>(*keys, newTrx());
    session->on_expired = [this]()
    {
        /// onExpired may be invoked on fdb network thread.
        /// Waiting for all trx to finish on network thread will lead to a deadlock.
        trx_tracker.gracefullyCancelAll(false);
    };
    cleaner = std::make_unique<FoundationDB::KeeperCleaner>(*keys, newTrx());

#ifdef ZOOKEEPER_LOG
    /// Setup logger
    keeper_logger = std::make_unique<FoundationDB::KeeperOperationLogger>(zk_log, *session);
#endif

    // Wait session ready
    {
        std::promise<void> promise;
        auto wait_session = [&]() -> Coroutine::Task<void>
        {
            auto trx = fdb_manage_object(newTrx());
            co_await session->currentSession(*trx);
        };
        wait_session().start(
            [&](std::exception_ptr eptr)
            {
                if (eptr)
                {
                    LOG_ERROR(log, "Failed to wait session ready");
                    promise.set_exception(eptr);
                }
                else
                {
                    promise.set_value();
                }
            });
        promise.get_future().get();
    }

    ProfileEvents::increment(ProfileEvents::ZooKeeperInit);
}

FDBKeeper::~FDBKeeper()
{
    fdb_database_destroy(db);
    ProfileEvents::increment(ProfileEvents::ZooKeeperClose);
};

FDBTransaction * FDBKeeper::newTrx()
{
    CurrentMetrics::set(CurrentMetrics::FDBClientBusyness, static_cast<Int64>(fdb_database_get_main_thread_busyness(db) * 100));

    FDBTransaction * tr;
    throwIfFDBError(fdb_database_create_transaction(db, &tr));
    // DO NOT set transaction option here, otherwise they will be lost during retry.
    return tr;
}

int64_t FDBKeeper::getSessionID() const
{
    return session->currentSessionSync();
};

void FDBKeeper::create(
    const String & path, const String & data, bool is_ephemeral, bool is_sequential, const ACLs & acls, CreateCallback callback)
{
    if (!acls.empty())
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "create with acl is not support yet");

    ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);

    COROUTINE_BEGIN(path, data, is_ephemeral, is_sequential, acls, callback)

    CreateResponse resp;
    String chrooted_path = chroot_path(chroot, path);

    auto lock_guard = co_await ZNodeLocker::Builder().create(chrooted_path).lock();

    co_await runTrx(
        self->newTrx(),
        resp,
        " during create " + chrooted_path,
#ifdef ZOOKEEPER_LOG
        keeper_logger->createRequest(path, data, is_ephemeral, is_sequential),
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            SessionID session_id = 0;
            if (is_ephemeral)
                session_id = co_await session->currentSession(trx);

            ZNodeLayer znode(trx, chrooted_path, *keys);
            co_await znode.create(data, is_sequential, resp);

            if (is_ephemeral)
                znode.registerEphemeralUnsafe(session_id, resp);

            if (!chroot.empty())
                removeRootPath(resp.path_created, chroot);

            co_await fdb_transaction_commit(&trx);
        });

    callback(resp);
    COROUTINE_END
};

void FDBKeeper::remove(const String & path, int32_t version, RemoveCallback callback)
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);

    COROUTINE_BEGIN(path, version, callback)
    String chrooted_path = chroot_path(chroot, path);
    RemoveResponse resp;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->removeRequest(path, version);
#endif

    auto lock = co_await ZNodeLocker::Builder().remove(chrooted_path).lock();

    co_await runTrx(
        self->newTrx(),
        resp,
        " during remove " + chrooted_path,
#ifdef ZOOKEEPER_LOG
        resp_logger,
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            ZNodeLayer znode(trx, chrooted_path, *keys);
            co_await znode.remove(version);
            co_await fdb_transaction_commit(&trx);
        });

    callback(resp);
    COROUTINE_END
};

void FDBKeeper::exists(const String & path, ExistsCallback callback, WatchCallbackPtr watch)
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);

    COROUTINE_BEGIN(path, callback, watch)
    String chrooted_path = chroot_path(chroot, path);
    ExistsResponse resp;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->existsRequest(path, watch != nullptr);
#endif

    co_await runTrx(
        self->newTrx(),
        resp,
        " during exists " + chrooted_path,
#ifdef ZOOKEEPER_LOG
        resp_logger,
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            ZNodeLayer znode(trx, chrooted_path, *keys);
            if (watch)
            {
                co_await znode.stat(resp, false);
                co_await znode.watchExists(resp, watch, path);
                co_await fdb_transaction_commit(&trx);
            }
            else
            {
                co_await znode.stat(resp);
            }
        });

    callback(resp);
    COROUTINE_END
};

void FDBKeeper::get(const String & path, GetCallback callback, WatchCallbackPtr watch)
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);

    COROUTINE_BEGIN(path, callback, watch)

    String chrooted_path = chroot_path(chroot, path);
    GetResponse resp;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->getRequest(path, watch != nullptr);
#endif

    co_await runTrx(
        self->newTrx(),
        resp,
        " during get " + chrooted_path,
#ifdef ZOOKEEPER_LOG
        resp_logger,
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            ZNodeLayer znode(trx, chrooted_path, *keys);

            co_await znode.get(resp);

            if (watch)
            {
                co_await znode.watch(watch, path);
                co_await fdb_transaction_commit(&trx);
            }
        });

    callback(resp);
    COROUTINE_END
};

void FDBKeeper::set(const String & path, const String & data, int32_t version, SetCallback callback)
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperSet);

    COROUTINE_BEGIN(path, data, version, callback)
    String chrooted_path = chroot_path(chroot, path);
    SetResponse resp;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->setRequest(path, data, version);
#endif

    auto lock = co_await ZNodeLocker::Builder().set(chrooted_path).lock();

    co_await runTrx(
        self->newTrx(),
        resp,
        " during set " + chrooted_path,
#ifdef ZOOKEEPER_LOG
        resp_logger,
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            ZNodeLayer znode(trx, chrooted_path, *keys);

            co_await znode.stat(resp);

            if (version >= 0 && resp.stat.version != version)
                throw DB::FoundationDB::KeeperException(Error::ZBADVERSION);

            resp.stat.version++;
            resp.stat.dataLength = static_cast<int32_t>(data.size());

            znode.setUnsafe(data);

            auto future_vs = fdb_manage_object(fdb_transaction_get_versionstamp(&trx));
            co_await fdb_transaction_commit(&trx);

            const uint8_t * vs_bytes;
            int vs_len;
            throwIfFDBError(fdb_future_get_key(future_vs.get(), &vs_bytes, &vs_len));
            assert(vs_len == 10);
            resp.stat.mzxid = *reinterpret_cast<const int64_t *>(vs_bytes);
        });

    callback(resp);
    COROUTINE_END
};

void FDBKeeper::list(const String & path, ListRequestType list_request_type, ListCallback callback, WatchCallbackPtr watch)
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperList);

    COROUTINE_BEGIN(path, list_request_type, callback, watch)
    String chrooted_path = chroot_path(chroot, path);
    ListResponse resp;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->listRequest(path, watch != nullptr);
#endif

    co_await runTrx(
        self->newTrx(),
        resp,
        " during list " + chrooted_path,
#ifdef ZOOKEEPER_LOG
        resp_logger,
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            ZNodeLayer znode(trx, chrooted_path, *keys);
            co_await znode.list(resp, list_request_type);

            if (watch)
            {
                co_await znode.watchChildren(watch, path);
                co_await fdb_transaction_commit(&trx);
            }
        });

    callback(resp);
    COROUTINE_END
};

void FDBKeeper::check(const String & path, int32_t version, CheckCallback callback)
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperCheck);

    COROUTINE_BEGIN(path, version, callback)
    String chrooted_path = chroot_path(chroot, path);
    CheckResponse resp;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->checkRequest(path, version);
#endif

    co_await runTrx(
        self->newTrx(),
        resp,
        " during check " + chrooted_path,
#ifdef ZOOKEEPER_LOG
        resp_logger,
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            ZNodeLayer znode(trx, chrooted_path, *keys);
            co_await znode.check(version);
        });

    callback(resp);
    COROUTINE_END
};

Coroutine::Task<bool>
checkMultiRequest(const Requests & requests, const String & chroot, MultiResponse & out_response, ZNodeLocker & out_lock)
{
    std::optional<bool> is_multi_read;
    auto assert_is_read = [&is_multi_read](bool is_read)
    {
        chassert(!is_multi_read.has_value() || *is_multi_read == is_read);
        is_multi_read = is_read;
    };

    auto lock_builder = ZNodeLocker::Builder();
    out_response.responses.reserve(requests.size());
    for (const auto & request : requests)
    {
        String chrooted_path = chroot_path(chroot, request->getPath());

        if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(request.get()))
        {
            assert_is_read(false);
            out_response.responses.emplace_back(std::make_shared<CreateResponse>());
            lock_builder.create(chrooted_path);
        }
        else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(request.get()))
        {
            assert_is_read(false);
            out_response.responses.emplace_back(std::make_shared<RemoveResponse>());
            lock_builder.remove(chrooted_path);
        }
        else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(request.get()))
        {
            assert_is_read(false);
            out_response.responses.emplace_back(std::make_shared<SetResponse>());
            lock_builder.set(chrooted_path);
        }
        else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(request.get()))
        {
            assert_is_read(false);
            out_response.responses.emplace_back(std::make_shared<CheckResponse>());
        }
        else if (const auto * concrete_request_get = dynamic_cast<const GetRequest *>(request.get()))
        {
            assert_is_read(true);
            out_response.responses.emplace_back(std::make_shared<GetResponse>());
        }
        else if (const auto * concrete_request_exists = dynamic_cast<const ExistsRequest *>(request.get()))
        {
            assert_is_read(true);
            out_response.responses.emplace_back(std::make_shared<ExistsResponse>());
        }
        else if (const auto * concrete_request_simple_list = dynamic_cast<const ZooKeeperSimpleListRequest *>(request.get()))
        {
            assert_is_read(true);
            out_response.responses.emplace_back(std::make_shared<ListResponse>());
        }
        else if (const auto * concrete_request_filted_list = dynamic_cast<const ZooKeeperFilteredListRequest *>(request.get()))
        {
            assert_is_read(true);
            out_response.responses.emplace_back(std::make_shared<ListResponse>());
        }
        else
            throw Exception(Error::ZBADARGUMENTS, "Illegal command as part of multi ZooKeeper request");

        out_response.responses.back()->error = Error::ZRUNTIMEINCONSISTENCY;
    }

    out_lock = co_await lock_builder.lock();

    co_return is_multi_read.value_or(false);
}

Coroutine::Task<void>
doMultiRead(FDBTransaction & trx, const String & chroot, const KeeperKeys & keys, const Requests & requests, MultiResponse & out_reponse)
{
    for (size_t i = 0; i < requests.size(); ++i)
    {
        const auto & request = requests[i];
        auto response = out_reponse.responses[i];

        String chrooted_path = chroot_path(chroot, request->getPath());
        ZNodeLayer znode(trx, chrooted_path, keys);

        try
        {
            response->error = Error::ZOK;

            if (const auto * concrete_request_get = dynamic_cast<const GetRequest *>(request.get()))
            {
                auto & resp_get = dynamic_cast<GetResponse &>(*response);
                co_await znode.get(resp_get, false);
            }
            else if (const auto * concrete_request_exists = dynamic_cast<const ExistsRequest *>(request.get()))
            {
                auto & resp_exists = dynamic_cast<ExistsResponse &>(*response);
                co_await znode.stat(resp_exists, false);
            }
            else if (const auto * concrete_request_simple_list = dynamic_cast<const ZooKeeperSimpleListRequest *>(request.get()))
            {
                auto & resp_list = dynamic_cast<ListResponse &>(*response);
                co_await znode.list(resp_list);
            }
            else if (const auto * concrete_request_filted_list = dynamic_cast<const ZooKeeperFilteredListRequest *>(request.get()))
            {
                auto & resp_list = dynamic_cast<ListResponse &>(*response);
                co_await znode.list(resp_list, concrete_request_filted_list->list_request_type);
            }
            else
                throw Exception(Error::ZBADARGUMENTS, "Illegal command as part of multi ZooKeeper request");
        }
        catch (...)
        {
            if (out_reponse.error != Error::ZOK)
                out_reponse.error = response->error;

            if (response->error != Error::ZNONODE)
                throw;
        }
    }
}

Coroutine::Task<void> doMultiWrite(
    FDBTransaction & trx,
    KeeperSession & session,
    const String & chroot,
    const KeeperKeys & keys,
    const Requests & requests,
    MultiResponse & out_response)
{
    /// CreateRequest set <path><meta><*zxid> = versionstamp.
    /// SetRequest require <path><meta><{c,p}zxid> to build stat in response.
    /// But versionstamp is not available until commit.

    /// New ZNodes
    std::unordered_set<std::string> pathset_create;
    /// ZNodes that children changed
    std::unordered_set<std::string> pathset_children_changed;

    /// set_resps_*zxid record SetResponse that needs to set the *zxid.
    std::vector<SetResponse *> resps_need_cpmzxid;
    std::vector<SetResponse *> resps_need_pmzxid;
    std::vector<SetResponse *> resps_need_mzxid;

    std::optional<SessionID> session_id;

    bool need_commit = false;

    for (size_t i = 0; i < requests.size(); ++i)
    {
        const auto & request = requests[i];
        auto response = out_response.responses[i];

        try
        {
            response->error = Error::ZOK;

            String chrooted_path = chroot_path(chroot, request->getPath());
            ZNodeLayer znode(trx, chrooted_path, keys);

            if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(request.get()))
            {
                auto & resp_create = dynamic_cast<CreateResponse &>(*response);

                if (concrete_request_create->is_ephemeral && !session_id.has_value())
                    session_id = co_await session.currentSession(trx);

                co_await znode.create(
                    concrete_request_create->data,
                    concrete_request_create->is_sequential,
                    resp_create,
                    concrete_request_create->not_exists);

                if (concrete_request_create->is_ephemeral)
                    znode.registerEphemeralUnsafe(session_id.value(), resp_create);

                if (!chroot.empty())
                    removeRootPath(resp_create.path_created, chroot);

                /// Record create log
                pathset_create.emplace(chrooted_path);
                pathset_children_changed.emplace(getParentPath(chrooted_path));

                /// Register response
                need_commit = true;
            }
            else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(request.get()))
            {
                co_await znode.remove(concrete_request_remove->version);

                /// Record create log
                pathset_children_changed.emplace(getParentPath(chrooted_path));
                need_commit = true;
            }
            else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(request.get()))
            {
                auto & resp_set = dynamic_cast<SetResponse &>(*response);

                /// Check version
                if (concrete_request_set->version >= 0)
                    co_await znode.check(concrete_request_set->version);
                else
                    co_await znode.assertExists();

                /// Set data
                znode.setUnsafe(concrete_request_set->data);

                /// Fill stat in SetResponse
                using StatMask = DB::FoundationDB::ZNodeLayer::StatSkip;
                if (pathset_create.contains(chrooted_path))
                {
                    /// New ZNode in trx. Its {c,p,m}zxid is versionstamp.
                    co_await znode.stat(resp_set, true, StatMask::SKIP_MPCZXID);
                    resps_need_cpmzxid.emplace_back(&resp_set);
                }
                else if (pathset_children_changed.contains(chrooted_path))
                {
                    /// ZNodes that children changed in trx. Its {p,m}zxid is versionstamp
                    co_await znode.stat(resp_set, true, StatMask::SKIP_MPZXID);
                    resps_need_pmzxid.emplace_back(&resp_set);
                }
                else
                {
                    /// Modify data. Its mzxid is versionstamp
                    co_await znode.stat(resp_set, true, StatMask::SKIP_MZXID);
                    resps_need_mzxid.emplace_back(&resp_set);
                }

                need_commit = true;
            }
            else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(request.get()))
            {
                if (concrete_request_check->not_exists)
                    co_await znode.checkNotExists(concrete_request_check->version);
                else
                    co_await znode.check(concrete_request_check->version);
            }
        }
        catch (...)
        {
            response->error = getKeeperErrorFromException(std::current_exception());
            throw;
        }
    }

    if (need_commit)
    {
        if (resps_need_cpmzxid.empty() && resps_need_pmzxid.empty() && resps_need_mzxid.empty())
        {
            co_await fdb_transaction_commit(&trx);
        }
        else
        {
            auto future_vs = fdb_manage_object(fdb_transaction_get_versionstamp(&trx));
            co_await fdb_transaction_commit(&trx);

            const uint8_t * vs_bytes;
            int vs_len;
            throwIfFDBError(fdb_future_get_key(future_vs.get(), &vs_bytes, &vs_len));
            assert(vs_len == 10);
            const auto & zxid = *reinterpret_cast<const int64_t *>(vs_bytes);

            for (auto * resp_set : resps_need_cpmzxid)
            {
                resp_set->stat.czxid = zxid;
                resp_set->stat.mzxid = zxid;
                resp_set->stat.pzxid = zxid;
            }
            for (auto * resp_set : resps_need_pmzxid)
            {
                resp_set->stat.mzxid = zxid;
                resp_set->stat.pzxid = zxid;
            }
            for (auto * resp_set : resps_need_mzxid)
                resp_set->stat.mzxid = zxid;
        }
    }
}

void FDBKeeper::multi(const Requests & requests, MultiCallback callback)
{
    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);

    COROUTINE_BEGIN(requests, callback)

    MultiResponse resp;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->multiRequest(requests);
#endif

    ZNodeLocker lock;
    bool is_multi_read = co_await checkMultiRequest(requests, chroot, resp, lock);

    co_await runTrx(
        self->newTrx(),
        resp,
        " during multi requests",
#ifdef ZOOKEEPER_LOG
        resp_logger,
#endif
        [&](FDBTransaction & trx) -> Coroutine::Task<void>
        {
            if (is_multi_read)
                co_await doMultiRead(trx, chroot, *keys, requests, resp);
            else
                co_await doMultiWrite(trx, *session, chroot, *keys, requests, resp);
        });

    callback(resp);
    COROUTINE_END
};

void FDBKeeper::setZooKeeperLog(std::shared_ptr<DB::ZooKeeperLog> zk_log_)
{
    keeper_logger->setZooKeeperLog(zk_log_);
}

bool FDBKeeper::isExpired() const
{
    return session->isExpired();
}

void FDBKeeper::finalize(const String & reason)
{
    LOG_TRACE(log, "FDBKeeper finalize due to {}", reason);
    trx_tracker.gracefullyCancelAll();
};

void FDBKeeper::sync(const String & path, SyncCallback callback)
{
    SyncResponse resp;
    resp.path = chroot_path(chroot, path);
    callback(resp);

    ProfileEvents::increment(ProfileEvents::ZooKeeperSync);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
}

std::future<void> FDBKeeper::cleanSession(int64_t session_id)
{
    auto * trx = newTrx();
    auto trx_guard = fdb_manage_object(trx);
    auto task = cleaner->clean(*trx, session_id);

    auto promise = std::make_shared<std::promise<void>>();
    task.start(
        [promise, trx_guard](std::exception_ptr eptr)
        {
            if (eptr)
                promise->set_exception(eptr);
            else
                promise->set_value();
        });
    return promise->get_future();
}
}

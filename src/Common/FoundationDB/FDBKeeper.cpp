#include <chrono>
#include <filesystem>
#include <unistd.h>
#include <Coordination/KeeperConstants.h>
#include <Core/BackgroundSchedulePool.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FDBKeeper.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include "internal/AsyncTrx.h"
#include "internal/KeeperCleaner.h"
#include "internal/KeeperCommon.h"
#include "internal/KeeperOperationLogger.h"
#include "internal/KeeperSession.h"
#include "internal/ZNodeLayer.h"

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

template <typename Callable>
struct keeper_callback_response;

template <typename R, typename Arg, typename... Args>
struct keeper_callback_response<std::function<R(Arg, Args...)>>
{
    using type = std::remove_cvref_t<Arg>;
};

template <typename Callable>
using first_arg_t = typename keeper_callback_response<Callable>::type;

template <typename VarResp, typename Callback>
auto handleKeeperCallback(
    VarResp var_resp,
    Callback callback,
#ifdef ZOOKEEPER_LOG
    KeeperResponseLogger resp_logger,
#endif
    const String & message)
{
    static_assert(std::is_same_v<VarResp, AsyncTrxBuilder::VarDesc<first_arg_t<Callback>>>);
    auto request_time = std::chrono::steady_clock::now();
    return [
#ifdef ZOOKEEPER_LOG
               resp_logger,
#endif
               var_resp,
               callback,
               message,
               request_time](AsyncTrxBuilder::Context & ctx, std::exception_ptr eptr)
    {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - request_time).count();

        if (eptr)
        {
            first_arg_t<Callback> resp;
            resp.error = DB::FoundationDB::getKeeperErrorFromException(eptr, message);
            callback(resp);
#ifdef ZOOKEEPER_LOG
            resp_logger.response(resp, elapsed_ms);
#endif
        }
        else
        {
            auto & resp = *ctx.getVar(var_resp);
            callback(resp);
#ifdef ZOOKEEPER_LOG
            resp_logger.response(resp, elapsed_ms);
#endif
        }

        ProfileEvents::increment(ProfileEvents::ZooKeeperWaitMicroseconds, elapsed_ms);
    };
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
    : log(&Poco::Logger::get("FDBKeeper"))
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
    session = std::make_unique<FoundationDB::KeeperSession>(*bg_pool, *keys, newTrx());
    session->onExpired = [this]()
    {
        /// onExpired may be invoked on fdb network thread.
        /// Waiting for all trx to finish on network thread will lead to a deadlock.
        trx_tracker.gracefullyCancelAll(false);
    };
    cleaner = std::make_unique<FoundationDB::KeeperCleaner>(*bg_pool, *keys, newTrx());

#ifdef ZOOKEEPER_LOG
    /// Setup logger
    keeper_logger = std::make_unique<FoundationDB::KeeperOperationLogger>(zk_log, *session);
#endif

    // Wait session ready
    {
        std::promise<void> promise;
        AsyncTrxBuilder trxb;
        session->currentSession(trxb, trxb.var<SessionID>());
        trxb.exec(
            newTrx(),
            [&promise, lambda_log = log](AsyncTrx::Context &, std::exception_ptr eptr)
            {
                if (eptr)
                {
                    LOG_ERROR(lambda_log, "Failed to wait session ready");
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

    AsyncTrxBuilder trxb;
    auto resp = trxb.var<CreateResponse>();

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->createRequest(path, data, is_ephemeral, is_sequential);
#endif

    AsyncTrxBuilder::VarDesc<SessionID> var_session;
    if (is_ephemeral)
    {
        var_session = trxb.var<SessionID>();
        session->currentSession(trxb, var_session);
    }

    String chrooted_path = chroot_path(chroot, path);
    ZNodeLayer znode(trxb, chrooted_path, *keys);
    znode.create(data, is_sequential, resp);

    if (is_ephemeral)
        znode.registerEphemeralUnsafe(var_session, resp);

    if (!chroot.empty())
        trxb.then(TRX_STEP(resp, local_chroot = chroot)
        {
            removeRootPath(ctx.getVar(resp)->path_created, local_chroot);
            return nullptr;
        });
    trxb.commit().exec(
        newTrx(),
        handleKeeperCallback(
            resp,
            callback,
#ifdef ZOOKEEPER_LOG
            resp_logger,
#endif
            " during create " + chrooted_path),
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
};

void FDBKeeper::remove(const String & path, int32_t version, RemoveCallback callback)
{
    String chrooted_path = chroot_path(chroot, path);

    AsyncTrxBuilder trxb;
    auto resp = trxb.var<RemoveResponse>();
    ZNodeLayer znode(trxb, chrooted_path, *keys);
    znode.remove(version);

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->removeRequest(path, version);
#endif

    trxb.commit().exec(
        newTrx(),
        handleKeeperCallback(
            resp,
            callback,
#ifdef ZOOKEEPER_LOG
            resp_logger,
#endif
            " during remove " + chrooted_path),
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
};

void FDBKeeper::exists(const String & path, ExistsCallback callback, WatchCallbackPtr watch)
{
    String chrooted_path = chroot_path(chroot, path);

    AsyncTrxBuilder trxb;
    ZNodeLayer znode(trxb, chrooted_path, *keys);
    auto var_resp = trxb.var<ExistsResponse>();

    if (watch)
    {
        znode.stat(var_resp, false);
        znode.watchExists(var_resp, watch, path);
        trxb.commit();
    }
    else
    {
        znode.stat(var_resp);
    }

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->existsRequest(path, watch != nullptr);
#endif

    trxb.exec(
        newTrx(),
        handleKeeperCallback(
            var_resp,
            callback,
#ifdef ZOOKEEPER_LOG
            resp_logger,
#endif
            " during exists " + chrooted_path),
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
};

void FDBKeeper::get(const String & path, GetCallback callback, WatchCallbackPtr watch)
{
    String chrooted_path = chroot_path(chroot, path);

    AsyncTrxBuilder trxb;
    ZNodeLayer znode(trxb, chrooted_path, *keys);
    auto var_resp = trxb.var<GetResponse>();
    znode.get(var_resp);

    if (watch)
    {
        znode.watch(watch, path);
        trxb.commit();
    }

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->getRequest(path, watch != nullptr);
#endif

    trxb.exec(
        newTrx(),
        handleKeeperCallback(
            var_resp,
            callback,
#ifdef ZOOKEEPER_LOG
            resp_logger,
#endif
            " during get " + chrooted_path),
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
};

void FDBKeeper::set(const String & path, const String & data, int32_t version, SetCallback callback)
{
    String chrooted_path = chroot_path(chroot, path);

    AsyncTrxBuilder trxb;
    ZNodeLayer znode(trxb, chrooted_path, *keys);
    auto var_resp = trxb.var<SetResponse>();
    znode.stat(var_resp);
    trxb.then(TRX_STEP(var_resp, version, data_length = data.size())
    {
        auto & resp = *ctx.getVar(var_resp);
        if (version >= 0 && resp.stat.version != version)
            throw DB::FoundationDB::KeeperException(Error::ZBADVERSION);

        resp.stat.version++;
        resp.stat.dataLength = static_cast<int32_t>(data_length);
        return nullptr;
    });
    znode.setUnsafe(data);

    auto var_vs = trxb.var<FDBVersionstamp>();
    trxb.commit(var_vs);
    trxb.then(TRX_STEP(var_vs, var_resp)
    {
        auto & resp = *ctx.getVar(var_resp);
        auto & vs = *ctx.getVar(var_vs);

        resp.stat.mzxid = *reinterpret_cast<const int64_t *>(vs.bytes);
        return nullptr;
    });

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->setRequest(path, data, version);
#endif

    trxb.exec(
        newTrx(),
        handleKeeperCallback(
            var_resp,
            callback,
#ifdef ZOOKEEPER_LOG
            resp_logger,
#endif
            " during set " + chrooted_path),
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperSet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
};

void FDBKeeper::list(const String & path, ListRequestType list_request_type, ListCallback callback, WatchCallbackPtr watch)
{
    String chrooted_path = chroot_path(chroot, path);

    AsyncTrxBuilder trxb;
    ZNodeLayer znode(trxb, chrooted_path, *keys);
    auto var_resp = trxb.var<ListResponse>();
    znode.list(var_resp, list_request_type);

    if (watch)
    {
        znode.watchChildren(watch, path);
        trxb.commit();
    }

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->listRequest(path, watch != nullptr);
#endif

    trxb.exec(
        newTrx(),
        handleKeeperCallback(
            var_resp,
            callback,
#ifdef ZOOKEEPER_LOG
            resp_logger,
#endif
            " during list " + chrooted_path),
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperList);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
};

void FDBKeeper::check(const String & path, int32_t version, CheckCallback callback)
{
    String chrooted_path = chroot_path(chroot, path);

    AsyncTrxBuilder trxb;
    ZNodeLayer znode(trxb, chrooted_path, *keys);
    auto resp = trxb.var<CheckResponse>();
    znode.check(version);

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->checkRequest(path, version);
#endif

    trxb.exec(
        newTrx(),
        handleKeeperCallback(
            resp,
            callback,
#ifdef ZOOKEEPER_LOG
            resp_logger,
#endif
            " during check " + chrooted_path),
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperCheck);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
};

template <typename TResponse>
static std::shared_ptr<Response> createResponseInMulti(AsyncTrxBuilder::Context & ctx, AsyncTrxBuilder::VarDesc<Response> var, Error error)
{
    TResponse & resp = *ctx.getVar(var.as<TResponse>());
    auto out_resp = std::make_shared<TResponse>();
    if (likely(error == Error::ZOK))
        *out_resp = std::move(resp);
    else
        out_resp->error = error;
    return out_resp;
}

void FDBKeeper::multi(const Requests & requests, MultiCallback callback)
{
    AsyncTrxBuilder trxb;

    std::vector<std::pair<AsyncTrxBuilder::VarDesc<Response>, decltype(&createResponseInMulti<Response>)>> all_responses_var;

    auto curr_request_var = trxb.varDefault<size_t>(0);
    auto add_curr_request = TRX_STEP(curr_request_var)
    {
        ++(*ctx.getVar(curr_request_var));
        return nullptr;
    };
    bool need_commit = false;

    std::optional<bool> is_multi_read;
    auto assert_is_read = [&is_multi_read](bool is_read)
    {
        chassert(!is_multi_read.has_value() || *is_multi_read == is_read);
        is_multi_read = is_read;
    };

    /// CreateRequest set <path><meta><*zxid> = versionstamp.
    /// SetRequest require <path><meta><{c,p}zxid> to build stat in response.
    /// But versionstamp is not available until commit.

    /// New ZNodes
    std::unordered_set<std::string> pathset_create;
    /// ZNodes that children changed
    std::unordered_set<std::string> pathset_children_changed;

    /// set_resps_*zxid record SetResponse that needs to set the *zxid.
    std::vector<AsyncTrxBuilder::VarDesc<SetResponse>> resps_need_cpmzxid;
    std::vector<AsyncTrxBuilder::VarDesc<SetResponse>> resps_need_pmzxid;
    std::vector<AsyncTrxBuilder::VarDesc<SetResponse>> resps_need_mzxid;

    AsyncTrxBuilder::VarDesc<SessionID> var_session;
    bool session_filled = false;

#ifdef ZOOKEEPER_LOG
    auto resp_logger = keeper_logger->multiRequest(requests);
#endif

    for (const auto & request : requests)
    {
        String chrooted_path = chroot_path(chroot, request->getPath());

        if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(request.get()))
        {
            assert_is_read(false);

            if (concrete_request_create->is_ephemeral && !session_filled)
            {
                session_filled = true;
                var_session = trxb.var<SessionID>();
                session->currentSession(trxb, var_session);
            }

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto resp = trxb.var<CreateResponse>();
            znode.create(concrete_request_create->data, concrete_request_create->is_sequential, resp, concrete_request_create->not_exists);

            if (concrete_request_create->is_ephemeral)
                znode.registerEphemeralUnsafe(var_session, resp);

            if (!chroot.empty())
            {
                trxb.then(TRX_STEP(resp, local_chroot = chroot)
                {
                    removeRootPath(ctx.getVar(resp)->path_created, local_chroot);
                    return nullptr;
                });
            }

            /// Record create log
            pathset_create.emplace(chrooted_path);
            pathset_children_changed.emplace(getParentPath(chrooted_path));

            /// Register response
            all_responses_var.emplace_back(resp.as<Response>(), createResponseInMulti<CreateResponse>);
            need_commit = true;
        }
        else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(request.get()))
        {
            assert_is_read(false);

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto resp = trxb.var<RemoveResponse>();
            znode.remove(concrete_request_remove->version);

            /// Record create log
            pathset_children_changed.emplace(getParentPath(chrooted_path));

            /// Register response
            all_responses_var.emplace_back(resp.as<Response>(), createResponseInMulti<RemoveResponse>);
            need_commit = true;
        }
        else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(request.get()))
        {
            assert_is_read(false);

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto resp = trxb.var<SetResponse>();

            /// Check version
            if (concrete_request_set->version >= 0)
                znode.check(concrete_request_set->version);
            else
                znode.assertExists();

            /// Set data
            znode.setUnsafe(concrete_request_set->data);

            /// Fill stat in SetResponse
            using StatMask = DB::FoundationDB::ZNodeLayer::StatSkip;
            if (pathset_create.contains(chrooted_path))
            {
                /// New ZNode in trx. Its {c,p,m}zxid is versionstamp.
                znode.stat(resp, true, StatMask::SKIP_MPCZXID);
                resps_need_cpmzxid.emplace_back(resp);
            }
            else if (pathset_children_changed.contains(chrooted_path))
            {
                /// ZNodes that children changed in trx. Its {p,m}zxid is versionstamp
                znode.stat(resp, true, StatMask::SKIP_MPZXID);
                resps_need_pmzxid.emplace_back(resp);
            }
            else
            {
                /// Modify data. Its mzxid is versionstamp
                znode.stat(resp, true, StatMask::SKIP_MZXID);
                resps_need_mzxid.emplace_back(resp);
            }

            /// Register response
            all_responses_var.emplace_back(resp.as<Response>(), createResponseInMulti<SetResponse>);
            need_commit = true;
        }
        else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(request.get()))
        {
            assert_is_read(false);

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto resp = trxb.var<CheckResponse>();
            if (concrete_request_check->not_exists)
                znode.checkNotExists(concrete_request_check->version);
            else
                znode.check(concrete_request_check->version);

            /// Register response
            all_responses_var.emplace_back(resp.as<Response>(), createResponseInMulti<CheckResponse>);
        }
        else if (const auto * concrete_request_get = dynamic_cast<const GetRequest *>(request.get()))
        {
            assert_is_read(true);

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto var_resp = trxb.var<GetResponse>();
            znode.get(var_resp, false);

            all_responses_var.emplace_back(var_resp.as<Response>(), createResponseInMulti<GetResponse>);
        }
        else if (const auto * concrete_request_exists = dynamic_cast<const ExistsRequest *>(request.get()))
        {
            assert_is_read(true);

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto var_resp = trxb.var<ExistsResponse>();
            znode.stat(var_resp, false);

            all_responses_var.emplace_back(var_resp.as<Response>(), createResponseInMulti<ExistsResponse>);
        }
        else if (const auto * concrete_request_simple_list = dynamic_cast<const ZooKeeperSimpleListRequest *>(request.get()))
        {
            assert_is_read(true);

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto var_resp = trxb.var<ListResponse>();
            znode.list(var_resp);
            ListResponse resp;

            all_responses_var.emplace_back(var_resp.as<Response>(), createResponseInMulti<ListResponse>);
        }
        else if (const auto * concrete_request_filted_list = dynamic_cast<const ZooKeeperFilteredListRequest *>(request.get()))
        {
            assert_is_read(true);

            ZNodeLayer znode(trxb, chrooted_path, *keys);
            auto var_resp = trxb.var<ListResponse>();
            znode.list(var_resp, concrete_request_filted_list->list_request_type);
            ListResponse resp;

            all_responses_var.emplace_back(var_resp.as<Response>(), createResponseInMulti<ListResponse>);
        }
        else
            throw Exception(Error::ZBADARGUMENTS, "Illegal command as part of multi ZooKeeper request");

        trxb.then(add_curr_request);
    }

    if (need_commit)
    {
        if (resps_need_cpmzxid.empty() && resps_need_pmzxid.empty() && resps_need_mzxid.empty())
        {
            trxb.commit();
        }
        else
        {
            auto var_vs = trxb.var<FDBVersionstamp>();
            trxb.commit(var_vs);
            trxb.then(TRX_STEP(var_vs, resps_need_cpmzxid, resps_need_pmzxid, resps_need_mzxid)
            {
                auto & zxid = *ctx.getVar(var_vs.as<int64_t>());
                for (const auto & var_resp : resps_need_cpmzxid)
                {
                    auto & resp = *ctx.getVar(var_resp);
                    resp.stat.czxid = zxid;
                    resp.stat.mzxid = zxid;
                    resp.stat.pzxid = zxid;
                }
                for (const auto & var_resp : resps_need_pmzxid)
                {
                    auto & resp = *ctx.getVar(var_resp);
                    resp.stat.mzxid = zxid;
                    resp.stat.pzxid = zxid;
                }
                for (const auto & var_resp : resps_need_mzxid)
                {
                    auto & resp = *ctx.getVar(var_resp);
                    resp.stat.mzxid = zxid;
                }
                return nullptr;
            });
        }
    }

    trxb.exec(
        newTrx(),
        [
#ifdef ZOOKEEPER_LOG
            resp_logger,
            request_time = std::chrono::steady_clock::now(),
#endif
            curr_request_var,
            all_responses_var,
            callback](AsyncTrxBuilder::Context & ctx, std::exception_ptr eptr)
        {
            MultiResponse multi_resp;
            auto resp_cnt = all_responses_var.size();
            auto & curr_request = *ctx.getVar(curr_request_var);

            if (unlikely(eptr))
                multi_resp.error = DB::FoundationDB::getKeeperErrorFromException(eptr);
            else
                multi_resp.error = Error::ZOK;

            size_t i = 0;

            /// Success requests
            for (; i < curr_request; i++)
                multi_resp.responses.emplace_back(all_responses_var[i].second(ctx, all_responses_var[i].first, Error::ZOK));

            /// Failed request
            if (i < resp_cnt)
                multi_resp.responses.emplace_back(all_responses_var[i].second(ctx, all_responses_var[i].first, multi_resp.error));

            /// Remaining unexecuted requests
            for (i += 1; i < resp_cnt; i++)
                multi_resp.responses.emplace_back(
                    all_responses_var[i].second(ctx, all_responses_var[i].first, Error::ZRUNTIMEINCONSISTENCY));

#ifdef ZOOKEEPER_LOG
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - request_time).count();
            resp_logger.response(multi_resp, elapsed_ms);
#endif
            callback(multi_resp);
        },
        trx_tracker.newToken());

    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
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
    KeeperCleaner temp_cleaner(*keys);
    return temp_cleaner.clean(session_id, newTrx());
}
}

#include "ZooKeeper.h"

#include <random>
#include <pcg_random.hpp>
#include <functional>
#include <common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/PODArray.h>
#include <Common/randomSeed.h>


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
    extern const Event ZooKeeperGetChildren;
}

namespace CurrentMetrics
{
    extern const Metric ZooKeeperWatch;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}


namespace zkutil
{

const int CreateMode::Persistent = 0;
const int CreateMode::Ephemeral = 1;
const int CreateMode::PersistentSequential = 2;
const int CreateMode::EphemeralSequential = 3;


static void check(int32_t code, const std::string & path)
{
    if (code)
        throw KeeperException(code, path);
}


void ZooKeeper::init(const std::string & hosts_, const std::string & identity_,
                     int32_t session_timeout_ms_, const std::string & chroot_)
{
    log = &Logger::get("ZooKeeper");
    hosts = hosts_;
    identity = identity_;
    session_timeout_ms = session_timeout_ms_;
    chroot = chroot_;

    std::string hosts_for_lib = hosts + chroot;
    impl = zookeeper_init(hosts_for_lib.c_str(), nullptr, session_timeout_ms, nullptr, nullptr, 0);
    ProfileEvents::increment(ProfileEvents::ZooKeeperInit);

    if (!identity.empty())
    {
        auto code = zoo_add_auth(impl, "digest", identity.c_str(), static_cast<int>(identity.size()), nullptr, nullptr);
        if (code != ZOK)
            throw KeeperException("Zookeeper authentication failed. Hosts are  " + hosts, code);

        default_acl = &ZOO_CREATOR_ALL_ACL;
    }
    else
        default_acl = &ZOO_OPEN_ACL_UNSAFE;

    LOG_TRACE(log, "initialized, hosts: " << hosts << (chroot.empty() ? "" : ", chroot: " + chroot));

    if (!chroot.empty() && !exists("/"))
        throw KeeperException("Zookeeper root doesn't exist. You should create root node " + chroot + " before start.");
}

ZooKeeper::ZooKeeper(const std::string & hosts, const std::string & identity,
                     int32_t session_timeout_ms, const std::string & chroot)
{
    init(hosts, identity, session_timeout_ms, chroot);
}

struct ZooKeeperArgs
{
    ZooKeeperArgs(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_name, keys);

        std::vector<std::string> hosts_strings;

        session_timeout_ms = DEFAULT_SESSION_TIMEOUT;
        for (const auto & key : keys)
        {
            if (startsWith(key, "node"))
            {
                hosts_strings.push_back(
                    config.getString(config_name + "." + key + ".host") + ":"
                    + config.getString(config_name + "." + key + ".port", "2181")
                );
            }
            else if (key == "session_timeout_ms")
            {
                session_timeout_ms = config.getInt(config_name + "." + key);
            }
            else if (key == "identity")
            {
                identity = config.getString(config_name + "." + key);
            }
            else if (key == "root")
            {
                chroot = config.getString(config_name + "." + key);
            }
            else throw KeeperException(std::string("Unknown key ") + key + " in config file");
        }

        /// Shuffle the hosts to distribute the load among ZooKeeper nodes.
        pcg64 rng(randomSeed());
        std::shuffle(hosts_strings.begin(), hosts_strings.end(), rng);

        for (auto & host : hosts_strings)
        {
            if (hosts.size())
                hosts += ",";
            hosts += host;
        }

        if (!chroot.empty())
        {
            if (chroot.front() != '/')
                throw KeeperException(std::string("Root path in config file should start with '/', but got ") + chroot);
            if (chroot.back() == '/')
                chroot.pop_back();
        }
    }

    std::string hosts;
    std::string identity;
    int session_timeout_ms;
    std::string chroot;
};

ZooKeeper::ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
{
    ZooKeeperArgs args(config, config_name);
    init(args.hosts, args.identity, args.session_timeout_ms, args.chroot);
}


static WatchCallback callbackForEvent(const EventPtr & watch)
{
    return [watch](const ZooKeeperImpl::ZooKeeper::WatchResponse &) { watch->set(); };
}


int32_t ZooKeeper::getChildrenImpl(const std::string & path, Strings & res,
                    Stat * stat,
                    WatchCallback watch_callback)
{
    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::ListResponse & response)
    {
        code = response.error;
        if (!code)
        {
            res = response.names;
            if (stat)
                *stat = response.stat;
        }
        event.set();
    };

    impl.list(path, callback, watch_callback);
    event.wait();
    return code;
}

Strings ZooKeeper::getChildren(
    const std::string & path, Stat * stat, const EventPtr & watch)
{
    Strings res;
    check(tryGetChildren(path, res, stat, watch), path);
    return res;
}

int32_t ZooKeeper::tryGetChildren(const std::string & path, Strings & res,
                                Stat * stat, const EventPtr & watch)
{
    int32_t code = getChildrenImpl(path, res, stat, callbackForEvent(watch));

    if (!(code == ZOK || code == ZNONODE))
        throw KeeperException(code, path);

    return code;
}

int32_t ZooKeeper::createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::CreateResponse & response)
    {
        code = response.error;
        if (!code)
            path_created = response.path_created;
        event.set();
    };

    impl.create(path, data, mode & 1, mode & 2, {}, callback);  /// TODO better mode
    event.wait();

    ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return code;
}

std::string ZooKeeper::create(const std::string & path, const std::string & data, int32_t type)
{
    std::string path_created;
    check(tryCreate(path, data, type, path_created), path);
    return path_created;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    int32_t code = createImpl(path, data, mode, path_created);

    if (!(code == ZOK ||
        code == ZNONODE ||
        code == ZNODEEXISTS ||
        code == ZNOCHILDRENFOREPHEMERALS))
        throw KeeperException(code, path);

    return code;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
    std::string path_created;
    return tryCreate(path, data, mode, path_created);
}

void ZooKeeper::createIfNotExists(const std::string & path, const std::string & data)
{
    std::string path_created;
    int32_t code = createImpl(path, data, CreateMode::Persistent, path_created);

    if (code == ZOK || code == ZNODEEXISTS)
        return;
    else
        throw KeeperException(code, path);
}

void ZooKeeper::createAncestors(const std::string & path)
{
    size_t pos = 1;
    while (true)
    {
        pos = path.find('/', pos);
        if (pos == std::string::npos)
            break;
        createIfNotExists(path.substr(0, pos), "");
        ++pos;
    }
}

int32_t ZooKeeper::removeImpl(const std::string & path, int32_t version)
{
    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::RemoveResponse & response)
    {
        if (response.error)
            code = response.error;
        event.set();
    };

    impl.remove(path, version, callback);
    event.wait();

    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return code;
}

void ZooKeeper::remove(const std::string & path, int32_t version)
{
    check(tryRemove(path, version), path);
}

int32_t ZooKeeper::tryRemove(const std::string & path, int32_t version)
{
    int32_t code = removeImpl(path, version);
    if (!(code == ZOK ||
        code == ZNONODE ||
        code == ZBADVERSION ||
        code == ZNOTEMPTY))
        throw KeeperException(code, path);
    return code;
}

int32_t ZooKeeper::existsImpl(const std::string & path, Stat * stat, WatchCallback watch_callback)
{
    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::ExistsResponse & response)
    {
        code = response.error;
        if (!code && stat)
            *stat = response.stat;
        event.set();
    };

    impl.exists(path, callback, watch_callback);
    event.wait();

    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return code;
}

bool ZooKeeper::exists(const std::string & path, Stat * stat, const EventPtr & watch)
{
    return existsWatch(path, stat, callbackForEvent(watch));
}

bool ZooKeeper::existsWatch(const std::string & path, Stat * stat, const WatchCallback & watch_callback)
{
    int32_t code = existsImpl(path, stat, watch_callback);

    if (!(code == ZOK || code == ZNONODE))
        throw KeeperException(code, path);
    if (code == ZNONODE)
        return false;
    return true;
}

int32_t ZooKeeper::getImpl(const std::string & path, std::string & res, Stat * stat, WatchCallback watch_callback)
{
    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::GetResponse & response)
    {
        code = response.error;
        if (!code)
        {
            res = response.data;
            if (stat)
                *stat = response.stat;
        }
        event.set();
    };

    impl.get(path, callback, watch_callback);
    event.wait();

    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return code;
}


std::string ZooKeeper::get(const std::string & path, Stat * stat, const EventPtr & watch)
{
    int32_t code = 0;
    std::string res;
    if (tryGet(path, res, stat, watch, &code))
        return res;
    else
        throw KeeperException("Can't get data for node " + path + ": node doesn't exist", code);
}

bool ZooKeeper::tryGet(const std::string & path, std::string & res, Stat * stat, const EventPtr & watch, int * return_code)
{
    return tryGetWatch(path, res, stat, callbackForEvent(watch), return_code);
}

bool ZooKeeper::tryGetWatch(const std::string & path, std::string & res, Stat * stat, const WatchCallback & watch_callback, int * return_code)
{
    int32_t code = getImpl(path, res, stat, watch_callback);

    if (!(code == ZOK || code == ZNONODE))
        throw KeeperException(code, path);

    if (return_code)
        *return_code = code;

    return code == ZOK;
}

int32_t ZooKeeper::setImpl(const std::string & path, const std::string & data,
                        int32_t version, Stat * stat)
{
    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::SetResponse & response)
    {
        code = response.error;
        if (!code && stat)
            *stat = response.stat;
        event.set();
    };

    impl.set(path, data, version, callback);
    event.wait();

    ProfileEvents::increment(ProfileEvents::ZooKeeperSet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return code;
}

void ZooKeeper::set(const std::string & path, const std::string & data, int32_t version, Stat * stat)
{
    check(trySet(path, data, version, stat), path);
}

void ZooKeeper::createOrUpdate(const std::string & path, const std::string & data, int32_t mode)
{
    int32_t code = trySet(path, data, -1);
    if (code == ZNONODE)
    {
        create(path, data, mode);
    }
    else if (code != ZOK)
        throw KeeperException(code, path);
}

int32_t ZooKeeper::trySet(const std::string & path, const std::string & data,
                                    int32_t version, Stat * stat)
{
    int32_t code = setImpl(path, data, version, stat);

    if (!(code == ZOK ||
        code == ZNONODE ||
        code == ZBADVERSION))
        throw KeeperException(code, path);
    return code;
}


int32_t ZooKeeper::multiImpl(const Requests & requests, Responses & responses)
{
    if (requests.empty())
        return ZOK;

    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::MultiResponse & response)
    {
        code = response.error;
        if (!code)
            responses = response.responses;
        event.set();
    };

    impl.multi(requests, callback);
    event.wait();

    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return code;
}

Responses ZooKeeper::multi(const Requests & requests)
{
    Responses responses;
    int32_t code = multiImpl(requests, responses);
    KeeperMultiException::check(code, requests, responses);
    return responses;
}

int32_t ZooKeeper::tryMulti(const Requests & requests)
{
    Responses responses;
    int32_t code = multiImpl(requests, responses);

    if (!(code == ZOK ||
          code == ZNONODE ||
          code == ZNODEEXISTS ||
          code == ZNOCHILDRENFOREPHEMERALS ||
          code == ZBADVERSION ||
          code == ZNOTEMPTY))
        throw KeeperException(code);
    return code;
}


void ZooKeeper::removeChildrenRecursive(const std::string & path)
{
    Strings children = getChildren(path);
    while (!children.empty())
    {
        Requests ops;
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            removeChildrenRecursive(path + "/" + children.back());

            ZooKeeperImpl::ZooKeeper::RemoveRequest request;
            request.path = path + "/" + children.back();

            ops.emplace_back(std::make_shared<ZooKeeperImpl::ZooKeeper::RemoveRequest>(std::move(request)));
            children.pop_back();
        }
        multi(ops);
    }
}

void ZooKeeper::tryRemoveChildrenRecursive(const std::string & path)
{
    Strings children;
    if (tryGetChildren(path, children) != ZOK)
        return;
    while (!children.empty())
    {
        Requests ops;
        Strings batch;
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            batch.push_back(path + "/" + children.back());
            children.pop_back();
            tryRemoveChildrenRecursive(batch.back());

            ZooKeeperImpl::ZooKeeper::RemoveRequest request;
            request.path = batch.back();

            ops.emplace_back(std::make_shared<ZooKeeperImpl::ZooKeeper::RemoveRequest>(std::move(request)));
        }

        /// Try to remove the children with a faster method - in bulk. If this fails,
        /// this means someone is concurrently removing these children and we will have
        /// to remove them one by one.
        if (tryMulti(ops) != ZOK)
            for (const std::string & child : batch)
                tryRemove(child);
    }
}

void ZooKeeper::removeRecursive(const std::string & path)
{
    removeChildrenRecursive(path);
    remove(path);
}

void ZooKeeper::tryRemoveRecursive(const std::string & path)
{
    tryRemoveChildrenRecursive(path);
    tryRemove(path);
}


void ZooKeeper::waitForDisappear(const std::string & path)
{
    while (true)
    {
        int32_t code = 0;
        int32_t event_type = 0;
        Poco::Event event;

        auto callback = [&](const ZooKeeperImpl::ZooKeeper::ExistsResponse & response)
        {
            code = response.error;
            if (code)
                event.set();
        };

        auto watch = [&](const ZooKeeperImpl::ZooKeeper::WatchResponse & response)
        {
            code = response.error;
            if (!code)
                event_type = response.type;
            event.set();
        };

        impl.exists(path, callback, watch);
        event.wait();

        ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
        ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

        if (code == ZNONODE)
            return;

        if (code)
            throw KeeperException(code, path);

        if (event_type == ZooKeeperImpl::ZooKeeper::DELETED)
            return;
    }
}

ZooKeeperPtr ZooKeeper::startNewSession() const
{
    return std::make_shared<ZooKeeper>(hosts, identity, session_timeout_ms, chroot);
}


std::string ZooKeeper::error2string(int32_t code)
{
    return ZooKeeperImpl::ZooKeeper::errorMessage(code);
}

bool ZooKeeper::expired()
{
    return impl.isExpired();
}

Int64 ZooKeeper::getClientID()
{
    return impl.getSessionID();
}


std::future<ZooKeeperImpl::ZooKeeper::GetResponse> ZooKeeper::asyncGet(const std::string & path)
{
    std::promise<ZooKeeperImpl::ZooKeeper::GetResponse> promise;
    auto future = promise.get_future();

    auto callback = [promise = std::move(promise)](const ZooKeeperImpl::ZooKeeper::GetResponse & response) mutable
    {
        if (response.error)
            promise.set_exception(std::make_exception_ptr(KeeperException(response.error)));
        else
            promise.set_value(response);
    };

    impl.get(path, callback, {});

    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return future;
}


std::future<ZooKeeperImpl::ZooKeeper::GetResponse> ZooKeeper::asyncTryGet(const std::string & path)
{
    std::promise<ZooKeeperImpl::ZooKeeper::GetResponse> promise;
    auto future = promise.get_future();

    auto callback = [promise = std::move(promise)](const ZooKeeperImpl::ZooKeeper::GetResponse & response) mutable
    {
        if (response.error && response.error != ZNONODE)
            promise.set_exception(std::make_exception_ptr(KeeperException(response.error)));
        else
            promise.set_value(response);
    };

    impl.get(path, callback, {});

    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::ExistsResponse> ZooKeeper::asyncExists(const std::string & path)
{
    std::promise<ZooKeeperImpl::ZooKeeper::ExistsResponse> promise;
    auto future = promise.get_future();

    auto callback = [promise = std::move(promise)](const ZooKeeperImpl::ZooKeeper::ExistsResponse & response) mutable
    {
        if (response.error && response.error != ZNONODE)
            promise.set_exception(std::make_exception_ptr(KeeperException(response.error)));
        else
            promise.set_value(response);
    };

    impl.exists(path, callback, {});

    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return future;
}


ZooKeeper::GetChildrenFuture ZooKeeper::asyncGetChildren(const std::string & path)
{
    GetChildrenFuture future {
        [path] (int rc, const String_vector * strings)
        {
            if (rc != ZOK)
                throw KeeperException(rc, path);

            Strings res;
            res.resize(strings->count);
            for (int i = 0; i < strings->count; ++i)
                res[i] = std::string(strings->data[i]);

            return res;
        }};

    int32_t code = zoo_aget_children(
        impl, path.c_str(), 0,
        [] (int rc, const String_vector * strings, const void * data)
        {
            GetChildrenFuture::TaskPtr owned_task =
                std::move(const_cast<GetChildrenFuture::TaskPtr &>(*static_cast<const GetChildrenFuture::TaskPtr *>(data)));
            (*owned_task)(rc, strings);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperGetChildren);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

ZooKeeper::RemoveFuture ZooKeeper::asyncRemove(const std::string & path, int32_t version)
{
    RemoveFuture future {
        [path] (int rc)
        {
            if (rc != ZOK)
                throw KeeperException(rc, path);
        }};

    int32_t code = zoo_adelete(
        impl, path.c_str(), version,
        [] (int rc, const void * data)
        {
            RemoveFuture::TaskPtr owned_task =
                std::move(const_cast<RemoveFuture::TaskPtr &>(*static_cast<const RemoveFuture::TaskPtr *>(data)));
            (*owned_task)(rc);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

ZooKeeper::TryRemoveFuture ZooKeeper::asyncTryRemove(const std::string & path, int32_t version)
{
    TryRemoveFuture future {
        [path] (int rc)
        {
            if (rc != ZOK && rc != ZNONODE && rc != ZBADVERSION && rc != ZNOTEMPTY)
                throw KeeperException(rc, path);

            return rc;
        }};

    int32_t code = zoo_adelete(
        impl, path.c_str(), version,
        [] (int rc, const void * data)
        {
            TryRemoveFuture::TaskPtr owned_task =
                std::move(const_cast<TryRemoveFuture::TaskPtr &>(*static_cast<const TryRemoveFuture::TaskPtr *>(data)));
            (*owned_task)(rc);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

ZooKeeper::MultiFuture ZooKeeper::asyncMultiImpl(const Ops & ops_, bool throw_exception)
{
    /// We need to hold all references to ops data until the end of multi callback
    struct OpsHolder
    {
        std::shared_ptr<Ops> ops_ptr;
        std::shared_ptr<std::vector<zoo_op_t>> ops_native;
        std::shared_ptr<std::vector<zoo_op_result_t>> op_results_native;
    } holder;

    /// Copy ops (swallow copy)
    holder.ops_ptr = std::make_shared<Ops>(ops_);
    /// Copy native ops to contiguous vector
    holder.ops_native = std::make_shared<std::vector<zoo_op_t>>();
    for (const OpPtr & op : *holder.ops_ptr)
        holder.ops_native->push_back(*op->data);
    /// Allocate native result holders
    holder.op_results_native = std::make_shared<std::vector<zoo_op_result_t>>(holder.ops_ptr->size());

    MultiFuture future{ [throw_exception, holder, zookeeper=this] (int rc) {
        OpResultsAndCode res;
        res.code = rc;
        convertOpResults(*holder.op_results_native, res.results, zookeeper);
        res.ops_ptr = holder.ops_ptr;
        if (throw_exception && rc != ZOK)
            throw KeeperException(rc);
        return res;
    }};

    if (ops_.empty())
    {
        (**future.task)(ZOK);
        return future;
    }

    /// Workaround of the libzookeeper bug.
    /// TODO: check if the bug is fixed in the latest version of libzookeeper.
    if (expired())
        throw KeeperException(ZINVALIDSTATE);

    int32_t code = zoo_amulti(impl, static_cast<int>(holder.ops_native->size()),
                              holder.ops_native->data(),
                              holder.op_results_native->data(),
                              [] (int rc, const void * data)
                              {
                                  MultiFuture::TaskPtr owned_task =
                                      std::move(const_cast<MultiFuture::TaskPtr &>(*static_cast<const MultiFuture::TaskPtr *>(data)));
                                  (*owned_task)(rc);
                              }, future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code);

    return future;
}

ZooKeeper::MultiFuture ZooKeeper::tryAsyncMulti(const Ops & ops)
{
    return asyncMultiImpl(ops, false);
}

ZooKeeper::MultiFuture ZooKeeper::asyncMulti(const Ops & ops)
{
    return asyncMultiImpl(ops, true);
}


size_t getFailedOpIndex(const OpResultsPtr & op_results, int32_t transaction_return_code)
{
    if (op_results == nullptr || op_results->empty())
        throw DB::Exception("OpResults is empty", DB::ErrorCodes::LOGICAL_ERROR);

    for (size_t index = 0; index < op_results->size(); ++index)
    {
        if ((*op_results)[index].err != ZOK)
            return index;
    }

    if (!isUserError(transaction_return_code))
    {
        throw DB::Exception("There are no failed OPs because '" + ZooKeeper::error2string(transaction_return_code) + "' is not valid response code for that",
                            DB::ErrorCodes::LOGICAL_ERROR);
    }

    throw DB::Exception("There is no failed OpResult", DB::ErrorCodes::LOGICAL_ERROR);
}


KeeperMultiException::KeeperMultiException(const MultiTransactionInfo & info_, size_t failed_op_index_)
    :KeeperException(
        "Transaction failed at op #" + std::to_string(failed_op_index_) + ": " + info_.ops.at(failed_op_index_)->describe(),
        info_.code),
    info(info_) {}

void KeeperMultiException::check(int32_t code, const Ops & ops, const OpResultsPtr & op_results)
{
    if (code == ZOK) {}
    else if (isUserError(code))
        throw KeeperMultiException(MultiTransactionInfo(code, ops, op_results), getFailedOpIndex(op_results, code));
    else
        throw KeeperException(code);
}

const Op & MultiTransactionInfo::getFailedOp() const
{
    return *ops.at(getFailedOpIndex(op_results, code));
}

}

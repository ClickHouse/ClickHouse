#include "ZooKeeper.h"
#include "KeeperException.h"

#include <random>
#include <pcg_random.hpp>
#include <functional>
#include <boost/algorithm/string.hpp>

#include <common/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/PODArray.h>
#include <Common/randomSeed.h>

#define ZOOKEEPER_CONNECTION_TIMEOUT_MS 1000
#define ZOOKEEPER_OPERATION_TIMEOUT_MS 10000


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

    if (hosts.empty())
        throw KeeperException("No addresses passed to ZooKeeper constructor.", ZooKeeperImpl::ZooKeeper::ZBADARGUMENTS);

    std::vector<std::string> addresses_strings;
    boost::split(addresses_strings, hosts, boost::is_any_of(","));
    ZooKeeperImpl::ZooKeeper::Addresses addresses;
    addresses.reserve(addresses_strings.size());
    for (const auto & address_string : addresses_strings)
        addresses.emplace_back(address_string);

    impl = std::make_unique<ZooKeeperImpl::ZooKeeper>(
        addresses,
        chroot,
        identity_.empty() ? "" : "digest",
        identity_,
        Poco::Timespan(0, session_timeout_ms_ * 1000),
        Poco::Timespan(0, ZOOKEEPER_CONNECTION_TIMEOUT_MS * 1000),
        Poco::Timespan(0, ZOOKEEPER_OPERATION_TIMEOUT_MS * 1000));

    LOG_TRACE(log, "initialized, hosts: " << hosts << (chroot.empty() ? "" : ", chroot: " + chroot));

    if (!chroot.empty() && !exists("/"))
        throw KeeperException("Zookeeper root doesn't exist. You should create root node " + chroot + " before start.", ZooKeeperImpl::ZooKeeper::ZNONODE);
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
            else
                throw KeeperException(std::string("Unknown key ") + key + " in config file", ZooKeeperImpl::ZooKeeper::ZBADARGUMENTS);
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
                throw KeeperException(std::string("Root path in config file should start with '/', but got ") + chroot, ZooKeeperImpl::ZooKeeper::ZBADARGUMENTS);
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
    if (!watch)
        return {};
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

    impl->list(path, callback, watch_callback);
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

Strings ZooKeeper::getChildrenWatch(
    const std::string & path, Stat * stat, WatchCallback watch_callback)
{
    Strings res;
    check(tryGetChildrenWatch(path, res, stat, watch_callback), path);
    return res;
}

int32_t ZooKeeper::tryGetChildren(const std::string & path, Strings & res,
                                Stat * stat, const EventPtr & watch)
{
    int32_t code = getChildrenImpl(path, res, stat, callbackForEvent(watch));

    if (!(code == ZooKeeperImpl::ZooKeeper::ZOK || code == ZooKeeperImpl::ZooKeeper::ZNONODE))
        throw KeeperException(code, path);

    return code;
}

int32_t ZooKeeper::tryGetChildrenWatch(const std::string & path, Strings & res,
                                Stat * stat, WatchCallback watch_callback)
{
    int32_t code = getChildrenImpl(path, res, stat, watch_callback);

    if (!(code == ZooKeeperImpl::ZooKeeper::ZOK || code == ZooKeeperImpl::ZooKeeper::ZNONODE))
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

    impl->create(path, data, mode & 1, mode & 2, {}, callback);  /// TODO better mode
    event.wait();
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

    if (!(code == ZooKeeperImpl::ZooKeeper::ZOK ||
        code == ZooKeeperImpl::ZooKeeper::ZNONODE ||
        code == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS ||
        code == ZooKeeperImpl::ZooKeeper::ZNOCHILDRENFOREPHEMERALS))
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

    if (code == ZooKeeperImpl::ZooKeeper::ZOK || code == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS)
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

    impl->remove(path, version, callback);
    event.wait();
    return code;
}

void ZooKeeper::remove(const std::string & path, int32_t version)
{
    check(tryRemove(path, version), path);
}

int32_t ZooKeeper::tryRemove(const std::string & path, int32_t version)
{
    int32_t code = removeImpl(path, version);
    if (!(code == ZooKeeperImpl::ZooKeeper::ZOK ||
        code == ZooKeeperImpl::ZooKeeper::ZNONODE ||
        code == ZooKeeperImpl::ZooKeeper::ZBADVERSION ||
        code == ZooKeeperImpl::ZooKeeper::ZNOTEMPTY))
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

    impl->exists(path, callback, watch_callback);
    event.wait();
    return code;
}

bool ZooKeeper::exists(const std::string & path, Stat * stat, const EventPtr & watch)
{
    return existsWatch(path, stat, callbackForEvent(watch));
}

bool ZooKeeper::existsWatch(const std::string & path, Stat * stat, WatchCallback watch_callback)
{
    int32_t code = existsImpl(path, stat, watch_callback);

    if (!(code == ZooKeeperImpl::ZooKeeper::ZOK || code == ZooKeeperImpl::ZooKeeper::ZNONODE))
        throw KeeperException(code, path);
    if (code == ZooKeeperImpl::ZooKeeper::ZNONODE)
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

    impl->get(path, callback, watch_callback);
    event.wait();
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

std::string ZooKeeper::getWatch(const std::string & path, Stat * stat, WatchCallback watch_callback)
{
    int32_t code = 0;
    std::string res;
    if (tryGetWatch(path, res, stat, watch_callback, &code))
        return res;
    else
        throw KeeperException("Can't get data for node " + path + ": node doesn't exist", code);
}

bool ZooKeeper::tryGet(const std::string & path, std::string & res, Stat * stat, const EventPtr & watch, int * return_code)
{
    return tryGetWatch(path, res, stat, callbackForEvent(watch), return_code);
}

bool ZooKeeper::tryGetWatch(const std::string & path, std::string & res, Stat * stat, WatchCallback watch_callback, int * return_code)
{
    int32_t code = getImpl(path, res, stat, watch_callback);

    if (!(code == ZooKeeperImpl::ZooKeeper::ZOK || code == ZooKeeperImpl::ZooKeeper::ZNONODE))
        throw KeeperException(code, path);

    if (return_code)
        *return_code = code;

    return code == ZooKeeperImpl::ZooKeeper::ZOK;
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

    impl->set(path, data, version, callback);
    event.wait();
    return code;
}

void ZooKeeper::set(const std::string & path, const std::string & data, int32_t version, Stat * stat)
{
    check(trySet(path, data, version, stat), path);
}

void ZooKeeper::createOrUpdate(const std::string & path, const std::string & data, int32_t mode)
{
    int32_t code = trySet(path, data, -1);
    if (code == ZooKeeperImpl::ZooKeeper::ZNONODE)
    {
        create(path, data, mode);
    }
    else if (code != ZooKeeperImpl::ZooKeeper::ZOK)
        throw KeeperException(code, path);
}

int32_t ZooKeeper::trySet(const std::string & path, const std::string & data,
                                    int32_t version, Stat * stat)
{
    int32_t code = setImpl(path, data, version, stat);

    if (!(code == ZooKeeperImpl::ZooKeeper::ZOK ||
        code == ZooKeeperImpl::ZooKeeper::ZNONODE ||
        code == ZooKeeperImpl::ZooKeeper::ZBADVERSION))
        throw KeeperException(code, path);
    return code;
}


int32_t ZooKeeper::multiImpl(const Requests & requests, Responses & responses)
{
    if (requests.empty())
        return ZooKeeperImpl::ZooKeeper::ZOK;

    int32_t code = 0;
    Poco::Event event;

    auto callback = [&](const ZooKeeperImpl::ZooKeeper::MultiResponse & response)
    {
        code = response.error;
        responses = response.responses;
        event.set();
    };

    impl->multi(requests, callback);
    event.wait();
    return code;
}

Responses ZooKeeper::multi(const Requests & requests)
{
    Responses responses;
    int32_t code = multiImpl(requests, responses);
    KeeperMultiException::check(code, requests, responses);
    return responses;
}

int32_t ZooKeeper::tryMulti(const Requests & requests, Responses & responses)
{
    int32_t code = multiImpl(requests, responses);
    if (code && !ZooKeeperImpl::ZooKeeper::isUserError(code))
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
            ops.emplace_back(makeRemoveRequest(path + "/" + children.back(), -1));
            children.pop_back();
        }
        multi(ops);
    }
}

void ZooKeeper::tryRemoveChildrenRecursive(const std::string & path)
{
    Strings children;
    if (tryGetChildren(path, children) != ZooKeeperImpl::ZooKeeper::ZOK)
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
        Responses responses;
        if (tryMulti(ops, responses) != ZooKeeperImpl::ZooKeeper::ZOK)
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


namespace
{
    struct WaitForDisappearState
    {
        int32_t code = 0;
        int32_t event_type = 0;
        Poco::Event event;
    };
    using WaitForDisappearStatePtr = std::shared_ptr<WaitForDisappearState>;
}

void ZooKeeper::waitForDisappear(const std::string & path)
{
    WaitForDisappearStatePtr state = std::make_shared<WaitForDisappearState>();

    while (true)
    {
        auto callback = [state](const ZooKeeperImpl::ZooKeeper::ExistsResponse & response)
        {
            state->code = response.error;
            if (state->code)
                state->event.set();
        };

        auto watch = [state](const ZooKeeperImpl::ZooKeeper::WatchResponse & response)
        {
            if (!state->code)
            {
                state->code = response.error;
                if (!state->code)
                    state->event_type = response.type;
                state->event.set();
            }
        };

        /// NOTE: if the node doesn't exist, the watch will leak.

        impl->exists(path, callback, watch);
        state->event.wait();

        if (state->code == ZooKeeperImpl::ZooKeeper::ZNONODE)
            return;

        if (state->code)
            throw KeeperException(state->code, path);

        if (state->event_type == ZooKeeperImpl::ZooKeeper::DELETED)
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
    return impl->isExpired();
}

Int64 ZooKeeper::getClientID()
{
    return impl->getSessionID();
}


std::future<ZooKeeperImpl::ZooKeeper::CreateResponse> ZooKeeper::asyncCreate(const std::string & path, const std::string & data, int32_t mode)
{
    /// https://stackoverflow.com/questions/25421346/how-to-create-an-stdfunction-from-a-move-capturing-lambda-expression
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::CreateResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::CreateResponse & response) mutable
    {
        if (response.error)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->create(path, data, mode & 1, mode & 2, {}, std::move(callback));
    return future;
}


std::future<ZooKeeperImpl::ZooKeeper::GetResponse> ZooKeeper::asyncGet(const std::string & path)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::GetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::GetResponse & response) mutable
    {
        if (response.error)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->get(path, std::move(callback), {});
    return future;
}


std::future<ZooKeeperImpl::ZooKeeper::GetResponse> ZooKeeper::asyncTryGet(const std::string & path)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::GetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::GetResponse & response) mutable
    {
        if (response.error && response.error != ZooKeeperImpl::ZooKeeper::ZNONODE)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->get(path, std::move(callback), {});
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::ExistsResponse> ZooKeeper::asyncExists(const std::string & path)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::ExistsResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::ExistsResponse & response) mutable
    {
        if (response.error && response.error != ZooKeeperImpl::ZooKeeper::ZNONODE)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->exists(path, std::move(callback), {});
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::SetResponse> ZooKeeper::asyncSet(const std::string & path, const std::string & data, int32_t version)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::SetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::SetResponse & response) mutable
    {
        if (response.error)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->set(path, data, version, std::move(callback));
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::ListResponse> ZooKeeper::asyncGetChildren(const std::string & path)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::ListResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::ListResponse & response) mutable
    {
        if (response.error)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->list(path, std::move(callback), {});
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::RemoveResponse> ZooKeeper::asyncRemove(const std::string & path, int32_t version)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::RemoveResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::RemoveResponse & response) mutable
    {
        if (response.error)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->remove(path, version, std::move(callback));
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::RemoveResponse> ZooKeeper::asyncTryRemove(const std::string & path, int32_t version)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::RemoveResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const ZooKeeperImpl::ZooKeeper::RemoveResponse & response) mutable
    {
        if (response.error && response.error != ZooKeeperImpl::ZooKeeper::ZNONODE && response.error != ZooKeeperImpl::ZooKeeper::ZBADVERSION && response.error != ZooKeeperImpl::ZooKeeper::ZNOTEMPTY)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->remove(path, version, std::move(callback));
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::MultiResponse> ZooKeeper::tryAsyncMulti(const Requests & ops)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::MultiResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const ZooKeeperImpl::ZooKeeper::MultiResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->multi(ops, std::move(callback));
    return future;
}

std::future<ZooKeeperImpl::ZooKeeper::MultiResponse> ZooKeeper::asyncMulti(const Requests & ops)
{
    auto promise = std::make_shared<std::promise<ZooKeeperImpl::ZooKeeper::MultiResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const ZooKeeperImpl::ZooKeeper::MultiResponse & response) mutable
    {
        if (response.error)
            promise->set_exception(std::make_exception_ptr(KeeperException(response.error)));
        else
            promise->set_value(response);
    };

    impl->multi(ops, std::move(callback));
    return future;
}

int32_t ZooKeeper::tryMultiNoThrow(const Requests & requests, Responses & responses)
{
    try
    {
        return multiImpl(requests, responses);
    }
    catch (const ZooKeeperImpl::Exception & e)
    {
        return e.code;
    }
}


size_t KeeperMultiException::getFailedOpIndex(int32_t code, const Responses & responses)
{
    if (responses.empty())
        throw DB::Exception("Responses for multi transaction is empty", DB::ErrorCodes::LOGICAL_ERROR);

    for (size_t index = 0, size = responses.size(); index < size; ++index)
        if (responses[index]->error)
            return index;

    if (!ZooKeeperImpl::ZooKeeper::isUserError(code))
        throw DB::Exception("There are no failed OPs because '" + ZooKeeper::error2string(code) + "' is not valid response code for that",
            DB::ErrorCodes::LOGICAL_ERROR);

    throw DB::Exception("There is no failed OpResult", DB::ErrorCodes::LOGICAL_ERROR);
}


KeeperMultiException::KeeperMultiException(int32_t code, const Requests & requests, const Responses & responses)
    : KeeperException("Transaction failed", code),
    requests(requests), responses(responses), failed_op_index(getFailedOpIndex(code, responses))
{
    addMessage("Op #" + std::to_string(failed_op_index) + ", path: " + getPathForFirstFailedOp());
}


std::string KeeperMultiException::getPathForFirstFailedOp() const
{
    return requests[failed_op_index]->getPath();
}

void KeeperMultiException::check(int32_t code, const Requests & requests, const Responses & responses)
{
    if (!code)
        return;

    if (ZooKeeperImpl::ZooKeeper::isUserError(code))
        throw KeeperMultiException(code, requests, responses);
    else
        throw KeeperException(code);
}


RequestPtr makeCreateRequest(const std::string & path, const std::string & data, int create_mode)
{
    auto request = std::make_shared<CreateRequest>();
    request->path = path;
    request->data = data;
    request->is_ephemeral = create_mode == CreateMode::Ephemeral || create_mode == CreateMode::EphemeralSequential;
    request->is_sequential = create_mode == CreateMode::PersistentSequential || create_mode == CreateMode::EphemeralSequential;
    return request;
}

RequestPtr makeRemoveRequest(const std::string & path, int version)
{
    auto request = std::make_shared<RemoveRequest>();
    request->path = path;
    request->version = version;
    return request;
}

RequestPtr makeSetRequest(const std::string & path, const std::string & data, int version)
{
    auto request = std::make_shared<SetRequest>();
    request->path = path;
    request->data = data;
    request->version = version;
    return request;
}

RequestPtr makeCheckRequest(const std::string & path, int version)
{
    auto request = std::make_shared<CheckRequest>();
    request->path = path;
    request->version = version;
    return request;
}

}

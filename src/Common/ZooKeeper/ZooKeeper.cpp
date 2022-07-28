#include "ZooKeeper.h"
#include "ZooKeeperImpl.h"
#include "KeeperException.h"
#include "TestKeeper.h"

#include <functional>
#include <filesystem>
#include <pcg-random/pcg_random.hpp>

#include <common/logger_useful.h>
#include <common/find_symbols.h>
#include <Common/randomSeed.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Exception.h>

#include <Poco/Net/NetException.h>


#define ZOOKEEPER_CONNECTION_TIMEOUT_MS 1000

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}


namespace zkutil
{

const int CreateMode::Persistent = 0;
const int CreateMode::Ephemeral = 1;
const int CreateMode::PersistentSequential = 2;
const int CreateMode::EphemeralSequential = 3;


static void check(Coordination::Error code, const std::string & path)
{
    if (code != Coordination::Error::ZOK)
        throw KeeperException(code, path);
}


void ZooKeeper::init(const std::string & implementation_, const Strings & hosts_, const std::string & identity_,
                     int32_t session_timeout_ms_, int32_t operation_timeout_ms_, const std::string & chroot_)
{
    log = &Poco::Logger::get("ZooKeeper");
    hosts = hosts_;
    identity = identity_;
    session_timeout_ms = session_timeout_ms_;
    operation_timeout_ms = operation_timeout_ms_;
    chroot = chroot_;
    implementation = implementation_;

    if (implementation == "zookeeper")
    {
        if (hosts.empty())
            throw KeeperException("No hosts passed to ZooKeeper constructor.", Coordination::Error::ZBADARGUMENTS);

        Coordination::ZooKeeper::Nodes nodes;
        nodes.reserve(hosts.size());

        Strings shuffled_hosts = hosts;
        /// Shuffle the hosts to distribute the load among ZooKeeper nodes.
        pcg64 generator(randomSeed());
        std::shuffle(shuffled_hosts.begin(), shuffled_hosts.end(), generator);

        bool dns_error = false;
        for (auto & host_string : shuffled_hosts)
        {
            try
            {
                bool secure = bool(startsWith(host_string, "secure://"));

                if (secure)
                    host_string.erase(0, strlen("secure://"));

                nodes.emplace_back(Coordination::ZooKeeper::Node{Poco::Net::SocketAddress{host_string}, secure});
            }
            catch (const Poco::Net::HostNotFoundException & e)
            {
                /// Most likely it's misconfiguration and wrong hostname was specified
                LOG_ERROR(log, "Cannot use ZooKeeper host {}, reason: {}", host_string, e.displayText());
            }
            catch (const Poco::Net::DNSException & e)
            {
                /// Most likely DNS is not available now
                dns_error = true;
                LOG_ERROR(log, "Cannot use ZooKeeper host {} due to DNS error: {}", host_string, e.displayText());
            }
        }

        if (nodes.empty())
        {
            /// For DNS errors we throw exception with ZCONNECTIONLOSS code, so it will be considered as hardware error, not user error
            if (dns_error)
                throw KeeperException("Cannot resolve any of provided ZooKeeper hosts due to DNS error", Coordination::Error::ZCONNECTIONLOSS);
            else
                throw KeeperException("Cannot use any of provided ZooKeeper nodes", Coordination::Error::ZBADARGUMENTS);
        }

        impl = std::make_unique<Coordination::ZooKeeper>(
                nodes,
                chroot,
                identity_.empty() ? "" : "digest",
                identity_,
                Poco::Timespan(0, session_timeout_ms_ * 1000),
                Poco::Timespan(0, ZOOKEEPER_CONNECTION_TIMEOUT_MS * 1000),
                Poco::Timespan(0, operation_timeout_ms_ * 1000));

        if (chroot.empty())
            LOG_TRACE(log, "Initialized, hosts: {}", fmt::join(hosts, ","));
        else
            LOG_TRACE(log, "Initialized, hosts: {}, chroot: {}", fmt::join(hosts, ","), chroot);
    }
    else if (implementation == "testkeeper")
    {
        impl = std::make_unique<Coordination::TestKeeper>(
                chroot,
                Poco::Timespan(0, operation_timeout_ms_ * 1000));
    }
    else
    {
        throw DB::Exception("Unknown implementation of coordination service: " + implementation, DB::ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!chroot.empty())
    {
        /// Here we check that zk root exists.
        /// This check is clumsy. The reason is we do this request under common mutex, and never want to hung here.
        /// Otherwise, all threads which need zk will wait for this mutex eternally.
        ///
        /// Usually, this was possible in case of memory limit exception happened inside zk implementation.
        /// This should not happen now, when memory tracker is disabled.
        /// But let's keep it just in case (it is also easy to backport).
        auto future = asyncExists("/");
        auto res = future.wait_for(std::chrono::milliseconds(operation_timeout_ms));
        if (res != std::future_status::ready)
            throw KeeperException("Cannot check if zookeeper root exists.", Coordination::Error::ZOPERATIONTIMEOUT);

        auto code = future.get().error;
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw KeeperException(code, "/");

        if (code == Coordination::Error::ZNONODE)
            throw KeeperException("ZooKeeper root doesn't exist. You should create root node " + chroot + " before start.", Coordination::Error::ZNONODE);
    }
}

ZooKeeper::ZooKeeper(const std::string & hosts_string, const std::string & identity_, int32_t session_timeout_ms_,
                     int32_t operation_timeout_ms_, const std::string & chroot_, const std::string & implementation_)
{
    Strings hosts_strings;
    splitInto<','>(hosts_strings, hosts_string);

    init(implementation_, hosts_strings, identity_, session_timeout_ms_, operation_timeout_ms_, chroot_);
}

ZooKeeper::ZooKeeper(const Strings & hosts_, const std::string & identity_, int32_t session_timeout_ms_,
                     int32_t operation_timeout_ms_, const std::string & chroot_, const std::string & implementation_)
{
    init(implementation_, hosts_, identity_, session_timeout_ms_, operation_timeout_ms_, chroot_);
}

struct ZooKeeperArgs
{
    ZooKeeperArgs(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_name, keys);

        session_timeout_ms = Coordination::DEFAULT_SESSION_TIMEOUT_MS;
        operation_timeout_ms = Coordination::DEFAULT_OPERATION_TIMEOUT_MS;
        implementation = "zookeeper";
        for (const auto & key : keys)
        {
            if (startsWith(key, "node"))
            {
                hosts.push_back(
                        (config.getBool(config_name + "." + key + ".secure", false) ? "secure://" : "") +
                        config.getString(config_name + "." + key + ".host") + ":"
                        + config.getString(config_name + "." + key + ".port", "2181")
                );
            }
            else if (key == "session_timeout_ms")
            {
                session_timeout_ms = config.getInt(config_name + "." + key);
            }
            else if (key == "operation_timeout_ms")
            {
                operation_timeout_ms = config.getInt(config_name + "." + key);
            }
            else if (key == "identity")
            {
                identity = config.getString(config_name + "." + key);
            }
            else if (key == "root")
            {
                chroot = config.getString(config_name + "." + key);
            }
            else if (key == "implementation")
            {
                implementation = config.getString(config_name + "." + key);
            }
            else
                throw KeeperException(std::string("Unknown key ") + key + " in config file", Coordination::Error::ZBADARGUMENTS);
        }

        if (!chroot.empty())
        {
            if (chroot.front() != '/')
                throw KeeperException(std::string("Root path in config file should start with '/', but got ") + chroot, Coordination::Error::ZBADARGUMENTS);
            if (chroot.back() == '/')
                chroot.pop_back();
        }
    }

    Strings hosts;
    std::string identity;
    int session_timeout_ms;
    int operation_timeout_ms;
    std::string chroot;
    std::string implementation;
};

ZooKeeper::ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
{
    ZooKeeperArgs args(config, config_name);
    init(args.implementation, args.hosts, args.identity, args.session_timeout_ms, args.operation_timeout_ms, args.chroot);
}

bool ZooKeeper::configChanged(const Poco::Util::AbstractConfiguration & config, const std::string & config_name) const
{
    ZooKeeperArgs args(config, config_name);

    // skip reload testkeeper cause it's for test and data in memory
    if (args.implementation == implementation && implementation == "testkeeper")
        return false;

    return std::tie(args.implementation, args.hosts, args.identity, args.session_timeout_ms, args.operation_timeout_ms, args.chroot)
        != std::tie(implementation, hosts, identity, session_timeout_ms, operation_timeout_ms, chroot);
}


static Coordination::WatchCallback callbackForEvent(const EventPtr & watch)
{
    if (!watch)
        return {};
    return [watch](const Coordination::WatchResponse &) { watch->set(); };
}


Coordination::Error ZooKeeper::getChildrenImpl(const std::string & path, Strings & res,
                                   Coordination::Stat * stat,
                                   Coordination::WatchCallback watch_callback)
{
    auto future_result = asyncTryGetChildrenNoThrow(path, watch_callback);

    if (future_result.wait_for(std::chrono::milliseconds(operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize();
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        Coordination::Error code = response.error;
        if (code == Coordination::Error::ZOK)
        {
            res = response.names;
            if (stat)
                *stat = response.stat;
        }
        return code;
    }
}

Strings ZooKeeper::getChildren(
        const std::string & path, Coordination::Stat * stat, const EventPtr & watch)
{
    Strings res;
    check(tryGetChildren(path, res, stat, watch), path);
    return res;
}

Strings ZooKeeper::getChildrenWatch(
        const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    Strings res;
    check(tryGetChildrenWatch(path, res, stat, watch_callback), path);
    return res;
}

Coordination::Error ZooKeeper::tryGetChildren(const std::string & path, Strings & res,
                                  Coordination::Stat * stat, const EventPtr & watch)
{
    Coordination::Error code = getChildrenImpl(path, res, stat, callbackForEvent(watch));

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException(code, path);

    return code;
}

Coordination::Error ZooKeeper::tryGetChildrenWatch(const std::string & path, Strings & res,
                                       Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    Coordination::Error code = getChildrenImpl(path, res, stat, watch_callback);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException(code, path);

    return code;
}

Coordination::Error ZooKeeper::createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    auto future_result = asyncTryCreateNoThrow(path, data, mode);

    if (future_result.wait_for(std::chrono::milliseconds(operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize();
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        Coordination::Error code = response.error;
        if (code == Coordination::Error::ZOK)
            path_created = response.path_created;
        return code;
    }
}

std::string ZooKeeper::create(const std::string & path, const std::string & data, int32_t mode)
{
    std::string path_created;
    check(tryCreate(path, data, mode, path_created), path);
    return path_created;
}

Coordination::Error ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    Coordination::Error code = createImpl(path, data, mode, path_created);

    if (!(code == Coordination::Error::ZOK ||
          code == Coordination::Error::ZNONODE ||
          code == Coordination::Error::ZNODEEXISTS ||
          code == Coordination::Error::ZNOCHILDRENFOREPHEMERALS))
        throw KeeperException(code, path);

    return code;
}

Coordination::Error ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
    std::string path_created;
    return tryCreate(path, data, mode, path_created);
}

void ZooKeeper::createIfNotExists(const std::string & path, const std::string & data)
{
    std::string path_created;
    Coordination::Error code = createImpl(path, data, CreateMode::Persistent, path_created);

    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
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

Coordination::Error ZooKeeper::removeImpl(const std::string & path, int32_t version)
{
    auto future_result = asyncTryRemoveNoThrow(path, version);


    if (future_result.wait_for(std::chrono::milliseconds(operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize();
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        return response.error;
    }
}

void ZooKeeper::remove(const std::string & path, int32_t version)
{
    check(tryRemove(path, version), path);
}

Coordination::Error ZooKeeper::tryRemove(const std::string & path, int32_t version)
{
    Coordination::Error code = removeImpl(path, version);
    if (!(code == Coordination::Error::ZOK ||
          code == Coordination::Error::ZNONODE ||
          code == Coordination::Error::ZBADVERSION ||
          code == Coordination::Error::ZNOTEMPTY))
        throw KeeperException(code, path);
    return code;
}

Coordination::Error ZooKeeper::existsImpl(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    auto future_result = asyncTryExistsNoThrow(path, watch_callback);

    if (future_result.wait_for(std::chrono::milliseconds(operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize();
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        Coordination::Error code = response.error;
        if (code == Coordination::Error::ZOK && stat)
            *stat = response.stat;

        return code;
    }
}

bool ZooKeeper::exists(const std::string & path, Coordination::Stat * stat, const EventPtr & watch)
{
    return existsWatch(path, stat, callbackForEvent(watch));
}

bool ZooKeeper::existsWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    Coordination::Error code = existsImpl(path, stat, watch_callback);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException(code, path);
    return code != Coordination::Error::ZNONODE;
}

Coordination::Error ZooKeeper::getImpl(const std::string & path, std::string & res, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    auto future_result = asyncTryGetNoThrow(path, watch_callback);

    if (future_result.wait_for(std::chrono::milliseconds(operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize();
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        Coordination::Error code = response.error;
        if (code == Coordination::Error::ZOK)
        {
            res = response.data;
            if (stat)
                *stat = response.stat;
        }
        return code;
    }
}


std::string ZooKeeper::get(const std::string & path, Coordination::Stat * stat, const EventPtr & watch)
{
    Coordination::Error code = Coordination::Error::ZOK;
    std::string res;
    if (tryGet(path, res, stat, watch, &code))
        return res;
    else
        throw KeeperException("Can't get data for node " + path + ": node doesn't exist", code);
}

std::string ZooKeeper::getWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    Coordination::Error code = Coordination::Error::ZOK;
    std::string res;
    if (tryGetWatch(path, res, stat, watch_callback, &code))
        return res;
    else
        throw KeeperException("Can't get data for node " + path + ": node doesn't exist", code);
}

bool ZooKeeper::tryGet(
    const std::string & path,
    std::string & res,
    Coordination::Stat * stat,
    const EventPtr & watch,
    Coordination::Error * return_code)
{
    return tryGetWatch(path, res, stat, callbackForEvent(watch), return_code);
}

bool ZooKeeper::tryGetWatch(
    const std::string & path,
    std::string & res,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::Error * return_code)
{
    Coordination::Error code = getImpl(path, res, stat, watch_callback);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException(code, path);

    if (return_code)
        *return_code = code;

    return code == Coordination::Error::ZOK;
}

Coordination::Error ZooKeeper::setImpl(const std::string & path, const std::string & data,
                           int32_t version, Coordination::Stat * stat)
{
    auto future_result = asyncTrySetNoThrow(path, data, version);

    if (future_result.wait_for(std::chrono::milliseconds(operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize();
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        Coordination::Error code = response.error;
        if (code == Coordination::Error::ZOK && stat)
            *stat = response.stat;

        return code;
    }
}

void ZooKeeper::set(const std::string & path, const std::string & data, int32_t version, Coordination::Stat * stat)
{
    check(trySet(path, data, version, stat), path);
}

void ZooKeeper::createOrUpdate(const std::string & path, const std::string & data, int32_t mode)
{
    Coordination::Error code = trySet(path, data, -1);
    if (code == Coordination::Error::ZNONODE)
    {
        create(path, data, mode);
    }
    else if (code != Coordination::Error::ZOK)
        throw KeeperException(code, path);
}

Coordination::Error ZooKeeper::trySet(const std::string & path, const std::string & data,
                          int32_t version, Coordination::Stat * stat)
{
    Coordination::Error code = setImpl(path, data, version, stat);

    if (!(code == Coordination::Error::ZOK ||
          code == Coordination::Error::ZNONODE ||
          code == Coordination::Error::ZBADVERSION))
        throw KeeperException(code, path);
    return code;
}


Coordination::Error ZooKeeper::multiImpl(const Coordination::Requests & requests, Coordination::Responses & responses)
{
    if (requests.empty())
        return Coordination::Error::ZOK;

    auto future_result = asyncTryMultiNoThrow(requests);

    if (future_result.wait_for(std::chrono::milliseconds(operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize();
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        Coordination::Error code = response.error;
        responses = response.responses;
        return code;
    }
}

Coordination::Responses ZooKeeper::multi(const Coordination::Requests & requests)
{
    Coordination::Responses responses;
    Coordination::Error code = multiImpl(requests, responses);
    KeeperMultiException::check(code, requests, responses);
    return responses;
}

Coordination::Error ZooKeeper::tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
{
    Coordination::Error code = multiImpl(requests, responses);
    if (code != Coordination::Error::ZOK && !Coordination::isUserError(code))
        throw KeeperException(code);
    return code;
}


void ZooKeeper::removeChildren(const std::string & path)
{
    Strings children = getChildren(path);
    while (!children.empty())
    {
        Coordination::Requests ops;
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            ops.emplace_back(makeRemoveRequest(fs::path(path) / children.back(), -1));
            children.pop_back();
        }
        multi(ops);
    }
}


void ZooKeeper::removeChildrenRecursive(const std::string & path, const String & keep_child_node)
{
    Strings children = getChildren(path);
    while (!children.empty())
    {
        Coordination::Requests ops;
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            removeChildrenRecursive(fs::path(path) / children.back());
            if (likely(keep_child_node.empty() || keep_child_node != children.back()))
                ops.emplace_back(makeRemoveRequest(fs::path(path) / children.back(), -1));
            children.pop_back();
        }
        multi(ops);
    }
}

void ZooKeeper::tryRemoveChildrenRecursive(const std::string & path, const String & keep_child_node)
{
    Strings children;
    if (tryGetChildren(path, children) != Coordination::Error::ZOK)
        return;
    while (!children.empty())
    {
        Coordination::Requests ops;
        Strings batch;
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            String child_path = fs::path(path) / children.back();
            tryRemoveChildrenRecursive(child_path);
            if (likely(keep_child_node.empty() || keep_child_node != children.back()))
            {
                batch.push_back(child_path);
                ops.emplace_back(zkutil::makeRemoveRequest(child_path, -1));
            }
            children.pop_back();
        }

        /// Try to remove the children with a faster method - in bulk. If this fails,
        /// this means someone is concurrently removing these children and we will have
        /// to remove them one by one.
        Coordination::Responses responses;
        if (tryMulti(ops, responses) != Coordination::Error::ZOK)
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
        std::atomic_int32_t code = 0;
        std::atomic_int32_t event_type = 0;
        Poco::Event event;
    };
    using WaitForDisappearStatePtr = std::shared_ptr<WaitForDisappearState>;
}

bool ZooKeeper::waitForDisappear(const std::string & path, const WaitCondition & condition)
{
    WaitForDisappearStatePtr state = std::make_shared<WaitForDisappearState>();

    auto callback = [state](const Coordination::GetResponse & response)
    {
        state->code = int32_t(response.error);
        if (state->code)
            state->event.set();
    };

    auto watch = [state](const Coordination::WatchResponse & response)
    {
        if (!state->code)
        {
            state->code = int32_t(response.error);
            if (!state->code)
                state->event_type = response.type;
            state->event.set();
        }
    };

    while (!condition || !condition())
    {
        /// Use getData insteand of exists to avoid watch leak.
        impl->get(path, callback, watch);

        if (!state->event.tryWait(1000))
            continue;

        if (state->code == int32_t(Coordination::Error::ZNONODE))
            return true;

        if (state->code)
            throw KeeperException(static_cast<Coordination::Error>(state->code.load(std::memory_order_seq_cst)), path);

        if (state->event_type == Coordination::DELETED)
            return true;
    }
    return false;
}

ZooKeeperPtr ZooKeeper::startNewSession() const
{
    return std::make_shared<ZooKeeper>(hosts, identity, session_timeout_ms, operation_timeout_ms, chroot, implementation);
}


bool ZooKeeper::expired()
{
    return impl->isExpired();
}

Int64 ZooKeeper::getClientID()
{
    return impl->getSessionID();
}


std::future<Coordination::CreateResponse> ZooKeeper::asyncCreate(const std::string & path, const std::string & data, int32_t mode)
{
    /// https://stackoverflow.com/questions/25421346/how-to-create-an-stdfunction-from-a-move-capturing-lambda-expression
    auto promise = std::make_shared<std::promise<Coordination::CreateResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::CreateResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->create(path, data, mode & 1, mode & 2, {}, std::move(callback));
    return future;
}

std::future<Coordination::CreateResponse> ZooKeeper::asyncTryCreateNoThrow(const std::string & path, const std::string & data, int32_t mode)
{
    auto promise = std::make_shared<std::promise<Coordination::CreateResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::CreateResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->create(path, data, mode & 1, mode & 2, {}, std::move(callback));
    return future;
}

std::future<Coordination::GetResponse> ZooKeeper::asyncGet(const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::GetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::GetResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->get(path, std::move(callback), watch_callback);
    return future;
}

std::future<Coordination::GetResponse> ZooKeeper::asyncTryGetNoThrow(const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::GetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::GetResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->get(path, std::move(callback), watch_callback);
    return future;
}


std::future<Coordination::GetResponse> ZooKeeper::asyncTryGet(const std::string & path)
{
    auto promise = std::make_shared<std::promise<Coordination::GetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::GetResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->get(path, std::move(callback), {});
    return future;
}

std::future<Coordination::ExistsResponse> ZooKeeper::asyncExists(const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::ExistsResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::ExistsResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->exists(path, std::move(callback), watch_callback);
    return future;
}

std::future<Coordination::ExistsResponse> ZooKeeper::asyncTryExistsNoThrow(const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::ExistsResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::ExistsResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->exists(path, std::move(callback), watch_callback);
    return future;
}

std::future<Coordination::SetResponse> ZooKeeper::asyncSet(const std::string & path, const std::string & data, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::SetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::SetResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->set(path, data, version, std::move(callback));
    return future;
}


std::future<Coordination::SetResponse> ZooKeeper::asyncTrySetNoThrow(const std::string & path, const std::string & data, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::SetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::SetResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->set(path, data, version, std::move(callback));
    return future;
}

std::future<Coordination::ListResponse> ZooKeeper::asyncGetChildren(const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::ListResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::ListResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->list(path, std::move(callback), watch_callback);
    return future;
}

std::future<Coordination::ListResponse> ZooKeeper::asyncTryGetChildrenNoThrow(const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::ListResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::ListResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->list(path, std::move(callback), watch_callback);
    return future;
}

std::future<Coordination::RemoveResponse> ZooKeeper::asyncRemove(const std::string & path, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::RemoveResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        else
            promise->set_value(response);
    };

    impl->remove(path, version, std::move(callback));
    return future;
}

std::future<Coordination::RemoveResponse> ZooKeeper::asyncTryRemove(const std::string & path, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::RemoveResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK
            && response.error != Coordination::Error::ZNONODE
            && response.error != Coordination::Error::ZBADVERSION
            && response.error != Coordination::Error::ZNOTEMPTY)
        {
            promise->set_exception(std::make_exception_ptr(KeeperException(path, response.error)));
        }
        else
            promise->set_value(response);
    };

    impl->remove(path, version, std::move(callback));
    return future;
}

std::future<Coordination::RemoveResponse> ZooKeeper::asyncTryRemoveNoThrow(const std::string & path, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::RemoveResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->remove(path, version, std::move(callback));
    return future;
}

std::future<Coordination::MultiResponse> ZooKeeper::asyncTryMultiNoThrow(const Coordination::Requests & ops)
{
    auto promise = std::make_shared<std::promise<Coordination::MultiResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::MultiResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->multi(ops, std::move(callback));
    return future;
}

std::future<Coordination::MultiResponse> ZooKeeper::asyncMulti(const Coordination::Requests & ops)
{
    auto promise = std::make_shared<std::promise<Coordination::MultiResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::MultiResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(response.error)));
        else
            promise->set_value(response);
    };

    impl->multi(ops, std::move(callback));
    return future;
}

Coordination::Error ZooKeeper::tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
{
    try
    {
        return multiImpl(requests, responses);
    }
    catch (const Coordination::Exception & e)
    {
        return e.code;
    }
}

void ZooKeeper::finalize()
{
    impl->finalize();
}

size_t KeeperMultiException::getFailedOpIndex(Coordination::Error exception_code, const Coordination::Responses & responses)
{
    if (responses.empty())
        throw DB::Exception("Responses for multi transaction is empty", DB::ErrorCodes::LOGICAL_ERROR);

    for (size_t index = 0, size = responses.size(); index < size; ++index)
        if (responses[index]->error != Coordination::Error::ZOK)
            return index;

    if (!Coordination::isUserError(exception_code))
        throw DB::Exception("There are no failed OPs because '" + std::string(Coordination::errorMessage(exception_code)) + "' is not valid response code for that",
                            DB::ErrorCodes::LOGICAL_ERROR);

    throw DB::Exception("There is no failed OpResult", DB::ErrorCodes::LOGICAL_ERROR);
}


KeeperMultiException::KeeperMultiException(Coordination::Error exception_code, const Coordination::Requests & requests_, const Coordination::Responses & responses_)
        : KeeperException("Transaction failed", exception_code),
          requests(requests_), responses(responses_), failed_op_index(getFailedOpIndex(exception_code, responses))
{
    addMessage("Op #" + std::to_string(failed_op_index) + ", path: " + getPathForFirstFailedOp());
}


std::string KeeperMultiException::getPathForFirstFailedOp() const
{
    return requests[failed_op_index]->getPath();
}

void KeeperMultiException::check(
    Coordination::Error exception_code, const Coordination::Requests & requests, const Coordination::Responses & responses)
{
    if (exception_code == Coordination::Error::ZOK)
        return;

    if (Coordination::isUserError(exception_code))
        throw KeeperMultiException(exception_code, requests, responses);
    else
        throw KeeperException(exception_code);
}


Coordination::RequestPtr makeCreateRequest(const std::string & path, const std::string & data, int create_mode)
{
    auto request = std::make_shared<Coordination::CreateRequest>();
    request->path = path;
    request->data = data;
    request->is_ephemeral = create_mode == CreateMode::Ephemeral || create_mode == CreateMode::EphemeralSequential;
    request->is_sequential = create_mode == CreateMode::PersistentSequential || create_mode == CreateMode::EphemeralSequential;
    return request;
}

Coordination::RequestPtr makeRemoveRequest(const std::string & path, int version)
{
    auto request = std::make_shared<Coordination::RemoveRequest>();
    request->path = path;
    request->version = version;
    return request;
}

Coordination::RequestPtr makeSetRequest(const std::string & path, const std::string & data, int version)
{
    auto request = std::make_shared<Coordination::SetRequest>();
    request->path = path;
    request->data = data;
    request->version = version;
    return request;
}

Coordination::RequestPtr makeCheckRequest(const std::string & path, int version)
{
    auto request = std::make_shared<Coordination::CheckRequest>();
    request->path = path;
    request->version = version;
    return request;
}

}

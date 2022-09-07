#include "ZooKeeper.h"
#include "ZooKeeperImpl.h"
#include "KeeperException.h"
#include "TestKeeper.h"

#include <functional>
#include <filesystem>

#include <Common/randomSeed.h>
#include <base/find_symbols.h>
#include <base/sort.h>
#include <base/getFQDNOrHostName.h>
#include "Common/ZooKeeper/IKeeper.h"
#include <Common/StringUtils/StringUtils.h>
#include <Common/Exception.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>


namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
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


void ZooKeeper::init(ZooKeeperArgs args_)

{
    args = std::move(args_);
    log = &Poco::Logger::get("ZooKeeper");

    if (args.implementation == "zookeeper")
    {
        if (args.hosts.empty())
            throw KeeperException("No hosts passed to ZooKeeper constructor.", Coordination::Error::ZBADARGUMENTS);

        Coordination::ZooKeeper::Nodes nodes;
        nodes.reserve(args.hosts.size());

        /// Shuffle the hosts to distribute the load among ZooKeeper nodes.
        std::vector<ShuffleHost> shuffled_hosts = shuffleHosts();

        bool dns_error = false;
        for (auto & host : shuffled_hosts)
        {
            auto & host_string = host.host;
            try
            {
                bool secure = startsWith(host_string, "secure://");

                if (secure)
                    host_string.erase(0, strlen("secure://"));

                LOG_TEST(log, "Adding ZooKeeper host {} ({})", host_string, Poco::Net::SocketAddress{host_string}.toString());
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

        impl = std::make_unique<Coordination::ZooKeeper>(nodes, args, zk_log);

        if (args.chroot.empty())
            LOG_TRACE(log, "Initialized, hosts: {}", fmt::join(args.hosts, ","));
        else
            LOG_TRACE(log, "Initialized, hosts: {}, chroot: {}", fmt::join(args.hosts, ","), args.chroot);
    }
    else if (args.implementation == "testkeeper")
    {
        impl = std::make_unique<Coordination::TestKeeper>(args);
    }
    else
    {
        throw DB::Exception("Unknown implementation of coordination service: " + args.implementation, DB::ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!args.chroot.empty())
    {
        /// Here we check that zk root exists.
        /// This check is clumsy. The reason is we do this request under common mutex, and never want to hung here.
        /// Otherwise, all threads which need zk will wait for this mutex eternally.
        ///
        /// Usually, this was possible in case of memory limit exception happened inside zk implementation.
        /// This should not happen now, when memory tracker is disabled.
        /// But let's keep it just in case (it is also easy to backport).
        auto future = asyncExists("/");
        auto res = future.wait_for(std::chrono::milliseconds(args.operation_timeout_ms));
        if (res != std::future_status::ready)
            throw KeeperException("Cannot check if zookeeper root exists.", Coordination::Error::ZOPERATIONTIMEOUT);

        auto code = future.get().error;
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw KeeperException(code, "/");

        if (code == Coordination::Error::ZNONODE)
            throw KeeperException("ZooKeeper root doesn't exist. You should create root node " + args.chroot + " before start.", Coordination::Error::ZNONODE);
    }
}

ZooKeeper::ZooKeeper(const ZooKeeperArgs & args_, std::shared_ptr<DB::ZooKeeperLog> zk_log_)
{
    zk_log = std::move(zk_log_);
    init(args_);
}

ZooKeeper::ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name, std::shared_ptr<DB::ZooKeeperLog> zk_log_)
    : zk_log(std::move(zk_log_))
{
    init(ZooKeeperArgs(config, config_name));
}

std::vector<ShuffleHost> ZooKeeper::shuffleHosts() const
{
    std::function<size_t(size_t index)> get_priority = args.get_priority_load_balancing.getPriorityFunc(args.get_priority_load_balancing.load_balancing, 0, args.hosts.size());
    std::vector<ShuffleHost> shuffle_hosts;
    for (size_t i = 0; i < args.hosts.size(); ++i)
    {
        ShuffleHost shuffle_host;
        shuffle_host.host = args.hosts[i];
        if (get_priority)
            shuffle_host.priority = get_priority(i);
        shuffle_host.randomize();
        shuffle_hosts.emplace_back(shuffle_host);
    }

    ::sort(
        shuffle_hosts.begin(), shuffle_hosts.end(),
        [](const ShuffleHost & lhs, const ShuffleHost & rhs)
        {
            return ShuffleHost::compare(lhs, rhs);
        });

    return shuffle_hosts;
}


bool ZooKeeper::configChanged(const Poco::Util::AbstractConfiguration & config, const std::string & config_name) const
{
    ZooKeeperArgs new_args(config, config_name);

    // skip reload testkeeper cause it's for test and data in memory
    if (new_args.implementation == args.implementation && args.implementation == "testkeeper")
        return false;

    return args != new_args;
}


static Coordination::WatchCallback callbackForEvent(const EventPtr & watch)
{
    if (!watch)
        return {};
    return [watch](const Coordination::WatchResponse &) { watch->set(); };
}


Coordination::Error ZooKeeper::getChildrenImpl(const std::string & path, Strings & res,
                                   Coordination::Stat * stat,
                                   Coordination::WatchCallback watch_callback,
                                   Coordination::ListRequestType list_request_type)
{
    auto future_result = asyncTryGetChildrenNoThrow(path, watch_callback, list_request_type);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::List), path));
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

Strings ZooKeeper::getChildren(const std::string & path, Coordination::Stat * stat, const EventPtr & watch, Coordination::ListRequestType list_request_type)
{
    Strings res;
    check(tryGetChildren(path, res, stat, watch, list_request_type), path);
    return res;
}

Strings ZooKeeper::getChildrenWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback, Coordination::ListRequestType list_request_type)
{
    Strings res;
    check(tryGetChildrenWatch(path, res, stat, watch_callback, list_request_type), path);
    return res;
}

Coordination::Error ZooKeeper::tryGetChildren(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    const EventPtr & watch,
    Coordination::ListRequestType list_request_type)
{
    Coordination::Error code = getChildrenImpl(path, res, stat, callbackForEvent(watch), list_request_type);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException(code, path);

    return code;
}

Coordination::Error ZooKeeper::tryGetChildrenWatch(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::ListRequestType list_request_type)
{
    Coordination::Error code = getChildrenImpl(path, res, stat, watch_callback, list_request_type);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException(code, path);

    return code;
}

Coordination::Error ZooKeeper::createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    auto future_result = asyncTryCreateNoThrow(path, data, mode);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::Create), path));
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


    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::Remove), path));
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

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::Exists), path));
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

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::Get), path));
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

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::Set), path));
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

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::Multi), requests[0]->getPath()));
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

Coordination::Error ZooKeeper::syncImpl(const std::string & path, std::string & returned_path)
{
    auto future_result = asyncTrySyncNoThrow(path);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", toString(Coordination::OpNum::Sync), path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }
    else
    {
        auto response = future_result.get();
        Coordination::Error code = response.error;
        returned_path = std::move(response.path);
        return code;
    }
}
std::string ZooKeeper::sync(const std::string & path)
{
    std::string returned_path;
    check(syncImpl(path, returned_path), path);
    return returned_path;
}

Coordination::Error ZooKeeper::trySync(const std::string & path, std::string & returned_path)
{
    return syncImpl(path, returned_path);
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

bool ZooKeeper::tryRemoveChildrenRecursive(const std::string & path, bool probably_flat, const String & keep_child_node)
{
    Strings children;
    if (tryGetChildren(path, children) != Coordination::Error::ZOK)
        return false;

    bool removed_as_expected = true;
    while (!children.empty())
    {
        Coordination::Requests ops;
        Strings batch;
        ops.reserve(MULTI_BATCH_SIZE);
        batch.reserve(MULTI_BATCH_SIZE);
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            String child_path = fs::path(path) / children.back();

            /// Will try to avoid recursive getChildren calls if child_path probably has no children.
            /// It may be extremely slow when path contain a lot of leaf children.
            if (!probably_flat)
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
        if (tryMulti(ops, responses) == Coordination::Error::ZOK)
            continue;

        removed_as_expected = false;

        std::vector<zkutil::ZooKeeper::FutureRemove> futures;
        futures.reserve(batch.size());
        for (const std::string & child : batch)
            futures.push_back(asyncTryRemoveNoThrow(child, -1));

        for (size_t i = 0; i < batch.size(); ++i)
        {
            auto res = futures[i].get();
            if (res.error == Coordination::Error::ZOK)
                continue;
            if (res.error == Coordination::Error::ZNONODE)
                continue;

            if (res.error == Coordination::Error::ZNOTEMPTY)
            {
                if (probably_flat)
                {
                    /// It actually has children, let's remove them
                    tryRemoveChildrenRecursive(batch[i]);
                    tryRemove(batch[i]);
                }
                continue;
            }

            throw KeeperException(res.error, batch[i]);
        }
    }
    return removed_as_expected;
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
        state->code = static_cast<int32_t>(response.error);
        if (state->code)
            state->event.set();
    };

    auto watch = [state](const Coordination::WatchResponse & response)
    {
        if (!state->code)
        {
            state->code = static_cast<int32_t>(response.error);
            if (!state->code)
                state->event_type = response.type;
            state->event.set();
        }
    };

    /// do-while control structure to allow using this function in non-blocking
    /// fashion with a wait condition which returns false by the time this
    /// method is called.
    do
    {
        /// Use getData insteand of exists to avoid watch leak.
        impl->get(path, callback, watch);

        if (!state->event.tryWait(1000))
            continue;

        if (state->code == static_cast<int32_t>(Coordination::Error::ZNONODE))
            return true;

        if (state->code)
            throw KeeperException(static_cast<Coordination::Error>(state->code.load(std::memory_order_seq_cst)), path);

        if (state->event_type == Coordination::DELETED)
            return true;
    } while (!condition || !condition());

    return false;
}

void ZooKeeper::waitForEphemeralToDisappearIfAny(const std::string & path)
{
    zkutil::EventPtr eph_node_disappeared = std::make_shared<Poco::Event>();
    String content;
    if (!tryGet(path, content, nullptr, eph_node_disappeared))
        return;

    int32_t timeout_ms = 3 * args.session_timeout_ms;
    if (!eph_node_disappeared->tryWait(timeout_ms))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                            "Ephemeral node {} still exists after {}s, probably it's owned by someone else. "
                            "Either session_timeout_ms in client's config is different from server's config or it's a bug. "
                            "Node data: '{}'", path, timeout_ms / 1000, content);
}

ZooKeeperPtr ZooKeeper::startNewSession() const
{
    return std::make_shared<ZooKeeper>(args, zk_log);
}


bool ZooKeeper::expired()
{
    return impl->isExpired();
}

DB::KeeperApiVersion ZooKeeper::getApiVersion()
{
    return impl->getApiVersion();
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

std::future<Coordination::ListResponse> ZooKeeper::asyncGetChildren(
    const std::string & path, Coordination::WatchCallback watch_callback, Coordination::ListRequestType list_request_type)
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

    impl->list(path, list_request_type, std::move(callback), watch_callback);
    return future;
}

std::future<Coordination::ListResponse> ZooKeeper::asyncTryGetChildrenNoThrow(
    const std::string & path, Coordination::WatchCallback watch_callback, Coordination::ListRequestType list_request_type)
{
    auto promise = std::make_shared<std::promise<Coordination::ListResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::ListResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->list(path, list_request_type, std::move(callback), watch_callback);
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

std::future<Coordination::SyncResponse> ZooKeeper::asyncTrySyncNoThrow(const std::string & path)
{
    auto promise = std::make_shared<std::promise<Coordination::SyncResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::SyncResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->sync(path, std::move(callback));
    return future;
}

std::future<Coordination::SyncResponse> ZooKeeper::asyncSync(const std::string & path)
{
    auto promise = std::make_shared<std::promise<Coordination::SyncResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::SyncResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(response.error)));
        else
            promise->set_value(response);
    };

    impl->sync(path, std::move(callback));
    return future;
}

void ZooKeeper::finalize(const String & reason)
{
    impl->finalize(reason);
}

void ZooKeeper::setZooKeeperLog(std::shared_ptr<DB::ZooKeeperLog> zk_log_)
{
    zk_log = std::move(zk_log_);
    if (auto * zk = dynamic_cast<Coordination::ZooKeeper *>(impl.get()))
        zk->setZooKeeperLog(zk_log);
}


size_t getFailedOpIndex(Coordination::Error exception_code, const Coordination::Responses & responses)
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

std::string normalizeZooKeeperPath(std::string zookeeper_path, bool check_starts_with_slash, Poco::Logger * log)
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
    {
        /// Do not allow this for new tables, print warning for tables created in old versions
        if (check_starts_with_slash)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path must starts with '/', got '{}'", zookeeper_path);
        if (log)
            LOG_WARNING(log, "ZooKeeper path ('{}') does not start with '/'. It will not be supported in future releases", zookeeper_path);
        zookeeper_path = "/" + zookeeper_path;
    }

    return zookeeper_path;
}

String extractZooKeeperName(const String & path)
{
    static constexpr auto default_zookeeper_name = "default";
    if (path.empty())
        throw DB::Exception("ZooKeeper path should not be empty", DB::ErrorCodes::BAD_ARGUMENTS);
    if (path[0] == '/')
        return default_zookeeper_name;
    auto pos = path.find(":/");
    if (pos != String::npos && pos < path.find('/'))
    {
        auto zookeeper_name = path.substr(0, pos);
        if (zookeeper_name.empty())
            throw DB::Exception("Zookeeper path should start with '/' or '<auxiliary_zookeeper_name>:/'", DB::ErrorCodes::BAD_ARGUMENTS);
        return zookeeper_name;
    }
    return default_zookeeper_name;
}

String extractZooKeeperPath(const String & path, bool check_starts_with_slash, Poco::Logger * log)
{
    if (path.empty())
        throw DB::Exception("ZooKeeper path should not be empty", DB::ErrorCodes::BAD_ARGUMENTS);
    if (path[0] == '/')
        return normalizeZooKeeperPath(path, check_starts_with_slash, log);
    auto pos = path.find(":/");
    if (pos != String::npos && pos < path.find('/'))
    {
        return normalizeZooKeeperPath(path.substr(pos + 1, String::npos), check_starts_with_slash, log);
    }
    return normalizeZooKeeperPath(path, check_starts_with_slash, log);
}

String getSequentialNodeName(const String & prefix, UInt64 number)
{
    /// NOTE Sequential counter in ZooKeeper is Int32.
    assert(number < std::numeric_limits<Int32>::max());
    constexpr size_t seq_node_digits = 10;
    String num_str = std::to_string(number);
    String name = prefix + String(seq_node_digits - num_str.size(), '0') + num_str;
    return name;
}

}

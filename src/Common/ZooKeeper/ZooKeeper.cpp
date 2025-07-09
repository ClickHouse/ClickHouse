#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperFeatureFlags.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/TestKeeper.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ShuffleHost.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerUUID.h>
#include <Interpreters/Context.h>
#include <base/getFQDNOrHostName.h>
#include <base/sort.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <chrono>
#include <functional>
#include <ranges>
#include <vector>

#include <fmt/ranges.h>


namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
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
        throw KeeperException::fromPath(code, path);
}

UInt64 getSecondsUntilReconnect(const ZooKeeperArgs & args)
{
    std::uniform_int_distribution<UInt32> fallback_session_lifetime_distribution
        {
            args.fallback_session_lifetime.min_sec,
            args.fallback_session_lifetime.max_sec,
        };
    UInt32 session_lifetime_seconds = fallback_session_lifetime_distribution(thread_local_rng);
    return session_lifetime_seconds;
}


void ZooKeeper::updateAvailabilityZones()
{
    ShuffleHosts shuffled_hosts = shuffleHosts();

    for (const auto & node : shuffled_hosts)
    {
        try
        {
            ShuffleHosts single_node{node};
            auto tmp_impl = std::make_unique<Coordination::ZooKeeper>(single_node, args, zk_log);
            auto idx = node.original_index;
            availability_zones[idx] = tmp_impl->tryGetAvailabilityZone();
            LOG_TEST(log, "Got availability zone for {}: {}", args.hosts[idx], availability_zones[idx]);
        }
        catch (...)
        {
            DB::tryLogCurrentException(log, "Failed to get availability zone for " + node.host);
        }
    }
    LOG_DEBUG(log, "Updated availability zones: [{}]", fmt::join(availability_zones, ", "));
}

void ZooKeeper::init(ZooKeeperArgs args_, std::unique_ptr<Coordination::IKeeper> existing_impl)
{
    args = std::move(args_);
    log = getLogger("ZooKeeper");

    if (existing_impl)
    {
        chassert(args.implementation == "zookeeper");
        impl = std::move(existing_impl);
        LOG_INFO(log, "Switching to connection to a more optimal node {}", impl->getConnectedHostPort());
    }
    else if (args.implementation == "zookeeper")
    {
        if (args.hosts.empty())
            throw KeeperException::fromMessage(Coordination::Error::ZBADARGUMENTS, "No hosts passed to ZooKeeper constructor.");

        chassert(args.availability_zones.size() == args.hosts.size());
        if (availability_zones.empty())
        {
            /// availability_zones is empty on server startup or after config reloading
            /// We will keep the az info when starting new sessions
            availability_zones = args.availability_zones;

            LOG_TEST(
                log,
                "Availability zones from config: [{}], client: {}",
                fmt::join(availability_zones | std::views::transform([](auto s) { return DB::quoteString(s); }), ", "),
                DB::quoteString(args.client_availability_zone));

            if (args.availability_zone_autodetect)
                updateAvailabilityZones();
        }
        chassert(availability_zones.size() == args.hosts.size());

        /// Shuffle the hosts to distribute the load among ZooKeeper nodes.
        ShuffleHosts shuffled_hosts = shuffleHosts();

        impl = std::make_unique<Coordination::ZooKeeper>(shuffled_hosts, args, zk_log);
        auto node_idx = impl->getConnectedNodeIdx();

        if (args.chroot.empty())
            LOG_TRACE(log, "Initialized, hosts: {}", fmt::join(args.hosts, ","));
        else
            LOG_TRACE(log, "Initialized, hosts: {}, chroot: {}", fmt::join(args.hosts, ","), args.chroot);

        /// If the balancing strategy has an optimal node then it will be the first in the list
        bool connected_to_suboptimal_node = node_idx && static_cast<UInt8>(*node_idx) != shuffled_hosts[0].original_index;
        bool respect_az = args.prefer_local_availability_zone && !args.client_availability_zone.empty();
        bool may_benefit_from_reconnecting = respect_az || args.get_priority_load_balancing.hasOptimalNode();
        if (connected_to_suboptimal_node && may_benefit_from_reconnecting)
        {
            auto reconnect_timeout_sec = getSecondsUntilReconnect(args);
            LOG_DEBUG(log, "Connected to a suboptimal ZooKeeper host ({}, index {})."
                           " To preserve balance in ZooKeeper usage, this ZooKeeper session will expire in {} seconds",
                      impl->getConnectedHostPort(), *node_idx, reconnect_timeout_sec);

            auto reconnect_task_holder = DB::Context::getGlobalContextInstance()->getSchedulePool().createTask("ZKReconnect", [this, optimal_host = shuffled_hosts[0]]()
            {
                try
                {
                    LOG_DEBUG(log, "Trying to connect to a more optimal node {}", optimal_host.host);
                    ShuffleHosts node{optimal_host};
                    std::unique_ptr<Coordination::IKeeper> new_impl = std::make_unique<Coordination::ZooKeeper>(node, args, zk_log);

                    auto new_node_idx = new_impl->getConnectedNodeIdx();
                    chassert(new_node_idx.has_value());

                    /// Maybe the node was unavailable when getting AZs first time, update just in case
                    if (args.availability_zone_autodetect && availability_zones[*new_node_idx].empty())
                    {
                        availability_zones[*new_node_idx] = new_impl->tryGetAvailabilityZone();
                        LOG_DEBUG(log, "Got availability zone for {}: {}", optimal_host.host, availability_zones[*new_node_idx]);
                    }

                    optimal_impl = std::move(new_impl);
                    impl->finalize("Connected to a more optimal node");
                }
                catch (...)
                {
                    LOG_WARNING(log, "Failed to connect to a more optimal ZooKeeper, will try again later: {}", DB::getCurrentExceptionMessage(/*with_stacktrace*/ false));
                    (*reconnect_task)->scheduleAfter(getSecondsUntilReconnect(args) * 1000);
                }
            });
            reconnect_task = std::make_unique<DB::BackgroundSchedulePoolTaskHolder>(std::move(reconnect_task_holder));
            (*reconnect_task)->activate();
            (*reconnect_task)->scheduleAfter(reconnect_timeout_sec * 1000);
        }
    }
    else if (args.implementation == "testkeeper")
    {
        impl = std::make_unique<Coordination::TestKeeper>(args);
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Unknown implementation of coordination service: {}", args.implementation);
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
            throw KeeperException::fromMessage(Coordination::Error::ZOPERATIONTIMEOUT, "Cannot check if zookeeper root exists.");

        auto code = future.get().error;
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw KeeperException::fromPath(code, "/");

        if (code == Coordination::Error::ZNONODE)
            throw KeeperException(Coordination::Error::ZNONODE, "ZooKeeper root doesn't exist. You should create root node {} before start.", args.chroot);
    }
}

ZooKeeper::~ZooKeeper()
{
    if (reconnect_task)
        (*reconnect_task)->deactivate();
}

ZooKeeper::ZooKeeper(const ZooKeeperArgs & args_, std::shared_ptr<DB::ZooKeeperLog> zk_log_)
    : zk_log(std::move(zk_log_))
{
    init(args_, /*existing_impl*/ {});
}


ZooKeeper::ZooKeeper(const ZooKeeperArgs & args_, std::shared_ptr<DB::ZooKeeperLog> zk_log_, Strings availability_zones_, std::unique_ptr<Coordination::IKeeper> existing_impl)
    : availability_zones(std::move(availability_zones_)), zk_log(std::move(zk_log_))
{
    if (availability_zones.size() != args_.hosts.size())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Argument sizes mismatch: availability_zones count {} and hosts count {}",
                            availability_zones.size(), args_.hosts.size());
    init(args_, std::move(existing_impl));
}


ZooKeeper::ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name, std::shared_ptr<DB::ZooKeeperLog> zk_log_)
    : zk_log(std::move(zk_log_))
{
    init(ZooKeeperArgs(config, config_name), /*existing_impl*/ {});
}

ShuffleHosts ZooKeeper::shuffleHosts() const
{
    std::function<Priority(size_t index)> get_priority = args.get_priority_load_balancing.getPriorityFunc(
        args.get_priority_load_balancing.load_balancing, /* offset for first_or_random */ 0, args.hosts.size());
    ShuffleHosts shuffle_hosts;
    for (size_t i = 0; i < args.hosts.size(); ++i)
    {
        ShuffleHost shuffle_host;
        shuffle_host.host = args.hosts[i];
        shuffle_host.original_index = static_cast<UInt8>(i);

        shuffle_host.secure = startsWith(shuffle_host.host, "secure://");
        if (shuffle_host.secure)
            shuffle_host.host.erase(0, strlen("secure://"));

        if (!args.client_availability_zone.empty() && !availability_zones[i].empty())
            shuffle_host.az_info = availability_zones[i] == args.client_availability_zone ? ShuffleHost::SAME : ShuffleHost::OTHER;

        if (get_priority)
            shuffle_host.priority = get_priority(i);
        shuffle_host.randomize();
        shuffle_hosts.emplace_back(shuffle_host);
    }

    ::sort(shuffle_hosts.begin(), shuffle_hosts.end(), ShuffleHost::compare);

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
                                   Coordination::WatchCallbackPtr watch_callback,
                                   Coordination::ListRequestType list_request_type)
{
    auto future_result = asyncTryGetChildrenNoThrow(path, watch_callback, list_request_type);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::List, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

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

Strings ZooKeeper::getChildrenWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallbackPtr watch_callback, Coordination::ListRequestType list_request_type)
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
    return tryGetChildrenWatch(
        path,
        res,
        stat,
        watch ? std::make_shared<Coordination::WatchCallback>(callbackForEvent(watch)) : Coordination::WatchCallbackPtr{},
        list_request_type);
}

Coordination::Error ZooKeeper::tryGetChildrenWatch(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::ListRequestType list_request_type)
{
    return tryGetChildrenWatch(path, res, stat,
        watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{},
        list_request_type);
}

Coordination::Error ZooKeeper::tryGetChildrenWatch(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    Coordination::WatchCallbackPtr watch_callback,
    Coordination::ListRequestType list_request_type)
{
    Coordination::Error code = getChildrenImpl(path, res, stat, watch_callback, list_request_type);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException::fromPath(code, path);

    return code;
}

Coordination::Error ZooKeeper::createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    auto future_result = asyncTryCreateNoThrow(path, data, mode);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::Create, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

    auto response = future_result.get();
    Coordination::Error code = response.error;
    if (code == Coordination::Error::ZOK)
        path_created = response.path_created;
    return code;
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

    if (code == Coordination::Error::ZNOTREADONLY && exists(path))
        return Coordination::Error::ZNODEEXISTS;

    if (!(code == Coordination::Error::ZOK ||
          code == Coordination::Error::ZNONODE ||
          code == Coordination::Error::ZNODEEXISTS ||
          code == Coordination::Error::ZNOCHILDRENFOREPHEMERALS))
        throw KeeperException::fromPath(code, path);

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
    if (code == Coordination::Error::ZNOTREADONLY && exists(path))
        return;
    throw KeeperException::fromPath(code, path);
}

void ZooKeeper::createAncestors(const std::string & path)
{
    size_t pos = 1;

    if (isFeatureEnabled(DB::KeeperFeatureFlag::CREATE_IF_NOT_EXISTS))
    {
        Coordination::Requests create_ops;

        while (true)
        {
            pos = path.find('/', pos);
            if (pos == std::string::npos)
                break;

            auto request = makeCreateRequest(path.substr(0, pos), "", CreateMode::Persistent, true);
            create_ops.emplace_back(request);

            ++pos;
        }

        Coordination::Responses responses;
        const auto & [code, failure_reason] = multiImpl(create_ops, responses, /*check_session_valid*/ false);

        if (code == Coordination::Error::ZOK)
            return;

        if (!failure_reason.empty())
            throw KeeperException::fromMessage(code, failure_reason);

        throw KeeperException::fromPath(code, path);
    }

    std::string data;
    std::string path_created; // Ignored
    std::vector<std::string> pending_nodes;

    size_t last_pos = path.rfind('/');
    if (last_pos == std::string::npos || last_pos == 0)
        return;
    std::string current_node = path.substr(0, last_pos);

    while (true)
    {
        Coordination::Error code = createImpl(current_node, data, CreateMode::Persistent, path_created);
        if (code == Coordination::Error::ZNONODE)
        {
            /// The parent node doesn't exist. Save the current node and try with the parent
            last_pos = current_node.rfind('/');
            if (last_pos == std::string::npos || last_pos == 0)
                throw KeeperException::fromPath(code, path);
            pending_nodes.emplace_back(std::move(current_node));
            current_node = path.substr(0, last_pos);
        }
        else if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
            break;
        else
            throw KeeperException::fromPath(code, path);
    }

    for (const std::string & pending : pending_nodes | std::views::reverse)
        createIfNotExists(pending, data);
}

void ZooKeeper::checkExistsAndGetCreateAncestorsOps(const std::string & path, Coordination::Requests & requests)
{
    std::vector<std::string> paths_to_check;
    size_t pos = 1;
    while (true)
    {
        pos = path.find('/', pos);
        if (pos == std::string::npos)
            break;
        paths_to_check.emplace_back(path.substr(0, pos));
        ++pos;
    }

    MultiExistsResponse response = exists(paths_to_check);

    for (size_t i = 0; i < paths_to_check.size(); ++i)
    {
        if (response[i].error != Coordination::Error::ZOK)
        {
            /// Ephemeral nodes cannot have children
            requests.emplace_back(makeCreateRequest(paths_to_check[i], "", CreateMode::Persistent));
        }
    }
}

Coordination::Error ZooKeeper::removeImpl(const std::string & path, int32_t version)
{
    auto future_result = asyncTryRemoveNoThrow(path, version);


    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::Remove, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

    auto response = future_result.get();
    return response.error;
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
        throw KeeperException::fromPath(code, path);
    return code;
}

Coordination::Error ZooKeeper::existsImpl(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    auto future_result = asyncTryExistsNoThrow(path, watch_callback);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::Exists, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

    auto response = future_result.get();
    Coordination::Error code = response.error;
    if (code == Coordination::Error::ZOK && stat)
        *stat = response.stat;

    return code;
}

bool ZooKeeper::exists(const std::string & path, Coordination::Stat * stat, const EventPtr & watch)
{
    return existsWatch(path, stat, callbackForEvent(watch));
}

bool ZooKeeper::anyExists(const std::vector<std::string> & paths)
{
    auto exists_multi_response = exists(paths);
    for (size_t i = 0; i < exists_multi_response.size(); ++i)
    {
        if (exists_multi_response[i].error == Coordination::Error::ZOK)
            return true;
    }
    return false;
}

bool ZooKeeper::existsWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    Coordination::Error code = existsImpl(path, stat, watch_callback);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException::fromPath(code, path);
    return code != Coordination::Error::ZNONODE;
}

Coordination::Error ZooKeeper::getImpl(
    const std::string & path, std::string & res, Coordination::Stat * stat, Coordination::WatchCallbackPtr watch_callback)
{
    auto future_result = asyncTryGetNoThrow(path, watch_callback);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::Get, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

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

Coordination::Error ZooKeeper::getImpl(const std::string & path, std::string & res, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    return getImpl(path, res, stat, watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
}

std::string ZooKeeper::get(const std::string & path, Coordination::Stat * stat, const EventPtr & watch)
{
    Coordination::Error code = Coordination::Error::ZOK;
    std::string res;
    if (tryGet(path, res, stat, watch, &code))
        return res;
    throw KeeperException(code, "Can't get data for node '{}': node doesn't exist", path);
}

std::string ZooKeeper::getWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback)
{
    Coordination::Error code = Coordination::Error::ZOK;
    std::string res;
    if (tryGetWatch(path, res, stat, watch_callback, &code))
        return res;
    throw KeeperException(code, "Can't get data for node '{}': node doesn't exist", path);
}


std::string ZooKeeper::getWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallbackPtr watch_callback)
{
    Coordination::Error code = Coordination::Error::ZOK;
    std::string res;
    if (tryGetWatch(path, res, stat, watch_callback, &code))
        return res;
    throw KeeperException(code, "Can't get data for node '{}': node doesn't exist", path);
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
    Coordination::WatchCallbackPtr watch_callback,
    Coordination::Error * return_code)
{
    Coordination::Error code = getImpl(path, res, stat, watch_callback);

    if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
        throw KeeperException::fromPath(code, path);

    if (return_code)
        *return_code = code;

    return code == Coordination::Error::ZOK;

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
        throw KeeperException::fromPath(code, path);

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
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::Set, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

    auto response = future_result.get();
    Coordination::Error code = response.error;
    if (code == Coordination::Error::ZOK && stat)
        *stat = response.stat;

    return code;
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
        throw KeeperException::fromPath(code, path);
}

Coordination::Error ZooKeeper::trySet(const std::string & path, const std::string & data,
                          int32_t version, Coordination::Stat * stat)
{
    Coordination::Error code = setImpl(path, data, version, stat);

    if (!(code == Coordination::Error::ZOK ||
          code == Coordination::Error::ZNONODE ||
          code == Coordination::Error::ZBADVERSION))
        throw KeeperException::fromPath(code, path);
    return code;
}


std::pair<Coordination::Error, std::string>
ZooKeeper::multiImpl(const Coordination::Requests & requests, Coordination::Responses & responses, bool check_session_valid)
{
    if (requests.empty())
        return {Coordination::Error::ZOK, ""};

    std::future<Coordination::MultiResponse> future_result;
    Coordination::Requests requests_with_check_session;
    if (check_session_valid)
    {
        requests_with_check_session = requests;
        addCheckSessionOp(requests_with_check_session);
        future_result = asyncTryMultiNoThrow(requests_with_check_session);
    }
    else
    {
        future_result = asyncTryMultiNoThrow(requests);
    }

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        auto & request = *requests[0];
        impl->finalize(fmt::format(
            "Operation timeout on {} of {} requests. First ({}): {}",
            Coordination::OpNum::Multi,
            requests.size(),
            demangle(typeid(request).name()),
            request.getPath()));
        return {Coordination::Error::ZOPERATIONTIMEOUT, ""};
    }

    auto response = future_result.get();
    Coordination::Error code = response.error;
    responses = response.responses;

    std::string reason;

    if (check_session_valid)
    {
        if (code != Coordination::Error::ZOK && !Coordination::isHardwareError(code)
            && getFailedOpIndex(code, responses) == requests.size())
        {
            reason = fmt::format("Session was killed: {}", requests_with_check_session.back()->getPath());
            impl->finalize(reason);
            code = Coordination::Error::ZSESSIONMOVED;
        }
        responses.pop_back();
        /// For some reason, for hardware errors we set ZOK codes for all responses.
        /// In other cases, if the multi-request status is not ZOK, then the last response status must indicate an error too
        chassert(
            code == Coordination::Error::ZOK || Coordination::isHardwareError(code) || responses.back()->error != Coordination::Error::ZOK);
    }

    return {code, std::move(reason)};
}

Coordination::Responses ZooKeeper::multi(const Coordination::Requests & requests, bool check_session_valid)
{
    Coordination::Responses responses;
    const auto & [code, failure_reason] = multiImpl(requests, responses, check_session_valid);
    if (!failure_reason.empty())
        throw KeeperException::fromMessage(code, failure_reason);

    KeeperMultiException::check(code, requests, responses);
    return responses;
}

Coordination::Error ZooKeeper::tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses, bool check_session_valid)
{
    const auto & [code, failure_reason] = multiImpl(requests, responses, check_session_valid);

    if (code != Coordination::Error::ZOK && !Coordination::isUserError(code))
    {
        if (!failure_reason.empty())
            throw KeeperException::fromMessage(code, failure_reason);

        throw KeeperException(code);
    }

    return code;
}

Coordination::Error ZooKeeper::syncImpl(const std::string & path, std::string & returned_path)
{
    auto future_result = asyncTrySyncNoThrow(path);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::Sync, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

    auto response = future_result.get();
    Coordination::Error code = response.error;
    returned_path = std::move(response.path);
    return code;
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


void ZooKeeper::removeChildrenRecursive(const std::string & path, RemoveException keep_child)
{
    Strings children = getChildren(path);
    while (!children.empty())
    {
        Coordination::Requests ops;
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            if (keep_child.path.empty() || keep_child.path != children.back()) [[likely]]
            {
                removeChildrenRecursive(fs::path(path) / children.back());
                ops.emplace_back(makeRemoveRequest(fs::path(path) / children.back(), -1));
            }
            else if (keep_child.remove_subtree)
            {
                removeChildrenRecursive(fs::path(path) / children.back());
            }

            children.pop_back();
        }
        multi(ops);
    }
}

bool ZooKeeper::tryRemoveChildrenRecursive(const std::string & path, bool probably_flat, RemoveException keep_child)
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

            if (keep_child.path.empty() || keep_child.path != children.back()) [[likely]]
            {
                /// Will try to avoid recursive getChildren calls if child_path probably has no children.
                /// It may be extremely slow when path contain a lot of leaf children.
                if (!probably_flat)
                    tryRemoveChildrenRecursive(child_path);

                batch.push_back(child_path);
                ops.emplace_back(zkutil::makeRemoveRequest(child_path, -1));
            }
            else if (keep_child.remove_subtree && !probably_flat)
            {
                tryRemoveChildrenRecursive(child_path);
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

            throw KeeperException::fromPath(res.error, batch[i]);
        }
    }
    return removed_as_expected;
}

void ZooKeeper::removeRecursive(const std::string & path, uint32_t remove_nodes_limit)
{
    if (!isFeatureEnabled(DB::KeeperFeatureFlag::REMOVE_RECURSIVE))
    {
        removeChildrenRecursive(path);
        remove(path);
        return;
    }

    check(tryRemoveRecursive(path, remove_nodes_limit), path);
}

Coordination::Error ZooKeeper::tryRemoveRecursive(const std::string & path, uint32_t remove_nodes_limit)
{
    const auto fallback_method = [&]
    {
        tryRemoveChildrenRecursive(path);
        return tryRemove(path);
    };

    if (!isFeatureEnabled(DB::KeeperFeatureFlag::REMOVE_RECURSIVE))
        return fallback_method();

    auto promise = std::make_shared<std::promise<Coordination::RemoveRecursiveResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::RemoveRecursiveResponse & response) mutable
    {
        promise->set_value(response);
    };

    impl->removeRecursive(path, remove_nodes_limit, std::move(callback));

    if (future.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {} {}", Coordination::OpNum::RemoveRecursive, path));
        return Coordination::Error::ZOPERATIONTIMEOUT;
    }

    auto response = future.get();

    if (response.error == Coordination::Error::ZNOTEMPTY) /// limit was too low, try without RemoveRecursive request
        return fallback_method();

    return response.error;
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
        /// Use getData instead of exists to avoid watch leak.
        impl->get(path, callback, std::make_shared<Coordination::WatchCallback>(watch));

        if (!state->event.tryWait(1000))
            continue;

        if (state->code == static_cast<int32_t>(Coordination::Error::ZNONODE))
            return true;

        if (state->code)
            throw KeeperException::fromPath(static_cast<Coordination::Error>(state->code.load(std::memory_order_seq_cst)), path);

        if (state->event_type == Coordination::DELETED)
            return true;
    } while (!condition || !condition());

    return false;
}

void ZooKeeper::deleteEphemeralNodeIfContentMatches(const std::string & path, const std::string & fast_delete_if_equal_value)
{
    zkutil::EventPtr eph_node_disappeared = std::make_shared<Poco::Event>();
    String content;
    Coordination::Stat stat;
    if (!tryGet(path, content, &stat, eph_node_disappeared))
        return;

    if (content == fast_delete_if_equal_value)
    {
        auto code = tryRemove(path, stat.version);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw Coordination::Exception::fromPath(code, path);
    }
    else
    {
        LOG_WARNING(log, "Ephemeral node ('{}') already exists but it isn't owned by us. Will wait until it disappears", path);
        int32_t timeout_ms = 3 * args.session_timeout_ms;
        if (!eph_node_disappeared->tryWait(timeout_ms))
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Ephemeral node {} still exists after {}s, probably it's owned by someone else. "
                "Either session_timeout_ms in client's config is different from server's config or it's a bug. "
                "Node data: '{}'",
                path,
                timeout_ms / 1000,
                content);
    }
}

Coordination::ReconfigResponse ZooKeeper::reconfig(
    const std::string & joining,
    const std::string & leaving,
    const std::string & new_members,
    int32_t version)
{
    auto future_result = asyncReconfig(joining, leaving, new_members, version);

    if (future_result.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
    {
        impl->finalize(fmt::format("Operation timeout on {}", Coordination::OpNum::Reconfig));
        throw KeeperException(Coordination::Error::ZOPERATIONTIMEOUT);
    }

    return future_result.get();
}

ZooKeeperPtr ZooKeeper::create(const Poco::Util::AbstractConfiguration & config, const std::string & config_name, std::shared_ptr<DB::ZooKeeperLog> zk_log_)
{
    auto res = std::shared_ptr<ZooKeeper>(new ZooKeeper(config, config_name, zk_log_));
    res->initSession();
    return res;
}

ZooKeeperPtr ZooKeeper::startNewSession() const
{
    if (reconnect_task)
        (*reconnect_task)->deactivate();

    auto res = std::shared_ptr<ZooKeeper>(new ZooKeeper(args, zk_log, availability_zones, std::move(optimal_impl)));
    res->initSession();
    return res;
}

void ZooKeeper::initSession()
{
    String session_path = fs::path(args.sessions_path) / args.zookeeper_name / toString(DB::ServerUUID::get());
    Coordination::Stat stat;
    if (trySet(session_path, "", -1, &stat) == Coordination::Error::ZOK)
    {
        session_node_version = stat.version;
        return;
    }

    createAncestors(session_path);
    create(session_path, "", zkutil::CreateMode::Persistent);
    session_node_version = 0;
}

void ZooKeeper::addCheckSessionOp(Coordination::Requests & requests) const
{
    String session_path = fs::path(args.sessions_path) / args.zookeeper_name / toString(DB::ServerUUID::get());
    requests.push_back(zkutil::makeCheckRequest(session_path, session_node_version));
}

bool ZooKeeper::expired()
{
    return impl->isExpired();
}

bool ZooKeeper::isFeatureEnabled(DB::KeeperFeatureFlag feature_flag) const
{
    return impl->isFeatureEnabled(feature_flag);
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
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
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
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
        else
            promise->set_value(response);
    };

    impl->get(path, std::move(callback),
        watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
    return future;
}

std::future<Coordination::GetResponse> ZooKeeper::asyncTryGetNoThrow(const std::string & path, Coordination::WatchCallback watch_callback)
{
    return asyncTryGetNoThrow(path, watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
}

std::future<Coordination::GetResponse> ZooKeeper::asyncTryGetNoThrow(const std::string & path, Coordination::WatchCallbackPtr watch_callback)
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
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
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
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
        else
            promise->set_value(response);
    };

    impl->exists(path, std::move(callback),
        watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
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

    impl->exists(path, std::move(callback),
        watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
    return future;
}

std::future<Coordination::SetResponse> ZooKeeper::asyncSet(const std::string & path, const std::string & data, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::SetResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::SetResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
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
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
        else
            promise->set_value(response);
    };

    impl->list(path, list_request_type, std::move(callback),
        watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
    return future;
}

std::future<Coordination::ListResponse> ZooKeeper::asyncTryGetChildrenNoThrow(
    const std::string & path, Coordination::WatchCallbackPtr watch_callback, Coordination::ListRequestType list_request_type)
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

std::future<Coordination::ListResponse>
ZooKeeper::asyncTryGetChildren(const std::string & path, Coordination::ListRequestType list_request_type)
{
    auto promise = std::make_shared<std::promise<Coordination::ListResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::ListResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE)
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
        else
            promise->set_value(response);
    };

    impl->list(path, list_request_type, std::move(callback), {});
    return future;
}

std::future<Coordination::RemoveResponse> ZooKeeper::asyncRemove(const std::string & path, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    auto future = promise->get_future();

    auto callback = [promise, path](const Coordination::RemoveResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
        else
            promise->set_value(response);
    };

    impl->remove(path, version, std::move(callback));
    return future;
}

/// Needs to match ZooKeeperWithInjection::asyncTryRemove implementation
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
            promise->set_exception(std::make_exception_ptr(KeeperException::fromPath(response.error, path)));
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
    return asyncTryMultiNoThrow(std::span(ops));
}

std::future<Coordination::MultiResponse> ZooKeeper::asyncTryMultiNoThrow(std::span<const Coordination::RequestPtr> ops)
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

Coordination::Error ZooKeeper::tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses, bool check_session_valid)
{
    try
    {
        return multiImpl(requests, responses, check_session_valid).first;
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

std::future<Coordination::ReconfigResponse> ZooKeeper::asyncReconfig(
    const std::string & joining,
    const std::string & leaving,
    const std::string & new_members,
    int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::ReconfigResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::ReconfigResponse & response) mutable
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(KeeperException(response.error)));
        else
            promise->set_value(response);
    };

    impl->reconfig(joining, leaving, new_members, version, std::move(callback));
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

void ZooKeeper::setServerCompletelyStarted()
{
    if (auto * zk = dynamic_cast<Coordination::ZooKeeper *>(impl.get()))
        zk->setServerCompletelyStarted();
}

std::optional<int8_t> ZooKeeper::getConnectedHostIdx() const
{
    return impl->getConnectedNodeIdx();
}

String ZooKeeper::getConnectedHostPort() const
{
    return impl->getConnectedHostPort();
}

int64_t ZooKeeper::getConnectionXid() const
{
    return impl->getConnectionXid();
}

String ZooKeeper::getConnectedHostAvailabilityZone() const
{
    if (args.implementation != "zookeeper" || !impl)
        return "";
    std::optional<int8_t> idx = impl->getConnectedNodeIdx();
    if (!idx)
        return "";     /// session expired
    return availability_zones.at(*idx);
}

size_t getFailedOpIndex(Coordination::Error exception_code, const Coordination::Responses & responses)
{
    if (responses.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Responses for multi transaction is empty");

    for (size_t index = 0, size = responses.size(); index < size; ++index)
        if (responses[index]->error != Coordination::Error::ZOK)
            return index;

    if (!Coordination::isUserError(exception_code))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                            "There are no failed OPs because '{}' is not valid response code for that",
                            exception_code);

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "There is no failed OpResult");
}


KeeperMultiException::KeeperMultiException(Coordination::Error exception_code, size_t failed_op_index_, const Coordination::Requests & requests_, const Coordination::Responses & responses_)
        : KeeperException(exception_code, "Transaction failed ({}): Op #{}, path", exception_code, failed_op_index_),
          requests(requests_), responses(responses_), failed_op_index(failed_op_index_)
{
    addMessage(getPathForFirstFailedOp());
}

KeeperMultiException::KeeperMultiException(Coordination::Error exception_code, const Coordination::Requests & requests_, const Coordination::Responses & responses_)
        : KeeperMultiException(exception_code, getFailedOpIndex(exception_code, responses_), requests_, responses_)
{
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
    throw KeeperException(exception_code);
}


Coordination::RequestPtr makeCreateRequest(const std::string & path, const std::string & data, int create_mode, bool ignore_if_exists)
{
    auto request = std::make_shared<Coordination::CreateRequest>();
    request->path = path;
    request->data = data;
    request->is_ephemeral = create_mode == CreateMode::Ephemeral || create_mode == CreateMode::EphemeralSequential;
    request->is_sequential = create_mode == CreateMode::PersistentSequential || create_mode == CreateMode::EphemeralSequential;
    request->not_exists = ignore_if_exists;
    return request;
}

Coordination::RequestPtr makeRemoveRequest(const std::string & path, int version)
{
    auto request = std::make_shared<Coordination::RemoveRequest>();
    request->path = path;
    request->version = version;
    return request;
}

Coordination::RequestPtr makeRemoveRecursiveRequest(const std::string & path, uint32_t remove_nodes_limit)
{
    auto request = std::make_shared<Coordination::RemoveRecursiveRequest>();
    request->path = path;
    request->remove_nodes_limit = remove_nodes_limit;
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

Coordination::RequestPtr makeGetRequest(const std::string & path)
{
    auto request = std::make_shared<Coordination::GetRequest>();
    request->path = path;
    return request;
}

Coordination::RequestPtr makeListRequest(const std::string & path, Coordination::ListRequestType list_request_type)
{
    // Keeper server that support MultiRead also support FilteredList
    auto request = std::make_shared<Coordination::ZooKeeperFilteredListRequest>();
    request->path = path;
    request->list_request_type = list_request_type;
    return request;
}

Coordination::RequestPtr makeSimpleListRequest(const std::string & path)
{
    auto request = std::make_shared<Coordination::ZooKeeperSimpleListRequest>();
    request->path = path;
    return request;
}

Coordination::RequestPtr makeExistsRequest(const std::string & path)
{
    auto request = std::make_shared<Coordination::ZooKeeperExistsRequest>();
    request->path = path;
    return request;
}

std::string normalizeZooKeeperPath(std::string zookeeper_path, bool check_starts_with_slash, LoggerPtr log)
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
    if (path.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path should not be empty");
    if (path[0] == '/')
        return String(DEFAULT_ZOOKEEPER_NAME);
    auto pos = path.find(":/");
    if (pos != String::npos && pos < path.find('/'))
    {
        auto zookeeper_name = path.substr(0, pos);
        if (zookeeper_name.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Zookeeper path should start with '/' or '<auxiliary_zookeeper_name>:/'");
        return zookeeper_name;
    }
    return String(DEFAULT_ZOOKEEPER_NAME);
}

String extractZooKeeperPath(const String & path, bool check_starts_with_slash, LoggerPtr log)
{
    if (path.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path should not be empty");
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

void validateZooKeeperConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("zookeeper") && config.has("keeper"))
        throw DB::Exception(DB::ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Both ZooKeeper and Keeper are specified");
}

bool hasZooKeeperConfig(const Poco::Util::AbstractConfiguration & config)
{
    return config.has("zookeeper") || config.has("keeper") || (config.has("keeper_server.raft_configuration") && config.getBool("keeper_server.use_cluster", true));
}

String getZooKeeperConfigName(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("zookeeper"))
        return "zookeeper";

    if (config.has("keeper"))
        return "keeper";

    if (config.has("keeper_server.raft_configuration") && config.getBool("keeper_server.use_cluster", true))
        return "keeper_server";

    throw DB::Exception(DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no Zookeeper configuration in server config");
}

}

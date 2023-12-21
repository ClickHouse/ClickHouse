#include <memory>
#include <mutex>

#include <base/types.h>
#include <base/sort.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperLoadBalancer.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/DNSResolver.h>

#include <Poco/Logger.h>
#include <Poco/Net/NetException.h>
#include <Poco/String.h>

namespace Coordination
{

std::pair<std::string, bool> parseForSocketAddress(const std::string & raw_host)
{
    std::pair<std::string, bool> result;
    bool secure = startsWith(raw_host, "secure://");
    if (secure)
        result.first = raw_host.substr(strlen("secure://"));
    else
        result.first = raw_host;
    result.second = secure;
    return result;
}

bool isKeeperHostDNSAvailable(Poco::Logger* log, const std::string & address, bool & dns_error_occurred)
{
    /// We want to resolve all hosts without DNS cache for keeper connection.
    Coordination::DNSResolver::instance().removeHostFromCache(address);
    try
    {
        const Poco::Net::SocketAddress host_socket_addr{address};
    }
    catch (const Poco::Net::HostNotFoundException & e)
    {
        /// Most likely it's misconfiguration and wrong hostname was specified
        LOG_ERROR(log, "Cannot use ZooKeeper host {}, reason: {}", address, e.displayText());
        return false;
    }
    catch (const Poco::Net::DNSException & e)
    {
        /// Most likely DNS is not available now
        dns_error_occurred = true;
        LOG_ERROR(log, "Cannot use ZooKeeper host {} due to DNS error: {}", address, e.displayText());
        return false;
    }
    return true;
}

ZooKeeperLoadBalancer & ZooKeeperLoadBalancer::instance()
{
    static ZooKeeperLoadBalancer instance;
    return instance;
}


void ZooKeeperLoadBalancer::init(zkutil::ZooKeeperArgs args_, std::shared_ptr<ZooKeeperLog> zk_log_)
{
    if (args_.hosts.empty())
        throw zkutil::KeeperException::fromMessage(Coordination::Error::ZBADARGUMENTS, "No hosts specified in ZooKeeperArgs.");
    std::lock_guard lock{mutex};
    args = args_;
    zk_log = std::move(zk_log_);
    log = &Poco::Logger::get("ZooKeeperLoadBalancer");
    // TODO(jianfei): need to make the host info info stateful to track connection states, rather than reset every time we call `init`.
    host_info_list = {};
    for (size_t i = 0; i < args_.hosts.size(); ++i)
    {
        HostInfo host_info;
        auto [address, secure] =  parseForSocketAddress(args_.hosts[i]);
        host_info.address = address;
        host_info.original_index = i;
        host_info.secure = secure;
        host_info_list.push_back(host_info);
    }
}

void ZooKeeperLoadBalancer::setBetterKeeperHostUpdater(BetterKeeperHostUpdater handler)
{
    std::lock_guard lock{mutex};
    better_keeper_host_updater = handler;
}


[[noreturn]] void throwWhenNoHostAvailable(bool dns_error_occurred)
{
    if (dns_error_occurred)
        throw zkutil::KeeperException::fromMessage(Coordination::Error::ZCONNECTIONLOSS, "Cannot resolve any of provided ZooKeeper hosts due to DNS error");
    else
        throw zkutil::KeeperException::fromMessage(Coordination::Error::ZCONNECTIONLOSS, "Cannot use any of provided ZooKeeper nodes");
}


void ZooKeeperLoadBalancer::shuffleHosts()
{
    std::function<Priority(size_t index)> get_priority = args.get_priority_load_balancing.getPriorityFunc(args.get_priority_load_balancing.load_balancing, 0, args.hosts.size());
    for (size_t i = 0; i < host_info_list.size(); ++i)
    {
        if (get_priority)
            host_info_list[i].priority = get_priority(i);
        host_info_list[i].randomize();
    }

    ::sort(host_info_list.begin(), host_info_list.end(), HostInfo::compare);
}

void ZooKeeperLoadBalancer::recordKeeperHostError(UInt8 original_index)
{
    std::lock_guard lock{mutex};
    for (auto & host_info : host_info_list)
    {
        if (host_info.original_index == original_index)
        {
            LOG_DEBUG(log, "Load Balancer records a failure on host {}, original index {}", host_info.address, static_cast<UInt32>(original_index));
            // TODO: add the actual implementation to lower the priority/scoring of the host.
            return;
        }
    }
    LOG_ERROR(log, "Does not find host to record the failure with original index {}", static_cast<UInt32>(original_index));
}

std::unique_ptr<Coordination::ZooKeeper> ZooKeeperLoadBalancer::createClient()
{
    std::lock_guard lock{mutex};
    shuffleHosts();
    bool dns_error_occurred = false;
    for (size_t i = 0; i < host_info_list.size(); ++i)
    {
        if (!isKeeperHostDNSAvailable(log, host_info_list[i].address, dns_error_occurred))
            continue;

        LOG_INFO(log, "Connecting to ZooKeeper host {}, number of attempted hosts {}", host_info_list[i].address, i+1);
        try
        {
            auto client  = std::make_unique<Coordination::ZooKeeper>(host_info_list[i].toZooKeeperNode(), args, zk_log);
            // Non optimal case: we connected to a keeper host that is not at highest priority, set a timeout to force reconnect.
            // Except for RANDOM and ROUND_ROBIN. For random, as it's random anyway regardless of first or second host.
            // For round robin, hosts tried later will be in the same order as well.
            const auto & lb_algorithm = args.get_priority_load_balancing.load_balancing;
            if (i != 0 && (lb_algorithm != LoadBalancing::RANDOM && lb_algorithm != LoadBalancing::ROUND_ROBIN))
            {
                auto session_timeout_seconds = client->setClientSessionDeadline(args.fallback_session_lifetime.min_sec, args.fallback_session_lifetime.max_sec);
                LOG_INFO(log, "Connecting to a different az ZooKeeper with session timeout {} seconds", session_timeout_seconds);
            }
            // This is the client we expect to be actually used, therefore we set the callback to monitor the potential send/receive errors.
            client->setSendRecvErrorCallback([&, original_index= host_info_list[i].original_index]()
            {
                recordKeeperHostError(original_index);
            });
            return client;
        }
        catch (DB::Exception& ex)
        {
            LOG_ERROR(log, "Failed to connect to ZooKeeper host {}, error {}", host_info_list[i].address, ex.what());
        }
    }
    throwWhenNoHostAvailable(dns_error_occurred);
}

}

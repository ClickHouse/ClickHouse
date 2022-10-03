#pragma once
#include <random>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/randomSeed.h>

namespace DB
{

class RandomFaultInjection
{
public:
    RandomFaultInjection() : rndgen(0), distribution(.0) { }
    RandomFaultInjection(double probability, UInt64 seed_) : rndgen(seed_), distribution(probability) { }

    void beforeOperation()
    {
        if (distribution(rndgen))
            throw zkutil::KeeperException("Fault injection before operation", Coordination::Error::ZSESSIONEXPIRED);
    }
    void afterOperation()
    {
        if (distribution(rndgen))
            throw zkutil::KeeperException("Fault injection after operation", Coordination::Error::ZOPERATIONTIMEOUT);
    }

private:
    std::mt19937_64 rndgen;
    std::bernoulli_distribution distribution;
};

///
/// KeeperAccess mimics keeper interface and inject failures according to failure policy if provided
///
class KeeperAccess
{
    using zk = zkutil::ZooKeeper;

    zk::Ptr keeper;
    RandomFaultInjection random_fault_injection;
    RandomFaultInjection * fault_policy = nullptr;
    std::string name;
    Poco::Logger * logger = nullptr;
    UInt64 calls_total = 0;
    UInt64 calls_without_fault_injection = 0;
    const UInt64 seed = 0;

    KeeperAccess(
        zk::Ptr const & keeper_,
        double fault_injection_probability,
        UInt64 fault_injection_seed,
        std::string name_,
        Poco::Logger * logger_ = nullptr)
        : keeper(keeper_)
        , random_fault_injection(fault_injection_probability, fault_injection_seed)
        , name(std::move(name_))
        , logger(logger_)
        , seed(fault_injection_seed)
    {
        if (fault_injection_probability > .0)
            fault_policy = &random_fault_injection;

        if (unlikely(logger))
            LOG_TRACE(logger, "KeeperAccess created: name={} seed={}", name, seed);
    }

public:
    using Ptr = std::shared_ptr<KeeperAccess>;

    static KeeperAccess::Ptr createInstance(
        double fault_injection_probability, UInt64 fault_injection_seed, const zk::Ptr & zookeeper, std::string name, Poco::Logger * logger)
    {
        /// validate all parameters here, constructor just accept everything

        if (fault_injection_probability < 0.0)
            fault_injection_probability = .0;
        else if (fault_injection_probability > 1.0)
            fault_injection_probability = 1.0;

        if (0 == fault_injection_seed)
            fault_injection_seed = randomSeed();

        if (fault_injection_probability > 0.0)
            return std::shared_ptr<KeeperAccess>(
                new KeeperAccess(zookeeper, fault_injection_probability, fault_injection_seed, std::move(name), logger));

        /// if no fault injection provided, create instance which will not log anything
        return std::make_shared<KeeperAccess>(zookeeper);
    }

    explicit KeeperAccess(zk::Ptr const & keeper_) : keeper(keeper_) { }

    ~KeeperAccess()
    {
        if (unlikely(logger))
            LOG_TRACE(
                logger,
                "KeeperAccess report: name={} seed={} calls_total={} calls_succeeded={} calls_failed={} failure_rate={}",
                name,
                seed,
                calls_total,
                calls_without_fault_injection,
                calls_total - calls_without_fault_injection,
                float(calls_total - calls_without_fault_injection) / calls_total);
    }

    void setKeeper(zk::Ptr const & keeper_) { keeper = keeper_; }

    ///
    /// mirror ZooKeeper interface
    ///

    Strings getChildren(
        const std::string & path,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL)
    {
        return access<Strings>("getChildren", path, [&]() { return keeper->getChildren(path, stat, watch, list_request_type); });
    }

    zk::FutureExists asyncExists(const std::string & path, Coordination::WatchCallback watch_callback = {})
    {
        return access<zk::FutureExists>("asyncExists", path, [&]() { return keeper->asyncExists(path, watch_callback); });
    }

    zk::FutureGet asyncTryGet(const std::string & path)
    {
        return access<zk::FutureGet>("asyncTryGet", path, [&]() { return keeper->asyncTryGet(path); });
    }

    bool tryGet(
        const std::string & path,
        std::string & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::Error * code = nullptr)
    {
        return access<bool>("tryGet", path, [&]() { return keeper->tryGet(path, res, stat, watch, code); });
    }

    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        return access<Coordination::Error>(
            "tryMulti", !requests.empty() ? requests.front()->getPath() : "", [&]() { return keeper->tryMulti(requests, responses); });
    }

    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        return access<Coordination::Error>(
            "tryMultiNoThrow",
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->tryMultiNoThrow(requests, responses); });
    }

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<std::string>("get", path, [&]() { return keeper->get(path, stat, watch); });
    }

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<bool>("exists", path, [&]() { return keeper->exists(path, stat, watch); });
    }

    std::string create(const std::string & path, const std::string & data, int32_t mode)
    {
        return access<std::string>("create", path, [&]() { return keeper->create(path, data, mode); });
    }

    Coordination::Responses multi(const Coordination::Requests & requests)
    {
        return access<Coordination::Responses>(
            "multi", !requests.empty() ? requests.front()->getPath() : "", [&]() { return keeper->multi(requests); });
    }

private:
    template <typename Result>
    Result access(const char * func_name, const std::string & path, auto && operation)
    {
        try
        {
            ++calls_total;
            if (unlikely(fault_policy))
                fault_policy->beforeOperation();
            Result res = operation();
            if (unlikely(fault_policy))
                fault_policy->afterOperation();
            ++calls_without_fault_injection;
            if (unlikely(logger))
                LOG_TRACE(logger, "KeeperAccess call SUCCEEDED: seed={} func={} path={}", seed, func_name, path);
            return res;
        }
        catch (const zkutil::KeeperException & e)
        {
            if (unlikely(logger))
                LOG_TRACE(
                    logger,
                    "KeeperAccess call FAILED: seed={} func={} path={} code={} message={} ",
                    seed,
                    func_name,
                    path,
                    e.code,
                    e.message());
            throw;
        }
        catch (const Exception & e)
        {
            if (unlikely(logger))
                LOG_TRACE(
                    logger,
                    "KeeperAccess call FAILED: seed={} func={} path={} code={} message={} ",
                    seed,
                    func_name,
                    path,
                    e.code(),
                    e.message());
            throw;
        }
    }
};

using KeeperAccessPtr = KeeperAccess::Ptr;
}

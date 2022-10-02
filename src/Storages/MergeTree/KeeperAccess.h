#pragma once
#include <random>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/randomSeed.h>

namespace DB
{

class FaultInjection
{
public:
    explicit FaultInjection(UInt64 seed_ = 0) : seed(seed_)
    {
        if (0 == seed)
            seed = randomSeed();
    }
    virtual ~FaultInjection() = default;
    virtual void beforeOperation() = 0;
    virtual void afterOperation() = 0;
    UInt64 getSeed() const { return seed; }

protected:
    UInt64 seed = 0;
};

class Random : public FaultInjection
{
public:
    static std::unique_ptr<Random> create(UInt64 seed_, double probability)
    {
        if (probability < 0.0)
            probability = .0;
        if (probability > 1.0)
            probability = 1.0;

        return std::make_unique<Random>(seed_, probability);
    }

    explicit Random(const UInt64 seed_, double probability) : FaultInjection(seed_), rndgen(getSeed()), distribution(probability) { }
    ~Random() override = default;

    void beforeOperation() override
    {
        if (distribution(rndgen))
            throw zkutil::KeeperException("Fault injection", Coordination::Error::ZSESSIONEXPIRED);
    }
    void afterOperation() override
    {
        if (distribution(rndgen))
            throw zkutil::KeeperException("Fault injection", Coordination::Error::ZOPERATIONTIMEOUT);
    }

private:
    std::mt19937_64 rndgen;
    std::bernoulli_distribution distribution;
};

class KeeperAccess
{
    using zk = zkutil::ZooKeeper;

    zk::Ptr keeper;
    std::unique_ptr<FaultInjection> fault_policy;
    std::string name;
    Poco::Logger * logger = nullptr;
    UInt64 calls_total = 0;
    UInt64 calls_without_fault_injection = 0;
    UInt64 seed = 0;

public:
    using Ptr = std::shared_ptr<KeeperAccess>;

    static KeeperAccess::Ptr createInstance(
        int fault_injection_mode,
        UInt64 fault_injection_seed,
        double fault_injection_probability,
        const zk::Ptr & zookeeper,
        std::string name,
        Poco::Logger * logger)
    {
        switch (fault_injection_mode)
        {
            case 1:
                return std::make_shared<KeeperAccess>(
                    zookeeper, Random::create(fault_injection_seed, fault_injection_probability), std::move(name), logger);
            default:
                return std::make_shared<KeeperAccess>(zookeeper);
        }
        __builtin_unreachable();
    }

    explicit KeeperAccess(zk::Ptr const & keeper_) : keeper(keeper_), fault_policy(nullptr) { }

    KeeperAccess(
        zk::Ptr const & keeper_, std::unique_ptr<FaultInjection> fault_policy_, std::string name_, Poco::Logger * logger_ = nullptr)
        : keeper(keeper_), fault_policy(std::move(fault_policy_)), name(std::move(name_)), logger(logger_), seed(fault_policy->getSeed())
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "KeeperAccess created: name={} seed={}", name, seed);
    }

    ~KeeperAccess()
    {
        if (unlikely(logger))
            LOG_TRACE(
                logger,
                "KeeperAccess report: name={} seed={} calls_total={} calls_succeeded={} calls_failed={} failure_rate={}",
                name,
                fault_policy->getSeed(),
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
                LOG_TRACE(logger, "KeeperAccess call SUCCEDED: seed={} func={} path={}", seed, func_name, path);
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

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
    virtual void reset() = 0;
    UInt64 getSeed() const { return seed; }

protected:
    UInt64 seed = 0;
};

class NoFaultInjection : public FaultInjection
{
public:
    ~NoFaultInjection() override = default;
    void beforeOperation() override { }
    void afterOperation() override { }
    void reset() override { }
};

class EachCallOnce : public FaultInjection
{
public:
    explicit EachCallOnce(UInt64 seed_) : FaultInjection(seed_) { }
    ~EachCallOnce() override = default;
    void beforeOperation() override
    {
        if (failed == succeeded)
        {
            /// todo: randomly decide if we should fail here or in afterOperation()
            ++failed;
            throw zkutil::KeeperException("Fault injection", Coordination::Error::ZSESSIONEXPIRED);
        }
        ++succeeded;
    }
    void afterOperation() override
    {
        if (failed == succeeded)
        {
            ++failed;
            throw zkutil::KeeperException("Fault injection", Coordination::Error::ZOPERATIONTIMEOUT);
        }
        ++succeeded;
    }
    void reset() override
    {
        succeeded = 0;
        failed = 0;
    }

private:
    size_t succeeded = 0;
    size_t failed = 0;
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
    void reset() override { }

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
            case 2:
                return std::make_shared<KeeperAccess>(
                    zookeeper, std::make_unique<EachCallOnce>(fault_injection_seed), std::move(name), logger);

            default:
                return std::make_shared<KeeperAccess>(zookeeper, std::move(name), logger);
        }
        __builtin_unreachable();
    }

    explicit KeeperAccess(zk::Ptr const & keeper_, std::string name_ = "", Poco::Logger * logger_ = nullptr)
        : keeper(keeper_), fault_policy(std::make_unique<NoFaultInjection>()), name(std::move(name_)), logger(logger_)
    {
    }

    explicit KeeperAccess(
        zk::Ptr const & keeper_, std::unique_ptr<FaultInjection> fault_policy_, std::string name_, Poco::Logger * logger_ = nullptr)
        : keeper(keeper_), fault_policy(std::move(fault_policy_)), name(std::move(name_)), logger(logger_)
    {
        if (unlikely(logger))
            LOG_DEBUG(logger, "Created KeeperAccess({}): seed={}", name, fault_policy->getSeed());
    }

    ~KeeperAccess()
    {
        if (unlikely(logger))
            LOG_DEBUG(
                logger,
                "KeeperAccess({}): seed={} calls_total={} calls_succeeded={} calls_failed={} failure_rate={}",
                name,
                fault_policy->getSeed(),
                calls_total,
                calls_without_fault_injection,
                calls_total - calls_without_fault_injection,
                float(calls_total - calls_without_fault_injection) / calls_total);
    }

    /// todo: remove getKeeper(), it should be a temporary measure
    zk::Ptr getKeeper() const { return keeper; }
    void setKeeper(zk::Ptr const & keeper_) { keeper = keeper_; }
    void resetFaultInjection() { fault_policy->reset(); }
    UInt64 callCount() const { return calls_total; }

    ///
    /// mirror ZooKeeper interface
    ///

    Strings getChildren(
        const std::string & path,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, path);
        return access<Strings>([&]() { return keeper->getChildren(path, stat, watch, list_request_type); });
    }

    zk::FutureExists asyncExists(const std::string & path, Coordination::WatchCallback watch_callback = {})
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, path);
        return access<zk::FutureExists>([&]() { return keeper->asyncExists(path, watch_callback); });
    }

    zk::FutureGet asyncTryGet(const std::string & path)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, path);
        return access<zk::FutureGet>([&]() { return keeper->asyncTryGet(path); });
    }

    bool tryGet(
        const std::string & path,
        std::string & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::Error * code = nullptr)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, path);
        return access<bool>([&]() { return keeper->tryGet(path, res, stat, watch, code); });
    }

    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, !requests.empty() ? requests.front()->getPath() : "");
        return access<Coordination::Error>([&]() { return keeper->tryMulti(requests, responses); });
    }

    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, requests.front()->getPath());
        return access<Coordination::Error>([&]() { return keeper->tryMultiNoThrow(requests, responses); });
    }

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, path);
        return access<std::string>([&]() { return keeper->get(path, stat, watch); });
    }

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, path);
        return access<bool>([&]() { return keeper->exists(path, stat, watch); });
    }

    std::string create(const std::string & path, const std::string & data, int32_t mode)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, path);
        return access<std::string>([&]() { return keeper->create(path, data, mode); });
    }

    Coordination::Responses multi(const Coordination::Requests & requests)
    {
        if (unlikely(logger))
            LOG_TRACE(logger, "{}({})", __PRETTY_FUNCTION__, !requests.empty() ? requests.front()->getPath() : "");
        return access<Coordination::Responses>([&]() { return keeper->multi(requests); });
    }

private:
    template <typename Result>
    Result access(auto && operation)
    {
        ++calls_total;
        fault_policy->beforeOperation();
        Result res = operation();
        fault_policy->afterOperation();
        ++calls_without_fault_injection;
        return res;
    }
};

using KeeperAccessPtr = KeeperAccess::Ptr;
}

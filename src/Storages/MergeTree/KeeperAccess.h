#pragma once
#include <random>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/randomSeed.h>

namespace DB
{

class FaultInjectionPolicy
{
public:
    virtual ~FaultInjectionPolicy() = default;
    virtual void beforeOperation() = 0;
    virtual void afterOperation() = 0;
    virtual void reset() = 0;
};

class NoOpPolicy : public FaultInjectionPolicy
{
public:
    ~NoOpPolicy() override = default;
    void beforeOperation() override { }
    void afterOperation() override { }
    void reset() override { }
};

class EachCallOnce : public FaultInjectionPolicy
{
public:
    ~EachCallOnce() override = default;
    void beforeOperation() override
    {
        if (failed == suceeded)
        {
            ++failed;
            throw zkutil::KeeperException("Fault injection", Coordination::Error::ZOPERATIONTIMEOUT);
        }
        ++suceeded;
    }
    void afterOperation() override { }
    void reset() override
    {
        suceeded = 0;
        failed = 0;
    }

private:
    size_t suceeded = 0;
    size_t failed = 0;
};

class Rand : public FaultInjectionPolicy
{
public:
    static std::unique_ptr<Rand> create(UInt64 seed, double probability)
    {
        if (0 == seed)
            seed = randomSeed();

        if (probability < 0.0 || probability > 1.0)
            probability = 0.01;

        return std::make_unique<Rand>(seed, probability);
    }

    explicit Rand(const UInt64 seed, double probability) : rndgen(seed), distribution(probability) { }
    ~Rand() override = default;

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
    std::unique_ptr<FaultInjectionPolicy> fault_policy;
    std::string name;
    Poco::Logger * logger = nullptr;
    UInt64 calls_total = 0;
    UInt64 calls_without_fault_injection = 0;

public:
    explicit KeeperAccess(zk::Ptr const & keeper_, std::string name_ = "", Poco::Logger * logger_ = nullptr)
        : keeper(keeper_), fault_policy(std::make_unique<NoOpPolicy>()), name(std::move(name_)), logger(logger_)
    {
    }

    explicit KeeperAccess(
        zk::Ptr const & keeper_, std::unique_ptr<FaultInjectionPolicy> fault_policy_, std::string name_, Poco::Logger * logger_ = nullptr)
        : keeper(keeper_), fault_policy(std::move(fault_policy_)), name(std::move(name_)), logger(logger_)
    {
    }

    ~KeeperAccess()
    {
        if (logger)
            LOG_DEBUG(
                logger,
                "KeeperAccess({}): calls_total={} calls_succeeded={} calls_failed={} failure_rate={}",
                name,
                calls_total,
                calls_without_fault_injection,
                calls_total - calls_without_fault_injection,
                float(calls_total - calls_without_fault_injection) / calls_total);
    }

    /// todo: remove getKeeper(), it's a temporary measure
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
        return access<Strings>([&]() { return keeper->getChildren(path, stat, watch, list_request_type); });
    }

    zk::FutureExists asyncExists(const std::string & path, Coordination::WatchCallback watch_callback = {})
    {
        return access<zk::FutureExists>([&]() { return keeper->asyncExists(path, watch_callback); });
    }

    zk::FutureGet asyncTryGet(const std::string & path)
    {
        return access<zk::FutureGet>([&]() { return keeper->asyncTryGet(path); });
    }

    bool tryGet(
        const std::string & path,
        std::string & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::Error * code = nullptr)
    {
        return access<bool>([&]() { return keeper->tryGet(path, res, stat, watch, code); });
    }

    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        return access<Coordination::Error>([&]() { return keeper->tryMulti(requests, responses); });
    }

    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        return access<Coordination::Error>([&]() { return keeper->tryMultiNoThrow(requests, responses); });
    }

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<std::string>([&]() { return keeper->get(path, stat, watch); });
    }

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<bool>([&]() { return keeper->exists(path, stat, watch); });
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

using KeeperAccessPtr = std::shared_ptr<KeeperAccess>;
}

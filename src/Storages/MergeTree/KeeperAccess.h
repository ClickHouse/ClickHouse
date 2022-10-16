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
/// ZooKeeperWithFailtInjection mimics ZooKeeper interface and inject failures according to failure policy if set
///
class ZooKeeperWithFailtInjection
{
    using zk = zkutil::ZooKeeper;

    zk::Ptr keeper;
    std::unique_ptr<RandomFaultInjection> fault_policy;
    std::string name;
    Poco::Logger * logger = nullptr;
    UInt64 calls_total = 0;
    UInt64 calls_without_fault_injection = 0;
    const UInt64 seed = 0;
    const std::function<void(const std::string &)> noop_cleanup = [](const std::string &) {};

    ZooKeeperWithFailtInjection(
        zk::Ptr const & keeper_,
        double fault_injection_probability,
        UInt64 fault_injection_seed,
        std::string name_,
        Poco::Logger * logger_ = nullptr)
        : keeper(keeper_), name(std::move(name_)), logger(logger_), seed(fault_injection_seed)
    {
        if (fault_injection_probability > .0)
            fault_policy = std::make_unique<RandomFaultInjection>(fault_injection_probability, fault_injection_seed);

        if (unlikely(logger))
            LOG_TRACE(logger, "KeeperAccess created: name={} seed={} fault_probability={}", name, seed, fault_injection_probability);
    }

public:
    using Ptr = std::shared_ptr<ZooKeeperWithFailtInjection>;

    static ZooKeeperWithFailtInjection::Ptr createInstance(
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
            return std::shared_ptr<ZooKeeperWithFailtInjection>(
                new ZooKeeperWithFailtInjection(zookeeper, fault_injection_probability, fault_injection_seed, std::move(name), logger));

        /// if no fault injection provided, create instance which will not log anything
        return std::make_shared<ZooKeeperWithFailtInjection>(zookeeper);
    }

    explicit ZooKeeperWithFailtInjection(zk::Ptr const & keeper_) : keeper(keeper_) { }

    ~ZooKeeperWithFailtInjection()
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
        return access<Strings>(
            "getChildren", path, [&]() { return keeper->getChildren(path, stat, watch, list_request_type); }, noop_cleanup);
    }

    zk::FutureExists asyncExists(const std::string & path, Coordination::WatchCallback watch_callback = {})
    {
        return access<zk::FutureExists>(
            "asyncExists", path, [&]() { return keeper->asyncExists(path, watch_callback); }, noop_cleanup);
    }

    zk::FutureGet asyncTryGet(const std::string & path)
    {
        return access<zk::FutureGet>(
            "asyncTryGet", path, [&]() { return keeper->asyncTryGet(path); }, noop_cleanup);
    }

    bool tryGet(
        const std::string & path,
        std::string & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::Error * code = nullptr)
    {
        return access<bool>(
            "tryGet", path, [&]() { return keeper->tryGet(path, res, stat, watch, code); }, noop_cleanup);
    }

    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        return access<Coordination::Error>(
            "tryMulti",
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->tryMulti(requests, responses); },
            noop_cleanup);
    }

    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        return access<Coordination::Error, false>(
            "tryMultiNoThrow",
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->tryMultiNoThrow(requests, responses); },
            noop_cleanup);
    }

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<std::string>(
            "get", path, [&]() { return keeper->get(path, stat, watch); }, noop_cleanup);
    }

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<bool>(
            "exists", path, [&]() { return keeper->exists(path, stat, watch); }, noop_cleanup);
    }

    std::string create(const std::string & path, const std::string & data, int32_t mode)
    {
        return access<std::string>(
            "create",
            path,
            [&]() { return keeper->create(path, data, mode); },
            [&](std::string const & result_path)
            {
                try
                {
                    if (mode | zkutil::CreateMode::EphemeralSequential)
                    {
                        keeper->remove(result_path);
                        if (unlikely(logger))
                            LOG_TRACE(logger, "KeeperAccess cleanup: seed={} func={} path={}", seed, "create", result_path);
                    }
                }
                catch (const zkutil::KeeperException & e)
                {
                    if (unlikely(logger))
                        LOG_TRACE(
                            logger,
                            "KeeperAccess cleanup FAILED: seed={} func={} path={} code={} message={} ",
                            seed,
                            "create",
                            result_path,
                            e.code,
                            e.message());
                }
            });
    }

    Coordination::Responses multi(const Coordination::Requests & requests)
    {
        return access<Coordination::Responses>(
            "multi", !requests.empty() ? requests.front()->getPath() : "", [&]() { return keeper->multi(requests); }, noop_cleanup);
    }

private:
    template <typename Result, bool inject_failure_before_op = true, int inject_failure_after_op = true>
    Result access(const char * func_name, const std::string & path, auto && operation, auto && fault_after_op_cleanup)
    {
        try
        {
            ++calls_total;
            if (!keeper)
                throw zkutil::KeeperException(
                    "Session is considered to be expired due to fault injection", Coordination::Error::ZSESSIONEXPIRED);

            if constexpr (inject_failure_before_op)
            {
                if (unlikely(fault_policy))
                    fault_policy->beforeOperation();
            }

            Result res = operation();

            if constexpr (inject_failure_after_op)
            {
                try
                {
                    if (unlikely(fault_policy))
                        fault_policy->afterOperation();
                }
                catch (...)
                {
                    if constexpr(std::is_same_v<Result, std::string>)
                        fault_after_op_cleanup(res);
                    else
                        fault_after_op_cleanup("");

                    throw;
                }
            }

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

            keeper.reset();

            if constexpr (std::is_same_v<Coordination::Error, Result>)
                return e.code;
            else
                throw;
        }
    }
};

using ZooKeeperWithFaultInjectionPtr = ZooKeeperWithFailtInjection::Ptr;
}

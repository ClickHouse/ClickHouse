#pragma once
#include <random>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/randomSeed.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
class ZooKeeperWithFaultInjection
{
    using zk = zkutil::ZooKeeper;

    zk::Ptr keeper;
    std::unique_ptr<RandomFaultInjection> fault_policy;
    std::string name;
    Poco::Logger * logger = nullptr;
    UInt64 calls_total = 0;
    UInt64 calls_without_fault_injection = 0;
    const UInt64 seed = 0;
    const std::function<void()> noop_cleanup = []() {};

    ZooKeeperWithFaultInjection(
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
            LOG_TRACE(
                logger,
                "ZooKeeperWithFailtInjection created: name={} seed={} fault_probability={}",
                name,
                seed,
                fault_injection_probability);
    }

public:
    using Ptr = std::shared_ptr<ZooKeeperWithFaultInjection>;

    static ZooKeeperWithFaultInjection::Ptr createInstance(
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
            return std::shared_ptr<ZooKeeperWithFaultInjection>(
                new ZooKeeperWithFaultInjection(zookeeper, fault_injection_probability, fault_injection_seed, std::move(name), logger));

        /// if no fault injection provided, create instance which will not log anything
        return std::make_shared<ZooKeeperWithFaultInjection>(zookeeper);
    }

    explicit ZooKeeperWithFaultInjection(zk::Ptr const & keeper_) : keeper(keeper_) { }

    ~ZooKeeperWithFaultInjection()
    {
        if (unlikely(logger))
            LOG_TRACE(
                logger,
                "ZooKeeperWithFailtInjection report: name={} seed={} calls_total={} calls_succeeded={} calls_failed={} failure_rate={}",
                name,
                seed,
                calls_total,
                calls_without_fault_injection,
                calls_total - calls_without_fault_injection,
                float(calls_total - calls_without_fault_injection) / calls_total);
    }

    void setKeeper(zk::Ptr const & keeper_) { keeper = keeper_; }
    bool isNull() const { return keeper.get() == nullptr; }

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

    Coordination::Error tryGetChildren(
        const std::string & path,
        Strings & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL)
    {
        return access<Coordination::Error>(
            "tryGetChildren",
            path,
            [&]() { return keeper->tryGetChildren(path, res, stat, watch, list_request_type); },
            [&](Coordination::Error) {});
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
            [&](Coordination::Error err)
            {
                if (err == Coordination::Error::ZOK)
                    faultInjectionCleanup("tryMulti", requests, responses);
            });
    }

    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        return access<Coordination::Error, false>(
            "tryMultiNoThrow",
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->tryMultiNoThrow(requests, responses); },
            [&](Coordination::Error err)
            {
                if (err == Coordination::Error::ZOK)
                    faultInjectionCleanup("tryMultiNoThrow", requests, responses);
            });
    }

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<std::string>(
            "get", path, [&]() { return keeper->get(path, stat, watch); }, [](const std::string &) {});
    }

    zkutil::ZooKeeper::MultiGetResponse get(const std::vector<std::string> & paths)
    {
        return access<zkutil::ZooKeeper::MultiGetResponse>(
            "get", paths.front(), [&]() { return keeper->get(paths); }, noop_cleanup);
    }

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<bool>(
            "exists", path, [&]() { return keeper->exists(path, stat, watch); }, noop_cleanup);
    }

    zkutil::ZooKeeper::MultiExistsResponse exists(const std::vector<std::string> & paths)
    {
        return access<zkutil::ZooKeeper::MultiExistsResponse>(
            "exists", paths.front(), [&]() { return keeper->exists(paths); }, noop_cleanup);
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
                    if (mode == zkutil::CreateMode::EphemeralSequential || mode == zkutil::CreateMode::Ephemeral)
                    {
                        keeper->remove(result_path);
                        if (unlikely(logger))
                            LOG_TRACE(logger, "ZooKeeperWithFailtInjection cleanup: seed={} func={} path={}", seed, "create", result_path);
                    }
                }
                catch (const zkutil::KeeperException & e)
                {
                    if (unlikely(logger))
                        LOG_TRACE(
                            logger,
                            "ZooKeeperWithFailtInjection cleanup FAILED: seed={} func={} path={} code={} message={} ",
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
            "multi",
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->multi(requests); },
            [&](Coordination::Responses const & responses) { faultInjectionCleanup("multi", requests, responses); });
    }

    void createAncestors(const std::string & path)
    {
        access<void>(
            "createAncestors", path, [&]() { return keeper->createAncestors(path); }, noop_cleanup);
    }

    Coordination::Error tryRemove(const std::string & path, int32_t version = -1)
    {
        return access<Coordination::Error>(
            "tryRemove", path, [&]() { return keeper->tryRemove(path, version); }, [&](Coordination::Error) {});
    }

private:
    template <typename Result, typename FaultCleanup>
    void faultInjectionAfter(const Result & res, FaultCleanup fault_cleanup)
    {
        try
        {
            if (unlikely(fault_policy))
                fault_policy->afterOperation();
        }
        catch (const zkutil::KeeperException &)
        {
            if constexpr (std::is_same_v<void, std::decay_t<Result>>)
                fault_cleanup();
            else
            {
                if constexpr (std::is_same_v<Result, std::string>)
                    fault_cleanup(res);
                else if constexpr (std::is_same_v<Result, Coordination::Responses>)
                    fault_cleanup(res);
                else if constexpr (std::is_same_v<Result, Coordination::Error>)
                    fault_cleanup(res);
                else
                    fault_cleanup();
            }

            throw;
        }
    }

    void faultInjectionCleanup(const char * method, const Coordination::Requests & requests, const Coordination::Responses & responses)
    {
        if (responses.empty())
            return;

        if (responses.size() != requests.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Number of responses doesn't match number of requests: method={} requests={} responses={}",
                method,
                requests.size(),
                responses.size());

        /// find create request with ephemeral flag
        std::vector<std::pair<size_t, const Coordination::CreateRequest *>> create_requests;
        for (size_t i = 0; i < requests.size(); ++i)
        {
            const auto * create_req = dynamic_cast<const Coordination::CreateRequest *>(requests[i].get());
            if (create_req && create_req->is_ephemeral)
                create_requests.emplace_back(i, create_req);
        }

        for (auto && [i, req] : create_requests)
        {
            const auto * create_resp = dynamic_cast<const Coordination::CreateResponse *>(responses.at(i).get());
            if (!create_resp)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Response should be CreateResponse: method={} index={} path={}", method, i, req->path);

            keeper->remove(create_resp->path_created);
        }
    }

    template <typename Result, bool inject_failure_before_op = true, int inject_failure_after_op = true, typename FaultCleanup>
    Result access(const char * func_name, const std::string & path, auto && operation, FaultCleanup fault_after_op_cleanup)
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

            if constexpr (!std::is_same_v<Result, void>)
            {
                Result res = operation();

                /// if connectivity error occurred w/o fault injection -> just return it
                if constexpr (std::is_same_v<Coordination::Error, Result>)
                {
                    if (Coordination::isHardwareError(res))
                        return res;
                }

                if constexpr (inject_failure_after_op)
                    faultInjectionAfter(res, fault_after_op_cleanup);

                ++calls_without_fault_injection;

                if (unlikely(logger))
                    LOG_TRACE(logger, "ZooKeeperWithFailtInjection call SUCCEEDED: seed={} func={} path={}", seed, func_name, path);

                return res;
            }
            else
            {
                operation();

                if constexpr (inject_failure_after_op)
                {
                    void * stub = nullptr; /// just for template overloading
                    faultInjectionAfter(stub, fault_after_op_cleanup);
                }

                ++calls_without_fault_injection;

                if (unlikely(logger))
                    LOG_TRACE(logger, "ZooKeeperWithFailtInjection call SUCCEEDED: seed={} func={} path={}", seed, func_name, path);
            }
        }
        catch (const zkutil::KeeperException & e)
        {
            if (unlikely(logger))
                LOG_TRACE(
                    logger,
                    "ZooKeeperWithFailtInjection call FAILED: seed={} func={} path={} code={} message={} ",
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

using ZooKeeperWithFaultInjectionPtr = ZooKeeperWithFaultInjection::Ptr;
}

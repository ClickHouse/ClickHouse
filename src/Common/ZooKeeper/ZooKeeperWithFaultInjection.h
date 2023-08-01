#pragma once
#include <random>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>
#include "Coordination/KeeperConstants.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class RandomFaultInjection
{
public:
    bool must_fail_after_op = false;
    bool must_fail_before_op = false;

    RandomFaultInjection(double probability, UInt64 seed_) : rndgen(seed_), distribution(probability) { }

    void beforeOperation()
    {
        if (distribution(rndgen) || must_fail_before_op)
        {
            must_fail_before_op = false;
            throw zkutil::KeeperException("Fault injection before operation", Coordination::Error::ZSESSIONEXPIRED);
        }
    }
    void afterOperation()
    {
        if (distribution(rndgen) || must_fail_after_op)
        {
            must_fail_after_op = false;
            throw zkutil::KeeperException("Fault injection after operation", Coordination::Error::ZOPERATIONTIMEOUT);
        }
    }

private:
    std::mt19937_64 rndgen;
    std::bernoulli_distribution distribution;
};

///
/// ZooKeeperWithFaultInjection mimics ZooKeeper interface and inject failures according to failure policy if set
///
class ZooKeeperWithFaultInjection
{
    template<bool async_insert>
    friend class ReplicatedMergeTreeSinkImpl;

    using zk = zkutil::ZooKeeper;

    zk::Ptr keeper;
    zk::Ptr keeper_prev;
    std::unique_ptr<RandomFaultInjection> fault_policy;
    std::string name;
    Poco::Logger * logger = nullptr;
    UInt64 calls_total = 0;
    UInt64 calls_without_fault_injection = 0;
    const UInt64 seed = 0;

    std::vector<std::string> ephemeral_nodes;

    ZooKeeperWithFaultInjection(
        zk::Ptr const & keeper_,
        double fault_injection_probability,
        UInt64 fault_injection_seed,
        std::string name_,
        Poco::Logger * logger_)
        : keeper(keeper_), name(std::move(name_)), logger(logger_), seed(fault_injection_seed)
    {
        fault_policy = std::make_unique<RandomFaultInjection>(fault_injection_probability, fault_injection_seed);

        if (unlikely(logger))
            LOG_TRACE(
                logger,
                "ZooKeeperWithFaultInjection created: name={} seed={} fault_probability={}",
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
                "ZooKeeperWithFaultInjection report: name={} seed={} calls_total={} calls_succeeded={} calls_failed={} failure_rate={}",
                name,
                seed,
                calls_total,
                calls_without_fault_injection,
                calls_total - calls_without_fault_injection,
                float(calls_total - calls_without_fault_injection) / calls_total);
    }

    void setKeeper(zk::Ptr const & keeper_) { keeper = keeper_; }
    bool isNull() const { return keeper.get() == nullptr; }
    bool expired() { return keeper->expired(); }

    ///
    /// mirror ZooKeeper interface
    ///

    Strings getChildren(
        const std::string & path,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL)
    {
        return access("getChildren", path, [&]() { return keeper->getChildren(path, stat, watch, list_request_type); });
    }

    Coordination::Error tryGetChildren(
        const std::string & path,
        Strings & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL)
    {
        return access("tryGetChildren", path, [&]() { return keeper->tryGetChildren(path, res, stat, watch, list_request_type); });
    }

    zk::FutureExists asyncExists(const std::string & path, Coordination::WatchCallback watch_callback = {})
    {
        return access("asyncExists", path, [&]() { return keeper->asyncExists(path, watch_callback); });
    }

    zk::FutureGet asyncTryGet(const std::string & path)
    {
        return access("asyncTryGet", path, [&]() { return keeper->asyncTryGet(path); });
    }

    bool tryGet(
        const std::string & path,
        std::string & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::Error * code = nullptr)
    {
        return access("tryGet", path, [&]() { return keeper->tryGet(path, res, stat, watch, code); });
    }

    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        constexpr auto method = "tryMulti";
        auto error = access(
            method,
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->tryMulti(requests, responses); },
            [&](const Coordination::Error & original_error)
            {
                if (original_error == Coordination::Error::ZOK)
                    faultInjectionPostAction(method, requests, responses);
            },
            [&]()
            {
                responses.clear();
                for (size_t i = 0; i < requests.size(); ++i)
                    responses.emplace_back(std::make_shared<Coordination::ZooKeeperErrorResponse>());
            });


        /// collect ephemeral nodes when no fault was injected (to clean up on demand)
        if (unlikely(fault_policy) && Coordination::Error::ZOK == error)
        {
            doForEachCreatedEphemeralNode(
                method, requests, responses, [&](const String & path_created) { ephemeral_nodes.push_back(path_created); });
        }
        return error;
    }

    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        constexpr auto method = "tryMultiNoThrow";
        constexpr auto no_throw = true;
        constexpr auto inject_failure_before_op = false;
        auto error = access<no_throw, inject_failure_before_op>(
            method,
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->tryMultiNoThrow(requests, responses); },
            [&](const Coordination::Error & original_error)
            {
                if (original_error == Coordination::Error::ZOK)
                    faultInjectionPostAction(method, requests, responses);
            },
            [&]()
            {
                responses.clear();
                for (size_t i = 0; i < requests.size(); ++i)
                    responses.emplace_back(std::make_shared<Coordination::ZooKeeperErrorResponse>());
            });

        /// collect ephemeral nodes when no fault was injected (to clean up later)
        if (unlikely(fault_policy) && Coordination::Error::ZOK == error)
        {
            doForEachCreatedEphemeralNode(
                method, requests, responses, [&](const String & path_created) { ephemeral_nodes.push_back(path_created); });
        }
        return error;
    }

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access("get", path, [&]() { return keeper->get(path, stat, watch); });
    }

    zkutil::ZooKeeper::MultiGetResponse get(const std::vector<std::string> & paths)
    {
        return access("get", !paths.empty() ? paths.front() : "", [&]() { return keeper->get(paths); });
    }

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access("exists", path, [&]() { return keeper->exists(path, stat, watch); });
    }

    bool existsNoFailureInjection(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr)
    {
        return access<false, false, false>("exists", path, [&]() { return keeper->exists(path, stat, watch); });
    }

    zkutil::ZooKeeper::MultiExistsResponse exists(const std::vector<std::string> & paths)
    {
        return access("exists", !paths.empty() ? paths.front() : "", [&]() { return keeper->exists(paths); });
    }

    std::string create(const std::string & path, const std::string & data, int32_t mode)
    {
        std::string path_created;
        auto code = tryCreate(path, data, mode, path_created);

        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException(code, path);

        return path_created;
    }

    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
    {
        path_created.clear();

        auto error = access(
            "tryCreate",
            path,
            [&]() { return keeper->tryCreate(path, data, mode, path_created); },
            [&](Coordination::Error & code)
            {
                try
                {
                    if (!path_created.empty() && (mode == zkutil::CreateMode::EphemeralSequential || mode == zkutil::CreateMode::Ephemeral))
                    {
                        keeper->remove(path_created);
                        if (unlikely(logger))
                            LOG_TRACE(logger, "ZooKeeperWithFaultInjection cleanup: seed={} func={} path={} path_created={} code={}",
                                seed, "tryCreate", path, path_created, code);
                    }
                }
                catch (const zkutil::KeeperException & e)
                {
                    if (unlikely(logger))
                        LOG_TRACE(
                            logger,
                            "ZooKeeperWithFaultInjection cleanup FAILED: seed={} func={} path={} path_created={} code={} message={} ",
                            seed,
                            "tryCreate",
                            path,
                            path_created,
                            e.code,
                            e.message());
                }
            });

        /// collect ephemeral nodes when no fault was injected (to clean up later)
        if (unlikely(fault_policy))
        {
            if (!path_created.empty() && (mode == zkutil::CreateMode::EphemeralSequential || mode == zkutil::CreateMode::Ephemeral))
                ephemeral_nodes.push_back(path_created);
        }

        return error;
    }

    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode)
    {
        String path_created;
        return tryCreate(path, data, mode, path_created);
    }

    void createIfNotExists(const std::string & path, const std::string & data)
    {
        std::string path_created;
        auto code = tryCreate(path, data, zkutil::CreateMode::Persistent, path_created);

        if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
            return;

        throw zkutil::KeeperException(code, path);
    }

    Coordination::Responses multi(const Coordination::Requests & requests)
    {
        constexpr auto method = "multi";
        auto result = access(
            method,
            !requests.empty() ? requests.front()->getPath() : "",
            [&]() { return keeper->multi(requests); },
            [&](Coordination::Responses & responses) { faultInjectionPostAction(method, requests, responses); });

        /// collect ephemeral nodes to clean up
        if (unlikely(fault_policy))
        {
            doForEachCreatedEphemeralNode(
                method, requests, result, [&](const String & path_created) { ephemeral_nodes.push_back(path_created); });
        }
        return result;
    }

    void createAncestors(const std::string & path)
    {
        access("createAncestors", path, [&]() { return keeper->createAncestors(path); });
    }

    Coordination::Error tryRemove(const std::string & path, int32_t version = -1)
    {
        return access("tryRemove", path, [&]() { return keeper->tryRemove(path, version); });
    }

    void removeRecursive(const std::string & path)
    {
        return access("removeRecursive", path, [&]() { return keeper->removeRecursive(path); });
    }

    std::string sync(const std::string & path)
    {
        return access("sync", path, [&]() { return keeper->sync(path); });
    }

    Coordination::Error trySet(const std::string & path, const std::string & data, int32_t version = -1, Coordination::Stat * stat = nullptr)
    {
        return access("trySet", path, [&]() { return keeper->trySet(path, data, version, stat); });
    }

    void checkExistsAndGetCreateAncestorsOps(const std::string & path, Coordination::Requests & requests)
    {
        return access("checkExistsAndGetCreateAncestorsOps", path, [&]() { return keeper->checkExistsAndGetCreateAncestorsOps(path, requests); });
    }

    void handleEphemeralNodeExistenceNoFailureInjection(const std::string & path, const std::string & fast_delete_if_equal_value)
    {
        return access<false, false, false>("handleEphemeralNodeExistence", path, [&]() { return keeper->handleEphemeralNodeExistence(path, fast_delete_if_equal_value); });
    }

    void cleanupEphemeralNodes()
    {
        for (const auto & path : ephemeral_nodes)
        {
            try
            {
                if (keeper_prev)
                    keeper_prev->tryRemove(path);
            }
            catch (...)
            {
                if (unlikely(logger))
                    tryLogCurrentException(logger, "Exception during ephemeral nodes clean up");
            }
        }

        ephemeral_nodes.clear();
    }

    bool isFeatureEnabled(KeeperFeatureFlag feature_flag) const
    {
        return keeper->isFeatureEnabled(feature_flag);
    }

private:
    void faultInjectionBefore(std::function<void()> fault_cleanup)
    {
        try
        {
            if (unlikely(fault_policy))
                fault_policy->beforeOperation();
        }
        catch (const zkutil::KeeperException &)
        {
            fault_cleanup();
            throw;
        }
    }
    void faultInjectionAfter(std::function<void()> fault_cleanup)
    {
        try
        {
            if (unlikely(fault_policy))
                fault_policy->afterOperation();
        }
        catch (const zkutil::KeeperException &)
        {
            fault_cleanup();
            throw;
        }
    }

    void doForEachCreatedEphemeralNode(
        const char * method, const Coordination::Requests & requests, const Coordination::Responses & responses, auto && action)
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

            action(create_resp->path_created);
        }
    }

    void faultInjectionPostAction(const char * method, const Coordination::Requests & requests, Coordination::Responses & responses)
    {
        doForEachCreatedEphemeralNode(method, requests, responses, [&](const String & path_created) { keeper->remove(path_created); });
    }

    template <typename T>
    struct FaultCleanupTypeImpl
    {
        using Type = std::function<void(T &)>;
    };

    template <>
    struct FaultCleanupTypeImpl<void>
    {
        using Type = std::function<void()>;
    };

    template <typename T>
    using FaultCleanupType = typename FaultCleanupTypeImpl<T>::Type;

    template <
        bool no_throw_access = false,
        bool inject_failure_before_op = true,
        int inject_failure_after_op = true,
        typename Operation,
        typename Result = std::invoke_result_t<Operation>>
    Result access(
        const char * func_name,
        const std::string & path,
        Operation operation,
        FaultCleanupType<Result> fault_after_op_cleanup = {},
        FaultCleanupType<void> fault_before_op_cleanup = {})
    {
        try
        {
            ++calls_total;

            if (!keeper)
                throw zkutil::KeeperException(
                    "Session is considered to be expired due to fault injection", Coordination::Error::ZSESSIONEXPIRED);

            if constexpr (inject_failure_before_op)
            {
                faultInjectionBefore(
                    [&]
                    {
                        if (fault_before_op_cleanup)
                            fault_before_op_cleanup();
                    });
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
                {
                    faultInjectionAfter(
                        [&]
                        {
                            if (fault_after_op_cleanup)
                                fault_after_op_cleanup(res);
                        });
                }

                ++calls_without_fault_injection;

                if (unlikely(logger))
                    LOG_TRACE(logger, "ZooKeeperWithFaultInjection call SUCCEEDED: seed={} func={} path={}", seed, func_name, path);

                return res;
            }
            else
            {
                operation();

                if constexpr (inject_failure_after_op)
                {
                    faultInjectionAfter(
                        [&fault_after_op_cleanup]
                        {
                            if (fault_after_op_cleanup)
                                fault_after_op_cleanup();
                        });
                }

                ++calls_without_fault_injection;

                if (unlikely(logger))
                    LOG_TRACE(logger, "ZooKeeperWithFaultInjection call SUCCEEDED: seed={} func={} path={}", seed, func_name, path);
            }
        }
        catch (const zkutil::KeeperException & e)
        {
            if (unlikely(logger))
                LOG_TRACE(
                    logger,
                    "ZooKeeperWithFaultInjection call FAILED: seed={} func={} path={} code={} message={} ",
                    seed,
                    func_name,
                    path,
                    e.code,
                    e.message());

            /// save valid pointer to clean up ephemeral nodes later if necessary
            if (keeper)
                keeper_prev = keeper;
            keeper.reset();

            /// for try*NoThrow() methods
            if constexpr (no_throw_access)
                return e.code;

            if constexpr (std::is_same_v<Coordination::Error, Result>)
            {
                /// try*() methods throws at least on hardware error and return only on user errors
                /// todo: the methods return only on subset of user errors, and throw on another errors
                ///       to mimic the methods exactly - we need to specify errors on which to return for each such method
                if (Coordination::isHardwareError(e.code))
                    throw;

                return e.code;
            }

            throw;
        }
    }
};

using ZooKeeperWithFaultInjectionPtr = ZooKeeperWithFaultInjection::Ptr;
}

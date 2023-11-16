#pragma once
#include <random>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>
#include "Coordination/KeeperConstants.h"
#include <pcg_random.hpp>

namespace DB
{


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
            throw zkutil::KeeperException::fromMessage(Coordination::Error::ZSESSIONEXPIRED, "Fault injection before operation");
        }
    }

    bool beforeOperationNoThrow()
    {
        if (distribution(rndgen) || must_fail_before_op)
        {
            must_fail_before_op = false;
            return true;
        }
        return false;
    }

    void afterOperation()
    {
        if (distribution(rndgen) || must_fail_after_op)
        {
            must_fail_after_op = false;
            throw zkutil::KeeperException::fromMessage(Coordination::Error::ZOPERATIONTIMEOUT, "Fault injection after operation");
        }
    }

    bool afterOperationNoThrow()
    {
        if (distribution(rndgen) || must_fail_after_op)
        {
            must_fail_after_op = false;
            return true;
        }
        return false;
    }

private:
    pcg64_fast rndgen;
    std::bernoulli_distribution distribution;
};

//template <
//        bool no_throw_access = false,
//        bool inject_failure_before_op = true,
//        bool inject_failure_after_op = true,
//        typename Operation,
//        typename Result = std::invoke_result_t<Operation>>
//Result faultExecuteSync(
//        const ZooKeeperWithFaultInjection & zk,
//        const char * func_name,
//        const std::string & path,
//        Operation operation,
//        FaultCleanupType<Result> fault_after_op_cleanup = {},
//        FaultCleanupType<void> fault_before_op_cleanup = {});

///
/// ZooKeeperWithFaultInjection mimics ZooKeeper interface and inject failures according to failure policy if set
///
class ZooKeeperWithFaultInjection
{
    template <bool, bool, bool, typename Operation, typename Result>
    friend Result executeWithFaultSync(ZooKeeperWithFaultInjection &, const char *, const std::string &, Operation);

    zkutil::ZooKeeper::Ptr keeper;
    std::unique_ptr<RandomFaultInjection> fault_policy;
    std::string name;
    Poco::Logger * logger = nullptr;
    const UInt64 seed = 0;

    std::vector<std::string> ephemeral_nodes;

public:
    using Ptr = std::shared_ptr<ZooKeeperWithFaultInjection>;

    ZooKeeperWithFaultInjection(
        zkutil::ZooKeeper::Ptr const & keeper_,
        double fault_injection_probability,
        UInt64 fault_injection_seed,
        std::string name_,
        Poco::Logger * logger_);

    explicit ZooKeeperWithFaultInjection(zkutil::ZooKeeper::Ptr const & keeper_) : keeper(keeper_) { }
    static ZooKeeperWithFaultInjection::Ptr createInstance(
        double fault_injection_probability,
        UInt64 fault_injection_seed,
        const zkutil::ZooKeeper::Ptr & zookeeper,
        std::string name,
        Poco::Logger * logger);

    void setKeeper(zkutil::ZooKeeper::Ptr const & keeper_) { keeper = keeper_; }
    zkutil::ZooKeeper::Ptr getKeeper() const { return keeper; }
    bool isNull() const { return keeper.get() == nullptr; }
    bool expired() const { return !keeper || keeper->expired(); }

    bool isFeatureEnabled(KeeperFeatureFlag feature_flag) const { return keeper->isFeatureEnabled(feature_flag); }

    ///
    /// mirror ZooKeeper interface: Sync functions
    ///

    Strings getChildren(
        const std::string & path,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    zkutil::ZooKeeper::MultiGetChildrenResponse getChildren(
        const std::vector<std::string> & paths, Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    Coordination::Error tryGetChildren(
        const std::string & path,
        Strings & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    zkutil::ZooKeeper::MultiTryGetChildrenResponse tryGetChildren(
        const std::vector<std::string> & paths, Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    Coordination::Error tryGetChildrenWatch(
        const std::string & path,
        Strings & res,
        Coordination::Stat * stat,
        Coordination::WatchCallback watch_callback,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    Strings getChildrenWatch(
        const std::string & path,
        Coordination::Stat * stat,
        Coordination::WatchCallback watch_callback,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);

    Strings getChildrenWatch(
        const std::string & path,
        Coordination::Stat * stat,
        Coordination::WatchCallbackPtr watch_callback,
        Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);
    //
    //    zk::FutureExists asyncExists(std::string path, Coordination::WatchCallback watch_callback = {})
    //    {
    //        auto promise = std::make_shared<std::promise<Coordination::ExistsResponse>>();
    //        auto future = promise->get_future();
    //        if (injectFailureBeforeOp(promise))
    //        {
    //            if (logger)
    //                LOG_TRACE(
    //                    logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func=asyncExists path={}", seed, path);
    //            return future;
    //        }
    //
    //        auto callback = [&, promise](const Coordination::ExistsResponse & response) mutable
    //        {
    //            if (injectFailureAfterOp(promise))
    //            {
    //                if (logger)
    //                    LOG_TRACE(
    //                        logger, "ZooKeeperWithFaultInjection injected fault after operation: seed={} func=asyncExists path={}", seed, path);
    //                return;
    //            }
    //
    //            if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE)
    //                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromPath(response.error, path)));
    //            else
    //                promise->set_value(response);
    //        };
    //
    //        keeper->impl->exists(
    //            path,
    //            std::move(callback),
    //            watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
    //        return future;
    //    }
    //
    //    zk::FutureGet asyncTryGet(std::string path)
    //    {
    //        auto promise = std::make_shared<std::promise<Coordination::GetResponse>>();
    //        auto future = promise->get_future();
    //        if (injectFailureBeforeOp(promise))
    //        {
    //            if (logger)
    //                LOG_TRACE(
    //                    logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func=asyncTryGet path={}", seed, path);
    //            return future;
    //        }
    //
    //        auto callback = [&, promise](const Coordination::GetResponse & response) mutable
    //        {
    //            if (injectFailureAfterOp(promise))
    //            {
    //                if (logger)
    //                    LOG_TRACE(
    //                        logger, "ZooKeeperWithFaultInjection injected fault after operation: seed={} func=asyncTryGet path={}", seed, path);
    //                return;
    //            }
    //
    //            if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE)
    //                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromPath(response.error, path)));
    //            else
    //                promise->set_value(response);
    //        };
    //
    //        keeper->impl->get(path, std::move(callback), {});
    //        return future;
    //    }
    //
    bool tryGet(
        const std::string & path,
        std::string & res,
        Coordination::Stat * stat = nullptr,
        const zkutil::EventPtr & watch = nullptr,
        Coordination::Error * code = nullptr);

    bool tryGetWatch(
        const std::string & path,
        std::string & res,
        Coordination::Stat * stat,
        Coordination::WatchCallback watch_callback,
        Coordination::Error * code = nullptr);
    //
    //    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
    //    {
    //        constexpr auto method = "tryMulti";
    //        auto error = access(
    //            method,
    //            !requests.empty() ? requests.front()->getPath() : "",
    //            [&]() { return keeper->tryMulti(requests, responses); },
    //            [&](const Coordination::Error & original_error)
    //            {
    //                if (original_error == Coordination::Error::ZOK)
    //                    faultInjectionPostAction(method, requests, responses);
    //            },
    //            [&]()
    //            {
    //                responses.clear();
    //                for (size_t i = 0; i < requests.size(); ++i)
    //                {
    //                    auto response = std::make_shared<Coordination::ZooKeeperErrorResponse>();
    //                    response->error = Coordination::Error::ZOPERATIONTIMEOUT;
    //                    responses.emplace_back(std::move(response));
    //                }
    //            });
    //
    //
    //        /// collect ephemeral nodes when no fault was injected (to clean up on demand)
    //        if (unlikely(fault_policy) && Coordination::Error::ZOK == error)
    //        {
    //            doForEachCreatedEphemeralNode(
    //                method, requests, responses, [&](const String & path_created) { ephemeral_nodes.push_back(path_created); });
    //        }
    //        return error;
    //    }
    //
    //    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
    //    {
    //        constexpr auto method = "tryMultiNoThrow";
    //        constexpr auto no_throw = true;
    //        constexpr auto inject_failure_before_op = false;
    //        auto error = access<no_throw, inject_failure_before_op>(
    //            method,
    //            !requests.empty() ? requests.front()->getPath() : "",
    //            [&]() { return keeper->tryMultiNoThrow(requests, responses); },
    //            [&](const Coordination::Error & original_error)
    //            {
    //                if (original_error == Coordination::Error::ZOK)
    //                    faultInjectionPostAction(method, requests, responses);
    //            },
    //            [&]()
    //            {
    //                responses.clear();
    //                for (size_t i = 0; i < requests.size(); ++i)
    //                {
    //                    auto response = std::make_shared<Coordination::ZooKeeperErrorResponse>();
    //                    response->error = Coordination::Error::ZOPERATIONTIMEOUT;
    //                    responses.emplace_back(std::move(response));
    //                }
    //            });
    //
    //        /// collect ephemeral nodes when no fault was injected (to clean up later)
    //        if (unlikely(fault_policy) && Coordination::Error::ZOK == error)
    //        {
    //            doForEachCreatedEphemeralNode(
    //                method, requests, responses, [&](const String & path_created) { ephemeral_nodes.push_back(path_created); });
    //        }
    //        return error;
    //    }
    //
    //    zk::FutureMulti asyncTryMultiNoThrow(const Coordination::Requests & ops)
    //    {
    //        auto promise = std::make_shared<std::promise<Coordination::MultiResponse>>();
    //        auto future = promise->get_future();
    //        size_t request_size = ops.size();
    //        String path = ops.empty() ? "" : ops.front()->getPath();
    //        if (!keeper || (unlikely(fault_policy) && fault_policy->beforeOperationNoThrow()))
    //        {
    //            if (logger)
    //                LOG_TRACE(
    //                    logger,
    //                    "ZooKeeperWithFaultInjection injected fault before operation: seed={} func=asyncTryMultiNoThrow path={}",
    //                    seed,
    //                    path);
    //            Coordination::MultiResponse errors;
    //            for (size_t i = 0; i < request_size; i++)
    //            {
    //                auto r = std::make_shared<Coordination::ZooKeeperErrorResponse>();
    //                r->error = Coordination::Error::ZSESSIONEXPIRED;
    //                errors.responses.emplace_back(std::move(r));
    //            }
    //            promise->set_value(errors);
    //            return future;
    //        }
    //
    //        auto callback = [&, promise](const Coordination::MultiResponse & response) mutable
    //        {
    //            if (unlikely(fault_policy) && fault_policy->afterOperationNoThrow())
    //            {
    //                if (logger)
    //                    LOG_TRACE(
    //                        logger,
    //                        "ZooKeeperWithFaultInjection injected fault after operation: seed={} func=asyncTryMultiNoThrow path={}",
    //                        seed,
    //                        path);
    //                Coordination::MultiResponse errors;
    //                for (size_t i = 0; i < request_size; i++)
    //                {
    //                    auto r = std::make_shared<Coordination::ZooKeeperErrorResponse>();
    //                    r->error = Coordination::Error::ZOPERATIONTIMEOUT;
    //                    errors.responses.emplace_back(std::move(r));
    //                }
    //                promise->set_value(errors);
    //            }
    //            else
    //            {
    //                promise->set_value(response);
    //            }
    //        };
    //
    //        keeper->impl->multi(ops, std::move(callback));
    //        return future;
    //    }
    //
    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr);

    zkutil::ZooKeeper::MultiGetResponse get(const std::vector<std::string> & paths);

    zkutil::ZooKeeper::MultiTryGetResponse tryGet(const std::vector<std::string> & paths);

    void set(const String & path, const String & data, int32_t version = -1, Coordination::Stat * stat = nullptr);

    void remove(const String & path, int32_t version = -1);

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr);

    zkutil::ZooKeeper::MultiExistsResponse exists(const std::vector<std::string> & paths);

    /// These used to handle deletion of ephemeral nodes because of faults, but that is wrong as it should be handled by
    /// the caller in the retries, as an injected fault is the same as a normal fault
    /// TODO: Pending to review if there is code depending on that (Replicated sink)
    std::string create(const std::string & path, const std::string & data, int32_t mode);
    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created);
    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode);
    Coordination::Responses multi(const Coordination::Requests & requests);

    void createOrUpdate(const std::string & path, const std::string & data, int32_t mode);
    void createAncestors(const std::string & path);
    Coordination::Error tryRemove(const std::string & path, int32_t version = -1);
    //
    //    zk::FutureRemove asyncTryRemove(std::string path, int32_t version = -1)
    //    {
    //        auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    //        auto future = promise->get_future();
    //        if (injectFailureBeforeOp(promise))
    //        {
    //            if (logger)
    //                LOG_TRACE(
    //                    logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func=asyncTryRemove path={}", seed, path);
    //            return future;
    //        }
    //
    //        auto callback = [&, promise](const Coordination::RemoveResponse & response) mutable
    //        {
    //            if (injectFailureAfterOp(promise))
    //            {
    //                if (logger)
    //                    LOG_TRACE(
    //                        logger,
    //                        "ZooKeeperWithFaultInjection injected fault before operation: seed={} func=asyncTryRemove path={}",
    //                        seed,
    //                        path);
    //                return;
    //            }
    //
    //            if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE
    //                && response.error != Coordination::Error::ZBADVERSION && response.error != Coordination::Error::ZNOTEMPTY)
    //            {
    //                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromPath(response.error, path)));
    //            }
    //            else
    //                promise->set_value(response);
    //        };
    //
    //        keeper->impl->remove(path, version, std::move(callback));
    //        return future;
    //    }
    //
    //    zk::FutureRemove asyncTryRemoveNoThrow(const std::string & path, int32_t version = -1)
    //    {
    //        auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    //        auto future = promise->get_future();
    //        if (!keeper || (unlikely(fault_policy) && fault_policy->beforeOperationNoThrow()))
    //        {
    //            if (logger)
    //                LOG_TRACE(
    //                    logger,
    //                    "ZooKeeperWithFaultInjection injected fault before operation: seed={} func=asyncTryRemoveNoThrow path={}",
    //                    seed,
    //                    path);
    //            Coordination::RemoveResponse r;
    //            r.error = Coordination::Error::ZSESSIONEXPIRED;
    //            promise->set_value(r);
    //            return future;
    //        }
    //
    //        auto callback = [&, promise](const Coordination::RemoveResponse & response) mutable
    //        {
    //            if (unlikely(fault_policy) && fault_policy->afterOperationNoThrow())
    //            {
    //                if (logger)
    //                    LOG_TRACE(
    //                        logger,
    //                        "ZooKeeperWithFaultInjection injected fault after operation: seed={} func=asyncTryRemoveNoThrow path={}",
    //                        seed,
    //                        path);
    //                Coordination::RemoveResponse r;
    //                r.error = Coordination::Error::ZOPERATIONTIMEOUT;
    //                promise->set_value(r);
    //            }
    //            else
    //            {
    //                promise->set_value(response);
    //            }
    //        };
    //
    //        keeper->impl->remove(path, version, std::move(callback));
    //
    //        return future;
    //    }
    //
    void removeRecursive(const std::string & path);

    void tryRemoveRecursive(const std::string & path);

    void removeChildren(const std::string & path);

    bool tryRemoveChildrenRecursive(
        const std::string & path, bool probably_flat = false, zkutil::RemoveException keep_child = zkutil::RemoveException{});

    bool waitForDisappear(const std::string & path, const zkutil::ZooKeeper::WaitCondition & condition = {});

    std::string sync(const std::string & path);

    Coordination::Error
    trySet(const std::string & path, const std::string & data, int32_t version = -1, Coordination::Stat * stat = nullptr);

    void checkExistsAndGetCreateAncestorsOps(const std::string & path, Coordination::Requests & requests);

    void deleteEphemeralNodeIfContentMatches(const std::string & path, const std::string & fast_delete_if_equal_value);

private:
    template <typename T>
    bool injectFailureBeforeOp(T & promise)
    {
        if (!keeper)
        {
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromMessage(
                Coordination::Error::ZSESSIONEXPIRED, "Session is considered to be expired due to fault injection")));
            return true;
        }

        if (unlikely(fault_policy) && fault_policy->beforeOperationNoThrow())
        {
            promise->set_exception(std::make_exception_ptr(
                zkutil::KeeperException::fromMessage(Coordination::Error::ZSESSIONEXPIRED, "Fault injection before operation")));
            return true;
        }
        return false;
    }

    template <typename T>
    bool injectFailureAfterOp(T & promise)
    {
        if (unlikely(fault_policy) && fault_policy->afterOperationNoThrow())
        {
            promise->set_exception(std::make_exception_ptr(
                zkutil::KeeperException::fromMessage(Coordination::Error::ZOPERATIONTIMEOUT, "Fault injection after operation")));
            return true;
        }

        return false;
    }
};

using ZooKeeperWithFaultInjectionPtr = ZooKeeperWithFaultInjection::Ptr;
}

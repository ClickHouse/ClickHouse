#include <base/defines.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace DB
{

ZooKeeperWithFaultInjection::ZooKeeperWithFaultInjection(
    zkutil::ZooKeeper::Ptr const & keeper_,
    double fault_injection_probability,
    UInt64 fault_injection_seed,
    std::string name_,
    LoggerPtr logger_)
    : keeper(keeper_)
    , fault_policy(std::make_unique<RandomFaultInjection>(fault_injection_probability, fault_injection_seed))
    , name(std::move(name_))
    , logger(logger_)
    , seed(fault_injection_seed)
{
}

void ZooKeeperWithFaultInjection::resetKeeper()
{
    /// When an error is injected, we need to reset keeper for several reasons
    /// a) Avoid processing further requests in this keeper (in async code)
    /// b) Simulate a fault as ZooKeeperImpl does, forcing a new session (which drops ephemeral nodes)
    ///
    /// Ideally we would call `keeper->finalize("Fault injection");` to force the session reload.
    /// The problem with that is that many operations currently aren't able to cope with keeper faults correctly,
    /// so they would fail. While this is what happens in production, it's not what we want in the CI.
    ///
    /// Until all the code can handle keeper session resets, we need to simulate it so the code that relies on its
    /// behaviour keeps working. An example of such code is insert block ids: If keeper dies between the block id being
    /// reserved (via ephemeral node) and the metadata being pushed, the reserved block id will be deleted automatically
    /// in keeper (connection drop == delete all ephemeral nodes attached to that connection). This way retrying and
    /// getting a new block id is ok. But without a connection reset (because ZooKeeperWithFaultInjection doesn't
    /// enforce it yet), the old ephemeral nodes associated with "committing_blocks" will still be there and operations
    /// such as block merges, mutations, etc., will think they are alive and wait for them to be ready (which will never
    /// happen)
    /// Our poor man session reload is to keep track of ephemeral nodes created by this Faulty keeper and delete
    /// them manually when we force a fault. This is obviously limited as it will only apply for operations processed by
    /// this instance, but let's trust more and more code can handle session reloads and we can eliminate the hack.
    /// Until that time, the hack remains.
    if (keeper)
    {
        for (const auto & path_created : session_ephemeral_nodes)
        {
            try
            {
                keeper->remove(path_created);
            }
            catch (const Coordination::Exception & e)
            {
                if (logger)
                    LOG_TRACE(logger, "Failed to delete ephemeral node ({}) during fault cleanup: {}", path_created, e.what());
            }
        }
    }

    session_ephemeral_nodes.clear();
    keeper.reset();
}

void ZooKeeperWithFaultInjection::multiResponseSaveEphemeralNodePaths(
    const Coordination::Requests & requests, const Coordination::Responses & responses)
{
    if (responses.empty())
        return;

    chassert(requests.size() == responses.size());

    for (size_t i = 0; i < requests.size(); i++)
    {
        const auto * create_req = dynamic_cast<const Coordination::CreateRequest *>(requests[i].get());
        if (create_req && create_req->is_ephemeral)
        {
            const auto * create_resp = dynamic_cast<const Coordination::CreateResponse *>(responses.at(i).get());
            chassert(create_resp);
            session_ephemeral_nodes.emplace_back(create_resp->path_created);
        }
    }
}

void ZooKeeperWithFaultInjection::injectFailureBeforeOperationThrow(const char * func_name, const String & path)
{
    if (unlikely(!keeper))
    {
        /// This is ok for async requests, where you call several of them and one introduced a fault
        /// In the faults we reset the pointer to mark the connection as failed and inject failures in any
        /// subsequent async requests
        if (logger)
            LOG_TRACE(logger, "ZooKeeperWithFaultInjection called after fault: seed={}, func={} path={}", seed, func_name, path);
        throw zkutil::KeeperException::fromMessage(RandomFaultInjection::error_before_op, RandomFaultInjection::msg_session_expired);
    }

    if (unlikely(fault_policy) && fault_policy->beforeOperation())
    {
        if (logger)
            LOG_TRACE(
                logger,
                "ZooKeeperWithFaultInjection call FAILED: seed={} func={} path={} code={} message={} ",
                seed,
                func_name,
                path,
                RandomFaultInjection::error_before_op,
                RandomFaultInjection::msg_before_op);
        resetKeeper();
        throw zkutil::KeeperException::fromMessage(RandomFaultInjection::error_before_op, RandomFaultInjection::msg_before_op);
    }
}

void ZooKeeperWithFaultInjection::injectFailureAfterOperationThrow(const char * func_name, const String & path)
{
    if (unlikely(fault_policy) && fault_policy->afterOperation())
    {
        if (logger)
            LOG_TRACE(
                logger,
                "ZooKeeperWithFaultInjection call FAILED: seed={} func={} path={} code={} message={} ",
                seed,
                func_name,
                path,
                RandomFaultInjection::error_after_op,
                RandomFaultInjection::msg_after_op);
        resetKeeper();
        throw zkutil::KeeperException::fromMessage(RandomFaultInjection::error_after_op, RandomFaultInjection::msg_after_op);
    }
}


template <typename Operation>
std::invoke_result_t<Operation>
ZooKeeperWithFaultInjection::executeWithFaultSync(const char * func_name, const std::string & path, Operation operation)
{
    injectFailureBeforeOperationThrow(func_name, path);

    if constexpr (!std::is_same_v<std::invoke_result_t<Operation>, void>)
    {
        auto res = operation();
        injectFailureAfterOperationThrow(func_name, path);
        return res;
    }
    else
    {
        operation();
        injectFailureAfterOperationThrow(func_name, path);
    }
}

template <typename Promise>
bool ZooKeeperWithFaultInjection::injectFailureBeforeOperationPromise(const char * func_name, Promise & promise, const String & path)
{
    if (unlikely(!keeper))
    {
        if (logger)
            LOG_ERROR(logger, "ZooKeeperWithFaultInjection called after fault injection: seed={}, func={} path={}", seed, func_name, path);
        promise->set_exception(std::make_exception_ptr(
            zkutil::KeeperException::fromMessage(RandomFaultInjection::error_before_op, RandomFaultInjection::msg_session_expired)));
    }

    if (unlikely(fault_policy) && fault_policy->beforeOperation())
    {
        if (logger)
            LOG_TRACE(
                logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func={} path={}", seed, func_name, path);
        resetKeeper();
        promise->set_exception(std::make_exception_ptr(
            zkutil::KeeperException::fromMessage(RandomFaultInjection::error_before_op, RandomFaultInjection::msg_before_op)));
        return true;
    }
    return false;
}

template <typename Promise>
bool ZooKeeperWithFaultInjection::injectFailureAfterOperationPromise(const char * func_name, Promise & promise, const String & path)
{
    if (unlikely(fault_policy) && fault_policy->afterOperation())
    {
        promise->set_exception(std::make_exception_ptr(
            zkutil::KeeperException::fromMessage(RandomFaultInjection::error_after_op, RandomFaultInjection::msg_after_op)));
        if (logger)
            LOG_TRACE(logger, "ZooKeeperWithFaultInjection injected fault after operation: seed={} func={} path={}", seed, func_name, path);
        resetKeeper();
        return true;
    }
    return false;
}

Strings ZooKeeperWithFaultInjection::getChildren(
    const std::string & path, Coordination::Stat * stat, const zkutil::EventPtr & watch, Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->getChildren(path, stat, watch, list_request_type); });
}

zkutil::ZooKeeper::MultiGetChildrenResponse
ZooKeeperWithFaultInjection::getChildren(const std::vector<std::string> & paths, Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        __func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->getChildren(paths, list_request_type); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryGetChildren(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    const zkutil::EventPtr & watch,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryGetChildren(path, res, stat, watch, list_request_type); });
}

zkutil::ZooKeeper::MultiTryGetChildrenResponse
ZooKeeperWithFaultInjection::tryGetChildren(const std::vector<std::string> & paths, Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        __func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->tryGetChildren(paths, list_request_type); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryGetChildrenWatch(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        __func__, path, [&]() { return keeper->tryGetChildrenWatch(path, res, stat, watch_callback, list_request_type); });
}

Strings ZooKeeperWithFaultInjection::getChildrenWatch(
    const std::string & path,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->getChildrenWatch(path, stat, watch_callback, list_request_type); });
}

Strings ZooKeeperWithFaultInjection::getChildrenWatch(
    const std::string & path,
    Coordination::Stat * stat,
    Coordination::WatchCallbackPtr watch_callback,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->getChildrenWatch(path, stat, watch_callback, list_request_type); });
}

bool ZooKeeperWithFaultInjection::tryGet(
    const std::string & path, std::string & res, Coordination::Stat * stat, const zkutil::EventPtr & watch, Coordination::Error * code)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryGet(path, res, stat, watch, code); });
}

bool ZooKeeperWithFaultInjection::tryGetWatch(
    const std::string & path,
    std::string & res,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::Error * code)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryGetWatch(path, res, stat, watch_callback, code); });
}

std::string ZooKeeperWithFaultInjection::get(const std::string & path, Coordination::Stat * stat, const zkutil::EventPtr & watch)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->get(path, stat, watch); });
}

zkutil::ZooKeeper::MultiGetResponse ZooKeeperWithFaultInjection::get(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(__func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->get(paths); });
}

zkutil::ZooKeeper::MultiTryGetResponse ZooKeeperWithFaultInjection::tryGet(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(__func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->tryGet(paths); });
}

void ZooKeeperWithFaultInjection::set(const String & path, const String & data, int32_t version, Coordination::Stat * stat)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->set(path, data, version, stat); });
}

void ZooKeeperWithFaultInjection::remove(const String & path, int32_t version)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->remove(path, version); });
}

bool ZooKeeperWithFaultInjection::exists(const std::string & path, Coordination::Stat * stat, const zkutil::EventPtr & watch)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->exists(path, stat, watch); });
}

bool ZooKeeperWithFaultInjection::anyExists(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(__func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->anyExists(paths); });
}

zkutil::ZooKeeper::MultiExistsResponse ZooKeeperWithFaultInjection::exists(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(__func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->exists(paths); });
}

std::string ZooKeeperWithFaultInjection::create(const std::string & path, const std::string & data, int32_t mode)
{
    return executeWithFaultSync(
        __func__,
        path,
        [&]()
        {
            auto path_created = keeper->create(path, data, mode);
            if (unlikely(fault_policy) && (mode == zkutil::CreateMode::EphemeralSequential || mode == zkutil::CreateMode::Ephemeral))
                session_ephemeral_nodes.emplace_back(path_created);
            return path_created;
        });
}

Coordination::Error
ZooKeeperWithFaultInjection::tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    return executeWithFaultSync(
        __func__,
        path,
        [&]()
        {
            Coordination::Error code = keeper->tryCreate(path, data, mode, path_created);
            if (unlikely(fault_policy) && code == Coordination::Error::ZOK
                && (mode == zkutil::CreateMode::EphemeralSequential || mode == zkutil::CreateMode::Ephemeral))
                session_ephemeral_nodes.emplace_back(path_created);
            return code;
        });
}

Coordination::Error ZooKeeperWithFaultInjection::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
    std::string path_created;
    return tryCreate(path, data, mode, path_created);
}

Coordination::Responses ZooKeeperWithFaultInjection::multi(const Coordination::Requests & requests, bool check_session_valid)
{
    return executeWithFaultSync(
        __func__,
        !requests.empty() ? requests.front()->getPath() : "",
        [&]()
        {
            auto responses = keeper->multi(requests, check_session_valid);
            if (unlikely(fault_policy))
                multiResponseSaveEphemeralNodePaths(requests, responses);
            return responses;
        });
}

void ZooKeeperWithFaultInjection::createIfNotExists(const std::string & path, const std::string & data)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->createIfNotExists(path, data); });
}

void ZooKeeperWithFaultInjection::createOrUpdate(const std::string & path, const std::string & data, int32_t mode)
{
    chassert(mode != zkutil::CreateMode::EphemeralSequential && mode != zkutil::CreateMode::Ephemeral);
    executeWithFaultSync(__func__, path, [&]() { keeper->createOrUpdate(path, data, mode); });
}

void ZooKeeperWithFaultInjection::createAncestors(const std::string & path)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->createAncestors(path); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryRemove(const std::string & path, int32_t version)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryRemove(path, version); });
}

void ZooKeeperWithFaultInjection::removeRecursive(const std::string & path)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->removeRecursive(path); });
}

void ZooKeeperWithFaultInjection::tryRemoveRecursive(const std::string & path)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->tryRemoveRecursive(path); });
}

void ZooKeeperWithFaultInjection::removeChildren(const std::string & path)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->removeChildren(path); });
}

bool ZooKeeperWithFaultInjection::tryRemoveChildrenRecursive(
    const std::string & path, bool probably_flat, zkutil::RemoveException keep_child)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryRemoveChildrenRecursive(path, probably_flat, keep_child); });
}

bool ZooKeeperWithFaultInjection::waitForDisappear(const std::string & path, const zkutil::ZooKeeper::WaitCondition & condition)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->waitForDisappear(path, condition); });
}

std::string ZooKeeperWithFaultInjection::sync(const std::string & path)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->sync(path); });
}

Coordination::Error
ZooKeeperWithFaultInjection::trySet(const std::string & path, const std::string & data, int32_t version, Coordination::Stat * stat)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->trySet(path, data, version, stat); });
}

void ZooKeeperWithFaultInjection::checkExistsAndGetCreateAncestorsOps(const std::string & path, Coordination::Requests & requests)
{
    executeWithFaultSync(__func__, path, [&]() { keeper->checkExistsAndGetCreateAncestorsOps(path, requests); });
}

void ZooKeeperWithFaultInjection::deleteEphemeralNodeIfContentMatches(
    const std::string & path, const std::string & fast_delete_if_equal_value)
{
    executeWithFaultSync(
        __func__, path, [&]() { keeper->deleteEphemeralNodeIfContentMatches(path, fast_delete_if_equal_value); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses, bool check_session_valid)
{
    return executeWithFaultSync(
        __func__,
        !requests.empty() ? requests.front()->getPath() : "",
        [&]()
        {
            auto code = keeper->tryMulti(requests, responses, check_session_valid);
            if (unlikely(fault_policy) && code == Coordination::Error::ZOK)
                multiResponseSaveEphemeralNodePaths(requests, responses);
            return code;
        });
}

Coordination::Error
ZooKeeperWithFaultInjection::tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses, bool check_session_valid)
{
    try
    {
        return tryMulti(requests, responses, check_session_valid);
    }
    catch (const Coordination::Exception & e)
    {
        return e.code;
    }
}

zkutil::ZooKeeper::FutureExists ZooKeeperWithFaultInjection::asyncExists(std::string path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::ExistsResponse>>();
    auto future = promise->get_future();
    if (injectFailureBeforeOperationPromise(__func__, promise, path))
        return future;

    const char * function_name = __func__;
    auto callback = [&, promise](const Coordination::ExistsResponse & response) mutable
    {
        if (injectFailureAfterOperationPromise(function_name, promise, path))
            return;

        if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromPath(response.error, path)));
        else
            promise->set_value(response);
    };

    keeper->impl->exists(
        path,
        std::move(callback),
        watch_callback ? std::make_shared<Coordination::WatchCallback>(watch_callback) : Coordination::WatchCallbackPtr{});
    return future;
}

zkutil::ZooKeeper::FutureGet ZooKeeperWithFaultInjection::asyncTryGet(std::string path)
{
    auto promise = std::make_shared<std::promise<Coordination::GetResponse>>();
    auto future = promise->get_future();
    if (injectFailureBeforeOperationPromise(__func__, promise, path))
        return future;

    const char * function_name = __func__;
    auto callback = [&, promise](const Coordination::GetResponse & response) mutable
    {
        if (injectFailureAfterOperationPromise(function_name, promise, path))
            return;

        if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromPath(response.error, path)));
        else
            promise->set_value(response);
    };

    keeper->impl->get(path, std::move(callback), {});
    return future;
}


zkutil::ZooKeeper::FutureMulti ZooKeeperWithFaultInjection::asyncTryMultiNoThrow(const Coordination::Requests & ops)
{
#ifndef NDEBUG
    /// asyncTryMultiNoThrow is not setup to handle faults with ephemeral nodes
    /// To do it we'd need to look at ops and save the indexes BEFORE the callback, as the ops are not
    /// guaranteed to live until then
    for (const auto & op : ops)
    {
        const auto * create_req = dynamic_cast<const Coordination::CreateRequest *>(op.get());
        if (create_req)
            chassert(!create_req->is_ephemeral);
    }
#endif

    auto promise = std::make_shared<std::promise<Coordination::MultiResponse>>();
    auto future = promise->get_future();
    size_t request_size = ops.size();
    String path = ops.empty() ? "" : ops.front()->getPath();

    if (!keeper || (unlikely(fault_policy) && fault_policy->beforeOperation()))
    {
        if (logger)
            LOG_TRACE(logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func={} path={}", seed, __func__, path);
        resetKeeper();
        Coordination::MultiResponse errors;
        for (size_t i = 0; i < request_size; i++)
        {
            auto r = std::make_shared<Coordination::ZooKeeperErrorResponse>();
            r->error = RandomFaultInjection::error_before_op;
            errors.responses.emplace_back(std::move(r));
        }
        promise->set_value(errors);
        return future;
    }

    const char * function_name = __func__;
    auto callback = [&, promise](const Coordination::MultiResponse & response) mutable
    {
        if (unlikely(fault_policy) && fault_policy->afterOperation())
        {
            if (logger)
                LOG_TRACE(
                    logger,
                    "ZooKeeperWithFaultInjection injected fault after operation: seed={} func={} path={}",
                    seed,
                    function_name,
                    path);
            resetKeeper();
            Coordination::MultiResponse errors;
            for (size_t i = 0; i < request_size; i++)
            {
                auto r = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                r->error = RandomFaultInjection::error_after_op;
                errors.responses.emplace_back(std::move(r));
            }
            promise->set_value(errors);
        }
        else
        {
            promise->set_value(response);
        }
    };

    keeper->impl->multi(ops, std::move(callback));
    return future;
}

/// Needs to match ZooKeeper::asyncTryRemove implementation
zkutil::ZooKeeper::FutureRemove ZooKeeperWithFaultInjection::asyncTryRemove(std::string path, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    auto future = promise->get_future();
    if (injectFailureBeforeOperationPromise(__func__, promise, path))
        return future;

    const char * function_name = __func__;
    auto callback = [&, promise](const Coordination::RemoveResponse & response) mutable
    {
        if (injectFailureAfterOperationPromise(function_name, promise, path))
            return;

        if (response.error != Coordination::Error::ZOK && response.error != Coordination::Error::ZNONODE
            && response.error != Coordination::Error::ZBADVERSION && response.error != Coordination::Error::ZNOTEMPTY)
        {
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromPath(response.error, path)));
        }
        else
            promise->set_value(response);
    };

    keeper->impl->remove(path, version, std::move(callback));
    return future;
}

zkutil::ZooKeeper::FutureRemove ZooKeeperWithFaultInjection::asyncTryRemoveNoThrow(const std::string & path, int32_t version)
{
    auto promise = std::make_shared<std::promise<Coordination::RemoveResponse>>();
    auto future = promise->get_future();

    if (!keeper || (unlikely(fault_policy) && fault_policy->beforeOperation()))
    {
        if (logger)
            LOG_TRACE(logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func={} path={}", seed, __func__, path);
        resetKeeper();
        Coordination::RemoveResponse r;
        r.error = RandomFaultInjection::error_before_op;
        promise->set_value(r);
        return future;
    }

    const char * function_name = __func__;
    auto callback = [&, promise](const Coordination::RemoveResponse & response) mutable
    {
        if (unlikely(fault_policy) && fault_policy->afterOperation())
        {
            if (logger)
                LOG_TRACE(
                    logger,
                    "ZooKeeperWithFaultInjection injected fault after operation: seed={} func={} path={}",
                    seed,
                    function_name,
                    path);
            resetKeeper();
            Coordination::RemoveResponse r;
            r.error = RandomFaultInjection::error_after_op;
            promise->set_value(r);
        }
        else
        {
            promise->set_value(response);
        }
    };

    keeper->impl->remove(path, version, std::move(callback));

    return future;
}
}

#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace DB
{

ZooKeeperWithFaultInjection::ZooKeeperWithFaultInjection(
    zkutil::ZooKeeper::Ptr const & keeper_,
    double fault_injection_probability,
    UInt64 fault_injection_seed,
    std::string name_,
    Poco::Logger * logger_)
    : keeper(keeper_)
    , fault_policy(std::make_unique<RandomFaultInjection>(fault_injection_probability, fault_injection_seed))
    , name(std::move(name_))
    , logger(logger_)
    , seed(fault_injection_seed)
{
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
        keeper.reset();
        if (logger)
            LOG_TRACE(
                logger,
                "ZooKeeperWithFaultInjection call FAILED: seed={} func={} path={} code={} message={} ",
                seed,
                func_name,
                path,
                RandomFaultInjection::error_before_op,
                RandomFaultInjection::msg_before_op);
        throw zkutil::KeeperException::fromMessage(RandomFaultInjection::error_before_op, RandomFaultInjection::msg_before_op);
    }
}

void ZooKeeperWithFaultInjection::injectFailureAfterOperationThrow(const char * func_name, const String & path)
{
    if (unlikely(fault_policy) && fault_policy->beforeOperation())
    {
        keeper.reset();
        if (logger)
            LOG_TRACE(
                logger,
                "ZooKeeperWithFaultInjection call FAILED: seed={} func={} path={} code={} message={} ",
                seed,
                func_name,
                path,
                RandomFaultInjection::error_after_op,
                RandomFaultInjection::msg_after_op);
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
        keeper.reset();
        if (logger)
            LOG_TRACE(
                logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func={} path={}", seed, func_name, path);
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
        keeper.reset();
        promise->set_exception(std::make_exception_ptr(
            zkutil::KeeperException::fromMessage(RandomFaultInjection::error_after_op, RandomFaultInjection::msg_after_op)));
        if (logger)
            LOG_TRACE(logger, "ZooKeeperWithFaultInjection injected fault after operation: seed={} func={} path={}", seed, func_name, path);
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
    return executeWithFaultSync(__func__, path, [&]() { return keeper->set(path, data, version, stat); });
}

void ZooKeeperWithFaultInjection::remove(const String & path, int32_t version)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->remove(path, version); });
}

bool ZooKeeperWithFaultInjection::exists(const std::string & path, Coordination::Stat * stat, const zkutil::EventPtr & watch)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->exists(path, stat, watch); });
}

zkutil::ZooKeeper::MultiExistsResponse ZooKeeperWithFaultInjection::exists(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(__func__, paths.empty() ? paths.front() : "", [&]() { return keeper->exists(paths); });
}

std::string ZooKeeperWithFaultInjection::create(const std::string & path, const std::string & data, int32_t mode)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->create(path, data, mode); });
}

Coordination::Error
ZooKeeperWithFaultInjection::tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryCreate(path, data, mode, path_created); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryCreate(path, data, mode); });
}

Coordination::Responses ZooKeeperWithFaultInjection::multi(const Coordination::Requests & requests)
{
    return executeWithFaultSync(__func__, !requests.empty() ? requests.front()->getPath() : "", [&]() { return keeper->multi(requests); });
}

void ZooKeeperWithFaultInjection::createIfNotExists(const std::string & path, const std::string & data)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->createIfNotExists(path, data); });
}

void ZooKeeperWithFaultInjection::createOrUpdate(const std::string & path, const std::string & data, int32_t mode)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->createOrUpdate(path, data, mode); });
}

void ZooKeeperWithFaultInjection::createAncestors(const std::string & path)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->createAncestors(path); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryRemove(const std::string & path, int32_t version)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryRemove(path, version); });
}

void ZooKeeperWithFaultInjection::removeRecursive(const std::string & path)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->removeRecursive(path); });
}

void ZooKeeperWithFaultInjection::tryRemoveRecursive(const std::string & path)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->tryRemoveRecursive(path); });
}

void ZooKeeperWithFaultInjection::removeChildren(const std::string & path)
{
    return executeWithFaultSync(__func__, path, [&]() { return keeper->removeChildren(path); });
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
    return executeWithFaultSync(__func__, path, [&]() { return keeper->checkExistsAndGetCreateAncestorsOps(path, requests); });
}

void ZooKeeperWithFaultInjection::deleteEphemeralNodeIfContentMatches(
    const std::string & path, const std::string & fast_delete_if_equal_value)
{
    return executeWithFaultSync(
        __func__, path, [&]() { return keeper->deleteEphemeralNodeIfContentMatches(path, fast_delete_if_equal_value); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses)
{
    return executeWithFaultSync(
        __func__, !requests.empty() ? requests.front()->getPath() : "", [&]() { return keeper->tryMulti(requests, responses); });
}

Coordination::Error
ZooKeeperWithFaultInjection::tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses)
{
    try
    {
        return tryMulti(requests, responses);
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

    auto callback = [&, promise](const Coordination::ExistsResponse & response) mutable
    {
        if (injectFailureAfterOperationPromise(__func__, promise, path))
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

    auto callback = [&, promise](const Coordination::GetResponse & response) mutable
    {
        if (injectFailureAfterOperationPromise(__func__, promise, path))
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
    auto promise = std::make_shared<std::promise<Coordination::MultiResponse>>();
    auto future = promise->get_future();
    size_t request_size = ops.size();
    String path = ops.empty() ? "" : ops.front()->getPath();

    if (!keeper || (unlikely(fault_policy) && fault_policy->beforeOperation()))
    {
        keeper.reset();
        if (logger)
            LOG_TRACE(logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func={} path={}", seed, __func__, path);
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

    auto callback = [&, promise](const Coordination::MultiResponse & response) mutable
    {
        if (unlikely(fault_policy) && fault_policy->afterOperation())
        {
            keeper.reset();
            if (logger)
                LOG_TRACE(
                    logger, "ZooKeeperWithFaultInjection injected fault after operation: seed={} func={} path={}", seed, __func__, path);
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

    auto callback = [&, promise](const Coordination::RemoveResponse & response) mutable
    {
        if (injectFailureAfterOperationPromise(__func__, promise, path))
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
        keeper.reset();
        if (logger)
            LOG_TRACE(logger, "ZooKeeperWithFaultInjection injected fault before operation: seed={} func={} path={}", seed, __func__, path);
        Coordination::RemoveResponse r;
        r.error = RandomFaultInjection::error_before_op;
        promise->set_value(r);
        return future;
    }

    auto callback = [&, promise](const Coordination::RemoveResponse & response) mutable
    {
        if (unlikely(fault_policy) && fault_policy->afterOperation())
        {
            keeper.reset();
            if (logger)
                LOG_TRACE(
                    logger, "ZooKeeperWithFaultInjection injected fault after operation: seed={} func={} path={}", seed, __func__, path);
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

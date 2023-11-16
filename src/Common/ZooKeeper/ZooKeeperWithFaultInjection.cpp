#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

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

ZooKeeperWithFaultInjection::Ptr createInstance(
    double fault_injection_probability,
    UInt64 fault_injection_seed,
    const zkutil::ZooKeeper::Ptr & zookeeper,
    std::string name,
    Poco::Logger * logger)
{
    /// validate all parameters here, constructor just accept everything
    if (fault_injection_probability < 0.0)
        fault_injection_probability = .0;
    else if (fault_injection_probability > 1.0)
        fault_injection_probability = 1.0;

    if (0 == fault_injection_seed)
        fault_injection_seed = randomSeed();

    if (fault_injection_probability > 0.0)
        return std::make_shared<ZooKeeperWithFaultInjection>(
            zookeeper, fault_injection_probability, fault_injection_seed, std::move(name), logger);

    /// if no fault injection provided, create instance which will not log anything
    return std::make_shared<ZooKeeperWithFaultInjection>(zookeeper);
}

template <
    bool no_throw_access = false,
    bool inject_failure_before_op = true,
    bool inject_failure_after_op = true,
    typename Operation,
    typename Result = std::invoke_result_t<Operation>>
Result executeWithFaultSync(ZooKeeperWithFaultInjection & zk, const char * func_name, const std::string & path, Operation operation)
{
    static_assert(std::is_same<Result, std::invoke_result_t<Operation>>::value);
    try
    {
        if (!zk.keeper)
            throw zkutil::KeeperException::fromMessage(
                Coordination::Error::ZSESSIONEXPIRED, "Session is considered to be expired due to fault injection");

        if constexpr (inject_failure_before_op)
            if (unlikely(zk.fault_policy))
                zk.fault_policy->beforeOperation();

        if constexpr (!std::is_same_v<Result, void>)
        {
            Result res = operation();
            if constexpr (inject_failure_after_op)
                if (unlikely(zk.fault_policy))
                    zk.fault_policy->afterOperation();
            return res;
        }
        else
        {
            operation();
            if constexpr (inject_failure_after_op)
                if (unlikely(zk.fault_policy))
                    zk.fault_policy->afterOperation();
        }
    }
    catch (const zkutil::KeeperException & e)
    {
        if (unlikely(zk.logger))
            LOG_TRACE(
                zk.logger,
                "ZooKeeperWithFaultInjection call FAILED: seed={} func={} path={} code={} message={} ",
                zk.seed,
                func_name,
                path,
                e.code,
                e.message());

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

Strings ZooKeeperWithFaultInjection::getChildren(
    const std::string & path, Coordination::Stat * stat, const zkutil::EventPtr & watch, Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->getChildren(path, stat, watch, list_request_type); });
}

zkutil::ZooKeeper::MultiGetChildrenResponse
ZooKeeperWithFaultInjection::getChildren(const std::vector<std::string> & paths, Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        *this, __func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->getChildren(paths, list_request_type); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryGetChildren(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    const zkutil::EventPtr & watch,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->tryGetChildren(path, res, stat, watch, list_request_type); });
}

zkutil::ZooKeeper::MultiTryGetChildrenResponse
ZooKeeperWithFaultInjection::tryGetChildren(const std::vector<std::string> & paths, Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        *this, __func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->tryGetChildren(paths, list_request_type); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryGetChildrenWatch(
    const std::string & path,
    Strings & res,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        *this, __func__, path, [&]() { return keeper->tryGetChildrenWatch(path, res, stat, watch_callback, list_request_type); });
}

Strings ZooKeeperWithFaultInjection::getChildrenWatch(
    const std::string & path,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        *this, __func__, path, [&]() { return keeper->getChildrenWatch(path, stat, watch_callback, list_request_type); });
}

Strings ZooKeeperWithFaultInjection::getChildrenWatch(
    const std::string & path,
    Coordination::Stat * stat,
    Coordination::WatchCallbackPtr watch_callback,
    Coordination::ListRequestType list_request_type)
{
    return executeWithFaultSync(
        *this, __func__, path, [&]() { return keeper->getChildrenWatch(path, stat, watch_callback, list_request_type); });
}

bool ZooKeeperWithFaultInjection::tryGet(
    const std::string & path, std::string & res, Coordination::Stat * stat, const zkutil::EventPtr & watch, Coordination::Error * code)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->tryGet(path, res, stat, watch, code); });
}

bool ZooKeeperWithFaultInjection::tryGetWatch(
    const std::string & path,
    std::string & res,
    Coordination::Stat * stat,
    Coordination::WatchCallback watch_callback,
    Coordination::Error * code)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->tryGetWatch(path, res, stat, watch_callback, code); });
}

std::string ZooKeeperWithFaultInjection::get(const std::string & path, Coordination::Stat * stat, const zkutil::EventPtr & watch)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->get(path, stat, watch); });
}

zkutil::ZooKeeper::MultiGetResponse ZooKeeperWithFaultInjection::get(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(*this, __func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->get(paths); });
}

zkutil::ZooKeeper::MultiTryGetResponse ZooKeeperWithFaultInjection::tryGet(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(*this, __func__, !paths.empty() ? paths.front() : "", [&]() { return keeper->tryGet(paths); });
}

void ZooKeeperWithFaultInjection::set(const String & path, const String & data, int32_t version, Coordination::Stat * stat)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->set(path, data, version, stat); });
}

void ZooKeeperWithFaultInjection::remove(const String & path, int32_t version)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->remove(path, version); });
}

bool ZooKeeperWithFaultInjection::exists(const std::string & path, Coordination::Stat * stat, const zkutil::EventPtr & watch)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->exists(path, stat, watch); });
}

zkutil::ZooKeeper::MultiExistsResponse ZooKeeperWithFaultInjection::exists(const std::vector<std::string> & paths)
{
    return executeWithFaultSync(*this, __func__, paths.empty() ? paths.front() : "", [&]() { return keeper->exists(paths); });
}

std::string ZooKeeperWithFaultInjection::create(const std::string & path, const std::string & data, int32_t mode)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->create(path, data, mode); });
}

Coordination::Error
ZooKeeperWithFaultInjection::tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->tryCreate(path, data, mode, path_created); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->tryCreate(path, data, mode); });
}

Coordination::Responses ZooKeeperWithFaultInjection::multi(const Coordination::Requests & requests)
{
    return executeWithFaultSync(
        *this, __func__, !requests.empty() ? requests.front()->getPath() : "", [&]() { return keeper->multi(requests); });
}

void ZooKeeperWithFaultInjection::createOrUpdate(const std::string & path, const std::string & data, int32_t mode)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->createOrUpdate(path, data, mode); });
}

void ZooKeeperWithFaultInjection::createAncestors(const std::string & path)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->createAncestors(path); });
}

Coordination::Error ZooKeeperWithFaultInjection::tryRemove(const std::string & path, int32_t version)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->tryRemove(path, version); });
}

void ZooKeeperWithFaultInjection::removeRecursive(const std::string & path)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->removeRecursive(path); });
}

void ZooKeeperWithFaultInjection::tryRemoveRecursive(const std::string & path)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->tryRemoveRecursive(path); });
}

void ZooKeeperWithFaultInjection::removeChildren(const std::string & path)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->removeChildren(path); });
}

bool ZooKeeperWithFaultInjection::tryRemoveChildrenRecursive(
    const std::string & path, bool probably_flat, zkutil::RemoveException keep_child)
{
    return executeWithFaultSync(
        *this, __func__, path, [&]() { return keeper->tryRemoveChildrenRecursive(path, probably_flat, keep_child); });
}

bool ZooKeeperWithFaultInjection::waitForDisappear(const std::string & path, const zkutil::ZooKeeper::WaitCondition & condition)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->waitForDisappear(path, condition); });
}

std::string ZooKeeperWithFaultInjection::sync(const std::string & path)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->sync(path); });
}

Coordination::Error
ZooKeeperWithFaultInjection::trySet(const std::string & path, const std::string & data, int32_t version, Coordination::Stat * stat)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->trySet(path, data, version, stat); });
}

void ZooKeeperWithFaultInjection::checkExistsAndGetCreateAncestorsOps(const std::string & path, Coordination::Requests & requests)
{
    return executeWithFaultSync(*this, __func__, path, [&]() { return keeper->checkExistsAndGetCreateAncestorsOps(path, requests); });
}

void ZooKeeperWithFaultInjection::deleteEphemeralNodeIfContentMatches(
    const std::string & path, const std::string & fast_delete_if_equal_value)
{
    return executeWithFaultSync(
        *this, __func__, path, [&]() { return keeper->deleteEphemeralNodeIfContentMatches(path, fast_delete_if_equal_value); });
}


}

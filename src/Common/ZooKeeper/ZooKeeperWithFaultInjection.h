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

    static constexpr auto msg_session_expired = "Called after fault injection";
    static constexpr auto error_before_op = Coordination::Error::ZSESSIONEXPIRED;
    static constexpr auto msg_before_op = "Fault injection before operation";
    static constexpr auto error_after_op = Coordination::Error::ZOPERATIONTIMEOUT;
    static constexpr auto msg_after_op = "Fault injection after operation";

    RandomFaultInjection(double probability, UInt64 seed_) : rndgen(seed_), distribution(probability) { }


    bool beforeOperation()
    {
        if (must_fail_before_op || distribution(rndgen))
        {
            must_fail_before_op = false;
            return true;
        }
        return false;
    }

    bool afterOperation()
    {
        if (must_fail_after_op || distribution(rndgen))
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

///
/// ZooKeeperWithFaultInjection mimics ZooKeeper interface and inject failures according to failure policy if set
///
class ZooKeeperWithFaultInjection
{
    zkutil::ZooKeeper::Ptr keeper;

    std::unique_ptr<RandomFaultInjection> fault_policy;
    std::string name;
    LoggerPtr logger = nullptr;
    const UInt64 seed = 0;

    std::vector<std::string> session_ephemeral_nodes;

    template <typename Operation>
    std::invoke_result_t<Operation> executeWithFaultSync(const char * func_name, const std::string & path, Operation);
    void injectFailureBeforeOperationThrow(const char * func_name, const String & path);
    void injectFailureAfterOperationThrow(const char * func_name, const String & path);
    template <typename Promise>
    bool injectFailureBeforeOperationPromise(const char * func_name, Promise & promise, const String & path);
    template <typename Promise>
    bool injectFailureAfterOperationPromise(const char * func_name, Promise & promise, const String & path);

    void resetKeeper();
    void multiResponseSaveEphemeralNodePaths(const Coordination::Requests & requests, const Coordination::Responses & responses);

public:
    using Ptr = std::shared_ptr<ZooKeeperWithFaultInjection>;

    ZooKeeperWithFaultInjection(
        zkutil::ZooKeeper::Ptr const & keeper_,
        double fault_injection_probability,
        UInt64 fault_injection_seed,
        std::string name_,
        LoggerPtr logger_);

    explicit ZooKeeperWithFaultInjection(zkutil::ZooKeeper::Ptr const & keeper_) : keeper(keeper_) { }
    static ZooKeeperWithFaultInjection::Ptr createInstance(
        double fault_injection_probability,
        UInt64 fault_injection_seed,
        zkutil::ZooKeeper::Ptr const & zookeeper,
        std::string name,
        LoggerPtr logger)
    {
        /// validate all parameters here, constructor just accept everything
        if (fault_injection_probability < 0.0)
            fault_injection_probability = .0;
        else if (fault_injection_probability > 1.0)
            fault_injection_probability = 1.0;

        if (fault_injection_seed == 0)
            fault_injection_seed = randomSeed();

        if (fault_injection_probability > 0.0)
            return std::make_shared<ZooKeeperWithFaultInjection>(
                zookeeper, fault_injection_probability, fault_injection_seed, std::move(name), logger);

        /// if no fault injection provided, create instance which will not log anything
        return std::make_shared<ZooKeeperWithFaultInjection>(zookeeper);
    }

    void setKeeper(zkutil::ZooKeeper::Ptr const & keeper_) { keeper = keeper_; }
    zkutil::ZooKeeper::Ptr getKeeper() const { return keeper; }
    bool isNull() const { return keeper.get() == nullptr; }
    bool expired() const { return !keeper || keeper->expired(); }
    bool isFeatureEnabled(KeeperFeatureFlag feature_flag) const { return keeper->isFeatureEnabled(feature_flag); }

    void forceFailureBeforeOperation()
    {
        if (!fault_policy)
            fault_policy = std::make_unique<RandomFaultInjection>(0, 0);
        fault_policy->must_fail_before_op = true;
    }

    void forceFailureAfterOperation()
    {
        if (!fault_policy)
            fault_policy = std::make_unique<RandomFaultInjection>(0, 0);
        fault_policy->must_fail_after_op = true;
    }

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

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr);

    zkutil::ZooKeeper::MultiGetResponse get(const std::vector<std::string> & paths);

    zkutil::ZooKeeper::MultiTryGetResponse tryGet(const std::vector<std::string> & paths);

    void set(const String & path, const String & data, int32_t version = -1, Coordination::Stat * stat = nullptr);

    void remove(const String & path, int32_t version = -1);

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const zkutil::EventPtr & watch = nullptr);

    zkutil::ZooKeeper::MultiExistsResponse exists(const std::vector<std::string> & paths);

    bool anyExists(const std::vector<std::string> & paths);

    std::string create(const std::string & path, const std::string & data, int32_t mode);

    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created);

    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode);

    Coordination::Responses multi(const Coordination::Requests & requests, bool check_session_valid = false);

    void createIfNotExists(const std::string & path, const std::string & data);

    void createOrUpdate(const std::string & path, const std::string & data, int32_t mode);

    void createAncestors(const std::string & path);

    Coordination::Error tryRemove(const std::string & path, int32_t version = -1);

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

    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses, bool check_session_valid = false);

    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses, bool check_session_valid = false);

    ///
    /// mirror ZooKeeper interface: Async functions
    /// Note that there is not guarantees that the parameters will live until the internal callback is called
    /// so we might need to copy them
    ///

    zkutil::ZooKeeper::FutureExists asyncExists(std::string path, Coordination::WatchCallback watch_callback = {});

    zkutil::ZooKeeper::FutureGet asyncTryGet(std::string path);

    zkutil::ZooKeeper::FutureMulti asyncTryMultiNoThrow(const Coordination::Requests & ops);

    zkutil::ZooKeeper::FutureRemove asyncTryRemove(std::string path, int32_t version = -1);

    zkutil::ZooKeeper::FutureRemove asyncTryRemoveNoThrow(const std::string & path, int32_t version = -1);
};

using ZooKeeperWithFaultInjectionPtr = ZooKeeperWithFaultInjection::Ptr;
}

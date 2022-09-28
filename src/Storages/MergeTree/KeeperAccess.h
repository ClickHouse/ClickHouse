#pragma once
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

class FaultInjectionPolicy
{
public:
    virtual ~FaultInjectionPolicy() = default;
    virtual void beforeOperation() = 0;
    virtual void afterOperation() = 0;
};

class NoOpPolicy : public FaultInjectionPolicy
{
public:
    ~NoOpPolicy() override = default;
    void beforeOperation() override { }
    void afterOperation() override { }
};

class KeeperAccess
{
    using zk = zkutil::ZooKeeper;
    
    zk::Ptr keeper;
    std::unique_ptr<FaultInjectionPolicy> fault_policy;

public:
    explicit KeeperAccess(zk::Ptr const & keeper_)
        : keeper(keeper_), fault_policy(std::make_unique<NoOpPolicy>())
    {
    }

    explicit KeeperAccess(zk::Ptr const & keeper_, std::unique_ptr<FaultInjectionPolicy> fault_policy_)
        : keeper(keeper_), fault_policy(std::move(fault_policy_))
    {
    }

    /// todo: remove getKeeper(), it's a temporary measure
    zk::Ptr getKeeper() const { return keeper; }
    void setKeeper(zk::Ptr const & keeper_) { keeper = keeper_; }

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
    Result access(auto && operation) const
    {
        fault_policy->beforeOperation();
        Result res = operation();
        fault_policy->afterOperation();
        return res;
    }
};

using KeeperAccessPtr = std::shared_ptr<KeeperAccess>;
}

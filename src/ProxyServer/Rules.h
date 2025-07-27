#pragma once

#include <vector>

#include <ProxyServer/ConnectionsCounter.h>
#include <ProxyServer/ILoadBalancer.h>

namespace Proxy
{

enum class LoadBalancingPolicy
{
    RoundRobin,
    LeastConnections,
};

constexpr LoadBalancingPolicy defaultLoadBalancingPolicy = LoadBalancingPolicy::RoundRobin;

LoadBalancingPolicy parseLoadBalancingPolicy(const std::string & policy_str);

enum class RuleActionType
{
    Route,
    Reject
};

struct RuleAction
{
    RuleActionType type = RuleActionType::Route;
    std::vector<std::string> target_servers;
};

struct DefaultRule
{
    RuleAction action;
    LoadBalancingPolicy policy;
    std::unique_ptr<ILoadBalancer> load_balancer;
    std::shared_ptr<ConnectionsCounter> connections_counter;

    DefaultRule() = default;
    DefaultRule(DefaultRule &&) = default;
    DefaultRule & operator=(DefaultRule &&) = default;

    virtual ~DefaultRule() = default;
};

struct FilterRule : public DefaultRule
{
    std::string database;
    std::string host;
    std::string user;

    FilterRule() = default;
    FilterRule(FilterRule &&) = default;
    FilterRule & operator=(FilterRule &&) = default;

    ~FilterRule() override = default;
};

}

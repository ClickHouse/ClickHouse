#pragma once

#include <vector>

#include <ProxyServer/ILoadBalancer.h>
#include <ProxyServer/ConnectionsManager.h>

namespace Proxy
{

enum class LoadBalancingPolicy
{
    RoundRobin,
    ActiveConnections,
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
    std::vector<std::string> route_to_servers;
};

struct DefaultRule
{
    RuleAction action;
    LoadBalancingPolicy policy;
    std::unique_ptr<ILoadBalancer> load_balancer;
    ActiveConnectionsManager connections_manager;

    DefaultRule() = default;
    DefaultRule(DefaultRule&&) = default;
    DefaultRule& operator=(DefaultRule&&) = default;

    virtual ~DefaultRule() = default;
};

struct FilterRule : public DefaultRule
{
    std::string database;
    std::string host;
    std::string user;

    FilterRule() = default;
    FilterRule(FilterRule&&) = default;
    FilterRule& operator=(FilterRule&&) = default;

    ~FilterRule() override = default;
};

}

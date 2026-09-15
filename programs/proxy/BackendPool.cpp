#include <BackendPool.h>

namespace DB::Proxy
{

BackendPool::BackendPool(const PoolConfig & config, const StickinessConfig & global_stickiness)
    : pool_name(config.name)
    , strategy(ILoadBalancingStrategy::create(config.load_balancing))
    , stickiness(config.stickiness.value_or(global_stickiness))
{
    for (const auto & backend_config : config.backends)
        all_backends.push_back(std::make_shared<Backend>(backend_config));
}

BackendPool::BackendPool(String name_, BackendPtr backend, const StickinessConfig & global_stickiness)
    : pool_name(std::move(name_))
    , strategy(ILoadBalancingStrategy::create("random"))
    , stickiness(global_stickiness)
{
    all_backends.push_back(std::move(backend));
}

BackendPtr BackendPool::choose(const RouteAttributes & attributes) const
{
    std::vector<BackendPtr> alive;
    alive.reserve(all_backends.size());
    for (const auto & backend : all_backends)
        if (backend->isAlive())
            alive.push_back(backend);

    if (alive.empty())
        return nullptr;

    if (alive.size() == 1)
        return alive.front();

    if (stickiness.by_session_id && !attributes.session_id.empty())
        return chooseByConsistentHash(alive, "session:" + attributes.session_id);

    if (stickiness.by_peer_address && !attributes.peer_address.empty())
        return chooseByConsistentHash(alive, "peer:" + attributes.peer_address);

    return strategy->choose(alive);
}

bool BackendPool::hasAliveBackends() const
{
    for (const auto & backend : all_backends)
        if (backend->isAlive())
            return true;
    return false;
}

}

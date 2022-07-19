#include "TinyContext.h"

#include <Common/Exception.h>
#include <Coordination/KeeperDispatcher.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void TinyContext::setConfig(const ConfigurationPtr & config_)
{
    std::lock_guard lock(keeper_dispatcher_mutex);
    config = config_;
}

const Poco::Util::AbstractConfiguration & TinyContext::getConfigRef() const
{
    std::lock_guard lock(keeper_dispatcher_mutex);
    return config ? *config : Poco::Util::Application::instance().config();
}


void TinyContext::initializeKeeperDispatcher([[maybe_unused]] bool start_async) const
{
    const auto & config_ref = getConfigRef();

    std::lock_guard lock(keeper_dispatcher_mutex);

    if (keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize Keeper multiple times");

    if (config_ref.has("keeper_server"))
    {
        keeper_dispatcher = std::make_shared<KeeperDispatcher>();
        keeper_dispatcher->initialize(config_ref, true, start_async);
    }
}

std::shared_ptr<KeeperDispatcher> TinyContext::getKeeperDispatcher() const
{
    std::lock_guard lock(keeper_dispatcher_mutex);
    if (!keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Keeper must be initialized before requests");

    return keeper_dispatcher;
}

std::shared_ptr<KeeperDispatcher> TinyContext::tryGetKeeperDispatcher() const
{
    std::lock_guard lock(keeper_dispatcher_mutex);
    return keeper_dispatcher;
}

void TinyContext::shutdownKeeperDispatcher() const
{
    std::lock_guard lock(keeper_dispatcher_mutex);
    if (keeper_dispatcher)
    {
        keeper_dispatcher->shutdown();
        keeper_dispatcher.reset();
    }
}

void TinyContext::updateKeeperConfiguration([[maybe_unused]] const Poco::Util::AbstractConfiguration & config_)
{
    std::lock_guard lock(keeper_dispatcher_mutex);
    if (!keeper_dispatcher)
        return;

    keeper_dispatcher->updateConfiguration(config_);
}

}

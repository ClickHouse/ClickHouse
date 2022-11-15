#pragma once
#include <memory>
#include <mutex>

#include <Poco/Util/Application.h>
#include <base/defines.h>

namespace DB
{

class KeeperDispatcher;

class TinyContext: public std::enable_shared_from_this<TinyContext>
{
public:
    std::shared_ptr<KeeperDispatcher> getKeeperDispatcher() const;
    std::shared_ptr<KeeperDispatcher> tryGetKeeperDispatcher() const;
    void initializeKeeperDispatcher(bool start_async) const;
    void shutdownKeeperDispatcher() const;
    void updateKeeperConfiguration(const Poco::Util::AbstractConfiguration & config);

    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

private:
    mutable std::mutex keeper_dispatcher_mutex;
    mutable std::shared_ptr<KeeperDispatcher> keeper_dispatcher TSA_GUARDED_BY(keeper_dispatcher_mutex);

    ConfigurationPtr config TSA_GUARDED_BY(keeper_dispatcher_mutex);
};

}

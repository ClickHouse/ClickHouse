#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoader.h>
#include <Functions/UserDefined/UserDefinedDriver.h>

namespace DB
{

class IExternalLoaderConfigRepository;

/// Manages external user-defined drivers.
class ExternalUserDefinedDriversLoader : public ExternalLoader, WithContext
{
public:
    /// External user-defined drivers will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    explicit ExternalUserDefinedDriversLoader(ContextPtr global_context_);

    UserDefinedDriverPtr getUserDefinedDriver(const std::string & user_defined_driver_name) const;

    UserDefinedDriverPtr tryGetUserDefinedDriver(const std::string & user_defined_driver_name) const;

    void reloadDriver(const std::string & user_defined_driver_name) const;

protected:
    LoadableMutablePtr createObject(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & key_in_config,
        const std::string & repository_name) const override;
};

}

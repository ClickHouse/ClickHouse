#pragma once

#include <Functions/UserDefined/UserDefinedExecutableFunctionDriver.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

/** Process-wide registry of drivers for executable user-defined functions.
  *
  * Drivers are loaded from XML configuration files referenced by the server setting
  * `<user_defined_executable_function_drivers_config>`. The registry is populated by
  * `loadDriversFromConfig`, called by the server during initialization and on config reload.
  */
class UserDefinedExecutableFunctionDriverRegistry
{
public:
    static UserDefinedExecutableFunctionDriverRegistry & instance();

    UserDefinedExecutableFunctionDriverPtr tryGet(const String & driver_name) const;

    /// Throws if the driver is not present.
    UserDefinedExecutableFunctionDriverPtr get(const String & driver_name) const;

    std::vector<String> getAllRegisteredNames() const;

    /// Replaces all currently loaded drivers with definitions parsed from the passed configurations.
    /// Each AbstractConfiguration entry corresponds to one XML/YAML file with the format described
    /// in `UserDefinedExecutableFunctionDriver.h`.
    void loadDriversFromConfigs(const std::vector<Poco::AutoPtr<Poco::Util::AbstractConfiguration>> & configs);

private:
    UserDefinedExecutableFunctionDriverRegistry() = default;

    mutable std::mutex mutex;
    std::unordered_map<String, UserDefinedExecutableFunctionDriverPtr> drivers;
};

}

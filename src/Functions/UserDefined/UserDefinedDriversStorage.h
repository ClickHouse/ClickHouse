#pragma once

#include <Functions/UserDefined/IUserDefinedDriversStorage.h>

#include <unordered_map>
#include <mutex>


namespace DB
{

class UserDefinedDriversStorage : public IUserDefinedDriversStorage
{
public:
    UserDefinedDriversStorage();

    DriverConfigurationPtr get(const String & driver_name) const override;

    DriverConfigurationPtr tryGet(const String & driver_name) const override;

    bool has(const String & driver_name) const override;

    std::vector<String> getAllDriverNames() const override;

    std::vector<std::pair<String, DriverConfigurationPtr>> getAllDrivers() const override;

    bool empty() const override;

protected:
    std::unordered_map<String, DriverConfigurationPtr> driver_name_to_configuration_map;
    mutable std::recursive_mutex mutex;
};

}

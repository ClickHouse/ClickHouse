#pragma once

#include <Functions/UserDefined/UserDefinedDriverConfiguration.h>

#include <vector>

namespace DB
{

/// Interface for a storage of drivers for driver UDF.
/// User cannot add, remove or update existing drivers.
class IUserDefinedDriversStorage
{
public:
    virtual ~IUserDefinedDriversStorage() = default;

    /// Get driver by name. If no driver stored with driver_name throws exception.
    virtual DriverConfigurationPtr get(const String & driver_name) const = 0;

    /// Get driver by name. If no driver stored with driver_name return nullptr.
    virtual DriverConfigurationPtr tryGet(const String & driver_name) const = 0;

    /// Check if driver with driver_name is stored.
    virtual bool has(const String & driver_name) const = 0;

    /// Get all driver names.
    virtual std::vector<String> getAllDriverNames() const = 0;

    /// Get all drivers.
    virtual std::vector<std::pair<String, DriverConfigurationPtr>> getAllDrivers() const = 0;

    /// Check whether any driver have been stored by loadDrivers.
    virtual bool empty() const = 0;
};

}

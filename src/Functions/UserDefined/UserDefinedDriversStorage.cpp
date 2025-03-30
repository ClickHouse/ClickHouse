#include <Functions/UserDefined/UserDefinedDriversStorage.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_DRIVER;
}

void UserDefinedDriversStorage::loadDrivers()
{
    std::lock_guard lock(mutex);

    driver_name_to_configuration_map["Python3"] = std::make_shared<DriverConfiguration>(
        DriverConfiguration("Python3")
            .setCommand("python3 -c")
            .setFormat("TabSeparated")
    );

    driver_name_to_configuration_map["DockerPython3"] = std::make_shared<DriverConfiguration>(
        DriverConfiguration("DockerPython3")
            .setCommand("python3 -c")
            .setContainer("docker run --rm -i python:3 /bin/bash -c")
            .setFormat("TabSeparated")
    );

    driver_name_to_configuration_map["Python3_JSON"] = std::make_shared<DriverConfiguration>(
        DriverConfiguration("Python3_JSON")
            .setCommand("python3 -c")
            .setFormat("JSONEachRow")
    );

    driver_name_to_configuration_map["DockerPython3_JSON"] = std::make_shared<DriverConfiguration>(
        DriverConfiguration("DockerPython3_JSON")
            .setCommand("python3 -c")
            .setContainer("docker run --rm -i python:3 /bin/bash -c")
            .setFormat("JSONEachRow")
    );
}

UserDefinedDriversStorage::UserDefinedDriversStorage()
{
    loadDrivers();
}

DriverConfigurationPtr UserDefinedDriversStorage::get(const String & driver_name) const
{
    std::lock_guard lock(mutex);

    auto it = driver_name_to_configuration_map.find(driver_name);
    if (it == driver_name_to_configuration_map.end())
        throw Exception(ErrorCodes::UNSUPPORTED_DRIVER,
            "The driver with name '{}' does not exist", driver_name);

    return it->second;
}

DriverConfigurationPtr UserDefinedDriversStorage::tryGet(const String & driver_name) const
{
    std::lock_guard lock(mutex);

    auto it = driver_name_to_configuration_map.find(driver_name);
    if (it == driver_name_to_configuration_map.end())
        return nullptr;

    return it->second;
}

bool UserDefinedDriversStorage::has(const String & driver_name) const
{
    return tryGet(driver_name) != nullptr;
}

std::vector<String> UserDefinedDriversStorage::getAllDriverNames() const
{
    std::vector<String> driver_names;

    std::lock_guard lock(mutex);
    driver_names.reserve(driver_name_to_configuration_map.size());

    for (const auto & [name, _] : driver_name_to_configuration_map) {
        driver_names.emplace_back(name);
    }

    return driver_names;
}

std::vector<std::pair<String, DriverConfigurationPtr>> UserDefinedDriversStorage::getAllDrivers() const
{
    std::vector<std::pair<String, DriverConfigurationPtr>> all_drivers;

    std::lock_guard lock{mutex};
    all_drivers.reserve(driver_name_to_configuration_map.size());
    std::copy(driver_name_to_configuration_map.begin(), driver_name_to_configuration_map.end(), std::back_inserter(all_drivers));
    return all_drivers;
}

bool UserDefinedDriversStorage::empty() const
{
    std::lock_guard lock(mutex);
    return driver_name_to_configuration_map.empty();
}

}

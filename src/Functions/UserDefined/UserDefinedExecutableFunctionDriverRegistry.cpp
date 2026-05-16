#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverRegistry.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_FUNCTION;
}

UserDefinedExecutableFunctionDriverRegistry & UserDefinedExecutableFunctionDriverRegistry::instance()
{
    static UserDefinedExecutableFunctionDriverRegistry the_instance;
    return the_instance;
}

UserDefinedExecutableFunctionDriverPtr UserDefinedExecutableFunctionDriverRegistry::tryGet(const String & driver_name) const
{
    std::lock_guard lock(mutex);
    auto it = drivers.find(driver_name);
    if (it == drivers.end())
        return nullptr;
    return it->second;
}

UserDefinedExecutableFunctionDriverPtr UserDefinedExecutableFunctionDriverRegistry::get(const String & driver_name) const
{
    auto driver = tryGet(driver_name);
    if (!driver)
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "Executable user-defined function driver '{}' is not registered", driver_name);
    return driver;
}

std::vector<String> UserDefinedExecutableFunctionDriverRegistry::getAllRegisteredNames() const
{
    std::lock_guard lock(mutex);
    std::vector<String> names;
    names.reserve(drivers.size());
    for (const auto & [name, _] : drivers)
        names.push_back(name);
    return names;
}

namespace
{
    UserDefinedExecutableFunctionDriverPtr parseDriverFromConfig(const Poco::Util::AbstractConfiguration & config, const String & path_prefix)
    {
        auto driver = std::make_shared<UserDefinedExecutableFunctionDriver>();

        driver->name = config.getString(path_prefix + ".name");
        if (driver->name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Driver name must not be empty");

        if (!config.has(path_prefix + ".create_command"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Driver '{}' is missing required field 'create_command'", driver->name);
        driver->create_command = config.getString(path_prefix + ".create_command");

        if (config.has(path_prefix + ".drop_command"))
            driver->drop_command = config.getString(path_prefix + ".drop_command");

        if (config.has(path_prefix + ".engine_arguments"))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(path_prefix + ".engine_arguments", keys);
            for (const auto & key : keys)
            {
                UserDefinedExecutableFunctionDriver::EngineArgument arg;
                arg.required = config.getBool(path_prefix + ".engine_arguments." + key + ".required", false);
                driver->engine_arguments.emplace(key, std::move(arg));
            }
        }

        if (config.has(path_prefix + ".env"))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(path_prefix + ".env", keys);
            for (const auto & key : keys)
                driver->env.emplace(key, config.getString(path_prefix + ".env." + key));
        }

        return driver;
    }
}

void UserDefinedExecutableFunctionDriverRegistry::loadDriversFromConfigs(
    const std::vector<Poco::AutoPtr<Poco::Util::AbstractConfiguration>> & configs)
{
    std::unordered_map<String, UserDefinedExecutableFunctionDriverPtr> new_drivers;

    auto log = getLogger("UserDefinedExecutableFunctionDriverRegistry");

    for (const auto & config : configs)
    {
        Poco::Util::AbstractConfiguration::Keys top_level_keys;
        config->keys(top_level_keys);

        for (const auto & top_key : top_level_keys)
        {
            if (top_key != "driver" && !top_key.starts_with("driver"))
                continue;

            auto driver = parseDriverFromConfig(*config, top_key);
            const String name = driver->name;
            auto [it, inserted] = new_drivers.try_emplace(name, std::move(driver));
            if (!inserted)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Driver '{}' is defined more than once", name);
            LOG_INFO(log, "Loaded executable UDF driver '{}'", name);
        }
    }

    std::lock_guard lock(mutex);
    drivers = std::move(new_drivers);
}

}

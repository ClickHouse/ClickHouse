#include "UserDefinedDriverFactory.h"

#include <Functions/UserDefined/ExternalUserDefinedDriversLoader.h>


namespace DB
{

UserDefinedDriverFactory & UserDefinedDriverFactory::instance()
{
    static UserDefinedDriverFactory result;
    return result;
}

UserDefinedDriverPtr UserDefinedDriverFactory::get(const String & driver_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedDriversLoader();
    return std::static_pointer_cast<const UserDefinedDriver>(loader.load(driver_name));
}

UserDefinedDriverPtr UserDefinedDriverFactory::tryGet(const String & driver_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedDriversLoader();
    auto load_result = loader.getLoadResult(driver_name);

    if (load_result.object)
    {
        return std::static_pointer_cast<const UserDefinedDriver>(load_result.object);
    }

    return nullptr;
}

bool UserDefinedDriverFactory::has(const String & driver_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedDriversLoader();
    auto load_result = loader.getLoadResult(driver_name);

    return load_result.object != nullptr;
}

std::vector<String> UserDefinedDriverFactory::getRegisteredNames(ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedDriversLoader();
    auto loaded_objects = loader.getLoadedObjects();

    std::vector<std::string> registered_names;
    registered_names.reserve(loaded_objects.size());

    for (auto & loaded_object : loaded_objects)
        registered_names.emplace_back(loaded_object->getLoadableName());

    return registered_names;
}

}

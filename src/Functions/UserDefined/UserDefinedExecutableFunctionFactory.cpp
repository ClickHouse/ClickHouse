#include "UserDefinedExecutableFunctionFactory.h"

#include <Functions/IFunctionAdaptors.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Functions/UserDefined/UserDefinedFunction.h>


namespace DB
{

UserDefinedExecutableFunctionFactory & UserDefinedExecutableFunctionFactory::instance()
{
    static UserDefinedExecutableFunctionFactory result;
    return result;
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::get(const String & function_name, ContextPtr context, Array parameters)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto executable_function = std::static_pointer_cast<const UserDefinedLoadableFunction>(loader.load(function_name));
    auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context), std::move(parameters));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::tryGet(const String & function_name, ContextPtr context, Array parameters)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedLoadableFunction>(load_result.object);
        auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context), std::move(parameters));
        return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
    }

    return nullptr;
}

bool UserDefinedExecutableFunctionFactory::has(const String & function_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto load_result = loader.getLoadResult(function_name);

    bool result = load_result.object != nullptr;
    return result;
}

std::vector<String> UserDefinedExecutableFunctionFactory::getRegisteredNames(ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto loaded_objects = loader.getLoadedObjects();

    std::vector<std::string> registered_names;
    registered_names.reserve(loaded_objects.size());

    for (auto & loaded_object : loaded_objects)
        registered_names.emplace_back(loaded_object->getLoadableName());

    return registered_names;
}

}

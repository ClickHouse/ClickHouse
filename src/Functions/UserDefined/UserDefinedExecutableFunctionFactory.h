#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <Core/Field.h>
#include <Core/Types.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

class UserDefinedExecutableFunctionFactory
{
public:
    using Creator = std::function<FunctionOverloadResolverPtr(ContextPtr)>;

    static UserDefinedExecutableFunctionFactory & instance();

    static FunctionOverloadResolverPtr get(const String & function_name, ContextPtr context, Array parameters = {});

    static FunctionOverloadResolverPtr tryGet(const String & function_name, ContextPtr context, Array parameters = {});

    static bool has(const String & function_name, ContextPtr context);

    /// Returns the `deterministic` flag from the configuration of a loaded function, or `std::nullopt`
    /// if there is no such function. Unlike `tryGet`, it does not construct the function, so it works
    /// for a function that declares command parameters without knowing their values.
    static std::optional<bool> tryGetIsDeterministic(const String & function_name, ContextPtr context);

    static VectorWithMemoryTracking<String> getRegisteredNames(ContextPtr context);

};

}

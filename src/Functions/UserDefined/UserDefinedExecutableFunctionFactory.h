#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>


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

    static std::vector<String> getRegisteredNames(ContextPtr context);

};

}

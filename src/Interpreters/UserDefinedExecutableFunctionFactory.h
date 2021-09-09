#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <Common/NamePrompter.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/IFunction.h>


namespace DB
{

class UserDefinedExecutableFunctionFactory : public IHints<1, UserDefinedExecutableFunctionFactory>
{
public:
    using Creator = std::function<FunctionOverloadResolverPtr(ContextPtr)>;

    static UserDefinedExecutableFunctionFactory & instance();

    void registerFunction(const String & function_name, Creator creator);

    void unregisterFunction(const String & function_name);

    FunctionOverloadResolverPtr get(const String & function_name, ContextPtr context) const;

    FunctionOverloadResolverPtr tryGet(const String & function_name, ContextPtr context) const;

    std::vector<String> getAllRegisteredNames() const override;

private:
    std::unordered_map<String, Creator> function_name_to_creator;
    mutable std::mutex mutex;
};

}

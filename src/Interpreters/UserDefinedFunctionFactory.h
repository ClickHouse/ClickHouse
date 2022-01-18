#pragma once

#include <unordered_map>
#include <mutex>

#include <Common/NamePrompter.h>

#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

///Factory for user defined functions stores functions.
class UserDefinedFunctionFactory : public IHints<1, UserDefinedFunctionFactory>
{
public:
    static UserDefinedFunctionFactory & instance();

    /// Register function with function_name. create_function_query pointer must be ASTCreateFunctionQuery.
    void registerFunction(const String & function_name, ASTPtr create_function_query);

    /// Unregister function with function_name.
    void unregisterFunction(const String & function_name);

    /// Throws an exception if not found. Result ast pointer safely can be casted to ASTCreateFunctionQuery.
    ASTPtr get(const String & function_name) const;

    /// Returns nullptr if not found. Result ast pointer safely can be casted to ASTCreateFunctionQuery.
    ASTPtr tryGet(const String & function_name) const;

    /// Get all registered function names.
    std::vector<String> getAllRegisteredNames() const override;

private:
    std::unordered_map<String, ASTPtr> function_name_to_create_query;
    mutable std::mutex mutex;
};

}

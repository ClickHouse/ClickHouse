#pragma once

#include <unordered_map>
#include <mutex>

#include <Common/NamePrompter.h>

#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

/// Factory for SQLUserDefinedFunctions
class UserDefinedSQLFunctionFactory : public IHints<1, UserDefinedSQLFunctionFactory>
{
public:
    static UserDefinedSQLFunctionFactory & instance();

    /** Register function for function_name in factory for specified create_function_query.
      * If replace = true and function with function_name already exists replace it with create_function_query.
      * Otherwise throws exception.
      */
    void registerFunction(const String & function_name, ASTPtr create_function_query, bool replace);

    /// Unregister function for function_name
    void unregisterFunction(const String & function_name);

    /// Get function create query for function_name. If no function registered with function_name throws exception.
    ASTPtr get(const String & function_name) const;

    /// Get function create query for function_name. If no function registered with function_name return nullptr.
    ASTPtr tryGet(const String & function_name) const;

    /// Check if function with function_name registered.
    bool has(const String & function_name) const;

    /// Get all user defined functions registered names.
    std::vector<String> getAllRegisteredNames() const override;

private:
    std::unordered_map<String, ASTPtr> function_name_to_create_query;
    mutable std::mutex mutex;
};

}

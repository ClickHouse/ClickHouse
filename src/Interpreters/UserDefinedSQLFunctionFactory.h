#pragma once

#include <unordered_map>
#include <mutex>

#include <Common/NamePrompter.h>

#include <Parsers/ASTCreateFunctionQuery.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Factory for SQLUserDefinedFunctions
class UserDefinedSQLFunctionFactory : public IHints<1, UserDefinedSQLFunctionFactory>
{
public:
    static UserDefinedSQLFunctionFactory & instance();

    /** Register function for function_name in factory for specified create_function_query.
      * If function exists and if_not_exists = false and replace = false throws exception.
      * If replace = true and sql user defined function with function_name already exists replace it with create_function_query.
      * If persist = true persist function on disk.
      */
    void registerFunction(ContextPtr context, const String & function_name, ASTPtr create_function_query, bool replace, bool if_not_exists, bool persist);

    /** Unregister function for function_name.
      * If if_exists = true then do not throw exception if function is not registered.
      * If if_exists = false then throw exception if function is not registered.
      */
    void unregisterFunction(ContextPtr context, const String & function_name, bool if_exists);

    /// Get function create query for function_name. If no function registered with function_name throws exception.
    ASTPtr get(const String & function_name) const;

    /// Get function create query for function_name. If no function registered with function_name return nullptr.
    ASTPtr tryGet(const String & function_name) const;

    /// Check if function with function_name registered.
    bool has(const String & function_name) const;

    /// Get all user defined functions registered names.
    std::vector<String> getAllRegisteredNames() const override;

    /// Check whether any UDFs have been registered
    bool empty() const;

private:
    std::unordered_map<String, ASTPtr> function_name_to_create_query;
    mutable std::mutex mutex;
};

}

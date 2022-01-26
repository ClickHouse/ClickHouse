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

    using Lock = std::unique_lock<std::mutex>;
    Lock getLock() const;

    /** Register function for function_name in factory for specified create_function_query.
      * If replace = true and sql user defined function with function_name already exists replace it with create_function_query.
      * If persist = true persist function on disk.
      */
    void registerFunction(
        Lock & lock,
        ContextPtr context,
        const String & function_name,
        ASTPtr create_function_query,
        bool replace,
        bool persist);

    /// Same as registerFunction method. Factory will take lock.
    void registerFunction(
        ContextPtr context,
        const String & function_name,
        ASTPtr create_function_query,
        bool replace,
        bool persist);

    /// Unregister function for function_name.
    void unregisterFunction(Lock & lock, ContextPtr context, const String & function_name);

    /// Same as unregisterFunction method. Factory will take lock.
    void unregisterFunction(ContextPtr context, const String & function_name);

    /// Get function create query for function_name. If no function registered with function_name throws exception.
    ASTPtr get(Lock & lock, const String & function_name) const;

    /// Same as get method. Factory will take lock.
    ASTPtr get(const String & function_name) const;

    /// Get function create query for function_name. If no function registered with function_name return nullptr.
    ASTPtr tryGet(Lock & lock, const String & function_name) const;

    /// Same as tryGet method. Factory will take lock.
    ASTPtr tryGet(const String & function_name) const;

    /// Check if function with function_name registered.
    bool has(Lock & lock, const String & function_name) const;

    /// Same as has method. Factory will take lock.
    bool has(const String & function_name) const;

    /// Get all user defined functions registered names.
    std::vector<String> getAllRegisteredNames(Lock & lock) const;

    /// Same as getAllRegisteredNames method. Factory will take lock.
    std::vector<String> getAllRegisteredNames() const override;

private:
    struct FunctionCreateQuery
    {
        ASTPtr create_query;
        bool persisted;
    };

    std::unordered_map<String, FunctionCreateQuery> function_name_to_create_query;
    mutable std::mutex mutex;
};

}

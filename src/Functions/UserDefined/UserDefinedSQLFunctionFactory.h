#pragma once

#include <unordered_map>
#include <mutex>

#include <Common/NamePrompter.h>

#include <Parsers/ASTCreateFunctionQuery.h>
#include <Interpreters/Context.h>


namespace DB
{
class BackupEntriesCollector;
class RestorerFromBackup;

/// Factory for SQLUserDefinedFunctions
class UserDefinedSQLFunctionFactory : public IHints<>
{
public:
    static UserDefinedSQLFunctionFactory & instance();

    /// Register function for function_name in factory for specified create_function_query.
    bool registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr create_function_query, bool throw_if_exists, bool replace_if_exists);

    /// Unregister function for function_name.
    bool unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists);

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

    /// Makes backup entries for all user-defined SQL functions.
    void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup) const;

    /// Restores user-defined SQL functions from the backup.
    void restore(RestorerFromBackup & restorer, const String & data_path_in_backup);

private:
    /// Checks that a specified function can be registered, throws an exception if not.
    static void checkCanBeRegistered(const ContextPtr & context, const String & function_name, const IAST & create_function_query);
    static void checkCanBeUnregistered(const ContextPtr & context, const String & function_name);

    ContextPtr global_context = Context::getGlobalContextInstance();
};

}

#pragma once

#include <Storages/IStorage.h>
#include <ext/singleton.h>
#include <unordered_map>


namespace DB
{

class Context;


/** Allows you to create a table by the name and parameters of the engine.
  * In 'columns', 'materialized_columns', etc., Nested data structures must be flattened.
  * You should subsequently call IStorage::startup method to work with table.
  */
class StorageFactory : public ext::singleton<StorageFactory>
{
public:
    struct Arguments
    {
        ASTs & args;
        const String & data_path;
        const String & table_name;
        const String & database_name;
        Context & local_context;
        Context & context;
        const NamesAndTypesList & columns;
        const NamesAndTypesList & materialized_columns;
        const NamesAndTypesList & alias_columns;
        const ColumnDefaults & column_defaults;
        bool attach;
        bool has_force_restore_data_flag;
    };

    using Creator = std::function<StoragePtr(const Arguments & arguments)>;

    StoragePtr get(
        ASTCreateQuery & query,
        const String & data_path,
        const String & table_name,
        const String & database_name,
        Context & local_context,
        Context & context,
        const NamesAndTypesList & columns,
        const NamesAndTypesList & materialized_columns,
        const NamesAndTypesList & alias_columns,
        const ColumnDefaults & column_defaults,
        bool attach,
        bool has_force_restore_data_flag) const;

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerStorage(const std::string & name, Creator creator);

private:
    using Storages = std::unordered_map<std::string, Creator>;
    Storages storages;
};

}

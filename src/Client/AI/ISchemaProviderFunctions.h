#pragma once

#include <string>
#include <vector>
#include <functional>

namespace DB
{

/// Interface for providing schema information through function calls
class ISchemaProviderFunctions
{
public:
    virtual ~ISchemaProviderFunctions() = default;
    
    /// List all available databases
    virtual std::vector<std::string> listDatabases() const = 0;
    
    /// List all tables in a specific database
    virtual std::vector<std::string> listTablesInDatabase(const std::string & database) const = 0;
    
    /// Get schema (CREATE TABLE statement) for a specific table
    virtual std::string getSchemaForTable(const std::string & database, const std::string & table) const = 0;
};

/// Schema provider implementation using function callbacks
class SchemaProviderFunctions : public ISchemaProviderFunctions
{
public:
    using ListDatabasesFunc = std::function<std::vector<std::string>()>;
    using ListTablesFunc = std::function<std::vector<std::string>(const std::string &)>;
    using GetSchemaFunc = std::function<std::string(const std::string &, const std::string &)>;
    
    SchemaProviderFunctions() = default;
    
    SchemaProviderFunctions(
        ListDatabasesFunc list_databases,
        ListTablesFunc list_tables,
        GetSchemaFunc get_schema)
        : list_databases_func(std::move(list_databases))
        , list_tables_func(std::move(list_tables))
        , get_schema_func(std::move(get_schema))
    {}
    
    void setListDatabasesFunction(ListDatabasesFunc func)
    {
        list_databases_func = std::move(func);
    }
    
    void setListTablesFunction(ListTablesFunc func)
    {
        list_tables_func = std::move(func);
    }
    
    void setGetSchemaFunction(GetSchemaFunc func)
    {
        get_schema_func = std::move(func);
    }
    
    std::vector<std::string> listDatabases() const override
    {
        if (list_databases_func)
            return list_databases_func();
        return {};
    }
    
    std::vector<std::string> listTablesInDatabase(const std::string & database) const override
    {
        if (list_tables_func)
            return list_tables_func(database);
        return {};
    }
    
    std::string getSchemaForTable(const std::string & database, const std::string & table) const override
    {
        if (get_schema_func)
            return get_schema_func(database, table);
        return {};
    }
    
private:
    ListDatabasesFunc list_databases_func;
    ListTablesFunc list_tables_func;
    GetSchemaFunc get_schema_func;
};

}

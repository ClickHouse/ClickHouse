#pragma once

#include <string>
#include <vector>

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

}

#pragma once

#include <Client/AI/ISchemaProviderFunctions.h>
#include <Common/quoteString.h>
#include <Common/escapeString.h>
#include <functional>
#include <sstream>

namespace DB
{

/// Implementation of schema provider functions using query execution callbacks
/// This allows the schema provider to work with both local and remote ClickHouse servers
class SchemaProviderFunctionsImpl : public ISchemaProviderFunctions
{
public:
    using QueryExecutor = std::function<std::string(const std::string & query)>;
    
    explicit SchemaProviderFunctionsImpl(QueryExecutor executor)
        : query_executor(std::move(executor))
    {}
    
    std::vector<std::string> listDatabases() const override
    {
        try
        {
            auto result = query_executor("SELECT name FROM system.databases ORDER BY name");
            return parseStringVector(result);
        }
        catch (...)
        {
            return {};
        }
    }
    
    std::vector<std::string> listTablesInDatabase(const std::string & database) const override
    {
        try
        {
            auto query = "SELECT name FROM system.tables WHERE database = '" + escapeString(database) + "' ORDER BY name";
            auto result = query_executor(query);
            return parseStringVector(result);
        }
        catch (...)
        {
            return {};
        }
    }
    
    std::string getSchemaForTable(const std::string & database, const std::string & table) const override
    {
        try
        {
            auto query = "SHOW CREATE TABLE " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
            return query_executor(query);
        }
        catch (...)
        {
            return "";
        }
    }
    
private:
    QueryExecutor query_executor;
    
    /// Parse a single column result into a vector of strings
    std::vector<std::string> parseStringVector(const std::string & result) const
    {
        std::vector<std::string> values;
        if (result.empty())
            return values;
            
        std::stringstream ss(result);
        std::string line;
        while (std::getline(ss, line))
        {
            if (!line.empty())
                values.push_back(line);
        }
        return values;
    }
};

}

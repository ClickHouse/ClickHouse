#pragma once

#include <functional>
#include <string>
#include <vector>
#include <ai/tools.h>

namespace DB
{

/// Function signature for executing a query and getting results
using QueryExecutor = std::function<std::string(const std::string & query)>;

/// Creates database schema exploration tools for AI-based SQL generation
class SchemaExplorationTools
{
public:
    /// Create tools with a query executor callback
    explicit SchemaExplorationTools(QueryExecutor executor);

    /// Get the ToolSet containing all schema exploration tools
    ai::ToolSet getToolSet() const { return tools; }

private:
    QueryExecutor query_executor;
    ai::ToolSet tools;

    /// Initialize all tools
    void initializeTools();

    /// Tool implementation functions
    ai::JsonValue listDatabases(const ai::JsonValue & args, const ai::ToolExecutionContext & context);
    ai::JsonValue listTablesInDatabase(const ai::JsonValue & args, const ai::ToolExecutionContext & context);
    ai::JsonValue getSchemaForTable(const ai::JsonValue & args, const ai::ToolExecutionContext & context);

    /// Helper to parse query results
    std::vector<std::string> parseStringVector(const std::string & result) const;
};

}

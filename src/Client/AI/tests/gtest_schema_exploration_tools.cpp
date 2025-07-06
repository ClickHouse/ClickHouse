#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/SchemaExplorationTools.h>
#include <ai/tools.h>
#include <ai/types/tool.h>
#include <functional>
#include <sstream>
#include <string>

namespace DB
{

namespace
{

using QueryExecutor = std::function<std::string(const std::string &)>;

/// Test query executor that simulates real-world edge cases
QueryExecutor createEdgeCaseQueryExecutor()
{
    return [](const std::string & query) -> std::string
    {
        // SQL injection attempt detection
        if (query.find("'; DROP TABLE") != std::string::npos || 
            query.find("--") != std::string::npos ||
            query.find("/*") != std::string::npos)
        {
            throw std::runtime_error("SQL injection detected");
        }
        
        // Test special characters in database/table names
        if (query.find("SELECT name FROM system.databases") != std::string::npos)
        {
            return "default\n`test-db`\n\"quoted.db\"\n`db-with-special@chars!`\n`db_with_123_numbers`";
        }
        else if (query.find("WHERE database = '`test-db`'") != std::string::npos)
        {
            return "`table-with-dash`\n\"table.with.dots\"\ntable_with_underscore";
        }
        else if (query.find("WHERE database = '\"quoted.db\"'") != std::string::npos)
        {
            return "normal_table\n`special-chars!@#`";
        }
        else if (query.find("SHOW CREATE TABLE") != std::string::npos)
        {
            if (query.find("`special-chars!@#`") != std::string::npos)
            {
                return "CREATE TABLE `quoted.db`.`special-chars!@#` (\n"
                       "    `column-with-dash` UInt64,\n"
                       "    `column.with.dots` String,\n"
                       "    `column_with_underscore` Nullable(String)\n"
                       ") ENGINE = MergeTree()\n"
                       "ORDER BY `column-with-dash`";
            }
            return "CREATE TABLE default.test (\n    id UInt64\n) ENGINE = Memory";
        }
        else if (query.find("WHERE database = ''") != std::string::npos)
        {
            // Empty database name - edge case
            return "";
        }
        return "";
    };
}

/// Test executor that simulates database errors
QueryExecutor createErrorQueryExecutor()
{
    return [](const std::string & query) -> std::string
    {
        if (query.find("error_db") != std::string::npos)
        {
            throw std::runtime_error("Database connection lost");
        }
        if (query.find("permission_db") != std::string::npos)
        {
            throw std::runtime_error("Access denied for database 'permission_db'");
        }
        return "default";
    };
}

}

/// Test handling of special characters in database/table names
TEST(SchemaExplorationTools, SpecialCharactersHandling)
{
    SchemaExplorationTools tools(createEdgeCaseQueryExecutor());
    auto tool_set = tools.getToolSet();
    
    // Test listing databases with special characters
    auto list_db_tool = tool_set.find("list_databases");
    ASSERT_NE(list_db_tool, tool_set.end());
    
    ai::JsonValue empty_args = ai::JsonValue::object();
    ai::ToolExecutionContext context;
    auto result = (*list_db_tool->second.execute)(empty_args, context);
    
    ASSERT_TRUE(result["success"].get<bool>());
    auto databases = result["databases"];
    ASSERT_TRUE(databases.is_array());
    ASSERT_EQ(5, databases.size());
    
    // Check that special character databases are included
    bool has_dash_db = false;
    bool has_quoted_db = false;
    bool has_special_chars_db = false;
    bool has_numbers_db = false;
    
    for (const auto & database : databases)
    {
        std::string db_name = database.get<std::string>();
        if (db_name == "`test-db`") has_dash_db = true;
        if (db_name == "\"quoted.db\"") has_quoted_db = true;
        if (db_name == "`db-with-special@chars!`") has_special_chars_db = true;
        if (db_name == "`db_with_123_numbers`") has_numbers_db = true;
    }
    
    EXPECT_TRUE(has_dash_db);
    EXPECT_TRUE(has_quoted_db);
    EXPECT_TRUE(has_special_chars_db);
    EXPECT_TRUE(has_numbers_db);
}

/// Test SQL injection protection
TEST(SchemaExplorationTools, SQLInjectionProtection)
{
    // This test verifies that the tools don't blindly concatenate user input
    SchemaExplorationTools tools(createEdgeCaseQueryExecutor());
    auto tool_set = tools.getToolSet();
    
    auto list_tables_tool = tool_set.find("list_tables_in_database");
    ASSERT_NE(list_tables_tool, tool_set.end());
    
    // Try SQL injection in database name
    ai::JsonValue args;
    args["database"] = "test'; DROP TABLE users; --";
    ai::ToolExecutionContext context;
    
    // The tool should either sanitize the input or reject it
    auto result = (*list_tables_tool->second.execute)(args, context);
    
    // Should either fail or handle safely
    if (result["success"].get<bool>())
    {
        // If it succeeds, it should have properly escaped the input
        EXPECT_TRUE(result["tables"].is_array());
        EXPECT_EQ(0, result["tables"].size()); // No tables found for invalid DB
    }
    else
    {
        // Or it should fail with an appropriate error
        EXPECT_TRUE(result.contains("error"));
    }
}

/// Test error handling and recovery
TEST(SchemaExplorationTools, ErrorHandling)
{
    SchemaExplorationTools tools(createErrorQueryExecutor());
    auto tool_set = tools.getToolSet();
    
    auto list_tables_tool = tool_set.find("list_tables_in_database");
    ASSERT_NE(list_tables_tool, tool_set.end());
    
    // Test connection error
    ai::JsonValue args;
    args["database"] = "error_db";
    ai::ToolExecutionContext context;
    auto result = (*list_tables_tool->second.execute)(args, context);
    
    ASSERT_FALSE(result["success"].get<bool>());
    ASSERT_TRUE(result.contains("error"));
    std::string error_msg = result["error"].get<std::string>();
    EXPECT_NE(error_msg.find("connection lost"), std::string::npos);
    
    // Test permission error
    args["database"] = "permission_db";
    result = (*list_tables_tool->second.execute)(args, context);
    
    ASSERT_FALSE(result["success"].get<bool>());
    ASSERT_TRUE(result.contains("error"));
    error_msg = result["error"].get<std::string>();
    EXPECT_NE(error_msg.find("Access denied"), std::string::npos);
}

/// Test handling of large schemas
TEST(SchemaExplorationTools, LargeSchemaHandling)
{
    // Create executor that returns many databases/tables
    auto large_schema_executor = [](const std::string & query) -> std::string
    {
        if (query.find("SELECT name FROM system.databases") != std::string::npos)
        {
            std::stringstream ss;
            for (int i = 0; i < 1000; ++i)
            {
                if (i > 0) ss << "\n";
                ss << "database_" << i;
            }
            return ss.str();
        }
        else if (query.find("SELECT name FROM system.tables") != std::string::npos)
        {
            std::stringstream ss;
            for (int i = 0; i < 5000; ++i)
            {
                if (i > 0) ss << "\n";
                ss << "table_" << i;
            }
            return ss.str();
        }
        return "";
    };
    
    SchemaExplorationTools tools(large_schema_executor);
    auto tool_set = tools.getToolSet();
    
    // Test listing many databases
    auto list_db_tool = tool_set.find("list_databases");
    ASSERT_NE(list_db_tool, tool_set.end());
    
    ai::JsonValue args = ai::JsonValue::object();
    ai::ToolExecutionContext context;
    auto result = (*list_db_tool->second.execute)(args, context);
    
    ASSERT_TRUE(result["success"].get<bool>());
    ASSERT_TRUE(result["databases"].is_array());
    EXPECT_EQ(1000, result["databases"].size());
    
    // Test listing many tables
    auto list_tables_tool = tool_set.find("list_tables_in_database");
    ASSERT_NE(list_tables_tool, tool_set.end());
    
    args["database"] = "database_0";
    result = (*list_tables_tool->second.execute)(args, context);
    
    ASSERT_TRUE(result["success"].get<bool>());
    ASSERT_TRUE(result["tables"].is_array());
    EXPECT_EQ(5000, result["tables"].size());
}

/// Test schema caching behavior
TEST(SchemaExplorationTools, SchemaCachingBehavior)
{
    std::atomic<int> query_count(0);
    auto caching_executor = [&query_count](const std::string & query) -> std::string
    {
        query_count++;
        if (query.find("SELECT name FROM system.databases") != std::string::npos)
        {
            return "default\ntest";
        }
        return "";
    };
    
    SchemaExplorationTools tools(caching_executor);
    auto tool_set = tools.getToolSet();
    
    auto list_db_tool = tool_set.find("list_databases");
    ASSERT_NE(list_db_tool, tool_set.end());
    
    // Call multiple times
    for (int i = 0; i < 5; ++i)
    {
        ai::JsonValue args = ai::JsonValue::object();
        ai::ToolExecutionContext context;
        auto result = (*list_db_tool->second.execute)(args, context);
        ASSERT_TRUE(result["success"].get<bool>());
    }
    
    // Each call should trigger a new query (no caching)
    EXPECT_EQ(5, query_count.load());
}

/// Test empty and null handling
TEST(SchemaExplorationTools, EmptyAndNullHandling)
{
    auto empty_executor = [](const std::string & query) -> std::string
    {
        if (query.find("empty_db") != std::string::npos)
        {
            return ""; // No tables
        }
        else if (query.find("SELECT name FROM system.databases") != std::string::npos)
        {
            return "default\nempty_db\n"; // Extra newline
        }
        else if (query.find("SHOW CREATE TABLE") != std::string::npos)
        {
            return ""; // No schema
        }
        return "test_table";
    };
    
    SchemaExplorationTools tools(empty_executor);
    auto tool_set = tools.getToolSet();
    
    // Test empty database (no tables)
    auto list_tables_tool = tool_set.find("list_tables_in_database");
    ASSERT_NE(list_tables_tool, tool_set.end());
    
    ai::JsonValue args;
    args["database"] = "empty_db";
    ai::ToolExecutionContext context;
    auto result = (*list_tables_tool->second.execute)(args, context);
    
    ASSERT_TRUE(result["success"].get<bool>());
    ASSERT_TRUE(result["tables"].is_array());
    EXPECT_EQ(0, result["tables"].size());
    
    // Test empty schema
    auto get_schema_tool = tool_set.find("get_schema_for_table");
    ASSERT_NE(get_schema_tool, tool_set.end());
    
    args["database"] = "default";
    args["table"] = "test_table";
    result = (*get_schema_tool->second.execute)(args, context);
    
    // Should fail or return empty schema
    if (result["success"].get<bool>())
    {
        EXPECT_TRUE(result["result"].get<std::string>().empty());
    }
}

}

#endif // USE_CLIENT_AI

#include <Client/AI/SchemaExplorationTools.h>
#include <Common/escapeString.h>
#include <Common/quoteString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

SchemaExplorationTools::SchemaExplorationTools(QueryExecutor executor)
    : query_executor(std::move(executor))
{
    initializeTools();
}

void SchemaExplorationTools::initializeTools()
{
    // list_databases tool
    tools["list_databases"] = ai::create_simple_tool(
        "list_databases",
        "List all available databases in the ClickHouse instance",
        {}, // No parameters
        [this](const ai::JsonValue & args, const ai::ToolExecutionContext & context) { return listDatabases(args, context); });

    // list_tables_in_database tool
    tools["list_tables_in_database"] = ai::create_simple_tool(
        "list_tables_in_database",
        "List all tables in a specific database",
        {{"database", "string"}},
        [this](const ai::JsonValue & args, const ai::ToolExecutionContext & context) { return listTablesInDatabase(args, context); });

    // get_schema_for_table tool
    tools["get_schema_for_table"] = ai::create_simple_tool(
        "get_schema_for_table",
        "Get the CREATE TABLE statement (schema) for a specific table",
        {{"database", "string"}, {"table", "string"}},
        [this](const ai::JsonValue & args, const ai::ToolExecutionContext & context) { return getSchemaForTable(args, context); });
}

ai::JsonValue SchemaExplorationTools::listDatabases(
    const ai::JsonValue & args [[maybe_unused]], const ai::ToolExecutionContext & context [[maybe_unused]])
{
    try
    {
        auto result = query_executor("SELECT name FROM system.databases ORDER BY name");
        auto databases = parseStringVector(result);

        WriteBufferFromOwnString buf;
        writeString("Found ", buf);
        writeIntText(databases.size(), buf);
        writeString(" databases:\n", buf);
        for (const auto & db : databases)
        {
            writeString("- ", buf);
            writeString(db, buf);
            writeChar('\n', buf);
        }

        return ai::JsonValue{{"success", true}, {"result", buf.str()}, {"databases", databases}};
    }
    catch (const std::exception & e)
    {
        return ai::JsonValue{{"success", false}, {"error", e.what()}};
    }
}

ai::JsonValue
SchemaExplorationTools::listTablesInDatabase(const ai::JsonValue & args, const ai::ToolExecutionContext & context [[maybe_unused]])
{
    try
    {
        std::string database = args["database"].get<std::string>();
        auto query = "SELECT name FROM system.tables WHERE database = '" + escapeString(database) + "' ORDER BY name";
        auto result = query_executor(query);
        auto tables = parseStringVector(result);

        WriteBufferFromOwnString buf;
        writeString("Found ", buf);
        writeIntText(tables.size(), buf);
        writeString(" tables in database '", buf);
        writeString(database, buf);
        writeString("':\n", buf);
        for (const auto & table : tables)
        {
            writeString("- ", buf);
            writeString(table, buf);
            writeChar('\n', buf);
        }
        if (tables.empty())
        {
            writeString("(No tables found in this database)\n", buf);
        }

        return ai::JsonValue{{"success", true}, {"result", buf.str()}, {"database", database}, {"tables", tables}};
    }
    catch (const std::exception & e)
    {
        return ai::JsonValue{{"success", false}, {"error", e.what()}};
    }
}

ai::JsonValue
SchemaExplorationTools::getSchemaForTable(const ai::JsonValue & args, const ai::ToolExecutionContext & context [[maybe_unused]])
{
    try
    {
        std::string database = args["database"].get<std::string>();
        std::string table = args["table"].get<std::string>();

        auto query = "SHOW CREATE TABLE " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
        std::string schema = query_executor(query);

        if (schema.empty())
        {
            return ai::JsonValue{{"success", false}, {"error", "Could not retrieve schema for " + database + "." + table}};
        }

        return ai::JsonValue{
            {"success", true},
            {"result", "Schema for " + database + "." + table + ":\n" + schema},
            {"database", database},
            {"table", table},
            {"schema", schema}};
    }
    catch (const std::exception & e)
    {
        return ai::JsonValue{{"success", false}, {"error", e.what()}};
    }
}

std::vector<std::string> SchemaExplorationTools::parseStringVector(const std::string & result) const
{
    std::vector<std::string> values;
    if (result.empty())
        return values;

    ReadBufferFromString buf(result);
    std::string line;
    while (!buf.eof())
    {
        readString(line, buf);
        if (!line.empty())
            values.push_back(line);
        skipWhitespaceIfAny(buf);
    }
    return values;
}

}

#include <Client/AI/AIAgentTools.h>

#include <Common/quoteString.h>

namespace DB
{

namespace
{

ai::JsonValue successResult(const String & text)
{
    return ai::JsonValue{{"success", true}, {"result", text}};
}

ai::JsonValue errorResult(const String & message)
{
    return ai::JsonValue{{"success", false}, {"error", message}};
}

/// Wrap a tool body so that exceptions become error results the model can react to.
template <typename F>
ai::JsonValue guarded(F && body)
{
    try
    {
        return body();
    }
    catch (const std::exception & e)
    {
        return errorResult(e.what());
    }
}

ai::JsonValue stringParameter(const String & description)
{
    return ai::JsonValue{{"type", "string"}, {"description", description}};
}

ai::Tool makeTool(const String & description, ai::JsonValue properties, std::vector<String> required, ai::ToolExecuteFunction execute)
{
    ai::JsonValue schema{{"type", "object"}, {"properties", std::move(properties)}, {"required", std::move(required)}};
    return ai::Tool(description, std::move(schema), std::move(execute));
}

}

ai::ToolSet buildAIAgentToolSet(const AIAgentHooks & hooks_, bool enable_schema_access)
{
    ai::ToolSet tools;

    /// The hooks are shared by the tool lambdas.
    auto hooks = std::make_shared<AIAgentHooks>(hooks_);

    if (enable_schema_access)
    {
        tools["list_databases"] = makeTool(
            "List all databases on the server. Runs internally, nothing is displayed to the user.",
            ai::JsonValue::object(),
            {},
            [hooks](const ai::JsonValue &, const ai::ToolExecutionContext &)
            {
                return guarded([&] { return successResult(hooks->execute_internal("SELECT name FROM system.databases ORDER BY name", {})); });
            });

        tools["list_tables"] = makeTool(
            "List the tables of a database with their engines. Runs internally, nothing is displayed to the user.",
            ai::JsonValue{{"database", stringParameter("Name of the database")}},
            {"database"},
            [hooks](const ai::JsonValue & args, const ai::ToolExecutionContext &)
            {
                return guarded(
                    [&]
                    {
                        return successResult(hooks->execute_internal(
                            "SELECT name, engine FROM system.tables WHERE database = {database:String} ORDER BY name",
                            {{"database", args.at("database").get<std::string>()}}));
                    });
            });

        tools["show_create_table"] = makeTool(
            "Get the CREATE TABLE statement (columns, engine, sorting key) of a table. "
            "Runs internally, nothing is displayed to the user.",
            ai::JsonValue{
                {"database", stringParameter("Name of the database")},
                {"table", stringParameter("Name of the table")}},
            {"database", "table"},
            [hooks](const ai::JsonValue & args, const ai::ToolExecutionContext &)
            {
                return guarded(
                    [&]
                    {
                        auto database = args.at("database").get<std::string>();
                        auto table = args.at("table").get<std::string>();
                        return successResult(hooks->execute_internal(
                            "SHOW CREATE TABLE " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table), {}));
                    });
            });
    }

    tools["read_query_log"] = makeTool(
        "Read the recent queries of the current user from the `system.user_query_log` table: "
        "query text, duration, resource usage and error messages. Useful to see what the user was doing "
        "beyond the recent activity included in the conversation, e.g. in previous sessions. "
        "Runs internally, nothing is displayed to the user. "
        "The table may be absent on older servers; then rely on the recent activity context.",
        ai::JsonValue{
            {"limit", ai::JsonValue{{"type", "integer"}, {"description", "Maximum number of queries to return, at most 100. Default 20."}}},
            {"only_errors", ai::JsonValue{{"type", "boolean"}, {"description", "Return only failed queries. Default false."}}}},
        {},
        [hooks](const ai::JsonValue & args, const ai::ToolExecutionContext &)
        {
            return guarded(
                [&]
                {
                    UInt64 limit = 20;
                    if (args.contains("limit") && args["limit"].is_number())
                        limit = std::min<UInt64>(std::max<Int64>(args["limit"].get<Int64>(), 1), 100);
                    bool only_errors = args.contains("only_errors") && args["only_errors"].is_boolean() && args["only_errors"].get<bool>();

                    String query = "SELECT event_time, query_duration_ms, read_rows, result_rows, formatReadableSize(memory_usage) AS memory, exception, query "
                        "FROM system.user_query_log WHERE type != 'QueryStart'";
                    if (only_errors)
                        query += " AND exception != ''";
                    query += " ORDER BY event_time DESC LIMIT {limit:UInt64}";

                    return successResult(hooks->execute_internal(query, {{"limit", std::to_string(limit)}}));
                });
        });

    tools["consult_documentation"] = makeTool(
        "Look up the embedded ClickHouse documentation (the `system.documentation` table) for a function, "
        "table engine, data type, format, setting or system table, like the `help` command of the client does. "
        "Use it whenever unsure about syntax or behavior instead of guessing. "
        "Runs internally, nothing is displayed to the user.",
        ai::JsonValue{{"name", stringParameter("The exact name to look up, e.g. `quantileTiming`, `MergeTree`, `max_threads`")}},
        {"name"},
        [hooks](const ai::JsonValue & args, const ai::ToolExecutionContext &)
        {
            return guarded(
                [&]
                {
                    auto word = args.at("name").get<std::string>();

                    /// Render the matches into a single blob so their multi-line descriptions keep their
                    /// real newlines (a raw scalar), instead of being TSV-escaped row by row.
                    String exact = hooks->execute_scalar(
                        "SELECT arrayStringConcat("
                        "  arrayMap(x -> x.1 || ' (' || x.2 || '):\n' || x.3, "
                        "  arraySort(groupArray((name, toString(type), description)))), '\n\n---\n\n') "
                        "FROM system.documentation WHERE lower(name) = lower({word:String})",
                        {{"word", word}});

                    if (!exact.empty())
                        return successResult(exact);

                    String similar = hooks->execute_internal(
                        "SELECT DISTINCT name, toString(type) AS type FROM system.documentation "
                        "WHERE (lengthUTF8({word:String}) >= 3 AND positionCaseInsensitive(name, {word:String}) > 0) "
                        "   OR editDistanceUTF8(lower(name), lower({word:String})) "
                        "      <= greatest(1, intDiv(lengthUTF8({word:String}), 3)) "
                        "ORDER BY editDistanceUTF8(lower(name), lower({word:String})), lengthUTF8(name), name "
                        "LIMIT 30",
                        {{"word", word}});

                    return successResult("No documentation found for '" + word + "'. Entities with similar names:\n" + similar);
                });
        });

    tools["run_readonly_query"] = makeTool(
        "Run a single read-only SQL statement (SELECT, SHOW, DESCRIBE, EXPLAIN, EXISTS, CHECK) on the user's connection, "
        "without confirmation. The query and its complete result are displayed in the user's terminal exactly as if "
        "the user ran it; you receive a summary truncated to the first and last rows. "
        "The query is executed in readonly mode with strict limits: 30 seconds and 10 GiB of memory. "
        "INTO OUTFILE, overriding the readonly/time/memory limit settings, and table functions reaching "
        "outside of the current server's tables (file, url, s3, remote, executable, ...) are rejected; "
        "use run_query for anything that does not fit these constraints. Add LIMIT to exploratory queries.",
        ai::JsonValue{{"query", stringParameter("The SQL statement to run")}},
        {"query"},
        [hooks](const ai::JsonValue & args, const ai::ToolExecutionContext &)
        {
            return guarded([&] { return successResult(hooks->run_visible(args.at("query").get<std::string>(), /*readonly=*/ true)); });
        });

    tools["run_query"] = makeTool(
        "Run any SQL on the user's connection: writes (INSERT, ALTER, DROP), DDL, SET, or read queries that need to "
        "exceed the limits of run_readonly_query. The user is asked to confirm every call, so prefer run_readonly_query "
        "when possible. The query and its output are displayed in the user's terminal exactly as if the user ran it; "
        "you receive a summary truncated to the first and last rows.",
        ai::JsonValue{{"query", stringParameter("The SQL to run (may contain several statements)")}},
        {"query"},
        [hooks](const ai::JsonValue & args, const ai::ToolExecutionContext &)
        {
            return guarded(
                [&]
                {
                    auto query = args.at("query").get<std::string>();
                    if (!hooks->confirm_query(query))
                        return errorResult("The user declined to run this query. Ask them how to proceed if unsure.");
                    return successResult(hooks->run_visible(query, /*readonly=*/ false));
                });
        });

    return tools;
}

}

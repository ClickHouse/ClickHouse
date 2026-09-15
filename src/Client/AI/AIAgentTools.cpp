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

ai::ToolSet buildAIAgentToolSet(const AIAgentHooks & hooks_, bool enable_schema_access, bool enable_query_log_access)
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
            "List the tables of a database with their engines. Runs internally, nothing is displayed to the user. "
            "Reads `system.tables` with the remote-database and data-lake-catalog visibility turned off, so a "
            "database backed by an external system is listed as empty rather than enumerated from that system. "
            "Those two settings travel with the query, so where they cannot - a session with `readonly = 1`, which "
            "accepts no setting change, or a server that does not support them - the visibility of the session "
            "applies and such a database is enumerated from that system instead. Reading the schema of a database "
            "this server does not own is allowed without confirmation either way; it reads metadata only.",
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
            "Runs internally, nothing is displayed to the user. The table is resolved the way any query resolves "
            "it, so for a database backed by an external system (MySQL, PostgreSQL, a data-lake catalog) the "
            "definition is fetched from that system - this is the one unconfirmed step that always leaves this "
            "server, and it reads metadata only. Credentials are masked in the result: where that masking cannot "
            "be installed for the query - a session with `readonly = 1` that displays secrets, or a server without "
            "the `format_display_secrets_in_show_and_select` setting - the tool refuses instead, and the definition "
            "has to be read through run_query so the user sees it.",
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
                        return successResult(hooks->execute_internal_masking_secrets(
                            "SHOW CREATE TABLE " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table), {}));
                    });
            });
    }

    if (enable_query_log_access)
    {
        tools["read_query_log"] = makeTool(
            "Read the recent queries of the current user from the `system.user_query_log` table: "
            "query text, duration, resource usage and error messages. Useful to see what the user was doing "
            "beyond the recent activity included in the conversation, e.g. in previous sessions. "
            "The queries the assistant ran on its own - schema probes, documentation lookups, this read - are "
            "always excluded. The queries the assistant ran on the user's connection and displayed to them are "
            "excluded only when the session of the time let the assistant tag them, which a read-only session "
            "does not, so a returned query from an earlier session may be one the assistant ran and showed the "
            "user rather than one they typed. "
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
                        /// Internal queries cannot be marked after `SET readonly = 1`, so do not
                        /// return potentially unmarked agent activity as user query history.
                        if (!hooks->can_read_query_log || !hooks->can_read_query_log())
                            return errorResult("The query-log tool is unavailable while readonly = 1.");

                        UInt64 limit = 20;
                        if (args.contains("limit") && args["limit"].is_number())
                            limit = std::min<UInt64>(std::max<Int64>(args["limit"].get<Int64>(), 1), 100);
                        bool only_errors = args.contains("only_errors") && args["only_errors"].is_boolean() && args["only_errors"].get<bool>();

                        /// The queries the agent ran itself must not come back as the history of the
                        /// user - the model would read its own earlier activity as what the user wanted;
                        /// the in-memory recent-query context hides those entries for the same reason.
                        /// Two markers are filtered out, because neither alone covers the log: the
                        /// `log_comment` one is a setting, so it is missing from every row that a session
                        /// accepting no setting change left behind, and the query-id one is only on the
                        /// queries the agent ran internally - the ones it ran *visibly* on the user's
                        /// connection keep the query id the client would have used anyway, since they are
                        /// echoed and displayed as if the user typed them. Between them they exclude
                        /// every query of the agent the user never saw, whichever session it ran in.
                        String query = "SELECT event_time, query_duration_ms, read_rows, result_rows, formatReadableSize(memory_usage) AS memory, exception, query "
                            "FROM system.user_query_log WHERE type != 'QueryStart' AND log_comment != {ai_marker:String} "
                            "AND NOT startsWith(query_id, {ai_query_id_prefix:String})";
                        if (only_errors)
                            query += " AND exception != ''";
                        query += " ORDER BY event_time DESC LIMIT {limit:UInt64}";

                        return successResult(hooks->execute_internal(
                            query,
                            {{"limit", std::to_string(limit)},
                             {"ai_marker", String(AI_AGENT_LOG_COMMENT)},
                             {"ai_query_id_prefix", String(AI_AGENT_QUERY_ID_PREFIX)}}));
                    });
            });
    }

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
        "Run a single read-only SQL statement (SELECT, SHOW, DESCRIBE, EXPLAIN, EXISTS) on the user's connection, "
        "without confirmation. The query and its complete result are displayed in the user's terminal exactly as if "
        "the user ran it; you receive a summary truncated to the first and last rows. "
        "The query is executed in readonly mode with strict limits: 30 seconds and 10 GiB of memory. "
        "It can select from any ordinary table of the server: the MergeTree and Log families, Memory, and the "
        "`system` and `information_schema` tables. The engine of every table named in the query is checked first, "
        "and the query is rejected when one of them does not simply hold data of this server - a view, a "
        "materialized view, a Merge or Buffer table, a Dictionary, a Distributed table, or a table over an "
        "external system (S3, URL, MySQL, Kafka, ...). Also rejected are INTO OUTFILE, overriding the "
        "readonly/time/memory limit settings, table functions reaching outside of the current server (file, url, "
        "s3, remote, executable, ...), the AI functions calling external providers (aiGenerate, ...), dictionary "
        "functions, and the `system` tables that read Keeper or object storage (system.zookeeper, "
        "system.replicas, ...). Use run_query for anything that does not fit these constraints; the error message "
        "says which table did not qualify. Add LIMIT to exploratory queries.",
        ai::JsonValue{{"query", stringParameter("The SQL statement to run")}},
        {"query"},
        [hooks, enable_schema_access](const ai::JsonValue & args, const ai::ToolExecutionContext &)
        {
            return guarded([&] { return successResult(hooks->run_visible(args.at("query").get<std::string>(), /*readonly=*/ true, enable_schema_access)); });
        });

    tools["run_query"] = makeTool(
        "Run any SQL on the user's connection: writes (INSERT, ALTER, DROP), DDL, SET, or read queries that need to "
        "exceed the limits of run_readonly_query. The user is asked to confirm the call, so prefer "
        "run_readonly_query when possible. A query the session "
        "does not allow at all is refused without asking them. The query and its output are displayed in the user's "
        "terminal exactly as if the user ran it; you receive a summary truncated to the first and last rows.",
        ai::JsonValue{{"query", stringParameter("The SQL to run (may contain several statements)")}},
        {"query"},
        [hooks, enable_schema_access](const ai::JsonValue & args, const ai::ToolExecutionContext &)
        {
            return guarded(
                [&]
                {
                    auto query = args.at("query").get<std::string>();

                    /// The client decides what happens to the query before the user is bothered
                    /// with it: a malformed query, or one the session rejects outright, is
                    /// reported back for correction instead of being confirmed and then failing,
                    /// and the confirmation is skipped when the session makes it pointless.
                    AIQueryRunDecision decision;
                    if (hooks->check_query)
                        decision = hooks->check_query(query);
                    if (decision.refusal)
                        return errorResult(*decision.refusal);

                    /// The unconfirmed read-only tool has an independent sandbox. A confirmed
                    /// query must use the normal visible path, even in a read-only session,
                    /// because `readonly = 1` prevents installing that sandbox's settings.
                    if (!decision.needs_confirmation)
                        return successResult(hooks->run_visible(query, /*readonly=*/ true, enable_schema_access));

                    if (!hooks->confirm_query(query))
                        return errorResult("The user declined to run this query. Ask them how to proceed if unsure.");
                    return successResult(hooks->run_visible(query, /*readonly=*/ false, enable_schema_access));
                });
        });

    return tools;
}

}

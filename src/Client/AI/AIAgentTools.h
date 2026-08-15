#pragma once

#include <functional>
#include <optional>
#include <Client/AI/AIAgentTransport.h>
#include <ai/tools.h>

namespace DB
{

/// The agent tags the queries it runs on the user's connection with this `log_comment`, so they can
/// be told apart from the queries the user typed themselves in `system.query_log` (the
/// `read_query_log` tool filters them out). The `client_agent` field is deliberately not used for
/// this: it reports the external AI coding tool that invoked the client (`claude-code`, `cursor`,
/// ...) and must keep reporting the real one.
inline constexpr std::string_view AI_AGENT_LOG_COMMENT = "clickhouse-ai-agent";

/// Callbacks the client provides to the tools of the AI agent.
struct AIAgentHooks
{
    /// Execute a query internally: not displayed to the user, the result is returned
    /// as tab-separated text with a header line. Throws on error.
    InternalQueryExecutor execute_internal;

    /// Execute a query internally and return the first cell of the result raw and unescaped
    /// (for free-form text with its own newlines, e.g. a pre-rendered documentation blob).
    ScalarQueryExecutor execute_scalar;

    /// Run a query through the normal client path: it is echoed and executed on the user's
    /// connection, and its output is displayed exactly as if the user typed the query.
    /// When `readonly` is set, the query is validated to be read-only and runs under
    /// restrictive limits. Returns a text summary of the outcome for the model.
    std::function<String(const String & query, bool readonly)> run_visible;

    /// Ask the user to confirm running a query. Returns false when declined.
    std::function<bool(const String & query)> confirm_query;

    /// Syntax-check a query before running it. Returns the parse error message when the query is
    /// malformed, or an empty optional when it parses.
    std::function<std::optional<String>(const String & query)> check_syntax;

    /// Whether internal agent queries can be reliably marked in `system.user_query_log` at this
    /// instant. A confirmed `SET readonly = 1` can change it after agent construction.
    std::function<bool()> can_read_query_log;
};

/// Builds the tool set of the client AI agent:
/// - schema exploration (databases, tables, CREATE TABLE statements) - internal, not displayed;
/// - the history of the user's queries from `system.user_query_log`;
/// - the embedded documentation from `system.documentation` (like the `help` command);
/// - read-only query execution without confirmation, under restrictive limits;
/// - arbitrary query execution with the user's confirmation.
/// When `enable_schema_access` is false, the internal schema exploration tools are omitted.
/// When `enable_query_log_access` is false, `read_query_log` is omitted. When the hook reports
/// that access is unsafe, the tool fails closed without querying the log.
ai::ToolSet buildAIAgentToolSet(const AIAgentHooks & hooks, bool enable_schema_access, bool enable_query_log_access);

}

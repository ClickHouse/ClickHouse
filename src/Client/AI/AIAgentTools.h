#pragma once

#include <functional>
#include <Client/AI/AIAgentTransport.h>
#include <ai/tools.h>

namespace DB
{

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
};

/// Builds the tool set of the client AI agent:
/// - schema exploration (databases, tables, CREATE TABLE statements) - internal, not displayed;
/// - the history of the user's queries from `system.user_query_log`;
/// - the embedded documentation from `system.documentation` (like the `help` command);
/// - read-only query execution without confirmation, under restrictive limits;
/// - arbitrary query execution with the user's confirmation.
/// When `enable_schema_access` is false, the internal schema exploration tools are omitted.
ai::ToolSet buildAIAgentToolSet(const AIAgentHooks & hooks, bool enable_schema_access);

}

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

/// What the client decided about a query the agent wants to run through the confirmed tool,
/// before the user is involved.
struct AIQueryRunDecision
{
    /// When set, the query is not run and this message goes back to the model instead: the query
    /// is malformed, or the session rejects it outright (a write in a read-only session).
    std::optional<String> refusal;

    /// Whether the user has to confirm the query. False when the session itself already restricts
    /// the query to what the unconfirmed read-only tool would run anyway, so asking adds nothing;
    /// such a query is then run through that same path.
    bool needs_confirmation = true;
};

/// Callbacks the client provides to the tools of the AI agent.
struct AIAgentHooks
{
    /// Execute a query internally: not displayed to the user, the result is returned
    /// as tab-separated text with a header line. Throws on error.
    InternalQueryExecutor execute_internal;

    /// The same, for a query whose result can render the credentials of an external-engine table
    /// (`SHOW CREATE`): the display of secrets is turned off for it. Only such a query uses this
    /// hook - turning the display off is a setting change, and a session that rejects it must not
    /// lose the tools that cannot render a secret in the first place.
    InternalQueryExecutor execute_internal_masking_secrets;

    /// Execute a query internally and return the first cell of the result raw and unescaped
    /// (for free-form text with its own newlines, e.g. a pre-rendered documentation blob).
    ScalarQueryExecutor execute_scalar;

    /// Run a query through the normal client path: it is echoed and executed on the user's
    /// connection, and its output is displayed exactly as if the user typed the query.
    /// When `readonly` is set, the query is validated to be read-only and runs under
    /// restrictive limits. Returns a text summary of the outcome for the model.
    std::function<String(const String & query, bool readonly, bool allow_schema_access)> run_visible;

    /// Ask the user to confirm running a query. Returns false when declined.
    std::function<bool(const String & query)> confirm_query;

    /// Decide whether the agent may run a query through the confirmed tool in the current session
    /// state, and whether the user has to confirm it. This also syntax-checks the query, so a
    /// malformed one is reported back for correction instead of being confirmed and failing.
    std::function<AIQueryRunDecision(const String & query)> check_query;

    /// The restrictions of the current session, appended to the system prompt so the model knows
    /// what the session does not allow (an empty string when it allows everything). Evaluated
    /// before every model call: a confirmed `SET readonly = 1` changes it mid-conversation.
    std::function<String()> session_restrictions;

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
/// When `enable_schema_access` is false, the internal schema exploration tools are omitted and
/// unconfirmed queries cannot inspect schema metadata.
/// When `enable_query_log_access` is false, `read_query_log` is omitted. When the hook reports
/// that access is unsafe, the tool fails closed without querying the log.
ai::ToolSet buildAIAgentToolSet(const AIAgentHooks & hooks, bool enable_schema_access, bool enable_query_log_access);

}

#pragma once

#include <functional>
#include <string>
#include <vector>
#include <Client/AI/AIConfiguration.h>
#include <Core/Names.h>
#include <ai/ai.h>

namespace DB
{

/// Executes a query internally (without displaying it to the user) and returns the result
/// formatted as tab-separated text with a header line. Throws on error.
/// `params` are query parameters referenced from the query text as {name:Type}.
using InternalQueryExecutor = std::function<String(const String & query, const NameToNameMap & params)>;

/// Executes a query internally and returns the first cell of the result as a raw, unescaped
/// string (empty when there are no rows). Used where the value is free-form text with its own
/// newlines - the model response of `aiGenerate`, or a pre-rendered documentation blob - that
/// TSV escaping would mangle. Throws on error.
using ScalarQueryExecutor = std::function<String(const String & query, const NameToNameMap & params)>;

/// The result of one model step of the agent loop: the assistant's visible text
/// and the tool calls it requested (not executed yet).
struct AIAgentStep
{
    String text;
    std::vector<ai::ToolCall> tool_calls;
    String error; /// non-empty when the step failed
    ai::Usage usage;

    bool ok() const { return error.empty(); }
};

/// A backend that can run one step of the agent conversation:
/// send the conversation to a model and get back text and tool calls.
class IAIAgentTransport
{
public:
    virtual ~IAIAgentTransport() = default;

    virtual AIAgentStep step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools) = 0;

    /// Human-readable description of the backend, e.g. `anthropic (claude-sonnet-4-5)`.
    virtual String description() const = 0;
};

/// Talks to an AI provider directly through the ai-sdk-cpp client, using native tool calling.
class AIClientTransport : public IAIAgentTransport
{
public:
    AIClientTransport(ai::Client client_, const AIConfiguration & config_);

    AIAgentStep step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools) override;
    String description() const override;

private:
    ai::Client client;
    AIConfiguration config;
    String model;
};

/// Uses the `aiGenerate` function of the connected server (or of clickhouse-local) as the
/// model backend when no client-side AI provider is configured. Tool calling is implemented
/// as a text protocol on top of plain text generation: the conversation is rendered into a
/// single prompt, and tool calls are parsed back from <tool_call>...</tool_call> blocks
/// of the response.
class AIServerFunctionTransport : public IAIAgentTransport
{
public:
    explicit AIServerFunctionTransport(ScalarQueryExecutor executor_);

    /// Whether the connected server has the `aiGenerate` function together with default
    /// credentials for it (a non-empty `ai_function_text_default_credentials` setting).
    static bool isAvailable(const ScalarQueryExecutor & executor);

    AIAgentStep step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools) override;
    String description() const override;

    /// The name the agent recognizes to report a malformed tool call block back to the model.
    static constexpr auto malformed_tool_call_name = "__malformed_tool_call__";

    /// Render the conversation and the tool protocol into plain text. Exposed for tests.
    static String renderSystemPrompt(const String & system_prompt, const ai::ToolSet & tools);
    static String renderConversation(const ai::Messages & messages);

    /// Extract the assistant text and the tool calls from a model response. Malformed
    /// tool call blocks are turned into calls of `malformed_tool_call_name`, so the error
    /// is reported back to the model instead of being silently dropped. Exposed for tests.
    static AIAgentStep parseResponse(const String & response, size_t & call_id_counter);

private:
    ScalarQueryExecutor executor;
    size_t call_id_counter = 1;
};

}

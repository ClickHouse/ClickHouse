#pragma once

#include <functional>
#include <string>
#include <vector>
#include <Client/AI/AIConfiguration.h>
#include <Core/Names.h>
#include <base/types.h>
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

    [[nodiscard]] bool ok() const { return error.empty(); }
};

/// A backend that can run one step of the agent conversation:
/// send the conversation to a model and get back text and tool calls.
class IAIAgentTransport
{
public:
    virtual ~IAIAgentTransport() = default;

    virtual AIAgentStep step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools) = 0;

    /// Human-readable description of the backend, e.g. `anthropic (claude-sonnet-4-5)`.
    [[nodiscard]] virtual String description() const = 0;
};

/// Talks to an AI provider directly through the ai-sdk-cpp client, using native tool calling.
class AIClientTransport : public IAIAgentTransport
{
public:
    AIClientTransport(ai::Client client_, const AIConfiguration & config_);

    AIAgentStep step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools) override;
    [[nodiscard]] String description() const override;

private:
    ai::Client client;
    AIConfiguration config;
    String model;
};

/// Uses the `aiGenerate` function of the connected server (or of clickhouse-local) as the
/// model backend when no client-side AI provider is configured. Tool calling is implemented
/// as a text protocol on top of plain text generation: the conversation is rendered into a
/// single prompt, and tool calls are parsed back from the `<tool_call-TOKEN>...</tool_call-TOKEN>`
/// blocks of the response, where `TOKEN` is a random token of the conversation.
class AIServerFunctionTransport : public IAIAgentTransport
{
public:
    AIServerFunctionTransport(ScalarQueryExecutor executor_, const AIConfiguration & config_);

    /// Whether the connected server has the `aiGenerate` function enabled (the
    /// `allow_ai_functions` setting, named `allow_experimental_ai_functions` on older servers)
    /// together with default credentials for it (a non-empty
    /// `ai_function_text_default_credentials` setting).
    static bool isAvailable(const ScalarQueryExecutor & executor);

    AIAgentStep step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools) override;
    [[nodiscard]] String description() const override;

    /// The name the agent recognizes to report a malformed tool call block back to the model.
    static constexpr auto malformed_tool_call_name = "__malformed_tool_call__";

    /// Render the conversation and the tool protocol into plain text. Exposed for tests.
    ///
    /// `call_token` is the token of the conversation (see the member of the same name): the tags
    /// of a tool call are written with it, `<tool_call-TOKEN>` and `</tool_call-TOKEN>`. The
    /// transport always passes its own. With an empty one the tags are bare, which is how the
    /// tests of the format of the transcript write them.
    static String renderSystemPrompt(const String & system_prompt, const ai::ToolSet & tools, std::string_view call_token = {});
    static String renderConversation(const ai::Messages & messages, std::string_view call_token = {});

    /// One message of the transcript, with everything the transport did not write itself quoted.
    static String renderMessage(const ai::Message & message, std::string_view call_token = {});

    /// The same transcript held to `max_bytes`, by giving up whole messages rather than raw bytes
    /// off the front: the question of the current turn is kept (cut down to half of the budget if
    /// it is oversized on its own), and the newest steps of the turn fill what is left. Exposed
    /// for tests.
    static String renderConversationWithinBudget(const ai::Messages & messages, size_t max_bytes, std::string_view call_token = {});

    /// Extract the assistant text and the tool calls from a model response. Malformed
    /// tool call blocks are turned into calls of `malformed_tool_call_name`, so the error
    /// is reported back to the model instead of being silently dropped. Only a block whose tags
    /// carry `call_token` is a call: any other one, a bare `<tool_call>` block included, is a part
    /// of the text. Exposed for tests.
    static AIAgentStep parseResponse(const String & response, size_t & call_id_counter, std::string_view call_token = {});

private:
    ScalarQueryExecutor executor;
    AIConfiguration config;
    size_t call_id_counter = 1;

    /// The model answers with text, and a tool call is a block of that text. So whatever the model
    /// writes in the form of a block would be a call, also when it is not meant as one: an example
    /// of the syntax the user asked to see, or a block from a query result that the model repeats.
    /// The tools that need no confirmation would run from it. So the tags of a call carry a token,
    /// random for every conversation, that appears only in the system prompt and in the calls of
    /// the earlier turns of the transcript: a text that does not come from the prompt cannot have
    /// it, and the model is told to write a block it only shows without it.
    String call_token;
};

}

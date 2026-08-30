#pragma once

#include <memory>
#include <ostream>
#include <Client/AI/AIAgentDisplay.h>
#include <Client/AI/AIAgentTools.h>
#include <Client/AI/AIAgentTransport.h>
#include <Client/AI/AIConfiguration.h>
#include <Client/AI/QueryContextBuffer.h>

namespace DB
{

/// The AI agent embedded into the interactive client (the `?` and `??` commands):
/// a conversation loop between the user, a model backend (an API provider or the
/// server-side `aiGenerate` function) and tools operating on the user's connection.
/// The conversation persists across invocations within the client session.
class AIAgent
{
public:
    AIAgent(
        const AIConfiguration & config_,
        std::unique_ptr<IAIAgentTransport> transport_,
        const AIAgentHooks & hooks_,
        std::shared_ptr<QueryContextBuffer> query_context_,
        std::ostream & output_stream,
        bool use_colors);

    /// Run one turn of the conversation.
    void chat(const String & user_text);

    /// Forget the conversation.
    void reset();

    /// A short status line for the bare `?` command.
    String status() const;

    bool schemaAccessEnabled() const { return config.enable_schema_access; }

private:
    AIConfiguration config;
    std::unique_ptr<IAIAgentTransport> transport;
    AIAgentHooks hooks;
    ai::ToolSet tools;
    std::shared_ptr<QueryContextBuffer> query_context;
    AIAgentDisplay display;

    ai::Messages messages;
    UInt64 last_seen_seqno = 0;
    size_t used_prompt_tokens = 0;
    size_t used_completion_tokens = 0;

    /// Whether `read_query_log` is currently offered to the model. The tool set is rebuilt when
    /// the session state stops or starts allowing it.
    bool query_log_access_enabled = true;

    /// The history must not grow without a bound: old turns are dropped from the front, and the
    /// tool results of a turn that is over the budget on its own are elided. Both caps matter:
    /// the message count bounds the number of turns, and the byte budget bounds the prompt
    /// itself - a few large tool results (a query log read, a documentation article) would
    /// outgrow the context window of the provider long before the count cap.
    static constexpr size_t max_history_messages = 80;
    static constexpr size_t max_history_bytes = 256 * 1024;

    /// A single stored tool result is truncated to this size, so that one oversized result
    /// cannot displace the whole rest of the conversation from the byte budget.
    static constexpr size_t max_tool_result_bytes = 32 * 1024;

    String systemPrompt() const;
    void refreshToolSet();
    void pushUserMessage(const String & text);
    void trimHistory();
    /// Replace the payload of the oldest tool results of the history until `total_bytes` (the
    /// size of the history at the call) is within the byte budget. The results of the newest
    /// tool-results message go last, and only its final result is guaranteed to survive: one
    /// step may return several large results at once, so keeping the whole message would leave
    /// the budget broken. Returns the size of the history that is left.
    size_t elideOldestToolResults(size_t total_bytes);
    /// Cut the question of the current turn down to the byte budget, with a notice of what was
    /// left out. This is the last resort: the question itself is never dropped, so an oversized
    /// one (a pasted log) would otherwise break the budget on its own.
    void truncateCurrentQuestion(size_t total_bytes);
    static ai::JsonValue truncateOversizedToolResult(ai::JsonValue value);
    ai::ToolResult executeToolCall(const ai::ToolCall & call);
};

}

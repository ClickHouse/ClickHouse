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
        const AIAgentHooks & hooks,
        std::shared_ptr<QueryContextBuffer> query_context_,
        std::ostream & output_stream,
        bool use_colors);

    /// Run one turn of the conversation.
    void chat(const String & user_text);

    /// Forget the conversation.
    void reset();

    /// A short status line for the bare `?` command.
    String status() const;

private:
    AIConfiguration config;
    std::unique_ptr<IAIAgentTransport> transport;
    ai::ToolSet tools;
    std::shared_ptr<QueryContextBuffer> query_context;
    AIAgentDisplay display;

    ai::Messages messages;
    UInt64 last_seen_seqno = 0;
    size_t used_prompt_tokens = 0;
    size_t used_completion_tokens = 0;

    /// The history must not grow without a bound: old turns are dropped from the front.
    static constexpr size_t max_history_messages = 80;

    String systemPrompt() const;
    void pushUserMessage(const String & text);
    void trimHistory();
    ai::ToolResult executeToolCall(const ai::ToolCall & call);
};

}

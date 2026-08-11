#include <Client/AI/AIAgent.h>

#include <Client/AI/AIPrompts.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/scope_guard.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

/// A compact rendering of the tool call arguments for the terminal. The queries of the
/// run tools are not shown here: they are echoed in full by the query display itself.
String summarizeArguments(const ai::ToolCall & call)
{
    if (call.tool_name == "run_readonly_query" || call.tool_name == "run_query")
        return "";

    if (!call.arguments.is_object() || call.arguments.empty())
        return "";

    WriteBufferFromOwnString out;
    bool first = true;
    for (const auto & [key, value] : call.arguments.items())
    {
        if (!first)
            writeString(", ", out);
        first = false;
        writeString(key, out);
        writeString(": ", out);
        writeString(value.is_string() ? value.get<std::string>() : value.dump(), out);
        if (out.count() > 100)
            break;
    }

    String summary = out.str();
    if (summary.size() > 100)
    {
        summary.resize(100);
        summary += "…";
    }
    return "(" + summary + ")";
}

/// Whether the tool reported success (tools return {"success": false, "error": ...} for
/// application-level failures without raising an executor-level error).
bool toolSucceeded(const ai::ToolResult & result)
{
    if (!result.is_success())
        return false;
    return !(result.result.is_object() && result.result.contains("success") && result.result["success"] == false);
}

String summarizeToolResult(const ai::ToolResult & result)
{
    if (!result.is_success())
        return result.error_message();
    if (result.result.is_object())
    {
        if (result.result.contains("error") && result.result["error"].is_string())
            return result.result["error"].get<std::string>();
        if (result.result.contains("result") && result.result["result"].is_string())
            return result.result["result"].get<std::string>();
    }
    return result.result.dump();
}

}

AIAgent::AIAgent(
    const AIConfiguration & config_,
    std::unique_ptr<IAIAgentTransport> transport_,
    const AIAgentHooks & hooks,
    std::shared_ptr<QueryContextBuffer> query_context_,
    std::ostream & output_stream,
    bool use_colors)
    : config(config_)
    , transport(std::move(transport_))
    , tools(buildAIAgentToolSet(hooks, config_.enable_schema_access))
    , query_context(std::move(query_context_))
    , display(output_stream, use_colors)
{
}

String AIAgent::systemPrompt() const
{
    if (!config.system_prompt.empty())
        return config.system_prompt;
    return AIPrompts::AGENT_SYSTEM_PROMPT;
}

void AIAgent::pushUserMessage(const String & text)
{
    /// After a failed step (a transport error, an empty response, or an exception) the history
    /// may already end with a user message. Providers require alternating roles, and merging
    /// the new question into the stale unanswered message would corrupt the turn boundaries,
    /// so the dangling turn is closed with a synthetic assistant message and a fresh user turn
    /// starts after it. For a trailing tool-results message this is also required for another
    /// reason: free-form text must not be mixed into it (the server-function transport
    /// serializes such a message as tool results only, dropping the text).
    if (!messages.empty() && messages.back().role == ai::kMessageRoleUser)
    {
        if (messages.back().has_tool_results())
            messages.push_back(ai::Message::assistant("(the model call failed after these tool results)"));
        else
            messages.push_back(ai::Message::assistant("(no response was produced for this message)"));
    }
    messages.push_back(ai::Message::user(text));
}

void AIAgent::chat(const String & user_text)
{
    String full_text;
    if (query_context)
    {
        String recent = query_context->format(last_seen_seqno, /*skip_ai_initiated=*/ true);
        last_seen_seqno = query_context->latestSeqno();
        if (!recent.empty())
            full_text = fmt::format("<recent_queries>\n{}\n\n{}</recent_queries>\n\n", AIPrompts::RECENT_QUERIES_HEADER, recent);
    }
    full_text += user_text;

    pushUserMessage(full_text);
    trimHistory();

    const String system_prompt = systemPrompt();

    for (size_t step_index = 0; step_index < config.max_steps; ++step_index)
    {
        AIAgentStep step;
        {
            /// The transports report failures as `step.error`, but if one ever throws, the
            /// thinking animation thread must not be left repainting the terminal.
            display.startThinking(step_index + 1);
            SCOPE_EXIT(display.stopThinking());
            step = transport->step(system_prompt, messages, tools);
        }

        if (!step.ok())
        {
            display.showError(step.error);
            return;
        }

        used_prompt_tokens += step.usage.prompt_tokens;
        used_completion_tokens += step.usage.completion_tokens;

        if (step.tool_calls.empty())
        {
            /// No tool calls: the text is the final answer of this turn.
            if (step.text.empty())
                display.showNotice("(the model returned an empty response)");
            else
            {
                display.showAssistantText(step.text, /*final=*/ true);
                messages.push_back(ai::Message::assistant(step.text));
            }
            return;
        }

        display.showAssistantText(step.text, /*final=*/ false);

        std::vector<ai::ToolCallContentPart> call_parts;
        call_parts.reserve(step.tool_calls.size());
        for (const auto & call : step.tool_calls)
            call_parts.emplace_back(call.id, call.tool_name, call.arguments);
        messages.push_back(ai::Message::assistant_with_tools(step.text, call_parts));

        std::vector<ai::ToolResultContentPart> result_parts;
        result_parts.reserve(step.tool_calls.size());
        for (const auto & call : step.tool_calls)
        {
            display.showToolCall(call.tool_name, summarizeArguments(call));

            ai::ToolResult result = executeToolCall(call);
            display.showToolResult(toolSucceeded(result), summarizeToolResult(result));

            if (result.is_success())
                result_parts.emplace_back(call.id, result.result, false);
            else
                result_parts.emplace_back(call.id, ai::JsonValue{{"error", result.error_message()}}, true);
        }
        messages.push_back(ai::Message::tool_results(result_parts));
    }

    display.showNotice(fmt::format(
        "Reached the limit of {} steps per turn (the `ai.max_steps` configuration parameter). "
        "Ask to continue if the task is not finished.",
        config.max_steps));
}

ai::ToolResult AIAgent::executeToolCall(const ai::ToolCall & call)
{
    /// A tool call block the server-function transport could not parse: report it back
    /// to the model so it can correct itself.
    if (call.tool_name == AIServerFunctionTransport::malformed_tool_call_name)
        return ai::ToolResult(
            call.id,
            call.tool_name,
            call.arguments,
            std::string(
                "Your <tool_call> block could not be parsed ("
                + call.arguments.value("parse_error", "invalid JSON")
                + R"(). Emit exactly: <tool_call>{"name": "<tool>", "arguments": {...}}</tool_call> with valid JSON inside.)"));

    return ai::ToolExecutor::execute_tool(call, tools, messages);
}

void AIAgent::trimHistory()
{
    if (messages.size() <= max_history_messages)
        return;

    /// Drop the oldest turns. The history must keep starting at a plain user message:
    /// tool results without the assistant message that requested them are rejected
    /// by the providers.
    size_t drop = messages.size() - max_history_messages;
    while (drop < messages.size()
           && (messages[drop].role != ai::kMessageRoleUser || messages[drop].has_tool_results()))
        ++drop;

    messages.erase(messages.begin(), messages.begin() + drop);
}

void AIAgent::reset()
{
    messages.clear();
    /// Baseline to the present: the buffered recent-query history was part of the conversation
    /// being cleared, so it must not be replayed into the first turn of the next one.
    if (query_context)
        last_seen_seqno = query_context->latestSeqno();
}

String AIAgent::status() const
{
    return fmt::format(
        "backend: {}; conversation: {} message{}; tokens used in this session: {} prompt + {} completion",
        transport->description(),
        messages.size(),
        messages.size() == 1 ? "" : "s",
        used_prompt_tokens,
        used_completion_tokens);
}

}

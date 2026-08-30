#include <Client/AI/AIAgent.h>

#include <Client/AI/AIPrompts.h>
#include <Client/AI/AITextTruncation.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/scope_guard.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

String escapeRecentQueryContext(const String & text)
{
    String escaped;
    escaped.reserve(text.size());
    for (char c : text)
    {
        switch (c)
        {
            case '&': escaped += "&amp;"; break;
            case '<': escaped += "&lt;"; break;
            case '>': escaped += "&gt;"; break;
            default: escaped += c; break;
        }
    }
    return escaped;
}

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

/// The approximate byte contribution of one message to the prompt.
size_t messageBytes(const ai::Message & message)
{
    size_t bytes = message.get_text().size();
    for (const auto & call : message.get_tool_calls())
        bytes += call.tool_name.size() + call.arguments.dump().size();
    for (const auto & result : message.get_tool_results())
        bytes += result.result.dump().size();
    return bytes;
}

}

AIAgent::AIAgent(
    const AIConfiguration & config_,
    std::unique_ptr<IAIAgentTransport> transport_,
    const AIAgentHooks & hooks_,
    std::shared_ptr<QueryContextBuffer> query_context_,
    std::ostream & output_stream,
    bool use_colors)
    : config(config_)
    , transport(std::move(transport_))
    , hooks(hooks_)
    /// The tool set is rebuilt by `refreshToolSet` before the first turn, when the session state
    /// (which requires a query to the server) is known.
    , tools(buildAIAgentToolSet(hooks, config_.enable_schema_access, /*enable_query_log_access=*/ true))
    , query_context(std::move(query_context_))
    , display(output_stream, use_colors)
{
}

void AIAgent::refreshToolSet()
{
    /// The query log of the user can only be read while the queries of the agent itself can be
    /// told apart from theirs, which a session with `readonly = 1` does not allow (the marker is a
    /// setting). This is session state, so it can change between the turns.
    const bool query_log_access = !hooks.can_read_query_log || hooks.can_read_query_log();
    if (query_log_access == query_log_access_enabled)
        return;

    query_log_access_enabled = query_log_access;
    tools = buildAIAgentToolSet(hooks, config.enable_schema_access, query_log_access_enabled);
}

String AIAgent::systemPrompt() const
{
    String prompt = config.system_prompt.empty() ? AIPrompts::AGENT_SYSTEM_PROMPT : config.system_prompt;

    /// What the session does not allow is part of the description of the environment, and the
    /// user can change it (a `SET readonly = 1` the model itself asked to run), so it is queried
    /// again for every model call rather than baked into the prompt once.
    if (hooks.session_restrictions)
    {
        if (const String restrictions = hooks.session_restrictions(); !restrictions.empty())
            prompt += "\n\n" + restrictions;
    }

    return prompt;
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
    refreshToolSet();

    String full_text;
    if (query_context)
    {
        String recent = query_context->format(last_seen_seqno, /*skip_ai_initiated=*/ true);
        last_seen_seqno = query_context->latestSeqno();
        if (!recent.empty())
            full_text = fmt::format("<recent_queries>\n{}\n\n{}</recent_queries>\n\n", AIPrompts::RECENT_QUERIES_HEADER, escapeRecentQueryContext(recent));
    }
    full_text += user_text;

    pushUserMessage(full_text);
    trimHistory();

    for (size_t step_index = 0; step_index < config.max_steps; ++step_index)
    {
        /// Built for every step: a query confirmed in the previous step can have changed the
        /// session (`SET readonly = 1`), and the model must see the new restrictions right away.
        const String system_prompt = systemPrompt();

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
                result_parts.emplace_back(call.id, truncateOversizedToolResult(std::move(result.result)), false);
            else
                result_parts.emplace_back(call.id, ai::JsonValue{{"error", result.error_message()}}, true);
        }
        messages.push_back(ai::Message::tool_results(result_parts));
        /// The history is trimmed whenever it grows, not once per turn: every step of one turn
        /// appends the tool calls of the model and their results, so a single long turn would
        /// otherwise grow the prompt of the next step without a bound.
        trimHistory();
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

ai::JsonValue AIAgent::truncateOversizedToolResult(ai::JsonValue value)
{
    /// Our tools return objects with the payload in a `result` string; truncate the payload
    /// itself so the object stays well-formed for the model.
    if (value.is_object() && value.contains("result") && value["result"].is_string())
    {
        auto text = value["result"].get<std::string>();
        if (text.size() <= max_tool_result_bytes)
            return value;
        const size_t original_size = text.size();
        truncateToUTF8Boundary(text, max_tool_result_bytes);
        value["result"] = std::move(text);
        value["truncated"] = fmt::format(
            "The result was cut to the first {} of its {} bytes to fit the conversation. "
            "Re-run with a stricter filter or a smaller limit to see the rest.",
            value["result"].get_ref<const std::string &>().size(), original_size);
        return value;
    }

    String dumped = value.dump();
    if (dumped.size() <= max_tool_result_bytes)
        return value;
    const size_t original_size = dumped.size();
    truncateToUTF8Boundary(dumped, max_tool_result_bytes);
    return ai::JsonValue{
        {"result", std::move(dumped)},
        {"truncated", fmt::format(
            "The result was cut to its first {} bytes (of {}) to fit the conversation. "
            "Re-run with a stricter filter or a smaller limit to see the rest.",
            max_tool_result_bytes, original_size)}};
}

void AIAgent::trimHistory()
{
    /// The history must keep starting at a plain user message: tool results without the
    /// assistant message that requested them are rejected by the providers.
    auto next_turn_start = [&](size_t pos)
    {
        while (pos < messages.size()
               && (messages[pos].role != ai::kMessageRoleUser || messages[pos].has_tool_results()))
            ++pos;
        return pos;
    };

    /// Drop the oldest turns over the message-count cap.
    size_t drop = 0;
    if (messages.size() > max_history_messages)
        drop = next_turn_start(messages.size() - max_history_messages);

    /// Then keep dropping whole turns while the prompt is over the byte budget, but never the
    /// last user message - the question of the current turn.
    size_t total_bytes = 0;
    for (size_t i = drop; i < messages.size(); ++i)
        total_bytes += messageBytes(messages[i]);

    while (total_bytes > max_history_bytes)
    {
        const size_t next = next_turn_start(drop + 1);
        if (next >= messages.size())
            break;
        for (size_t i = drop; i < next; ++i)
            total_bytes -= messageBytes(messages[i]);
        drop = next;
    }

    messages.erase(messages.begin(), messages.begin() + drop);

    truncateCurrentQuestion(elideOldestToolResults(total_bytes));
}

size_t AIAgent::elideOldestToolResults(size_t total_bytes)
{
    /// Dropping whole turns cannot bound a single long turn: its steps keep appending tool calls
    /// and their results after the question of the turn, which must stay. Give up the payload of
    /// the oldest tool results instead. They are elided in place rather than removed, so that
    /// every tool call keeps the result that answers it - providers reject an unanswered call.

    /// The newest tool results are what the model is about to reason over, so they are kept
    /// when possible: the messages before the last tool-results message of the history go first.
    size_t last = messages.size();
    while (last > 0 && !messages[last - 1].has_tool_results())
        --last;
    if (last > 0)
        --last;

    const ai::JsonValue elided{
        {"elided",
         "This result was dropped from the conversation to fit its size budget. "
         "Re-run the tool with a stricter filter or a smaller limit if you still need it."}};
    const size_t elided_bytes = elided.dump().size();

    const auto elide = [&](ai::ToolResultContentPart & result)
    {
        /// Nothing to gain from an already elided or a small result, and the accounting below
        /// must not go backwards.
        const size_t result_bytes = result.result.dump().size();
        if (result_bytes <= elided_bytes)
            return;

        result.result = elided;
        total_bytes -= result_bytes - elided_bytes;
    };

    for (size_t i = 0; i < last && total_bytes > max_history_bytes; ++i)
        for (auto & part : messages[i].content)
            if (auto * result = std::get_if<ai::ToolResultContentPart>(&part))
                elide(*result);

    /// One step may return several tool calls at once, so the last tool-results message alone
    /// can be over the budget even though each of its results is within the per-result cap.
    /// Elide its results too, oldest first, but keep the final one: the model must have at
    /// least something left to reason over, and one result is bounded by the per-result cap.
    if (total_bytes > max_history_bytes && last < messages.size() && messages[last].has_tool_results())
    {
        auto & content = messages[last].content;
        size_t final_result_pos = 0;
        for (size_t i = 0; i < content.size(); ++i)
            if (std::holds_alternative<ai::ToolResultContentPart>(content[i]))
                final_result_pos = i;
        for (size_t i = 0; i < final_result_pos && total_bytes > max_history_bytes; ++i)
            if (auto * result = std::get_if<ai::ToolResultContentPart>(&content[i]))
                elide(*result);
    }

    return total_bytes;
}

void AIAgent::truncateCurrentQuestion(size_t total_bytes)
{
    static constexpr std::string_view QUESTION_CUT_NOTICE
        = "\n\n[The question was cut here: {} of its {} bytes did not fit the conversation. "
          "Answer what is left, or ask the user to send the rest in smaller parts.]";

    /// The question of the current turn is never dropped and never elided - it is what the model
    /// is asked about - so a single oversized question (a pasted log, a long instruction) keeps
    /// the prompt over the budget however much of the older history is given up. Cut it instead,
    /// keeping its beginning, and say by how much: the model then sees a question it can answer
    /// rather than a request the provider rejects for its size.
    if (total_bytes <= max_history_bytes)
        return;

    size_t question = messages.size();
    while (question > 0
           && (messages[question - 1].role != ai::kMessageRoleUser || messages[question - 1].has_tool_results()))
        --question;
    if (question == 0)
        return;
    --question;

    auto & content = messages[question].content;
    if (content.empty())
        return;
    auto * text_part = std::get_if<ai::TextContentPart>(&content.front());
    if (!text_part)
        return;

    const size_t original_size = text_part->text.size();
    const size_t excess = total_bytes - max_history_bytes;

    /// The notice is appended to the kept part, so it has to fit in the budget as well. Its size
    /// is measured with the largest numbers it can carry, so the final one is never longer.
    const size_t notice_bytes = fmt::format(QUESTION_CUT_NOTICE, original_size, original_size).size();
    const size_t keep = original_size > excess + notice_bytes ? original_size - excess - notice_bytes : 0;
    if (keep >= original_size)
        return;

    truncateToUTF8Boundary(text_part->text, keep);
    text_part->text += fmt::format(QUESTION_CUT_NOTICE, original_size - text_part->text.size(), original_size);
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

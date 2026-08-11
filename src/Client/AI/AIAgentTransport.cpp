#include <Client/AI/AIAgentTransport.h>

#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <ai/logger.h>

namespace DB
{

/// AIClientTransport

AIClientTransport::AIClientTransport(ai::Client client_, const AIConfiguration & config_)
    : client(std::move(client_))
    , config(config_)
    , model(config_.model.empty() ? client.default_model() : config_.model)
{
    /// The SDK's default `ConsoleLogger` prints INFO lines (e.g. "Text generation successful")
    /// to stdout, which would corrupt the client's terminal output. Silence it.
    ai::logger::install_logger(std::make_shared<ai::logger::NullLogger>());
}

AIAgentStep AIClientTransport::step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools)
{
    ai::GenerateOptions options;
    options.model = model;
    options.system = system_prompt;
    options.messages = messages;
    options.max_tokens = static_cast<int>(config.max_tokens);
    options.temperature = config.temperature;

    /// The agent runs the tool loop itself (one model call per step), so the tools are passed
    /// as schemas only: the SDK sends their definitions to the provider but does not execute them.
    options.max_steps = 1;
    for (const auto & [name, tool] : tools)
        options.tools.emplace(name, ai::create_tool_schema(tool.description, tool.parameters_schema));

    auto result = client.generate_text(options);

    AIAgentStep step;
    if (!result.is_success())
    {
        step.error = result.error_message();
        if (step.error.empty())
            step.error = "AI generation failed";
        return step;
    }

    step.text = result.text;
    step.tool_calls = result.tool_calls;
    step.usage = result.usage;
    return step;
}

String AIClientTransport::description() const
{
    return client.provider_name() + " (" + model + ")";
}

/// AIServerFunctionTransport

namespace
{

void renderToolResultValue(const ai::JsonValue & value, WriteBufferFromOwnString & out)
{
    /// Our tools return objects with a human-readable `result` or `error` field;
    /// render them as plain text and everything else as JSON.
    if (value.is_object() && value.contains("result") && value["result"].is_string())
        writeString(value["result"].get<std::string>(), out);
    else if (value.is_string())
        writeString(value.get<std::string>(), out);
    else
        writeString(value.dump(), out);
}

}

AIServerFunctionTransport::AIServerFunctionTransport(ScalarQueryExecutor executor_)
    : executor(std::move(executor_))
{
}

bool AIServerFunctionTransport::isAvailable(const ScalarQueryExecutor & executor)
{
    try
    {
        auto result = executor(
            "SELECT (SELECT count() FROM system.functions WHERE name = 'aiGenerate') > 0"
            " AND (SELECT count() FROM system.settings WHERE name = 'ai_function_text_default_credentials' AND value != '') > 0",
            {});
        return trim(result, [](char c) { return isWhitespaceASCII(c); }) == "1";
    }
    catch (...)
    {
        /// Ok: this is only a capability probe - an old server without `aiGenerate`, or one that
        /// denies access to `system.functions`/`system.settings`, simply means the transport is
        /// not available. The real error is reported when the transport is actually used.
        return false;
    }
}

String AIServerFunctionTransport::renderSystemPrompt(const String & system_prompt, const ai::ToolSet & tools)
{
    WriteBufferFromOwnString out;
    writeString(system_prompt, out);

    writeString("\n\n## Tool protocol\n\n", out);
    writeString(
        "You have access to the tools listed below. To call a tool, output a block of the form\n"
        "<tool_call>{\"name\": \"<tool name>\", \"arguments\": {...}}</tool_call>\n"
        "exactly, with valid JSON inside. You may output several blocks to call several tools at once, "
        "and any text outside the blocks is shown to the user as your commentary. "
        "After your tool calls, stop and wait: the results will be provided in the next message as "
        "'Tool result [<n>]' entries, in the order of your calls. "
        "When you do not call any tools, your message is the final answer.\n\nAvailable tools:\n",
        out);

    for (const auto & [name, tool] : tools)
    {
        writeString("- ", out);
        writeString(name, out);
        writeString(": ", out);
        writeString(tool.description, out);
        writeString("\n  arguments schema: ", out);
        writeString(tool.parameters_schema.dump(), out);
        writeChar('\n', out);
    }

    return out.str();
}

String AIServerFunctionTransport::renderConversation(const ai::Messages & messages)
{
    WriteBufferFromOwnString out;

    for (const auto & message : messages)
    {
        switch (message.role)
        {
            case ai::kMessageRoleSystem:
                /// The system prompt is passed separately.
                break;
            case ai::kMessageRoleUser:
            {
                if (message.has_tool_results())
                {
                    for (const auto & result : message.get_tool_results())
                    {
                        writeString("Tool result [", out);
                        writeString(result.tool_call_id, out);
                        writeString("]:\n", out);
                        renderToolResultValue(result.result, out);
                        writeString("\n\n", out);
                    }

                    /// A message normally carries either tool results or text, but if both are
                    /// present (should not happen), the text must not be silently dropped.
                    if (const auto text = message.get_text(); !text.empty())
                    {
                        writeString("User:\n", out);
                        writeString(text, out);
                        writeString("\n\n", out);
                    }
                }
                else
                {
                    writeString("User:\n", out);
                    writeString(message.get_text(), out);
                    writeString("\n\n", out);
                }
                break;
            }
            case ai::kMessageRoleAssistant:
            {
                writeString("Assistant:\n", out);
                const auto text = message.get_text();
                if (!text.empty())
                {
                    writeString(text, out);
                    writeChar('\n', out);
                }
                for (const auto & call : message.get_tool_calls())
                {
                    ai::JsonValue rendered{{"name", call.tool_name}, {"arguments", call.arguments}};
                    writeString("<tool_call>", out);
                    writeString(rendered.dump(), out);
                    writeString("</tool_call>\n", out);
                }
                writeChar('\n', out);
                break;
            }
        }
    }

    writeString("Assistant:\n", out);
    return out.str();
}

namespace
{

/// The position right after the end of the JSON value starting at `start` (which must point
/// at `{` or `[`), respecting string literals and escapes, or `npos` if the value never closes.
size_t findJsonValueEnd(const String & s, size_t start)
{
    size_t depth = 0;
    bool in_string = false;
    bool escaped = false;
    for (size_t i = start; i < s.size(); ++i)
    {
        char c = s[i];
        if (in_string)
        {
            if (escaped)
                escaped = false;
            else if (c == '\\')
                escaped = true;
            else if (c == '"')
                in_string = false;
        }
        else if (c == '"')
            in_string = true;
        else if (c == '{' || c == '[')
            ++depth;
        else if (c == '}' || c == ']')
        {
            if (depth > 0)
                --depth;
            if (depth == 0)
                return i + 1;
        }
    }
    return String::npos;
}

}

AIAgentStep AIServerFunctionTransport::parseResponse(const String & response, size_t & call_id_counter)
{
    static constexpr std::string_view open_tag = "<tool_call>";
    static constexpr std::string_view close_tag = "</tool_call>";

    AIAgentStep step;
    WriteBufferFromOwnString text;

    size_t pos = 0;
    while (pos < response.size())
    {
        size_t open_pos = response.find(open_tag, pos);
        if (open_pos == String::npos)
        {
            writeString(response.substr(pos), text);
            break;
        }

        writeString(response.substr(pos, open_pos - pos), text);

        size_t content_pos = open_pos + open_tag.size();

        /// The close tag can legitimately appear inside a JSON string of the arguments
        /// (e.g. a query `SELECT '</tool_call>'`), so the end of the block is found by
        /// scanning the JSON value itself when possible, not by searching for the close tag.
        size_t content_end = String::npos;
        size_t next_pos = String::npos;
        size_t json_pos = content_pos;
        while (json_pos < response.size() && isWhitespaceASCII(response[json_pos]))
            ++json_pos;
        if (json_pos < response.size() && response[json_pos] == '{')
        {
            if (size_t json_end = findJsonValueEnd(response, json_pos); json_end != String::npos)
            {
                size_t tag_pos = json_end;
                while (tag_pos < response.size() && isWhitespaceASCII(response[tag_pos]))
                    ++tag_pos;
                if (response.compare(tag_pos, close_tag.size(), close_tag) == 0)
                {
                    content_end = json_end;
                    next_pos = tag_pos + close_tag.size();
                }
                else if (json_end == response.size())
                {
                    /// An unclosed block ending in a complete JSON value.
                    content_end = json_end;
                    next_pos = json_end;
                }
            }
        }
        if (content_end == String::npos)
        {
            size_t close_pos = response.find(close_tag, content_pos);
            content_end = close_pos == String::npos ? response.size() : close_pos;
            next_pos = close_pos == String::npos ? response.size() : close_pos + close_tag.size();
        }

        String content = response.substr(content_pos, content_end - content_pos);
        pos = next_pos;

        String call_id = "call_" + std::to_string(call_id_counter++);
        try
        {
            ai::JsonValue parsed = ai::JsonValue::parse(content);
            String name = parsed.at("name").get<std::string>();
            ai::JsonValue arguments = parsed.contains("arguments") ? parsed["arguments"] : ai::JsonValue::object();
            /// Some models pass the arguments as a JSON-encoded string.
            if (arguments.is_string())
                arguments = ai::JsonValue::parse(arguments.get<std::string>());
            if (!arguments.is_object())
                throw std::runtime_error("the 'arguments' field is not a JSON object");
            step.tool_calls.emplace_back(call_id, name, arguments);
        }
        catch (const std::exception & e)
        {
            step.tool_calls.emplace_back(call_id, malformed_tool_call_name, ai::JsonValue{{"raw", content}, {"parse_error", e.what()}});
        }
    }

    step.text = trim(text.str(), [](char c) { return isWhitespaceASCII(c); });
    return step;
}

AIAgentStep AIServerFunctionTransport::step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools)
{
    try
    {
        /// The prompt and the system message are inlined as escaped SQL string literals rather than
        /// passed as `{name:String}` query parameters: a String query parameter is read with
        /// escaped-text deserialization, which stops at the first newline, and both values are
        /// multi-line. `quoteString` escapes newlines and quotes so the literal round-trips exactly.
        String response = executor(
            "SELECT aiGenerate(" + quoteString(renderConversation(messages))
                + ", map('system_prompt', " + quoteString(renderSystemPrompt(system_prompt, tools)) + "))",
            {});

        if (response.empty())
        {
            AIAgentStep step;
            step.error = "The aiGenerate function returned an empty response";
            return step;
        }

        return parseResponse(response, call_id_counter);
    }
    catch (const std::exception & e)
    {
        AIAgentStep step;
        step.error = e.what();
        return step;
    }
}

String AIServerFunctionTransport::description() const
{
    return "server-side aiGenerate function";
}

}

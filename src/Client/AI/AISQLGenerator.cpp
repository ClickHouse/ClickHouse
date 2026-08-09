#include <Client/AI/AIClientFactory.h>
#include <Client/AI/AIPrompts.h>
#include <Client/AI/AISQLGenerator.h>
#include <Client/AI/AIToolExecutionDisplay.h>
#include <base/terminalColors.h>
#include <Common/Exception.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NETWORK_ERROR;
}

AISQLGenerator::AISQLGenerator(const AIConfiguration & config_, ai::Client client_, QueryExecutor executor, std::ostream & output_stream_)
    : config(config_)
    , client(std::move(client_))
    , schema_tools(config_.enable_schema_access ? std::make_unique<SchemaExplorationTools>(std::move(executor)) : nullptr)
    , output_stream(output_stream_)
{
}

std::string AISQLGenerator::generateSQL(const std::string & prompt)
{
    try
    {
        AIToolExecutionDisplay display(output_stream, true);

        display.showProgress("Starting AI SQL generation with schema discovery...");
        display.showSeparator();

        // Set up generation options
        ai::GenerateOptions options;
        options.model = getModelString();
        options.system = buildSystemPrompt();
        options.prompt = buildCompletePrompt(prompt);
        options.temperature = config.temperature;
        options.max_tokens = config.max_tokens;
        options.max_steps = static_cast<int>(config.max_steps);
        if (schema_tools)
            options.tools = schema_tools->getToolSet();

        // Set up callbacks to display tool calls in real-time
        options.on_tool_call_start = [&display](const ai::ToolCall & tool_call)
        { display.showToolCall(tool_call.id, tool_call.tool_name, tool_call.arguments.dump()); };

        options.on_tool_call_finish = [&display](const ai::ToolResult & tool_result)
        {
            std::string result_text;
            if (tool_result.is_success())
            {
                // Extract the result string from the JSON if it has a "result" field
                if (tool_result.result.contains("result") && tool_result.result["result"].is_string())
                {
                    result_text = tool_result.result["result"].get<std::string>();
                }
                else
                {
                    result_text = tool_result.result.dump();
                }
            }
            else
            {
                result_text = tool_result.error_message();
            }
            display.showToolResult(tool_result.tool_name, result_text, tool_result.is_success());
        };

        // Show thinking indicator
        display.startThinking();

        // Generate SQL with multi-step support
        auto result = client.generate_text(options);

        // Stop thinking animation
        display.stopThinking();

        if (!result)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AI generation failed: {}", result.error_message());
        }

        display.showSeparator();

        // Display the generated SQL
        const std::string & final_text = result.steps.empty() ? result.text : result.steps.back().text;
        std::string sql = cleanSQL(final_text);
        if (!sql.empty())
        {
            display.showProgress("✨ SQL query generated successfully!");
        }
        else if (result.finish_reason == ai::kFinishReasonToolCalls)
        {
            /// The generation ended by exhausting `max_steps` while the model was still
            /// calling schema exploration tools, before it produced its final answer.
            display.showProgress(fmt::format(
                "⚠️  No SQL query was generated: the schema exploration step limit was reached (ai.max_steps = {})",
                config.max_steps));
        }
        else
        {
            display.showProgress("⚠️  No SQL query was generated");
            /// The model replied with prose instead of a query (e.g. an explanation of
            /// why it cannot be answered) - show it to the user.
            if (!final_text.empty())
                output_stream << final_text << std::endl;
        }

        return sql;
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::NETWORK_ERROR, "AI SQL generation error: {}", e.what());
    }
}

bool AISQLGenerator::isAvailable() const
{
    return client.is_valid();
}

std::string AISQLGenerator::getProviderName() const
{
    return client.provider_name();
}

std::string AISQLGenerator::buildSystemPrompt() const
{
    if (!config.system_prompt.empty())
        return config.system_prompt;

    if (config.enable_schema_access)
        return AIPrompts::SQL_GENERATOR_WITH_SCHEMA_ACCESS;
    else
        return AIPrompts::SQL_GENERATOR_WITHOUT_SCHEMA_ACCESS;
}

std::string AISQLGenerator::buildCompletePrompt(const std::string & user_prompt) const
{
    return std::string(AIPrompts::USER_PROMPT_PREFIX) + user_prompt;
}

std::string AISQLGenerator::cleanSQL(const std::string & sql)
{
    /// The model is instructed to wrap the final query in `<sql>` tags, possibly
    /// surrounded by prose. Extract the outermost tagged block - the first opening and
    /// the last closing tag - because the tags can also appear inside the query itself
    /// (e.g. in a string literal), where an inner pair would truncate the query.
    /// No tags means no query was generated: return an empty string rather than
    /// mistaking the prose for SQL.
    static constexpr std::string_view open_tag = "<sql>";
    static constexpr std::string_view close_tag = "</sql>";

    const size_t start_tag = sql.find(open_tag);
    if (start_tag == std::string::npos)
        return {};
    const size_t query_begin = start_tag + open_tag.size();
    const size_t end_tag = sql.rfind(close_tag);
    if (end_tag == std::string::npos || end_tag < query_begin)
        return {};

    std::string cleaned = sql.substr(query_begin, end_tag - query_begin);

    // Trim whitespace
    cleaned.erase(0, cleaned.find_first_not_of(" \n\r\t"));
    cleaned.erase(cleaned.find_last_not_of(" \n\r\t") + 1);

    // Convert newlines to spaces
    std::replace(cleaned.begin(), cleaned.end(), '\n', ' ');
    std::replace(cleaned.begin(), cleaned.end(), '\r', ' ');

    return cleaned;
}

std::string AISQLGenerator::getModelString() const
{
    if (config.model.empty())
        return client.default_model();
    else
        return config.model;
}

}

#include <Client/AI/AIClientFactory.h>
#include <Client/AI/AISQLGenerator.h>
#include <Client/AI/AIToolExecutionDisplay.h>
#include <base/terminalColors.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NETWORK_ERROR;
}

AISQLGenerator::AISQLGenerator(const AIConfiguration & config_, QueryExecutor executor, std::ostream & output_stream_)
    : config(config_)
    , client(AIClientFactory::createClient(config))
    , schema_tools(std::make_unique<SchemaExplorationTools>(std::move(executor)))
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
        options.max_steps = config.max_steps;
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
        display.showThinking();

        // Generate SQL with multi-step support
        auto result = client.generate_text(options);

        if (!result)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AI generation failed: {}", result.error_message());
        }

        display.showSeparator();

        // Display the generated SQL
        std::string sql = cleanSQL(result.text);
        if (!sql.empty())
        {
            display.showGeneratedQuery(sql);
            display.showSeparator();
            display.showProgress("✨ SQL query generated successfully!");
        }
        else
        {
            display.showProgress("⚠️  No SQL query was generated");
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

    return R"(You are a ClickHouse SQL expert. Your task is to convert natural language queries into valid ClickHouse SQL.

You have access to functions that help you explore the database schema:
- list_databases(): Lists all available databases
- list_tables_in_database(database): Lists all tables in a specific database
- get_schema_for_table(database, table): Gets the CREATE TABLE statement for a specific table

Your workflow should be:
1. Use list_databases() to see available databases
2. Use list_tables_in_database(database) to find relevant tables
3. Use get_schema_for_table(database, table) to understand the table structure
4. Based on the discovered schema, generate the appropriate SQL query

CRITICAL RESPONSE FORMAT:
- During schema exploration: Use tool calls with arguments
- After exploring the schema: You MUST provide a final text response containing ONLY the SQL query
- For final response: Return ONLY the executable SQL query with NO explanations, NO markdown, NO additional text
- If no existing tables are suitable, CREATE a new table as requested

EXAMPLES OF CORRECT FINAL RESPONSES:

User: "Show me all users from the users table"
Assistant: SELECT * FROM users;

User: "Count how many orders were placed yesterday"
Assistant: SELECT COUNT(*) FROM orders WHERE date = yesterday();

User: "Insert 5 rows from S3 into salesforce_data table"
Assistant: INSERT INTO salesforce_data SELECT * FROM s3('s3://bucket/file.csv', 'CSV') LIMIT 5;

User: "Get top 10 customers by revenue"
Assistant: SELECT customer_id, SUM(amount) as revenue FROM orders GROUP BY customer_id ORDER BY revenue DESC LIMIT 10;

IMPORTANT RULES:
- Always explore the schema first before writing SQL
- You can ignore information_schema and system databases typically unless user asks for them
- The functions return actual results that you should use to inform your query
- Pay attention to the actual column names and types discovered in the schema
- Your final SQL query MUST be executable by ClickHouse
- DO NOT include explanations, markdown formatting, or any text other than the SQL query in your final response
- DO NOT say "Here's the SQL query:" or similar phrases
- DO NOT wrap SQL in code blocks or markdown

Remember: You are in an interactive session. Each function call returns real data about the database that you should use to construct accurate queries.)";
}

std::string AISQLGenerator::buildCompletePrompt(const std::string & user_prompt) const
{
    return "Convert this to a ClickHouse SQL query: " + user_prompt;
}

std::string AISQLGenerator::cleanSQL(const std::string & sql)
{
    std::string cleaned = sql;

    // Remove markdown code blocks if present
    if (cleaned.starts_with("```sql"))
    {
        cleaned = cleaned.substr(6);
        auto end_pos = cleaned.find("```");
        if (end_pos != std::string::npos)
            cleaned = cleaned.substr(0, end_pos);
    }
    else if (cleaned.starts_with("```"))
    {
        cleaned = cleaned.substr(3);
        auto end_pos = cleaned.find("```");
        if (end_pos != std::string::npos)
            cleaned = cleaned.substr(0, end_pos);
    }

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
    return config.model;
}

}

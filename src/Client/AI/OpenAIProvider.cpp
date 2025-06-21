#include <Client/AI/OpenAIProvider.h>
#include <Client/AI/OpenAIClient/OpenAIClient.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
}

OpenAIProvider::OpenAIProvider(const AIConfiguration & config_)
    : config(config_)
{
    if (config.api_key.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "OpenAI API key is required");

    if (config.model.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "OpenAI model is required");

    if (config.model_provider.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "OpenAI model provider is required");
}

std::string OpenAIProvider::generateSQL(const std::string & prompt)
{
    try
    {
        OpenAIClient client(config.api_key);
        
        /// Create chat completion request
        OpenAIClient::ChatCompletionRequest request;
        request.model = config.model;
        request.temperature = config.temperature;
        request.max_tokens = config.max_tokens;
        
        /// Add system message
        request.messages.push_back({"system", buildSystemPrompt()});
        
        /// Add user message
        request.messages.push_back({"user", buildCompletePrompt(prompt)});
        
        /// Send request and get response
        auto response = client.createChatCompletion(request);
        
        /// Extract the generated SQL from response
        if (!response.choices.empty())
        {
            std::string sql = response.choices[0].message.content;
            
            /// Clean up the SQL - remove markdown code blocks if present
            if (sql.starts_with("```sql"))
            {
                sql = sql.substr(6); /// Remove ```sql
                auto end_pos = sql.find("```");
                if (end_pos != std::string::npos)
                    sql = sql.substr(0, end_pos);
            }
            else if (sql.starts_with("```"))
            {
                sql = sql.substr(3); /// Remove ```
                auto end_pos = sql.find("```");
                if (end_pos != std::string::npos)
                    sql = sql.substr(0, end_pos);
            }
            
            /// Trim whitespace
            sql.erase(0, sql.find_first_not_of(" \n\r\t"));
            sql.erase(sql.find_last_not_of(" \n\r\t") + 1);

            // convert new lines to spaces
            std::replace(sql.begin(), sql.end(), '\n', ' ');
            std::replace(sql.begin(), sql.end(), '\r', ' ');

            return sql;
        }
        
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No response from OpenAI API");
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::NETWORK_ERROR, "OpenAI API error: {}", e.what());
    }
}

bool OpenAIProvider::isAvailable() const
{
    return !config.api_key.empty();
}

std::string OpenAIProvider::buildSystemPrompt() const
{
    // Use custom system prompt if provided, otherwise use default
    if (!config.system_prompt.empty())
        return config.system_prompt;
    
    return R"(You are a ClickHouse SQL expert. Your task is to convert natural language queries into valid ClickHouse SQL.
Follow these guidelines:
1. Generate only executable SQL queries, no explanations or markdown
2. Use ClickHouse-specific syntax and functions when appropriate
3. Be precise and efficient in your queries
4. If the request is ambiguous, make reasonable assumptions
5. Always return a single SQL query that can be executed directly

Context:
- You're helping a user query their ClickHouse database
- Assume common table and column names unless specified
- Use proper ClickHouse data types and functions)";
}

std::string OpenAIProvider::buildCompletePrompt(const std::string & user_prompt) const
{
    return "Convert this to a ClickHouse SQL query: " + user_prompt;
}

} 

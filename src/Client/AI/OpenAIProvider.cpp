#include <Client/AI/OpenAIProvider.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#if USE_LIBOAI
#include <liboai.h>
#endif

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
#if USE_LIBOAI
    try
    {
        liboai::OpenAI oai;
        
        /// Set API key
        if (!oai.auth.SetKey(config.api_key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to set OpenAI API key");
        
        /// Create conversation with system and user messages
        liboai::Conversation conversation;
        
        /// Add system message for SQL generation context
        if (!conversation.SetSystemData(buildSystemPrompt()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set system prompt");
        
        /// Add user prompt
        if (!conversation.AddUserData(buildCompletePrompt(prompt)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to add user data to conversation");
        
        /// Create chat completion request
        liboai::Response response = oai.ChatCompletion->create(
            config.model,
            conversation,
            std::nullopt,                           /// messages
            config.temperature,                      /// temperature
            std::nullopt,                           /// top_p
            std::nullopt,                           /// n
            std::nullopt,                           /// stream
            std::nullopt,                           /// stop
            config.max_tokens,                      /// max_tokens
            std::nullopt,                           /// presence_penalty
            std::nullopt,                           /// frequency_penalty
            std::nullopt,                           /// logit_bias
            std::nullopt                            /// user
        );
        
        /// Extract the generated SQL from response  
        if (response.raw_json.contains("choices") && response.raw_json["choices"].is_array() && !response.raw_json["choices"].empty())
        {
            auto message = response.raw_json["choices"][0]["message"];
            if (message.contains("content"))
            {
                std::string sql = message["content"].get<std::string>();
                
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
        }
        
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid response format from OpenAI API");
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::NETWORK_ERROR, "OpenAI API error: {}", e.what());
    }
#else
    UNUSED(prompt);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "OpenAI provider is not available. ClickHouse was built without liboai support.");
#endif
}

bool OpenAIProvider::isAvailable() const
{
#if USE_LIBOAI
    return !config.api_key.empty();
#else
    return false;
#endif
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

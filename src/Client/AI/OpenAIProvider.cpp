#include <Client/AI/OpenAIProvider.h>
#include <Client/AI/OpenAIClient/OpenAIClient.h>
#include <Client/AI/OpenAIClient/Conversation.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <sstream>

namespace DB
{

using openai::OpenAIClient;
using openai::Conversation;

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
    /// Use function calling if schema provider is available
    if (schema_provider)
        return generateSQLWithFunctions(prompt);
    
    /// Otherwise use the standard approach
    try
    {
        OpenAIClient client(config.api_key);
        
        /// Create chat completion request
        OpenAIClient::ChatCompletionRequest request;
        request.model = config.model;
        request.temperature = config.temperature;
        request.max_tokens = config.max_tokens;
        
        /// Add system message
        OpenAIClient::ChatCompletionRequest::Message system_msg;
        system_msg.role = "system";
        system_msg.content = buildSystemPrompt();
        request.messages.push_back(system_msg);
        
        /// Add user message
        OpenAIClient::ChatCompletionRequest::Message user_msg;
        user_msg.role = "user";
        user_msg.content = buildCompletePrompt(prompt);
        request.messages.push_back(user_msg);
        
        /// Send request and get response
        auto response = client.createChatCompletion(request);
        
        /// Extract the generated SQL from response
        if (!response.choices.empty())
        {
            std::string sql = response.choices[0].message.content;
            
            return cleanSQL(sql);
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

std::string OpenAIProvider::generateSQLWithFunctions(const std::string & prompt)
{
    try
    {
        OpenAIClient client(config.api_key);
        Conversation conversation;
        
        /// Set system prompt
        conversation.setSystemData(buildSystemPrompt());
        
        /// Add user prompt
        conversation.addUserData(buildCompletePrompt(prompt));
        
        /// Set up function definitions
        conversation.setFunctions(createSchemaFunctions());
        conversation.setFunctionCallMode("auto");
        
        /// Maximum iterations to prevent infinite loops
        const size_t max_iterations = 10;
        std::string final_sql;
        
        for (size_t i = 0; i < max_iterations; ++i)
        {
            /// Create request
            OpenAIClient::ChatCompletionRequest request;
            request.model = config.model;
            request.temperature = config.temperature;
            request.max_tokens = config.max_tokens;
            request.messages = conversation.getMessages();
            request.functions = conversation.getFunctions();
            request.function_call = conversation.getFunctionCallMode();
            
            /// Send request
            auto response = client.createChatCompletion(request);
            
            /// Process response
            if (!processOpenAIResponse(conversation, response))
            {
                /// We got the final SQL response
                final_sql = conversation.getLastResponse();
                break;
            }
        }
        
        if (final_sql.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to generate SQL after {} iterations", max_iterations);
        
        return cleanSQL(final_sql);
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

std::vector<openai::OpenAIClient::FunctionDefinition> OpenAIProvider::createSchemaFunctions() const
{
    std::vector<OpenAIClient::FunctionDefinition> functions;
    
    /// list_databases function
    {
        OpenAIClient::FunctionDefinition func;
        func.name = "list_databases";
        func.description = "List all available databases in the ClickHouse instance";
        func.parameters.type = "object";
        func.parameters.properties = {};
        func.parameters.required = {};
        functions.push_back(func);
    }
    
    /// list_tables_in_database function
    {
        OpenAIClient::FunctionDefinition func;
        func.name = "list_tables_in_database";
        func.description = "List all tables in a specific database";
        func.parameters.type = "object";
        
        OpenAIClient::FunctionParameter db_param;
        db_param.type = "string";
        db_param.description = "The name of the database";
        func.parameters.properties["database"] = db_param;
        
        func.parameters.required = {"database"};
        functions.push_back(func);
    }
    
    /// get_schema_for_table function
    {
        OpenAIClient::FunctionDefinition func;
        func.name = "get_schema_for_table";
        func.description = "Get the CREATE TABLE statement (schema) for a specific table";
        func.parameters.type = "object";
        
        OpenAIClient::FunctionParameter db_param;
        db_param.type = "string";
        db_param.description = "The name of the database";
        func.parameters.properties["database"] = db_param;
        
        OpenAIClient::FunctionParameter table_param;
        table_param.type = "string";
        table_param.description = "The name of the table";
        func.parameters.properties["table"] = table_param;
        
        func.parameters.required = {"database", "table"};
        functions.push_back(func);
    }
    
    return functions;
}

std::string OpenAIProvider::executeFunctionCall(const std::string & function_name, const std::string & arguments) const
{
    try
    {
        /// Parse arguments JSON
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(arguments);
        const Poco::JSON::Object::Ptr & args = result.extract<Poco::JSON::Object::Ptr>();
        
        return executeSchemaFunction(function_name, args);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to parse function arguments: {}", e.displayText());
    }
}

std::string OpenAIProvider::cleanSQL(const std::string & sql)
{
    std::string cleaned = sql;
    
    /// Remove markdown code blocks if present
    if (cleaned.starts_with("```sql"))
    {
        cleaned = cleaned.substr(6); /// Remove ```sql
        auto end_pos = cleaned.find("```");
        if (end_pos != std::string::npos)
            cleaned = cleaned.substr(0, end_pos);
    }
    else if (cleaned.starts_with("```"))
    {
        cleaned = cleaned.substr(3); /// Remove ```
        auto end_pos = cleaned.find("```");
        if (end_pos != std::string::npos)
            cleaned = cleaned.substr(0, end_pos);
    }
    
    /// Trim whitespace
    cleaned.erase(0, cleaned.find_first_not_of(" \n\r\t"));
    cleaned.erase(cleaned.find_last_not_of(" \n\r\t") + 1);
    
    /// Convert new lines to spaces
    std::replace(cleaned.begin(), cleaned.end(), '\n', ' ');
    std::replace(cleaned.begin(), cleaned.end(), '\r', ' ');
    
    return cleaned;
}

bool OpenAIProvider::processOpenAIResponse(openai::Conversation & conversation, 
                                           const openai::OpenAIClient::ChatCompletionResponse & response)
{
    if (!response.choices.empty())
    {
        /// Update conversation with response
        conversation.update(response);
        
        /// Check if we got a function call
        if (conversation.lastResponseIsFunctionCall())
        {
            /// Execute the function call
            std::string function_name = conversation.getLastFunctionCallName();
            std::string arguments = conversation.getLastFunctionCallArguments();
            std::string result = executeFunctionCall(function_name, arguments);
            
            /// Add function result to conversation
            conversation.addFunctionData(function_name, result);
            return true; /// Continue conversation
        }
        
        return false; /// Got final response
    }
    
    throw Exception(ErrorCodes::LOGICAL_ERROR, "No response from OpenAI API");
}

std::string OpenAIProvider::executeSchemaFunction(const std::string & function_name, 
                                                   const Poco::JSON::Object::Ptr & args) const
{
    if (!schema_provider)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema provider not set");
    
    if (function_name == "list_databases")
    {
        auto databases = schema_provider->listDatabases();
        std::ostringstream oss;
        oss << "Available databases: ";
        for (size_t i = 0; i < databases.size(); ++i)
        {
            if (i > 0)
                oss << ", ";
            oss << databases[i];
        }
        return oss.str();
    }
    else if (function_name == "list_tables_in_database")
    {
        std::string database = args->getValue<std::string>("database");
        auto tables = schema_provider->listTablesInDatabase(database);
        std::ostringstream oss;
        oss << "Tables in database '" << database << "': ";
        for (size_t i = 0; i < tables.size(); ++i)
        {
            if (i > 0)
                oss << ", ";
            oss << tables[i];
        }
        return oss.str();
    }
    else if (function_name == "get_schema_for_table")
    {
        std::string database = args->getValue<std::string>("database");
        std::string table = args->getValue<std::string>("table");
        std::string schema = schema_provider->getSchemaForTable(database, table);
        return "Schema for " + database + "." + table + ":\n" + schema;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown function: {}", function_name);
    }
}

} 

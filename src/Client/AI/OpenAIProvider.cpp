#include <Client/AI/OpenAIProvider.h>
#include <Client/AI/OpenAIClient/OpenAIClient.h>
#include <Client/AI/OpenAIClient/Conversation.h>
#include <Client/AI/AIToolExecutionDisplay.h>
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
- For final response: Return ONLY the executable SQL query with NO explanations, NO markdown, NO additional text

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
        AIToolExecutionDisplay display(true);
        
        display.showProgress("Starting AI SQL generation with schema discovery...");
        display.showSeparator();
        
        /// Set system prompt
        std::string system_prompt = buildSystemPrompt();
        conversation.setSystemData(system_prompt);
        
        /// Add user prompt
        std::string user_prompt = buildCompletePrompt(prompt);
        conversation.addUserData(user_prompt);
        
        /// Set up function definitions (we'll use the same format for tools)
        auto functions = createSchemaFunctions();
        conversation.setFunctions(functions);
        
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
            /// Use new tools API
            if (conversation.hasFunctions())
            {
                const auto & funcs = conversation.getFunctions();
                request.tools = std::vector<OpenAIClient::FunctionDefinition>(funcs.begin(), funcs.end());
                
                /// After several iterations, discourage more tool use
                if (i >= max_iterations - 2)
                {
                    request.tool_choice = "none";  // Force the model to respond without tools
                }
                else
                {
                    request.tool_choice = "auto";
                }
            }
            
            /// Show thinking indicator before sending request
            display.showThinking();
            
            /// Send request
            auto response = client.createChatCompletion(request);
            
            /// Process response
            if (!processOpenAIResponse(conversation, response, i, max_iterations))
            {
                /// We got the final SQL response
                final_sql = conversation.getLastResponse();
                break;
            }
        }
        
        if (final_sql.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to generate SQL after {} iterations", max_iterations);
        
        display.showSeparator();
        display.showProgress("✨ SQL query generated successfully!");
        
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
                                           const openai::OpenAIClient::ChatCompletionResponse & response,
                                           size_t iteration, size_t max_iterations)
{
    if (!response.choices.empty())
    {
        /// Update conversation with response
        conversation.update(response);
        
        /// Check if we got function calls
        if (conversation.lastResponseIsFunctionCall())
        {
            /// Get all tool calls from the last message
            const auto & last_msg = conversation.getMessages().back();
            
            /// Process each tool call
            AIToolExecutionDisplay display(true); // Enable colors
            
            for (const auto & tool_call : last_msg.tool_calls)
            {
                /// Execute the function call
                std::string function_name = tool_call.function.name;
                std::string arguments = tool_call.function.arguments;
                std::string tool_call_id = tool_call.id;
                
                display.showToolCall(tool_call_id, function_name, arguments);
                
                std::string result = executeFunctionCall(function_name, arguments);
                
                display.showToolResult(function_name, result, true);
                
                /// Add function result to conversation with the specific tool_call_id
                conversation.addToolData(tool_call_id, result);
            }
            
            /// If we're getting close to max iterations, guide the model to generate SQL
            if (iteration >= max_iterations - 3 && iteration < max_iterations - 1)
            {
                /// Count how many schema-related calls we've made
                size_t schema_calls = 0;
                const auto & messages = conversation.getMessages();
                for (const auto & msg : messages)
                {
                    if (msg.role == "assistant" && !msg.tool_calls.empty())
                        schema_calls++;
                }
                
                /// If we've made enough schema discovery calls, nudge towards SQL generation
                if (schema_calls >= 3)
                {
                    std::string context_msg = "You have gathered information about the database schema. "
                                             "Based on what you've discovered, please generate the SQL query now.";
                    conversation.addUserData(context_msg);
                }
            }
            
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
        oss << "Found " << databases.size() << " databases:\n";
        for (const auto & db : databases)
        {
            oss << "- " << db << "\n";
        }
        return oss.str();
    }
    else if (function_name == "list_tables_in_database")
    {
        std::string database = args->getValue<std::string>("database");
        auto tables = schema_provider->listTablesInDatabase(database);
        std::ostringstream oss;
        oss << "Found " << tables.size() << " tables in database '" << database << "':\n";
        for (const auto & table : tables)
        {
            oss << "- " << table << "\n";
        }
        if (tables.empty())
        {
            oss << "(No tables found in this database)\n";
        }
        return oss.str();
    }
    else if (function_name == "get_schema_for_table")
    {
        std::string database = args->getValue<std::string>("database");
        std::string table = args->getValue<std::string>("table");
        std::string schema = schema_provider->getSchemaForTable(database, table);
        
        if (schema.empty())
        {
            return "Error: Could not retrieve schema for " + database + "." + table + 
                   ". The table might not exist or you might not have permissions to view it.";
        }
        
        return "Schema for " + database + "." + table + ":\n" + schema;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown function: {}", function_name);
    }
}

} 

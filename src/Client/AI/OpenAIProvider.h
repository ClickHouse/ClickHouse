#pragma once

#include <Client/AI/IAIProvider.h>
#include <Client/AI/ISchemaProviderFunctions.h>
#include <Client/AI/OpenAIClient/OpenAIClient.h>
#include <Client/AI/OpenAIClient/Conversation.h>
#include <memory>
#include <Poco/JSON/Object.h>

namespace DB
{

/// OpenAI provider implementation for generating SQL queries
class OpenAIProvider : public IAIProvider
{
public:
    explicit OpenAIProvider(const AIConfiguration & config);

    std::string generateSQL(const std::string & prompt) override;
    
    std::string getName() const override { return "openai"; }
    
    bool isAvailable() const override;
    
    /// Set schema provider for function calling
    void setSchemaProvider(std::shared_ptr<ISchemaProviderFunctions> provider) override
    {
        schema_provider = provider;
    }

private:
    AIConfiguration config;
    std::shared_ptr<ISchemaProviderFunctions> schema_provider;
    
    /// Build the system prompt for SQL generation
    std::string buildSystemPrompt() const;
    
    /// Build the complete prompt with context
    std::string buildCompletePrompt(const std::string & user_prompt) const;
    
    /// Generate SQL with function calling support
    std::string generateSQLWithFunctions(const std::string & prompt);
    
    /// Create function definitions for OpenAI
    std::vector<openai::OpenAIClient::FunctionDefinition> createSchemaFunctions() const;
    
    /// Execute a function call and return the result
    std::string executeFunctionCall(const std::string & function_name, const std::string & arguments) const;
    
    /// Clean SQL from markdown formatting and whitespace
    static std::string cleanSQL(const std::string & sql);
    
    /// Process a single OpenAI response and update conversation
    bool processOpenAIResponse(openai::Conversation & conversation, 
                               const openai::OpenAIClient::ChatCompletionResponse & response,
                               size_t iteration, size_t max_iterations);
    
    /// Execute schema-related function calls
    std::string executeSchemaFunction(const std::string & function_name, 
                                      const Poco::JSON::Object::Ptr & args) const;
};

} 

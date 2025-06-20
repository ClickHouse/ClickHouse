#pragma once

#include <Client/AI/IAIProvider.h>

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

private:
    AIConfiguration config;
    
    /// Build the system prompt for SQL generation
    std::string buildSystemPrompt() const;
    
    /// Build the complete prompt with context
    std::string buildCompletePrompt(const std::string & user_prompt) const;
};

} 

#pragma once

#include <memory>
#include <string>
#include <ostream>
#include <Client/AI/AIConfiguration.h>
#include <Client/AI/SchemaExplorationTools.h>
#include <ai/ai.h>

namespace DB
{

/// Main interface for AI-based SQL generation from natural language
class AISQLGenerator
{
public:
    /// Constructor with configuration, AI client, and query executor
    AISQLGenerator(const AIConfiguration & config, ai::Client client, QueryExecutor executor, std::ostream & output_stream);

    /// Generate SQL from natural language prompt
    /// Returns the generated SQL query on success, throws exception on error
    std::string generateSQL(const std::string & prompt);

    /// Check if the generator is available and properly configured
    bool isAvailable() const;

    /// Get the provider name
    std::string getProviderName() const;

private:
    AIConfiguration config;
    ai::Client client;
    std::unique_ptr<SchemaExplorationTools> schema_tools;
    std::ostream & output_stream;

    /// Build the system prompt for SQL generation
    std::string buildSystemPrompt() const;

    /// Build the complete prompt with context
    std::string buildCompletePrompt(const std::string & user_prompt) const;

    /// Clean SQL from markdown formatting and whitespace
    static std::string cleanSQL(const std::string & sql);

    /// Get the appropriate model string for the provider
    std::string getModelString() const;
};

}

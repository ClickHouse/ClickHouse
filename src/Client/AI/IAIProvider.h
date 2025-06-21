#pragma once

#include "config.h"

#include <memory>
#include <string>

namespace DB
{

class ISchemaProviderFunctions;

/// Configuration for AI providers
struct AIConfiguration
{
    std::string model_provider;
    std::string api_key;
    std::string model;
    double temperature = 0.0;
    size_t max_tokens = 1000;
    size_t timeout_seconds = 30;
    std::string system_prompt; /// Optional custom system prompt
};

/// Base interface for AI providers that can generate SQL queries from natural language
class IAIProvider
{
public:
    virtual ~IAIProvider() = default;

    /// Generate SQL query from natural language prompt
    /// Returns the generated SQL query on success, throws exception on error
    virtual std::string generateSQL(const std::string & prompt) = 0;

    /// Get the name of the provider
    virtual std::string getName() const = 0;

    /// Check if the provider is properly configured and available
    virtual bool isAvailable() const = 0;
    
    /// Set schema provider for function calling
    virtual void setSchemaProvider(std::shared_ptr<ISchemaProviderFunctions> provider) = 0;
};

using AIProviderPtr = std::unique_ptr<IAIProvider>;

}

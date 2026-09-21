#pragma once

#include <string>

namespace DB
{

/// Configuration for AI-based SQL generation
struct AIConfiguration
{
    /// Provider type: "openai" or "anthropic"
    std::string provider;

    /// API key for the provider
    std::string api_key;

    /// Custom API base URL for OpenAI-compatible services (optional)
    std::string base_url;

    /// Model to use (e.g., "gpt-4", "claude-3-opus-20240229")
    std::string model;

    /// Temperature for generation (0.0 = deterministic, higher = more creative)
    double temperature = 0.0;

    /// Maximum tokens to generate
    size_t max_tokens = 1000;

    /// Request timeout in seconds
    size_t timeout_seconds = 30;

    /// Custom system prompt (optional)
    std::string system_prompt;

    /// Maximum steps for multi-step tool calling
    size_t max_steps = 5;

    /// Enable schema access - allows AI to query database/table information
    bool enable_schema_access = true;
};

}

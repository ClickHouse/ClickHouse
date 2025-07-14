#pragma once

#include <Client/AI/AIConfiguration.h>
#include <ai/ai.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Result of AI client creation with metadata
struct AIClientResult
{
    ai::Client client;
    bool inferred_from_env = false;
    std::string provider;
};

/// Factory for creating AI provider clients
class AIClientFactory
{
public:
    /// Create an AI client based on configuration
    static AIClientResult createClient(const AIConfiguration & config);

    /// Load AI configuration from Poco configuration
    static AIConfiguration loadConfiguration(const Poco::Util::AbstractConfiguration & config);
};

}

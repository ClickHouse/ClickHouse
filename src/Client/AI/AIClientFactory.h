#pragma once

#include <ai/ai.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Client/AI/AIConfiguration.h>

namespace DB
{

/// Result of AI client creation with metadata
struct AIClientResult
{
    std::optional<ai::Client> client;
    bool inferred_from_env = false;
    std::string provider;
    bool no_configuration_found = false;
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

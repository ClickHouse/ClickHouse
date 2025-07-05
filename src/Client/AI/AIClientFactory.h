#pragma once

#include <Client/AI/AIConfiguration.h>
#include <ai/ai.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Factory for creating AI provider clients
class AIClientFactory
{
public:
    /// Create an AI client based on configuration
    static ai::Client createClient(const AIConfiguration & config);

    /// Load AI configuration from Poco configuration
    static AIConfiguration loadConfiguration(const Poco::Util::AbstractConfiguration & config);
};

}

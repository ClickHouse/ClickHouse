#pragma once

#include <Client/AI/IAIProvider.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Factory for creating AI providers
class AIProviderFactory
{
public:
    /// Create an AI provider based on configuration
    static AIProviderPtr createProvider(const AIConfiguration & config);
    
    /// Load AI configuration from Poco configuration
    static AIConfiguration loadConfiguration(const Poco::Util::AbstractConfiguration & config);

};

} 


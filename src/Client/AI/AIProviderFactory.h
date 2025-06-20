#pragma once

#include <Client/AI/IAIProvider.h>

namespace DB
{

/// Factory for creating AI providers
class AIProviderFactory
{
public:
    /// Create an AI provider based on configuration
    static AIProviderPtr createProvider(const AIConfiguration & config);

};

} 


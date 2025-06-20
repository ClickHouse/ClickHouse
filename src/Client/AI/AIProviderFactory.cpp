#include <Client/AI/AIProviderFactory.h>
#include <Client/AI/OpenAIProvider.h>
#include <Common/Exception.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

AIProviderPtr AIProviderFactory::createProvider(const AIConfiguration & config)
{
    if (config.model_provider == "openai")
    {
            return std::make_unique<OpenAIProvider>(config);
    }
    
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown AI provider type. Currently only 'openai' is supported");
}

}

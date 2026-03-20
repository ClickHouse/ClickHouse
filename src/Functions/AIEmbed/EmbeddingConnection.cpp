#include <Functions/AIEmbed/EmbeddingConnection.h>
#include <Functions/AIEmbed/OpenAIProvider.h>
#include <Functions/AIEmbed/HuggingFaceTEIProvider.h>

#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
}

namespace
{

bool looksLikeURL(const String & s)
{
    return s.starts_with("http://") || s.starts_with("https://");
}

}

EmbeddingConnectionConfig resolveConnectionConfig(
    const String & connection_name_or_url,
    ContextPtr /* context */)
{
    EmbeddingConnectionConfig config;

    if (looksLikeURL(connection_name_or_url))
    {
        /// Inline URL mode: treat as HuggingFace TEI
        config.provider = "huggingface_tei";
        config.endpoint = connection_name_or_url;
        return config;
    }

    /// Named collection mode
    auto & factory = NamedCollectionFactory::instance();
    auto collection = factory.tryGet(connection_name_or_url);
    if (!collection)
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Named collection '{}' for AI_EMBED does not exist. "
            "Create it with: CREATE NAMED COLLECTION {} AS provider = '...', endpoint = '...'",
            connection_name_or_url, connection_name_or_url);

    config.provider = collection->getOrDefault<String>("provider", "openai");
    config.endpoint = collection->get<String>("endpoint");
    config.api_key = collection->getOrDefault<String>("api_key", "");
    config.max_batch_size = collection->getOrDefault<UInt64>("max_batch_size", 256);
    config.timeout_ms = collection->getOrDefault<UInt64>("timeout_ms", 30000);
    config.api_version = collection->getOrDefault<String>("api_version", "");

    return config;
}

EmbeddingProviderPtr resolveEmbeddingProvider(
    const String & connection_name_or_url,
    ContextPtr context)
{
    auto config = resolveConnectionConfig(connection_name_or_url, context);

    if (config.provider == "huggingface_tei")
        return std::make_shared<HuggingFaceTEIProvider>(config.endpoint, config.api_key, config.max_batch_size);

    if (config.provider == "openai" || config.provider == "azure_openai")
        return std::make_shared<OpenAIProvider>(config.endpoint, config.api_key, config.max_batch_size, config.api_version);

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown embedding provider '{}'. Supported providers: 'openai', 'azure_openai', 'huggingface_tei'",
        config.provider);
}

}

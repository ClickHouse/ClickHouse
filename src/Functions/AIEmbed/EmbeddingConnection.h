#pragma once

#include <Functions/AIEmbed/EmbeddingProvider.h>

#include <Interpreters/Context_fwd.h>
#include <base/types.h>


namespace DB
{

/// Configuration resolved from a named collection or inline URL.
struct EmbeddingConnectionConfig
{
    String provider;      /// "openai", "huggingface_tei", "clickhouse_cloud"
    String endpoint;
    String api_key;
    size_t max_batch_size = 256;
    size_t timeout_ms = 30000;
    String api_version;   /// For Azure OpenAI
};

/// Resolve a named collection (or inline URL) into an EmbeddingProvider.
/// Caches the provider for reuse within the same function instance.
EmbeddingProviderPtr resolveEmbeddingProvider(
    const String & connection_name_or_url,
    ContextPtr context);

/// Parse the connection config from a named collection.
EmbeddingConnectionConfig resolveConnectionConfig(
    const String & connection_name_or_url,
    ContextPtr context);

}

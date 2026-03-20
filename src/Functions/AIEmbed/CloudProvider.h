#pragma once

#include "config.h"

#ifdef USE_CLICKHOUSE_CLOUD_AI

#include <Functions/AIEmbed/EmbeddingProvider.h>
#include <base/types.h>


namespace DB
{

/// Cloud-managed embedding provider.
/// Uses an internal ClickHouse Cloud endpoint with authentication
/// injected by the control plane via server config.
class CloudProvider : public EmbeddingProvider
{
public:
    CloudProvider(const String & endpoint_, const String & auth_token_);

    std::vector<std::vector<Float32>> embed(
        const String & model,
        const std::vector<std::string_view> & texts,
        size_t timeout_ms,
        size_t max_retries) override;

    String getName() const override { return "clickhouse_cloud"; }

private:
    String endpoint;
    String auth_token;
};

}

#endif

#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

namespace DB
{

/// `cancellation` is handed to every source created; see `StreamingExchangeSource`. Null on a worker.
class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
struct Settings;

/// The codec the exchange compresses its packets with: `distributed_plan_exchange_compression_method` and,
/// for `ZSTD`, `distributed_plan_exchange_zstd_compression_level`. When the method is empty, the codec of the
/// native protocol: `network_compression_method` and `network_zstd_compression_level`.
CompressionCodecPtr streamingExchangeCompressionCodec(const Settings & settings);

ExchangeLookupPtr createStreamingExchangeLookup(
    const String & query_id,
    ExchangeConnectionsPtr connections,
    const ExchangeStreamSources & exchange_stream_sources,
    DistributedQueryCancellationPtr cancellation,
    /// Auth token presented when opening the exchange connection; empty leaves the
    /// connection unauthenticated.
    const String & auth_token,
    CompressionCodecPtr codec);

}

#endif

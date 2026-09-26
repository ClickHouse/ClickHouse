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

/// The codec the exchange compresses its packets with: `network_compression_method` and, for `ZSTD`,
/// `network_zstd_compression_level`. This is the same choice the native protocol makes for a connection.
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

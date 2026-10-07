#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/DistributedQuery/StreamingExchangeLookup.h>
#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <Server/DistributedQuery/StreamingExchangeSerializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeDeserializingTransform.h>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <base/types.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <Poco/String.h>

namespace DB
{

namespace Setting
{
    extern const SettingsString network_compression_method;
    extern const SettingsInt64 network_zstd_compression_level;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

CompressionCodecPtr streamingExchangeCompressionCodec(const Settings & settings)
{
    const String method = Poco::toUpper(settings[Setting::network_compression_method].toString());
    /// The factory also knows codecs such as `Delta` that work only on some column types. Only the
    /// general codecs can compress any data.
    if (method != "NONE" && method != "ZSTD" && method != "LZ4" && method != "LZ4HC")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting 'network_compression_method' must be NONE, ZSTD, LZ4 or LZ4HC");

    std::optional<int> level;
    if (method == "ZSTD")
        level = static_cast<int>(settings[Setting::network_zstd_compression_level]);

    CompressionCodecFactory::instance().validateCodec(method, level, CodecValidationSettings(settings));
    return CompressionCodecFactory::instance().get(method, level);
}

class StreamingExchangeLookup : public IExchangeLookup
{
public:
    explicit StreamingExchangeLookup(
        const String & query_id_,
        ExchangeConnectionsPtr connections_,
        const ExchangeStreamSources & exchange_stream_sources_,
        DistributedQueryCancellationPtr cancellation_,
        const String & auth_token_,
        CompressionCodecPtr codec_)
        : query_id(query_id_)
        , connections(connections_)
        , exchange_stream_sources(exchange_stream_sources_)
        , cancellation(std::move(cancellation_))
        , auth_token(auth_token_)
        , codec(std::move(codec_))
    {
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id) override
    {
        auto stream_name = exchange_stream_id.toString();
        auto future_connection = connections->getConnection(query_id, stream_name);
        return std::make_shared<StreamingExchangeSink>(input_header, future_connection, stream_name);
    }

    std::shared_ptr<IProcessor> createSerializer(SharedHeader input_header, const String &) override
    {
        return std::make_shared<StreamingExchangeSerializingTransform>(std::move(input_header), codec);
    }

    std::shared_ptr<IProcessor> createDeserializer(SharedHeader output_header, const String & exchange_id) override
    {
        return std::make_shared<StreamingExchangeDeserializingTransform>(std::move(output_header), exchange_id);
    }

    std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & exchange_stream_id, bool output_is_serialized) override
    {
        auto stream_name = exchange_stream_id.toString();
        auto it = exchange_stream_sources.stream_hosts.find(stream_name);
        if (it == exchange_stream_sources.stream_hosts.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No host found for exchange stream {}", stream_name);
        if (it->second.port == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "No streaming exchange port for exchange stream {} on host {}", stream_name, it->second.host);
        return std::make_shared<StreamingExchangeSource>(
            output_header, query_id, stream_name, it->second.host, it->second.port, cancellation, auth_token, output_is_serialized);
    }

private:
    const String query_id;
    const ExchangeConnectionsPtr connections;
    const ExchangeStreamSources exchange_stream_sources;
    const DistributedQueryCancellationPtr cancellation;
    const String auth_token;
    const CompressionCodecPtr codec;
};

ExchangeLookupPtr createStreamingExchangeLookup(
    const String & query_id,
    ExchangeConnectionsPtr connections,
    const ExchangeStreamSources & exchange_stream_sources,
    DistributedQueryCancellationPtr cancellation,
    const String & auth_token,
    CompressionCodecPtr codec)
{
    return std::make_shared<StreamingExchangeLookup>(query_id, connections, exchange_stream_sources, std::move(cancellation), auth_token, std::move(codec));
}

}

#endif

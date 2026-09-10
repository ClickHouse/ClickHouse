#include <gtest/gtest.h>
#include <fmt/format.h>

#include <cstring>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Server/DistributedQuery/StreamingExchangeDeserializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeSerializingTransform.h>
#include <Common/ThreadStatus.h>
#include <Common/typeid_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

namespace
{

constexpr size_t rows_per_chunk = 1000;
constexpr size_t chunks_per_stream = 4;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(type->createColumn(), type, "k")});
}

/// A plain data stream whose only column looks like the one the serializer emits its packets in.
SharedHeader makePacketLikeHeader()
{
    auto type = std::make_shared<DataTypeString>();
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(type->createColumn(), type, "__streaming_exchange_packet")});
}

/// Zero-padded numbers, so every stream is sorted by the column.
Chunks makeSortedStringChunks(size_t stream_index)
{
    Chunks chunks;
    for (size_t chunk_index = 0; chunk_index < chunks_per_stream; ++chunk_index)
    {
        auto column = ColumnString::create();
        const size_t first_key = (stream_index * chunks_per_stream + chunk_index) * rows_per_chunk;
        for (size_t row = 0; row < rows_per_chunk; ++row)
            column->insertData(fmt::format("{:012}", first_key + row).data(), 12);
        Columns columns;
        columns.emplace_back(std::move(column));
        chunks.emplace_back(std::move(columns), rows_per_chunk);
    }
    return chunks;
}

/// Distinct keys for every stream, so the hash spreads them over all buckets. With
/// `with_rowless_info_chunk` the stream ends with a chunk that has no rows but carries an
/// aggregation info, as an aggregation may emit for an empty bucket.
Chunks makeChunks(size_t stream_index, bool with_rowless_info_chunk = false)
{
    Chunks chunks;
    for (size_t chunk_index = 0; chunk_index < chunks_per_stream; ++chunk_index)
    {
        auto column = ColumnUInt64::create();
        const size_t first_key = (stream_index * chunks_per_stream + chunk_index) * rows_per_chunk;
        for (size_t row = 0; row < rows_per_chunk; ++row)
            column->insertValue(first_key + row);
        Columns columns;
        columns.emplace_back(std::move(column));
        chunks.emplace_back(std::move(columns), rows_per_chunk);
    }
    if (with_rowless_info_chunk)
    {
        Columns columns;
        columns.emplace_back(ColumnUInt64::create());
        Chunk chunk(std::move(columns), 0);
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = static_cast<Int32>(stream_index);
        chunk.getChunkInfos().add(std::move(info));
        chunks.emplace_back(std::move(chunk));
    }
    return chunks;
}

/// The chunks of `makeChunks` as the packets the serializer makes of them, one per chunk.
Chunks makePacketChunks(size_t stream_index)
{
    Chunks packets;
    const auto header = makeHeader();
    for (const auto & chunk : makeChunks(stream_index))
    {
        auto column = ColumnString::create();
        auto & chars = column->getChars();
        size_t packet_offset = 0;
        {
            WriteBufferFromVector<ColumnString::Chars> out(chars);
            packet_offset = StreamingExchangeProtocol::writeDataPacket(chunk, header, out);
            out.finalize();
        }
        StreamingExchangeProtocol::finishDataPacket(reinterpret_cast<char *>(chars.data()) + packet_offset, chars.size() - packet_offset);
        column->getOffsets().push_back(chars.size());
        packets.emplace_back(Columns{std::move(column)}, 1);
    }
    return packets;
}

/// Every task of the test is bucket 0.
class FixedBucketParameterLookup : public IParameterLookup
{
public:
    Field getParameter(const String & name) const override
    {
        chassert(name == "bucket_id");
        return "0";
    }
};

/// Counts the chunks it receives. Ready packets are checked to start with a packet header whose
/// size field matches the packet.
class CountingSink : public ISink
{
public:
    CountingSink(SharedHeader header, bool receives_packets_)
        : ISink(std::move(header))
        , receives_packets(receives_packets_)
    {
    }

    String getName() const override { return "CountingSink"; }

    size_t chunks = 0;
    size_t rows = 0;
    size_t malformed_packets = 0;

protected:
    void consume(Chunk chunk) override
    {
        ++chunks;
        rows += chunk.getNumRows();
        if (!receives_packets)
            return;

        const std::string_view packet = chunk.getColumns().front()->getDataAt(0);
        StreamingExchangeProtocol::PacketHeader packet_header{};
        if (chunk.getNumRows() != 1 || packet.size() < sizeof(packet_header))
        {
            ++malformed_packets;
            return;
        }
        memcpy(&packet_header, packet.data(), sizeof(packet_header));
        if (packet_header.packet_type != StreamingExchangeProtocol::PacketType::Data
            || packet_header.bytes_size != packet.size() - sizeof(packet_header))
            ++malformed_packets;
    }

private:
    const bool receives_packets;
};

/// The streaming exchange without sockets: the real serializer and deserializer, sinks that discard
/// the packets, and sources that replay the chunks of the sending bucket, as data or as packets.
class TestExchangeLookup : public IExchangeLookup
{
public:
    std::shared_ptr<IProcessor> createSerializer(SharedHeader input_header, const String &) override
    {
        return std::make_shared<StreamingExchangeSerializingTransform>(std::move(input_header));
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId &, bool input_is_serialized) override
    {
        return std::make_shared<CountingSink>(std::move(input_header), input_is_serialized);
    }

    std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & stream_id, bool output_is_serialized) override
    {
        const size_t bucket = parse<size_t>(stream_id.source_bucket);
        if (output_is_serialized)
            return std::make_shared<SourceFromChunks>(StreamingExchangeProtocol::packetStreamHeader(), makePacketChunks(bucket));
        return std::make_shared<SourceFromChunks>(std::move(output_header), makeChunks(bucket));
    }

    std::shared_ptr<IProcessor> createDeserializer(SharedHeader output_header, const String & exchange_id) override
    {
        return std::make_shared<StreamingExchangeDeserializingTransform>(std::move(output_header), exchange_id);
    }
};

BuildQueryPipelineSettings makeSettings(ContextPtr context, size_t max_threads)
{
    BuildQueryPipelineSettings settings(context);
    settings.max_threads = max_threads;
    settings.parameter_lookup = std::make_shared<FixedBucketParameterLookup>();
    settings.exchange_lookup = std::make_shared<TestExchangeLookup>();
    return settings;
}

/// Statistics of a sending pipeline, collected by `runSendingStep`.
struct SendingStats
{
    /// Data rows received by the busiest processor behind the sources, and its name. Processors
    /// that receive serialized packets have another header and are not counted.
    size_t max_rows_into_one_processor = 0;
    String max_rows_processor;
    /// Rows that went into the serializers; the sinks then receive one-row packets.
    size_t rows_serialized = 0;
    size_t rows_into_sinks = 0;
    size_t chunks_in_sinks = 0;
    size_t malformed_packets = 0;
    size_t sinks = 0;
    size_t serializers = 0;
};

/// Feeds `num_streams` sources with `makeChunks` into the sending step, runs the pipeline on
/// `num_streams` threads and collects the statistics.
using ChunksForStream = std::function<Chunks(size_t stream_index)>;

SendingStats runSendingStep(
    IQueryPlanStep & step,
    size_t num_streams,
    const SharedHeader & header,
    const BuildQueryPipelineSettings & settings,
    const ChunksForStream & chunks_for_stream = [](size_t stream_index) { return makeChunks(stream_index); })
{
    Pipes pipes;
    for (size_t stream = 0; stream < num_streams; ++stream)
        pipes.emplace_back(std::make_shared<SourceFromChunks>(header, chunks_for_stream(stream)));

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));
    EXPECT_EQ(builder->getNumStreams(), num_streams);

    QueryPipelineBuilders builders;
    builders.emplace_back(std::move(builder));
    auto sending = step.updatePipeline(std::move(builders), settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*sending));
    pipeline.setNumThreads(num_streams);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    SendingStats stats;
    for (const auto & processor : pipeline.getProcessors())
    {
        if (processor->getInputs().empty())
            continue;

        const size_t input_rows = processor->getProcessorDataStats().input_rows;
        if (const auto * sink = typeid_cast<const CountingSink *>(processor.get()))
        {
            ++stats.sinks;
            stats.rows_into_sinks += input_rows;
            stats.chunks_in_sinks += sink->chunks;
            stats.malformed_packets += sink->malformed_packets;
        }
        if (processor->getName() == "StreamingExchangeSerializingTransform")
        {
            ++stats.serializers;
            stats.rows_serialized += input_rows;
        }

        if (!blocksHaveEqualStructure(processor->getInputs().front().getHeader(), *header))
            continue;
        if (input_rows > stats.max_rows_into_one_processor)
        {
            stats.max_rows_into_one_processor = input_rows;
            stats.max_rows_processor = processor->getName();
        }
    }
    EXPECT_EQ(stats.malformed_packets, 0u);
    return stats;
}

constexpr size_t streams = 8;
constexpr size_t total_rows = streams * chunks_per_stream * rows_per_chunk;
constexpr size_t rows_per_stream = total_rows / streams;

/// The work after the sources must stay spread over the streams: no processor may receive more than
/// one stream's worth of rows, plus a margin for uneven hashing.
void expectSpreadOverStreams(const SendingStats & stats)
{
    EXPECT_LE(stats.max_rows_into_one_processor, rows_per_stream * 3 / 2)
        << stats.max_rows_processor << " received " << stats.max_rows_into_one_processor << " of " << total_rows
        << " rows, one stream carries " << rows_per_stream;
}

}

/// A receiving task gets one exchange source per sending task. Deserialization must not stay in
/// those sources: with 3 senders and 8 threads it runs on 8 streams, and every row still arrives.
TEST(ShuffleExchangeParallelism, ReceiverDeserializesOnEveryStream)
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();

    constexpr size_t senders = 3;
    constexpr size_t max_threads = 8;

    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, max_threads);
    auto header = makeHeader();

    Strings source_shards;
    for (size_t sender = 0; sender < senders; ++sender)
        source_shards.push_back(toString(sender));

    ShuffleReceiveStep receive(header, "exchange_0", source_shards);
    QueryPipelineBuilder builder;
    receive.initializePipeline(builder, settings);
    EXPECT_EQ(builder.getNumStreams(), max_threads);

    size_t deserializers = 0;
    for (const auto & processor : builder.getProcessors())
        if (processor->getName() == "StreamingExchangeDeserializingTransform")
            ++deserializers;
    EXPECT_EQ(deserializers, max_threads);

    builder.resize(1);
    auto sink = std::make_shared<CountingSink>(header, /*receives_packets_=*/ false);
    builder.setSinks([&](const SharedHeader &, Pipe::StreamType) { return sink; });
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    pipeline.setNumThreads(max_threads);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
    EXPECT_EQ(sink->rows, senders * chunks_per_stream * rows_per_chunk);
}

/// A sending task scatters its read streams into the destination buckets. The work after the
/// scatter must stay spread over the streams; otherwise a third of all rows would go through one
/// processor on one thread while 8 threads are available.
TEST(ShuffleExchangeParallelism, SenderKeepsBucketWorkSpreadOverStreams)
{
    MainThreadStatus::getInstance();

    constexpr size_t buckets = 3;
    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    ShuffleSendStep send(header, "exchange_0", Names{"k"}, buckets);
    auto stats = runSendingStep(send, streams, header, settings);

    expectSpreadOverStreams(stats);
    EXPECT_EQ(stats.sinks, buckets);
    EXPECT_EQ(stats.rows_serialized, total_rows);
    /// Every scattered piece of a chunk becomes one packet.
    EXPECT_EQ(stats.chunks_in_sinks, streams * chunks_per_stream * buckets);
}

/// A gather sends everything to one destination. When no order has to be kept, the serialization
/// must still run on every stream ahead of the merge into the single sink.
TEST(ShuffleExchangeParallelism, UnsortedGatherKeepsSerializationSpreadOverStreams)
{
    MainThreadStatus::getInstance();

    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    GatherSendStep gather(header, "exchange_0");
    auto stats = runSendingStep(gather, streams, header, settings);

    expectSpreadOverStreams(stats);
    EXPECT_EQ(stats.serializers, streams);
    EXPECT_EQ(stats.sinks, 1u);
    EXPECT_EQ(stats.rows_serialized, total_rows);
    EXPECT_EQ(stats.chunks_in_sinks, streams * chunks_per_stream);
}

/// A sorted gather merges the streams first, so the serialization can only follow the merge:
/// the single sink serializes itself and no serializer sits in front of the merge.
TEST(ShuffleExchangeParallelism, SortedGatherSerializesAfterTheMerge)
{
    MainThreadStatus::getInstance();

    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    SortDescription by_key;
    by_key.emplace_back("k", 1, 1);
    GatherSendStep gather(header, "exchange_0", by_key);
    auto stats = runSendingStep(gather, streams, header, settings);

    EXPECT_EQ(stats.serializers, 0u);
    EXPECT_EQ(stats.sinks, 1u);
    EXPECT_EQ(stats.rows_into_sinks, total_rows);
}

/// A broadcast sends the same rows to every destination. The serialization must run once per
/// stream, not once per destination: with 8 streams and 3 destinations there are 8 serializers,
/// every sink receives all rows, and the copies for the destinations share the packets.
TEST(ShuffleExchangeParallelism, BroadcastSerializesOncePerStream)
{
    MainThreadStatus::getInstance();

    constexpr size_t buckets = 3;
    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    BroadcastSendStep broadcast(header, "exchange_0", buckets);
    auto stats = runSendingStep(broadcast, streams, header, settings);

    expectSpreadOverStreams(stats);
    EXPECT_EQ(stats.serializers, streams);
    EXPECT_EQ(stats.sinks, buckets);
    EXPECT_EQ(stats.rows_serialized, total_rows);
    EXPECT_EQ(stats.chunks_in_sinks, streams * chunks_per_stream * buckets);
}

/// A keyless scatter spreads whole chunks round-robin over the buckets. It must keep the streams
/// like the keyed scatter does, so the serialization runs on all of them.
TEST(ShuffleExchangeParallelism, KeylessScatterKeepsBucketWorkSpreadOverStreams)
{
    MainThreadStatus::getInstance();

    constexpr size_t buckets = 3;
    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    ShuffleSendStep send(header, "exchange_0", Names{}, buckets);
    auto stats = runSendingStep(send, streams, header, settings);

    expectSpreadOverStreams(stats);
    EXPECT_EQ(stats.serializers, streams * buckets);
    EXPECT_EQ(stats.sinks, buckets);
    EXPECT_EQ(stats.rows_serialized, total_rows);
    EXPECT_EQ(stats.chunks_in_sinks, streams * chunks_per_stream);
}

/// A chunk without rows may carry aggregation bucket information. Its packet must reach every
/// destination of a broadcast like any other.
TEST(ShuffleExchangeParallelism, BroadcastKeepsRowlessPackets)
{
    MainThreadStatus::getInstance();

    constexpr size_t buckets = 3;
    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makeHeader();

    BroadcastSendStep broadcast(header, "exchange_0", buckets);
    auto stats = runSendingStep(broadcast, streams, header, settings, [](size_t stream_index) { return makeChunks(stream_index, /*with_rowless_info_chunk=*/ true); });

    EXPECT_EQ(stats.rows_serialized, total_rows);
    EXPECT_EQ(stats.chunks_in_sinks, streams * (chunks_per_stream + 1) * buckets);
}

/// Whether a sink receives packets or data is decided by the send step, which knows whether it put
/// serializers in front of the sink. A sorted gather has none, so a plain stream reaches its sink as
/// data even when its only column looks like the packet column.
TEST(ShuffleExchangeParallelism, PlainStreamWithPacketLikeColumnStaysData)
{
    MainThreadStatus::getInstance();

    auto context = Context::createCopy(getContext().context);
    auto settings = makeSettings(context, streams);
    auto header = makePacketLikeHeader();

    SortDescription by_column;
    by_column.emplace_back("__streaming_exchange_packet", 1, 1);
    GatherSendStep gather(header, "exchange_0", by_column);
    auto stats = runSendingStep(gather, streams, header, settings, makeSortedStringChunks);

    EXPECT_EQ(stats.serializers, 0u);
    EXPECT_EQ(stats.sinks, 1u);
    EXPECT_EQ(stats.rows_into_sinks, total_rows);
}

/// A source that hands packets on drops the end-of-stream marker after reading only its fields, so
/// the fields must prove that the marker is the empty one: rows or columns in it would be lost, and
/// a truncated or overlong marker is a protocol violation.
TEST(ShuffleExchangeParallelism, OnlyTheEmptyEndOfStreamMarkerIsAccepted)
{
    auto body_of = [](std::initializer_list<UInt64> fields, const String & trailing = {})
    {
        WriteBufferFromOwnString body;
        for (UInt64 field : fields)
            writeVarUInt(field, body);
        body.write(trailing.data(), trailing.size());
        body.finalize();
        return body.str();
    };
    auto prefix_of = [](const String & body)
    {
        return StreamingExchangeProtocol::readDataPacketPrefix(body.data(), body.size(), "test stream");
    };
    auto expect_rejected = [&](const String & body, const char * what)
    {
        try
        {
            prefix_of(body);
            FAIL() << what << " was accepted as the end-of-stream marker";
        }
        catch (const Exception & e)
        {
            EXPECT_TRUE(e.code() == ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT || e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
                << what << ": " << e.message();
        }
    };

    /// flags, rows, columns; flag 1 = end of stream, flag 2 = an aggregation chunk number follows.
    EXPECT_TRUE(prefix_of(body_of({1, 0, 0})).end_of_stream);
    EXPECT_TRUE(prefix_of(body_of({1 | 2, 0, 0, /*chunk_num*/ 7})).end_of_stream);
    const auto data_prefix = prefix_of(body_of({0, 5, 1}, "block bytes"));
    EXPECT_FALSE(data_prefix.end_of_stream);
    EXPECT_EQ(data_prefix.num_rows, 5u);

    expect_rejected(body_of({1, 5, 0}), "a marker with rows");
    expect_rejected(body_of({1, 0, 1}), "a marker with columns");
    expect_rejected(body_of({1, 0}), "a marker without the column count");
    expect_rejected(body_of({1, 0, 0}, "x"), "a marker with bytes after its fields");
}

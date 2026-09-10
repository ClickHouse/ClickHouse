#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

#include <gtest/gtest.h>
#include <fmt/format.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/ThreadStatus.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/ExchangeServer.h>
#include <Server/DistributedQuery/StreamingExchangeDeserializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Server/DistributedQuery/StreamingExchangeSerializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <Server/DistributedQuery/tests/FakeExchangePeer.h>

namespace DB::ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int EXCHANGE_PEER_DISCONNECTED;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(type->createColumn(), type, "v")});
}

Chunk makeChunk(size_t first_value, size_t rows)
{
    auto column = ColumnUInt64::create();
    for (size_t row = 0; row < rows; ++row)
        column->insertValue(first_value + row);
    return Chunk(Columns{std::move(column)}, rows);
}

std::shared_ptr<AggregatedChunkInfo> makeAggregatedInfo(Int32 bucket_num, UInt64 chunk_num)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = bucket_num;
    info->chunk_num = chunk_num;
    return info;
}

/// Keeps every chunk it gets, for the assertions after the run. Its `gate` holds the first chunk back
/// until released, which stalls the receiving pipeline and with it the sender behind the socket.
class CollectingSink : public ISink
{
public:
    /// With `hold_chunks` the sink keeps its first chunk waiting until `release`.
    explicit CollectingSink(SharedHeader header, bool hold_chunks = false) : ISink(std::move(header)), released(!hold_chunks) { }

    String getName() const override { return "CollectingSink"; }

    void release()
    {
        std::lock_guard lock(mutex);
        released = true;
        gate.notify_all();
    }

    std::vector<Chunk> chunks;

protected:
    void consume(Chunk chunk) override
    {
        std::unique_lock lock(mutex);
        gate.wait(lock, [this] { return released; });
        chunks.push_back(std::move(chunk));
    }

private:
    std::mutex mutex;
    std::condition_variable gate;
    bool released;
};

/// A real exchange server on a loopback port with its own rendezvous table.
struct LoopbackExchange
{
    ExchangeConnectionsPtr connections = std::make_shared<ExchangeConnections>();
    ExchangeServer server{"127.0.0.1", 0, connections};

    LoopbackExchange() { server.start(); }
};

/// Runs `pipeline` to the end; returns the code it threw with, if any.
std::optional<int> run(QueryPipeline & pipeline, size_t threads)
{
    pipeline.setNumThreads(threads);
    CompletedPipelineExecutor executor(pipeline);
    try
    {
        executor.execute();
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return std::nullopt;
}

/// The sending task: `chunks_per_stream` streams into one real sink. With `sink_takes_packets` the
/// streams serialize their chunks first, as the send steps arrange it; otherwise the sink serializes.
QueryPipeline makeSendingPipeline(
    const SharedHeader & header, std::vector<Chunks> chunks_per_stream, LoopbackExchange & exchange, bool sink_takes_packets)
{
    Pipes pipes;
    for (auto & chunks : chunks_per_stream)
        pipes.emplace_back(std::make_shared<SourceFromChunks>(header, std::move(chunks)));

    QueryPipelineBuilder builder;
    builder.init(Pipe::unitePipes(std::move(pipes)));
    if (sink_takes_packets)
        builder.addSimpleTransform([](const SharedHeader & stream_header)
        {
            return std::make_shared<StreamingExchangeSerializingTransform>(stream_header);
        });
    builder.resize(1);

    auto future_connection = exchange.connections->getConnection("query", "stream");
    builder.setSinks([&](const SharedHeader & stream_header, Pipe::StreamType)
    {
        return std::make_shared<StreamingExchangeSink>(stream_header, future_connection, "stream", sink_takes_packets);
    });
    return QueryPipelineBuilder::getPipeline(std::move(builder));
}

/// The receiving task: one real source connected to `port`. With `source_hands_packets` a
/// deserializer follows the source, as the receive steps arrange it; otherwise the source deserializes.
QueryPipeline makeReceivingPipeline(const SharedHeader & header, UInt16 port, bool source_hands_packets, std::shared_ptr<CollectingSink> sink)
{
    auto source = std::make_shared<StreamingExchangeSource>(
        header, "query", "stream", "127.0.0.1", port, /*cancellation_=*/ nullptr, /*auth_token_=*/ String{}, source_hands_packets);

    QueryPipelineBuilder builder;
    builder.init(Pipe(source));
    if (source_hands_packets)
        builder.addSimpleTransform([&](const SharedHeader &)
        {
            return std::make_shared<StreamingExchangeDeserializingTransform>(header, "exchange");
        });
    builder.setSinks([&](const SharedHeader &, Pipe::StreamType) { return sink; });
    return QueryPipelineBuilder::getPipeline(std::move(builder));
}

UInt64 sumOfValues(const std::vector<Chunk> & chunks)
{
    UInt64 sum = 0;
    for (const auto & chunk : chunks)
        if (chunk.getNumRows() != 0)
            for (UInt64 value : assert_cast<const ColumnUInt64 &>(*chunk.getColumns().front()).getData())
                sum += value;
    return sum;
}

}

/// Every packet shape crosses a real socket between a real sink and a real source: data chunks, a
/// data chunk with an aggregation info, a chunk without rows that carries one (an aggregation emits
/// those for empty buckets), and the end-of-stream marker. The bytes on the wire do not depend on
/// which side serializes or deserializes, so all four pairings must interoperate.
TEST(StreamingExchangeTransport, EveryPacketShapeCrossesTheSocket)
{
    MainThreadStatus::getInstance();

    constexpr size_t streams = 3;
    constexpr size_t chunks_per_stream = 4;
    constexpr size_t rows_per_chunk = 1000;
    constexpr size_t total_rows = streams * chunks_per_stream * rows_per_chunk;
    constexpr UInt64 expected_sum = UInt64(total_rows) * (total_rows - 1) / 2;

    for (bool sink_takes_packets : {false, true})
    {
        for (bool source_hands_packets : {false, true})
        {
            SCOPED_TRACE(fmt::format("sink_takes_packets={} source_hands_packets={}", sink_takes_packets, source_hands_packets));

            std::vector<Chunks> chunks_per_stream_list(streams);
            for (size_t stream = 0; stream < streams; ++stream)
                for (size_t index = 0; index < chunks_per_stream; ++index)
                    chunks_per_stream_list[stream].push_back(makeChunk((stream * chunks_per_stream + index) * rows_per_chunk, rows_per_chunk));
            chunks_per_stream_list[0].front().getChunkInfos().add(makeAggregatedInfo(/*bucket_num=*/ 1, /*chunk_num=*/ 5));
            Chunk rowless(Columns{ColumnUInt64::create()}, 0);
            rowless.getChunkInfos().add(makeAggregatedInfo(/*bucket_num=*/ 7, /*chunk_num=*/ 9));
            chunks_per_stream_list[1].push_back(std::move(rowless));

            auto header = makeHeader();
            LoopbackExchange exchange;
            auto sending = makeSendingPipeline(header, std::move(chunks_per_stream_list), exchange, sink_takes_packets);
            auto sink = std::make_shared<CollectingSink>(header);
            auto receiving = makeReceivingPipeline(header, exchange.server.port(), source_hands_packets, sink);

            std::optional<int> sending_code;
            std::thread sender([&] { sending_code = run(sending, streams); });
            const auto receiving_code = run(receiving, 2);
            sender.join();

            EXPECT_EQ(sending_code, std::nullopt);
            EXPECT_EQ(receiving_code, std::nullopt);

            size_t rows = 0;
            size_t rowless_with_info = 0;
            size_t data_chunks_with_info = 0;
            for (const auto & chunk : sink->chunks)
            {
                rows += chunk.getNumRows();
                auto info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
                if (!info)
                    continue;
                if (chunk.getNumRows() == 0)
                {
                    ++rowless_with_info;
                    EXPECT_EQ(info->bucket_num, 7);
                    EXPECT_EQ(info->chunk_num, 9u);
                }
                else
                {
                    ++data_chunks_with_info;
                    EXPECT_EQ(info->bucket_num, 1);
                    EXPECT_EQ(info->chunk_num, 5u);
                    EXPECT_EQ(chunk.getNumRows(), rows_per_chunk);
                }
            }
            EXPECT_EQ(rows, total_rows);
            EXPECT_EQ(sumOfValues(sink->chunks), expected_sum);
            EXPECT_EQ(rowless_with_info, 1u);
            EXPECT_EQ(data_chunks_with_info, 1u);
        }
    }
}

/// A receiver that does not drain stalls the sender at its pending-bytes cap through the socket.
/// The receiver is held back until the sender has certainly hit the cap; then everything must
/// still arrive, with nothing lost and nobody stuck.
TEST(StreamingExchangeTransport, SenderStallsAtThePendingCapAndResumes)
{
    MainThreadStatus::getInstance();

    /// Values that do not compress, so the cap of the sink is reached by packet bytes: 96 chunks of
    /// 512 KiB are far more than the cap plus what the loopback socket buffers can hold.
    constexpr size_t chunks = 96;
    constexpr size_t rows_per_chunk = 64 * 1024;
    std::atomic<size_t> chunks_emitted = 0;
    Chunks input;
    UInt64 value = 1;
    UInt64 expected_sum = 0;
    for (size_t index = 0; index < chunks; ++index)
    {
        auto column = ColumnUInt64::create();
        for (size_t row = 0; row < rows_per_chunk; ++row)
        {
            value = value * 6364136223846793005ULL + 1442695040888963407ULL;
            column->insertValue(value);
            expected_sum += value;
        }
        input.emplace_back(Columns{std::move(column)}, rows_per_chunk);
    }

    auto header = makeHeader();
    LoopbackExchange exchange;
    /// Emits the chunks and counts them. The sink accepts chunks until its pending bytes reach the
    /// cap, whatever the socket does, so at least the cap's worth is always emitted.
    class CountingSource : public ISource
    {
    public:
        CountingSource(SharedHeader header_, Chunks chunks_, std::atomic<size_t> & emitted_)
            : ISource(std::move(header_)), chunks(std::move(chunks_)), emitted(emitted_)
        {
        }

        String getName() const override { return "CountingSource"; }

    protected:
        Chunk generate() override
        {
            if (next == chunks.size())
                return {};
            ++emitted;
            return std::move(chunks[next++]);
        }

    private:
        Chunks chunks;
        size_t next = 0;
        std::atomic<size_t> & emitted;
    };

    QueryPipelineBuilder builder;
    builder.init(Pipe(std::make_shared<CountingSource>(header, std::move(input), chunks_emitted)));
    builder.addSimpleTransform([](const SharedHeader & stream_header) { return std::make_shared<StreamingExchangeSerializingTransform>(stream_header); });
    auto future_connection = exchange.connections->getConnection("query", "stream");
    builder.setSinks([&](const SharedHeader & stream_header, Pipe::StreamType)
    {
        return std::make_shared<StreamingExchangeSink>(stream_header, future_connection, "stream", /*input_is_serialized_=*/ true);
    });
    auto sending = QueryPipelineBuilder::getPipeline(std::move(builder));

    auto sink = std::make_shared<CollectingSink>(header, /*hold_chunks=*/ true);
    auto receiving = makeReceivingPipeline(header, exchange.server.port(), /*source_hands_packets=*/ true, sink);

    /// Chunks the sink certainly took before the receiver drains anything: the cap (16 MiB) over
    /// 512 KiB packets, minus one for the packet in flight.
    constexpr size_t chunks_the_cap_holds = 16 * 1024 * 1024 / (rows_per_chunk * sizeof(UInt64)) - 1;

    std::optional<int> sending_code;
    std::optional<int> receiving_code;
    std::thread sender([&] { sending_code = run(sending, 2); });
    std::thread receiver([&] { receiving_code = run(receiving, 2); });

    while (chunks_emitted.load() < chunks_the_cap_holds)
        std::this_thread::yield();
    sink->release();

    sender.join();
    receiver.join();

    EXPECT_EQ(sending_code, std::nullopt);
    EXPECT_EQ(receiving_code, std::nullopt);
    size_t rows = 0;
    for (const auto & chunk : sink->chunks)
        rows += chunk.getNumRows();
    EXPECT_EQ(rows, chunks * rows_per_chunk);
    EXPECT_EQ(sumOfValues(sink->chunks), expected_sum);
}

namespace
{

/// The bytes of a Data packet whose body is the given varints, optionally followed by raw bytes.
std::string dataPacket(std::initializer_list<UInt64> fields, const std::string & trailing = {})
{
    WriteBufferFromOwnString body;
    for (UInt64 field : fields)
        writeVarUInt(field, body);
    body.write(trailing.data(), trailing.size());
    body.finalize();
    StreamingExchangeProtocol::PacketHeader header{.packet_type = StreamingExchangeProtocol::PacketType::Data, .bytes_size = body.str().size()};
    return std::string(reinterpret_cast<const char *>(&header), sizeof(header)) + body.str();
}

std::string rawHeader(UInt64 packet_type, UInt64 bytes_size)
{
    StreamingExchangeProtocol::PacketHeader header{.packet_type = packet_type, .bytes_size = bytes_size};
    return std::string(reinterpret_cast<const char *>(&header), sizeof(header));
}

/// A peer that completes the handshake and then sends `bytes`; with `then_reset` it cuts the
/// connection right after, otherwise it keeps it open until the test ends.
std::function<void(Poco::Net::StreamSocket &)> sendAfterHandshake(std::string bytes, bool then_reset = false)
{
    return [packet_bytes = std::move(bytes), then_reset](Poco::Net::StreamSocket & socket)
    {
        ExchangeTest::completeSinkHandshake(socket);
        StreamingExchangeProtocol::sendAll(socket, packet_bytes.data(), packet_bytes.size(), "test packet");
        if (then_reset)
        {
            socket.setLinger(true, 0);
            socket.close();
        }
    };
}

/// Runs a real source in the given mode against `peer`; returns the code the pipeline threw with,
/// and the rows that arrived.
std::pair<std::optional<int>, size_t> receiveFrom(const ExchangeTest::FakePeer & peer, bool source_hands_packets)
{
    auto header = makeHeader();
    auto sink = std::make_shared<CollectingSink>(header);
    auto receiving = makeReceivingPipeline(header, peer.port(), source_hands_packets, sink);
    auto code = run(receiving, 2);
    size_t rows = 0;
    for (const auto & chunk : sink->chunks)
        rows += chunk.getNumRows();
    return {code, rows};
}

}

/// What a real source does with packets a well-behaved sink never sends, in both of its modes: the
/// end-of-stream marker with rows or truncated, a packet of an unknown type, an oversized body, and a
/// connection cut in the middle of a packet. A proper packet followed by the marker ends cleanly.
TEST(StreamingExchangeTransport, SourceRejectsMalformedPackets)
{
    MainThreadStatus::getInstance();

    for (bool source_hands_packets : {false, true})
    {
        SCOPED_TRACE(fmt::format("source_hands_packets={}", source_hands_packets));

        {
            /// flags, rows, columns: flag 1 is the end of stream.
            ExchangeTest::FakePeer peer(sendAfterHandshake(dataPacket({1, 5, 0})));
            EXPECT_EQ(receiveFrom(peer, source_hands_packets).first, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT) << "a marker with rows";
        }
        {
            ExchangeTest::FakePeer peer(sendAfterHandshake(dataPacket({1, 0})));
            EXPECT_EQ(receiveFrom(peer, source_hands_packets).first, ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF) << "a marker without the column count";
        }
        {
            ExchangeTest::FakePeer peer(sendAfterHandshake(rawHeader(/*packet_type=*/ 0xbad, /*bytes_size=*/ 0)));
            EXPECT_EQ(receiveFrom(peer, source_hands_packets).first, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT) << "an unknown packet type";
        }
        {
            ExchangeTest::FakePeer peer(sendAfterHandshake(rawHeader(StreamingExchangeProtocol::PacketType::Data, StreamingExchangeProtocol::MAX_DATA_PACKET_BODY_BYTES + 1)));
            EXPECT_EQ(receiveFrom(peer, source_hands_packets).first, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT) << "an oversized body";
        }
        {
            ExchangeTest::FakePeer peer(sendAfterHandshake(rawHeader(StreamingExchangeProtocol::PacketType::Data, 100) + std::string(10, 'x'), /*then_reset=*/ true));
            EXPECT_EQ(receiveFrom(peer, source_hands_packets).first, ErrorCodes::EXCHANGE_PEER_DISCONNECTED) << "a connection cut in the middle of a packet";
        }
        {
            WriteBufferFromOwnString packets;
            const auto header = makeHeader();
            const size_t first = StreamingExchangeProtocol::writeDataPacket(makeChunk(0, 3), header, packets);
            const size_t marker = StreamingExchangeProtocol::writeDataPacket(Chunk(), header, packets);
            packets.finalize();
            std::string bytes = packets.str();
            StreamingExchangeProtocol::finishDataPacket(bytes.data() + first, marker - first);
            StreamingExchangeProtocol::finishDataPacket(bytes.data() + marker, bytes.size() - marker);

            ExchangeTest::FakePeer peer(sendAfterHandshake(bytes));
            const auto [code, rows] = receiveFrom(peer, source_hands_packets);
            EXPECT_EQ(code, std::nullopt) << "a proper packet and the marker";
            EXPECT_EQ(rows, 3u);
        }
    }
}

#endif

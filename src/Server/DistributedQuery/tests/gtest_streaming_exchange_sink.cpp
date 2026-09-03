#include <gtest/gtest.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Server/DistributedQuery/FutureConnection.h>
#include <Server/DistributedQuery/StreamingExchangeSink.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>

#include <csignal>
#include <thread>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto column = ColumnUInt64::create();
    Block block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt64>(), "x")};
    return std::make_shared<const Block>(std::move(block));
}

/// Values from a multiplicative hash compress poorly, so the serialized stream is large
/// enough to overflow the socket buffers and leave the sink with unsent bytes.
Chunk makeChunk(size_t rows, size_t seed)
{
    auto column = ColumnUInt64::create();
    column->reserve(rows);
    for (size_t i = 0; i < rows; ++i)
        column->insertValue((seed * rows + i) * 2654435761ULL);
    Columns columns;
    columns.push_back(std::move(column));
    return Chunk(std::move(columns), rows);
}

/// The consumer side of the exchange: reads a little and disconnects without sending
/// `NoMoreDataNeeded`, the way a task torn down mid-delivery does.
void readALittleAndDisconnect(Poco::Net::StreamSocket & socket)
{
    char buffer[1024];
    socket.receiveBytes(buffer, sizeof(buffer));
    socket.shutdown();
    socket.close();
}

QueryPipeline makeSinkPipeline(std::shared_ptr<FutureConnection> future_connection, bool advisory)
{
    auto header = makeHeader();
    Chunks chunks;
    for (size_t i = 0; i < 4; ++i)
        chunks.push_back(makeChunk(1024 * 1024, i));

    QueryPipeline pipeline(Pipe(std::make_shared<SourceFromChunks>(header, std::move(chunks))));
    pipeline.complete(std::make_shared<StreamingExchangeSink>(header, std::move(future_connection), "test_stream", advisory));
    return pipeline;
}

/// Runs the sink against a peer that reads a little and disconnects without sending
/// `NoMoreDataNeeded`, the way a task torn down mid-delivery does. Returns the executor error.
std::exception_ptr runSinkAgainstDisconnectingPeer(bool advisory)
{
    /// A send to a peer that already reset the connection raises SIGPIPE otherwise.
    signal(SIGPIPE, SIG_IGN);

    Poco::Net::ServerSocket server(Poco::Net::SocketAddress("127.0.0.1", 0));
    Poco::Net::StreamSocket client;
    client.connect(server.address());
    Poco::Net::StreamSocket accepted = server.acceptConnection();

    auto future_connection = std::make_shared<FutureConnection>();
    future_connection->setSocket(accepted);

    std::thread peer([&client] { readALittleAndDisconnect(client); });

    std::exception_ptr error;
    {
        auto pipeline = makeSinkPipeline(future_connection, advisory);
        CompletedPipelineExecutor executor(pipeline);
        try
        {
            executor.execute();
        }
        catch (...)
        {
            error = std::current_exception();
        }
    }

    peer.join();
    return error;
}

}

/// A runtime filter receiver is free to disconnect at any moment (its task may have finished
/// before the filter was built). The sink that carries the filter must treat this as "this
/// destination gets nothing", never as an error that would fail the producing task.
TEST(StreamingExchangeSink, PeerDisconnectWhileDataPendingIsBenignForFilterDelivery)
{
    auto error = runSinkAgainstDisconnectingPeer(/*advisory*/ true);
    if (error)
        std::rethrow_exception(error);
}

/// For a data stream the same disconnect stays an error: losing a data destination means a
/// wrong result, so the task must fail.
TEST(StreamingExchangeSink, PeerDisconnectWhileDataPendingFailsDataExchange)
{
    auto error = runSinkAgainstDisconnectingPeer(/*advisory*/ false);
    EXPECT_TRUE(error);
}

#endif

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <functional>
#include <optional>
#include <thread>

#include <gtest/gtest.h>
#include <poll.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Port.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <base/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int EXCHANGE_PEER_DISCONNECTED;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}
}

using namespace DB;

namespace
{
    void recordFailure(DistributedQueryCancellation & cancellation, int code)
    {
        cancellation.recordException(std::make_exception_ptr(Exception(code, "failure")));
    }

    std::optional<int> recordedCode(const DistributedQueryCancellation & cancellation)
    {
        auto failure = cancellation.getFailure();
        if (!failure)
            return std::nullopt;
        return getExceptionErrorCode(failure);
    }

    /// The error code the callable throws with, or nothing if it returns.
    template <typename F>
    std::optional<int> thrownCode(F && callable)
    {
        try
        {
            callable();
        }
        catch (const Exception & e)
        {
            return e.code();
        }
        return std::nullopt;
    }
}

/// A consequence yields to an unclassified failure, which yields to a root cause; among failures of
/// one rank the first stays.
TEST(DistributedQueryCancellation, FailureRanking)
{
    DistributedQueryCancellation cancellation;
    EXPECT_FALSE(cancellation.isCancelled());

    recordFailure(cancellation, ErrorCodes::EXCHANGE_PEER_DISCONNECTED);
    EXPECT_TRUE(cancellation.isCancelled());
    EXPECT_EQ(recordedCode(cancellation), ErrorCodes::EXCHANGE_PEER_DISCONNECTED);

    recordFailure(cancellation, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_EQ(recordedCode(cancellation), ErrorCodes::EXCHANGE_PEER_DISCONNECTED);

    recordFailure(cancellation, ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
    EXPECT_EQ(recordedCode(cancellation), ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);

    recordFailure(cancellation, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    EXPECT_EQ(recordedCode(cancellation), ErrorCodes::MEMORY_LIMIT_EXCEEDED);

    recordFailure(cancellation, ErrorCodes::QUERY_WAS_CANCELLED);
    recordFailure(cancellation, ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
    recordFailure(cancellation, ErrorCodes::EXCHANGE_PEER_DISCONNECTED);
    EXPECT_EQ(recordedCode(cancellation), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    EXPECT_EQ(thrownCode([&] { cancellation.throwIfCancelled(); }), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
}

/// Once the pipeline cancelled the query, only a root cause is recorded, so the client gets the
/// cancellation it asked for and not a closed exchange socket. A consequence recorded before stays.
TEST(DistributedQueryCancellation, PipelineCancel)
{
    DistributedQueryCancellation cancellation;
    cancellation.cancel();
    EXPECT_TRUE(cancellation.isCancelled());
    EXPECT_TRUE(cancellation.isCancelledByPipeline());
    EXPECT_EQ(thrownCode([&] { cancellation.throwIfCancelled(); }), ErrorCodes::QUERY_WAS_CANCELLED);

    recordFailure(cancellation, ErrorCodes::EXCHANGE_PEER_DISCONNECTED);
    recordFailure(cancellation, ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
    EXPECT_EQ(recordedCode(cancellation), std::nullopt);

    recordFailure(cancellation, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    EXPECT_EQ(recordedCode(cancellation), ErrorCodes::MEMORY_LIMIT_EXCEEDED);

    DistributedQueryCancellation failing_first;
    recordFailure(failing_first, ErrorCodes::EXCHANGE_PEER_DISCONNECTED);
    failing_first.cancel();
    EXPECT_EQ(recordedCode(failing_first), ErrorCodes::EXCHANGE_PEER_DISCONNECTED);
}

namespace
{
    /// A producer's end of an exchange connection, driven by a thread: what the teardown of a failed
    /// producer task looks like to a consumer.
    class Peer
    {
    public:
        /// `behaviour` runs on the accepted connection.
        explicit Peer(std::function<void(Poco::Net::StreamSocket &)> behaviour_)
            : listener(Poco::Net::SocketAddress("127.0.0.1", 0))
            , thread([this, behaviour = std::move(behaviour_)]
            {
                accepted = listener.acceptConnection();
                behaviour(accepted);
            })
        {
        }

        ~Peer()
        {
            join();
            accepted.close();
            listener.close();
        }

        void join()
        {
            if (thread.joinable())
                thread.join();
        }

        UInt16 port() const { return listener.address().port(); }

    private:
        Poco::Net::ServerSocket listener;
        Poco::Net::StreamSocket accepted;
        std::thread thread;
    };

    /// Half-closes at once, so the source's handshake read hits EOF. Only the sending direction is
    /// shut down: the peer still absorbs the source's SourceHello, so the read is what fails.
    void halfCloseAtOnce(Poco::Net::StreamSocket & socket)
    {
        socket.shutdownSend();
    }

    /// Completes the handshake, then resets the connection: closing with a zero linger sends RST, so
    /// the source's next write fails instead of being buffered.
    void handshakeThenReset(Poco::Net::StreamSocket & socket)
    {
        using namespace StreamingExchangeProtocol;
        PacketHeader header{};
        size_t position = 0;
        while (position < sizeof(header))
            position += socket.receiveBytes(reinterpret_cast<char *>(&header) + position, static_cast<int>(sizeof(header) - position));
        std::string body(header.bytes_size, '\0');
        position = 0;
        while (position < body.size())
            position += socket.receiveBytes(body.data() + position, static_cast<int>(body.size() - position));

        WriteBufferFromOwnString reply_body;
        SinkHelloBody{.sink_version = PROTOCOL_VERSION}.write(reply_body);
        reply_body.finalize();
        PacketHeader reply_header{.packet_type = PacketType::SinkHello, .bytes_size = reply_body.str().size()};
        socket.sendBytes(&reply_header, sizeof(reply_header));
        socket.sendBytes(reply_body.str().data(), static_cast<int>(reply_body.str().size()));

        socket.setLinger(true, 0);
        socket.close();
    }

    std::shared_ptr<StreamingExchangeSource> makeSource(const Peer & peer, DistributedQueryCancellationPtr cancellation)
    {
        return std::make_shared<StreamingExchangeSource>(
            std::make_shared<const Block>(), "query", "final_result__0_0", "127.0.0.1", peer.port(), std::move(cancellation));
    }

    /// Runs the source until it ends or throws; returns the code it threw with, if any.
    std::optional<int> runToEnd(StreamingExchangeSource & source, InputPort & downstream)
    {
        return thrownCode([&]
        {
            for (size_t step = 0; step < 100; ++step)
            {
                auto status = source.prepare();
                if (status == IProcessor::Status::Finished)
                    return;
                if (status == IProcessor::Status::PortFull)
                    downstream.pull();
                else
                    source.work();
            }
            FAIL() << "the source did not end";
        });
    }
}

/// On the initiator the source records the lost peer as a consequence and ends its stream without
/// failing the pipeline: the driving source reports the outcome, once it knows the task statuses.
TEST(StreamingExchangeSourceFailureReport, RecordsLostPeerAndEndsWhileTheDrivingSourceRuns)
{
    Peer peer(halfCloseAtOnce);
    auto cancellation = std::make_shared<DistributedQueryCancellation>();
    auto source = makeSource(peer, cancellation);
    InputPort downstream(source->getPort().getHeader());
    connect(source->getPort(), downstream);
    downstream.setNeeded();

    EXPECT_EQ(runToEnd(*source, downstream), std::nullopt);
    EXPECT_TRUE(cancellation->isCancelled());
    EXPECT_EQ(recordedCode(*cancellation), ErrorCodes::EXCHANGE_PEER_DISCONNECTED);
}

/// When the driving source is done already, nobody else reports, so the source throws the recorded
/// root cause if there is one, else the lost peer. After a pipeline cancel it ends quietly instead.
TEST(StreamingExchangeSourceFailureReport, ReportsLostPeerWhenTheDrivingSourceIsDone)
{
    {
        Peer peer(halfCloseAtOnce);
        auto cancellation = std::make_shared<DistributedQueryCancellation>();
        cancellation->markExecutionFinished();
        auto source = makeSource(peer, cancellation);
        InputPort downstream(source->getPort().getHeader());
        connect(source->getPort(), downstream);
        downstream.setNeeded();

        EXPECT_EQ(runToEnd(*source, downstream), ErrorCodes::EXCHANGE_PEER_DISCONNECTED);
    }
    {
        Peer peer(halfCloseAtOnce);
        auto cancellation = std::make_shared<DistributedQueryCancellation>();
        recordFailure(*cancellation, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
        cancellation->markExecutionFinished();
        auto source = makeSource(peer, cancellation);
        InputPort downstream(source->getPort().getHeader());
        connect(source->getPort(), downstream);
        downstream.setNeeded();

        EXPECT_EQ(runToEnd(*source, downstream), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }
    {
        Peer peer(halfCloseAtOnce);
        auto cancellation = std::make_shared<DistributedQueryCancellation>();
        cancellation->cancel();
        cancellation->markExecutionFinished();
        auto source = makeSource(peer, cancellation);
        InputPort downstream(source->getPort().getHeader());
        connect(source->getPort(), downstream);
        downstream.setNeeded();

        EXPECT_EQ(runToEnd(*source, downstream), std::nullopt);
        EXPECT_EQ(recordedCode(*cancellation), std::nullopt);
    }
}

/// A source whose output is closed (a `LIMIT` downstream is satisfied) needs nothing more from its
/// peer, so a peer that is gone when it sends `NoMoreDataNeeded` is not a failure.
TEST(StreamingExchangeSourceFailureReport, NoMoreDataNeededToGonePeerIsNotAFailure)
{
    Peer peer(handshakeThenReset);
    auto cancellation = std::make_shared<DistributedQueryCancellation>();
    auto source = makeSource(peer, cancellation);

    /// Connects and completes the handshake.
    ASSERT_NO_THROW(source->work());
    peer.join();
    /// Wait until the reset arrived, so the send fails instead of being buffered.
    pollfd descriptor{.fd = source->schedule(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(poll(&descriptor, 1, 5000), 1) << "the peer's reset did not arrive";

    InputPort downstream(source->getPort().getHeader());
    connect(source->getPort(), downstream);
    downstream.close();

    ASSERT_EQ(source->prepare(), IProcessor::Status::Ready);
    EXPECT_NO_THROW(source->work());
    EXPECT_EQ(source->prepare(), IProcessor::Status::Finished);
    EXPECT_FALSE(cancellation->isCancelled());
}

#endif

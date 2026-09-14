#include "config.h"

#if USE_AWS_S3

#include <gtest/gtest.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/Credentials.h>
#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/StreamSocketImpl.h>
#include <Poco/Exception.h>

#include <array>
#include <cstring>
#include <stdexcept>

namespace
{

/// Supplies one packet to the real Poco HTTP buffer. Any further socket read fails immediately,
/// so the tests detect blocking cleanup without relying on timing or a running object store.
class PacketSocket : public Poco::Net::StreamSocketImpl
{
public:
    explicit PacketSocket(std::string packet_) : packet(std::move(packet_))
    {
    }

    int receiveBytes(void * buffer, int length, int) override
    {
        ++reads;
        if (reads != 1)
            throw Poco::TimeoutException("Unexpected socket read during S3 cleanup");
        if (packet.size() > static_cast<size_t>(length))
            throw Poco::InvalidArgumentException("Test packet does not fit in the HTTP buffer");
        std::memcpy(buffer, packet.data(), packet.size());
        return static_cast<int>(packet.size());
    }

    size_t reads = 0;

private:
    std::string packet;
};

class BufferedSession : public Poco::Net::HTTPClientSession
{
public:
    explicit BufferedSession(PacketSocket * socket) : HTTPClientSession(Poco::Net::StreamSocket(socket))
    {
        /// The sentinel stands in for the last byte of the HTTP headers. The body stays buffered.
        if (get() != '#')
            throw Poco::InvalidArgumentException("Missing test packet sentinel");
    }
};

/// Owns the actual fixed-length stream whose completion predicate the connection pool uses.
struct Response
{
    explicit Response(std::string buffered_body = "abcdefghij", size_t content_length = 10)
        : socket(new PacketSocket("#" + buffered_body))
        , session(std::make_shared<BufferedSession>(socket))
        , body(*session, content_length)
    {
    }

    PacketSocket * socket;
    DB::HTTPSessionPtr session;
    Poco::Net::HTTPFixedLengthInputStream body;
};

class Client : public DB::S3::Client
{
public:
    explicit Client(Response & response_)
        : DB::S3::Client(
              1,
              DB::S3::ServerSideEncryptionKMSConfig(),
              std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test_access_key", "test_secret"),
              DB::S3::ClientFactory::instance().createClientConfiguration(
                  "test_region", DB::RemoteHostFilter(), 1,
                  DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
                  true, true, true, false, {}, {}, "http"),
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              DB::S3::ClientSettings())
        , response(response_)
    {
    }

    Aws::S3::Model::GetObjectOutcome GetObject(const Aws::S3::Model::GetObjectRequest &) const override
    {
        auto stream = Aws::Utils::Stream::ResponseStream(
            Aws::New<DB::SessionAwareIOStream<DB::HTTPSessionPtr>>(
                "test S3 response", response.session, response.body.rdbuf()));
        Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> result(
            std::move(stream), Aws::Http::HeaderValueCollection{});
        return Aws::S3::Model::GetObjectResult(std::move(result));
    }

private:
    Response & response;
};

std::unique_ptr<DB::ReadBufferFromS3> makeReader(
    Response & response, bool external = false, size_t limit = 32, size_t read_until = 0,
    std::optional<size_t> file_size = std::nullopt)
{
    DB::ReadSettings settings;
    settings.remote_fs_settings.buffer_size = 4;
    settings.remote_fs_settings.min_bytes_for_seek = limit;
    return std::make_unique<DB::ReadBufferFromS3>(
        std::make_shared<Client>(response), "bucket", "key", "", DB::S3::S3RequestSettings(), settings,
        external, 0, read_until, false, file_size);
}

TEST(S3DrainRemainder, DrainsFromHTTPPositionWithUnknownFileSize)
{
    Response response;
    auto reader = makeReader(response);
    char first;
    ASSERT_TRUE(reader->read(first));
    ASSERT_EQ(first, 'a');
    ASSERT_FALSE(response.body.isComplete());
    reader.reset();
    EXPECT_TRUE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, DoesNotOverwriteExternalBuffer)
{
    Response response;
    std::array<char, 4> external{};
    auto reader = makeReader(response, true, 32, 0, 10);
    reader->set(external.data(), external.size());
    ASSERT_TRUE(reader->next());
    ASSERT_EQ(std::string(external.data(), external.size()), "abcd");
    reader.reset();
    EXPECT_TRUE(response.body.isComplete());
    EXPECT_EQ(std::string(external.data(), external.size()), "abcd");
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, ExternalBufferMayBeFreedBeforeReader)
{
    Response response;
    auto external = std::make_unique<char[]>(4);
    auto reader = makeReader(response, true, 32, 0, 10);
    reader->set(external.get(), 4);
    ASSERT_TRUE(reader->next());
    external.reset();
    reader.reset();
    EXPECT_TRUE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, RangeChangeUsesOldResponseRatherThanConsumerPosition)
{
    Response response;
    auto reader = makeReader(response, false, 6, 10, 1ULL << 30);
    char first;
    ASSERT_TRUE(reader->read(first));
    reader->setReadUntilPosition(20);
    EXPECT_TRUE(response.body.isComplete());
    EXPECT_EQ(reader->getPosition(), 1);
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, OpenEndedRangeChangeUsesOldResponse)
{
    Response response;
    auto reader = makeReader(response, false, 6, 10, 1ULL << 30);
    char first;
    ASSERT_TRUE(reader->read(first));
    reader->setReadUntilEnd();
    EXPECT_TRUE(response.body.isComplete());
    EXPECT_EQ(reader->getPosition(), 1);
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, SeekDrainsDiscardedResponse)
{
    Response response;
    auto reader = makeReader(response);
    ASSERT_TRUE(reader->next());
    EXPECT_EQ(reader->seek(100, SEEK_SET), 100);
    EXPECT_TRUE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, DoesNotWaitForMissingBodyBytes)
{
    Response response("abcdef", 10);
    auto reader = makeReader(response);
    ASSERT_TRUE(reader->next());
    reader.reset();
    EXPECT_FALSE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, RespectsByteLimit)
{
    Response response;
    auto reader = makeReader(response, false, 5);
    ASSERT_TRUE(reader->next());
    reader.reset();
    EXPECT_FALSE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, ZeroLimitDisablesDrain)
{
    Response response;
    auto reader = makeReader(response, false, 0);
    ASSERT_TRUE(reader->next());
    reader.reset();
    EXPECT_FALSE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, DoesNotDrainCanceledReader)
{
    Response response;
    auto reader = makeReader(response);
    ASSERT_TRUE(reader->next());
    reader->cancel();
    reader.reset();
    EXPECT_FALSE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, DoesNotDrainDuringExceptionUnwinding)
{
    Response response;
    EXPECT_THROW(
        {
            auto reader = makeReader(response);
            ASSERT_TRUE(reader->next());
            throw std::runtime_error("Test exception");
        },
        std::runtime_error);
    EXPECT_FALSE(response.body.isComplete());
    EXPECT_EQ(response.socket->reads, 1);
}

TEST(S3DrainRemainder, AlreadyReleasedResponseIsNotReadAgain)
{
    Response response;
    auto reader = makeReader(response);
    std::array<char, 10> data{};
    reader->readStrict(data.data(), data.size());
    ASSERT_TRUE(response.body.isComplete());
    reader.reset();
    EXPECT_EQ(std::string(data.data(), data.size()), "abcdefghij");
    EXPECT_EQ(response.socket->reads, 1);
}

}

#endif

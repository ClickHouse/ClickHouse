#include <gtest/gtest.h>

#include "IO/S3/Credentials.h"
#include "config.h"

#if USE_AWS_S3

#include <IO/ReadBufferFromS3.h>
#include <Poco/Net/HTTPBasicStreamBuf.h>

class CountedSession
{
public:
    CountedSession() { ++total; }
    CountedSession(const CountedSession &) { ++total; }
    ~CountedSession() { --total; }

    static int OustandingObjects() { return total; }

private:
    static int total;
};

int CountedSession::total = 0;

using CountedSessionPtr = std::shared_ptr<CountedSession>;


class StringHTTPBasicStreamBuf : public Poco::Net::HTTPBasicStreamBuf
{
public:
    explicit StringHTTPBasicStreamBuf(std::string body) : BasicBufferedStreamBuf(body.size(), IOS::in), bodyStream(std::stringstream(body))
    {
    }

private:
    std::stringstream bodyStream;

    int readFromDevice(char_type * buf, std::streamsize n) override
    {
        bodyStream.read(buf, n);
        return static_cast<int>(bodyStream.gcount());
    }
};

using GetObjectFn = std::function<Aws::S3::Model::GetObjectOutcome(const Aws::S3::Model::GetObjectRequest & request)>;

struct ClientFake : DB::S3::Client
{
    explicit ClientFake()
        : DB::S3::Client(
              1,
              DB::S3::ServerSideEncryptionKMSConfig(),
              std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test_access_key", "test_secret"),
              DB::S3::ClientFactory::instance().createClientConfiguration(
                  "test_region", DB::RemoteHostFilter(), 1, 0, true, false, {}, {}, "http"),
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              DB::S3::ClientSettings())
    {
    }

    std::optional<GetObjectFn> getObjectImpl;

    void setGetObjectSuccess(const std::shared_ptr<CountedSession> & session, std::streambuf * sb)
    {
        std::weak_ptr weak_session_ptr = session;
        getObjectImpl
            = [weak_session_ptr, sb]([[maybe_unused]] const Aws::S3::Model::GetObjectRequest & request) -> Aws::S3::Model::GetObjectOutcome
        {
            auto response_stream = Aws::Utils::Stream::ResponseStream(
                Aws::New<DB::SessionAwareIOStream<CountedSessionPtr>>("test response stream", weak_session_ptr.lock(), sb));
            Aws::AmazonWebServiceResult aws_result(std::move(response_stream), Aws::Http::HeaderValueCollection());
            DB::S3::Model::GetObjectResult result(std::move(aws_result));
            return DB::S3::Model::GetObjectOutcome(std::move(result));
        };
    }

    Aws::S3::Model::GetObjectOutcome GetObject([[maybe_unused]] const Aws::S3::Model::GetObjectRequest & request) const override
    {
        assert(getObjectImpl);
        return (*getObjectImpl)(request);
    }
};

static void readAndAssert(DB::ReadBuffer & buf, const char * str)
{
    size_t n = strlen(str);
    std::vector<char> tmp(n);
    buf.readStrict(tmp.data(), n);
    ASSERT_EQ(strncmp(tmp.data(), str, n), 0);
}

TEST(ReadBufferFromS3Test, RetainsSessionWhenPending)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings readSettings;
    readSettings.remote_fs_buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), readSettings);

    auto session = std::make_shared<CountedSession>();
    auto stream_buf = std::make_shared<StringHTTPBasicStreamBuf>("123456789");
    client->setGetObjectSuccess(session, stream_buf.get());

    readAndAssert(subject, "123");

    session.reset();
    ASSERT_EQ(CountedSession::OustandingObjects(), 1);

    readAndAssert(subject, "45");
    ASSERT_EQ(CountedSession::OustandingObjects(), 1);
}

TEST(ReadBufferFromS3Test, ReleaseSessionWhenStreamEof)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings readSettings;
    readSettings.remote_fs_buffer_size = 10;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), readSettings);

    auto session = std::make_shared<CountedSession>();
    const auto stream_buf = std::make_shared<StringHTTPBasicStreamBuf>("1234");
    client->setGetObjectSuccess(session, stream_buf.get());

    readAndAssert(subject, "1234");

    session.reset();
    ASSERT_EQ(CountedSession::OustandingObjects(), 0);

    ASSERT_TRUE(subject.eof());
    ASSERT_FALSE(subject.nextImpl());
}

TEST(ReadBufferFromS3Test, ReleaseSessionWhenReadUntilPosition)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings readSettings;
    readSettings.remote_fs_buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), readSettings);

    auto session = std::make_shared<CountedSession>();
    const auto stream_buf = std::make_shared<StringHTTPBasicStreamBuf>("123456");
    client->setGetObjectSuccess(session, stream_buf.get());

    subject.setReadUntilPosition(4);
    readAndAssert(subject, "1234");

    session.reset();
    ASSERT_EQ(CountedSession::OustandingObjects(), 0);

    ASSERT_TRUE(subject.eof());
    ASSERT_FALSE(subject.nextImpl());
}

#endif

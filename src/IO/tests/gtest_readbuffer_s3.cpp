#include <gtest/gtest.h>

#include <IO/S3/Credentials.h>
#include "config.h"

#if USE_AWS_S3

#include <Core/ServerUUID.h>
#include <IO/ReadBufferFromS3.h>
#include <Poco/Net/HTTPBasicStreamBuf.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/Priority.h>
#include <Poco/ConsoleChannel.h>
#include <Core/Settings.h>

static constexpr auto TEST_LOG_LEVEL = "debug";
static fs::path caches_dir = fs::current_path() / "readbuffer_s3";
static std::string cache_base_path = caches_dir / "cache1" / "";

namespace DB::Setting
{
    extern const SettingsUInt64 max_read_buffer_size_remote_fs;
    extern const SettingsString filesystem_cache_name;
    extern const SettingsBool filesystem_cache_prefer_bigger_buffer_size;
    extern const SettingsBool read_from_filesystem_cache_if_exists_otherwise_bypass_cache;
    extern const SettingsUInt64 remote_read_min_bytes_for_seek;
}

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsUInt64 background_download_threads;
}

class ReadBufferFromS3Test : public ::testing::Test
{
public:
    static void setupLogs(const std::string & level)
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel(level);
    }

    void SetUp() override
    {
        if (const char * test_log_level = std::getenv("TEST_LOG_LEVEL")) // NOLINT(concurrency-mt-unsafe)
            setupLogs(test_log_level);
        else
            setupLogs(TEST_LOG_LEVEL);

        if (fs::exists(cache_base_path))
            fs::remove_all(cache_base_path);
        fs::create_directories(cache_base_path);
    }

    void TearDown() override
    {
        if (fs::exists(cache_base_path))
            fs::remove_all(cache_base_path);
    }
};

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
                  "test_region",
                  DB::RemoteHostFilter(),
                  1,
                  DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
                  true,
                  true,
                  true,
                  false,
                  {},
                  /* request_throttler = */ {},
                  "http"),
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
            Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> aws_result(std::move(response_stream), Aws::Http::HeaderValueCollection());
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

TEST_F(ReadBufferFromS3Test, RetainsSessionWhenPending)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings read_settings;
    read_settings.remote_fs_buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), read_settings);

    auto session = std::make_shared<CountedSession>();
    auto stream_buf = std::make_shared<StringHTTPBasicStreamBuf>("123456789");
    client->setGetObjectSuccess(session, stream_buf.get());

    readAndAssert(subject, "123");

    session.reset();
    ASSERT_EQ(CountedSession::OustandingObjects(), 1);

    readAndAssert(subject, "45");
    ASSERT_EQ(CountedSession::OustandingObjects(), 1);
}

TEST_F(ReadBufferFromS3Test, ReleaseSessionWhenStreamEof)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings read_settings;
    read_settings.remote_fs_buffer_size = 10;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), read_settings);

    auto session = std::make_shared<CountedSession>();
    const auto stream_buf = std::make_shared<StringHTTPBasicStreamBuf>("1234");
    client->setGetObjectSuccess(session, stream_buf.get());

    readAndAssert(subject, "1234");

    session.reset();
    ASSERT_EQ(CountedSession::OustandingObjects(), 0);

    ASSERT_TRUE(subject.eof());
    ASSERT_FALSE(subject.nextImpl());
}

TEST_F(ReadBufferFromS3Test, ReleaseSessionWhenReadUntilPosition)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings read_settings;
    read_settings.remote_fs_buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), read_settings);

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

TEST_F(ReadBufferFromS3Test, HavingZeroBytes)
{
    /// This test fails to reproduce "Having zero bytes..." exception,
    /// but let's leave it here anyway as an example how to write tests with S3 engine source reader with enabled cache,
    /// as it takes time to set up.

    DB::ServerUUID::setRandomForUnitTests();
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    std::string query_id = "query_id";
    query_context->setCurrentQueryId(query_id);
    const auto & settings = query_context->getSettingsRef();
    const_cast<DB::Settings &>(settings)[DB::Setting::max_read_buffer_size_remote_fs] = 4;
    const_cast<DB::Settings &>(settings)[DB::Setting::filesystem_cache_name] = "cache1";
    const_cast<DB::Settings &>(settings)[DB::Setting::filesystem_cache_prefer_bigger_buffer_size] = false;
    //const_cast<DB::Settings &>(settings)[DB::Setting::read_from_filesystem_cache_if_exists_otherwise_bypass_cache] = true;
    const_cast<DB::Settings &>(settings)[DB::Setting::remote_read_min_bytes_for_seek] = 0;

    DB::FileCacheSettings cache_settings;
    cache_settings[DB::FileCacheSetting::path] = cache_base_path;
    cache_settings[DB::FileCacheSetting::max_size] = 100;
    cache_settings[DB::FileCacheSetting::max_elements] = 5;
    cache_settings[DB::FileCacheSetting::boundary_alignment] = 5;
    cache_settings[DB::FileCacheSetting::max_file_segment_size] = 100;
    cache_settings[DB::FileCacheSetting::background_download_threads] = 0;
    auto cache = DB::FileCacheFactory::instance().getOrCreate("cache1", cache_settings, "");
    cache->initialize();
    cache.reset();

    std::unique_ptr<DB::S3::Client> client = std::make_unique<ClientFake>();
    DB::S3::URI uri;
    uri.bucket = "test_bucket";
    DB::S3Capabilities cap;
    String disk_name = "s3";
    DB::ObjectStorageKeysGeneratorPtr gen;
    auto object_storage = std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(), std::move(uri), cap, gen, disk_name);

    auto log = getLogger("test");
    DB::ObjectMetadata object_metadata;
    std::string data = "12345678901234567890";
    object_metadata.size_bytes = data.size();
    object_metadata.etag = "tag1";
    DB::RelativePathWithMetadata relative_path_with_metadata("test_key", object_metadata);
    auto buf = DB::createReadBuffer(relative_path_with_metadata, object_storage, query_context, log);

    auto session = std::make_shared<CountedSession>();
    const auto stream_buf = std::make_shared<StringHTTPBasicStreamBuf>(data);
    auto storage_client = object_storage->getS3StorageClient();
    dynamic_cast<ClientFake *>(const_cast<DB::S3::Client *>(storage_client.get()))->setGetObjectSuccess(session, stream_buf.get());

    auto * async_buf = dynamic_cast<DB::AsynchronousBoundedReadBuffer *>(buf.get());
    auto * cached_buf = dynamic_cast<DB::CachedOnDiskReadBufferFromFile *>(async_buf->getImpl().get());
    ASSERT_TRUE(async_buf);
    ASSERT_TRUE(cached_buf);

    async_buf->prefetch(Priority{0});
    async_buf->next();
    ASSERT_EQ(async_buf->available(), 4);
    async_buf->position() = async_buf->buffer().end();
    async_buf->prefetch(Priority{0});
    async_buf->seek(16, SEEK_SET);
    ASSERT_EQ(async_buf->available(), 0);
    async_buf->prefetch(Priority{0});
    async_buf->next();
    ASSERT_EQ(async_buf->available(), 4);
    async_buf->position() = async_buf->buffer().end();
    async_buf->prefetch(Priority{0});
    async_buf->next();
    ASSERT_EQ(async_buf->available(), 0);

    DB::FileCacheFactory::instance().clear();
}
#endif

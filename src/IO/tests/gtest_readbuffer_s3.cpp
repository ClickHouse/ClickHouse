#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <IO/S3/Credentials.h>
#include "config.h"

#if USE_AWS_S3

#include <Core/ServerUUID.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/SharedThreadPools.h>
#include <IO/StdStreamFromReadBuffer.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/Priority.h>
#include <Common/ProfileEvents.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Poco/ConsoleChannel.h>
#include <Core/Settings.h>

static constexpr auto TEST_LOG_LEVEL = "debug";
static fs::path caches_dir = fs::current_path() / "readbuffer_s3";
static std::string cache_base_path = caches_dir / "cache1" / "";

namespace DB::Setting
{
    extern const SettingsUInt64 max_download_buffer_size;
    extern const SettingsUInt64 max_read_buffer_size_remote_fs;
    extern const SettingsString filesystem_cache_name;
    extern const SettingsBool filesystem_cache_prefer_bigger_buffer_size;
    extern const SettingsBool read_from_filesystem_cache_if_exists_otherwise_bypass_cache;
    extern const SettingsUInt64 remote_read_min_bytes_for_seek;
}

namespace DB::ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int HTTP_RANGE_NOT_SATISFIABLE;
}

namespace ProfileEvents
{
    extern const Event ReadBufferFromS3RequestsErrors;
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
    static std::atomic<int> total;
};

std::atomic<int> CountedSession::total = 0;

using CountedSessionPtr = std::shared_ptr<CountedSession>;


/// Stands in for the body of a response: hands the data out through the memory of the caller,
/// the way the buffer of a real response reads from a socket, and holds the session while it is
/// alive.
class FakeResponseBody : public DB::ReadBuffer
{
public:
    FakeResponseBody(std::string data_, CountedSessionPtr session_)
        : DB::ReadBuffer(nullptr, 0), data(std::move(data_)), session(std::move(session_))
    {
    }

    bool supportsExternalBufferMode() const override { return true; }

    /// Like the body of a real response, reads straight into the memory of the caller: this
    /// buffer has no memory of its own, so the default `readBig` would see an empty buffer.
    size_t readBig(char * to, size_t n) override
    {
        /// Nothing may be left in the buffer, whose memory is then null: passing that to `memcpy`
        /// is undefined behavior even for zero bytes.
        size_t copied = std::min(available(), n);
        if (copied)
        {
            memcpy(to, position(), copied);
            position() += copied;
        }

        const size_t rest = std::min(n - copied, data.size() - position_in_data);
        memcpy(to + copied, data.data() + position_in_data, rest);
        position_in_data += rest;
        return copied + rest;
    }

private:
    bool nextImpl() override
    {
        if (position_in_data >= data.size() || internal_buffer.empty())
            return false;

        const size_t size = std::min(internal_buffer.size(), data.size() - position_in_data);
        memcpy(internal_buffer.begin(), data.data() + position_in_data, size);
        position_in_data += size;

        working_buffer = internal_buffer;
        working_buffer.resize(size);
        return true;
    }

    std::string data;
    CountedSessionPtr session;
    size_t position_in_data = 0;
};

/// Stands in for the body of a response that reads through a small buffer of its own, so that
/// consuming it through `std::istream` takes many `underflow` calls.
class ChunkedResponseBody : public DB::BufferWithOwnMemory<DB::ReadBuffer>
{
public:
    ChunkedResponseBody(std::string data_, size_t chunk_size)
        : DB::BufferWithOwnMemory<DB::ReadBuffer>(chunk_size), data(std::move(data_))
    {
    }

private:
    bool nextImpl() override
    {
        if (position_in_data >= data.size())
            return false;

        const size_t size = std::min(internal_buffer.size(), data.size() - position_in_data);
        memcpy(internal_buffer.begin(), data.data() + position_in_data, size);
        position_in_data += size;

        working_buffer = internal_buffer;
        working_buffer.resize(size);
        return true;
    }

    std::string data;
    size_t position_in_data = 0;
};

/// Fails on the very first read, so that `readBigAt` takes its retry path while the session of
/// the failed request is still owned by the body of the response.
class ThrowingResponseBody : public DB::ReadBuffer
{
public:
    ThrowingResponseBody(std::atomic<bool> & read_attempted_, CountedSessionPtr session_)
        : DB::ReadBuffer(nullptr, 0), read_attempted(read_attempted_), session(std::move(session_))
    {
    }

    bool supportsExternalBufferMode() const override { return true; }

private:
    bool nextImpl() override
    {
        read_attempted = true;
        throw std::runtime_error("injected response body failure");
    }

    std::atomic<bool> & read_attempted;
    CountedSessionPtr session;
};

using GetObjectFn = std::function<Aws::S3::Model::GetObjectOutcome(const Aws::S3::Model::GetObjectRequest & request)>;

/// The configuration holds the filter by reference, so the filter must outlive every client built from it.
static const DB::RemoteHostFilter & allowAnyHost()
{
    static const DB::RemoteHostFilter filter;
    return filter;
}

struct ClientFake : DB::S3::Client
{
    explicit ClientFake()
        : DB::S3::Client(
              1,
              DB::S3::ServerSideEncryptionKMSConfig(),
              std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test_access_key", "test_secret"),
              DB::S3::ClientFactory::instance().createClientConfiguration(
                  "test_region",
                  allowAnyHost(),
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
    using ListObjectsV2Fn = std::function<Aws::S3::Model::ListObjectsV2Outcome(const Aws::S3::Model::ListObjectsV2Request &)>;
    std::optional<ListObjectsV2Fn> listObjectsV2Impl;
    mutable std::optional<std::string> last_start_after;
    struct ListRequest
    {
        std::string start_after;
        std::string continuation_token;
        bool start_after_has_been_set{false};
    };
    mutable std::vector<ListRequest> list_requests;

    void setGetObjectSuccess(const std::shared_ptr<CountedSession> & session, std::string body)
    {
        std::weak_ptr weak_session_ptr = session;
        getObjectImpl
            = [weak_session_ptr, body]([[maybe_unused]] const Aws::S3::Model::GetObjectRequest & request) -> Aws::S3::Model::GetObjectOutcome
        {
            auto response_stream = Aws::Utils::Stream::ResponseStream(Aws::New<DB::StdStreamFromReadBuffer>(
                "test response stream",
                std::make_unique<FakeResponseBody>(body, weak_session_ptr.lock()),
                body.size()));
            Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> aws_result(
                std::move(response_stream), Aws::Http::HeaderValueCollection{{"content-length", std::to_string(body.size())}});
            DB::S3::Model::GetObjectResult result(std::move(aws_result));
            return DB::S3::Model::GetObjectOutcome(std::move(result));
        };
    }

    /// Serves `object` honoring the `Range` header as S3 does: `bytes=a-b` (inclusive) or `bytes=a-`, with a `Content-Range`.
    /// A response carries at most `max_response_size` bytes. With `ignore_range` every request is answered with the object
    /// from byte 0 and no `Content-Range`, as a `200 OK`. The start offset of every request is recorded in `getObjectOffsets`.
    void setGetObjectRanges(std::string object, size_t max_response_size = std::numeric_limits<size_t>::max(), bool ignore_range = false)
    {
        object_size = object.size();
        getObjectImpl = [this, body = std::move(object), max_response_size, ignore_range](
                            const Aws::S3::Model::GetObjectRequest & request) -> Aws::S3::Model::GetObjectOutcome
        {
            size_t begin = 0;
            size_t end = body.size();
            if (request.RangeHasBeenSet())
            {
                const std::string & range = request.GetRange();
                const size_t prefix = std::string_view("bytes=").size();
                const size_t dash = range.find('-');
                begin = std::stoull(range.substr(prefix, dash - prefix));
                if (dash + 1 < range.size())
                    end = std::min<size_t>(end, std::stoull(range.substr(dash + 1)) + 1);
            }
            const size_t requested_begin = begin;

            if (ignore_range)
            {
                begin = 0;
                end = body.size();
            }
            begin = std::min(begin, body.size());
            end = std::min(std::max(begin, end), begin + std::min(max_response_size, body.size() - begin));
            const size_t size = end - begin;

            Aws::Http::HeaderValueCollection headers{{"content-length", std::to_string(size)}};
            if (!ignore_range && request.RangeHasBeenSet())
                headers.emplace("content-range", fmt::format("bytes {}-{}/{}", begin, end > begin ? end - 1 : begin, body.size()));

            {
                std::lock_guard lock(get_object_offsets_mutex);
                get_object_offsets.push_back(requested_begin);
            }
            auto response_stream = Aws::Utils::Stream::ResponseStream(Aws::New<DB::StdStreamFromReadBuffer>(
                "test response stream",
                std::make_unique<FakeResponseBody>(body.substr(begin, size), std::make_shared<CountedSession>()),
                size));
            Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> aws_result(std::move(response_stream), std::move(headers));
            return DB::S3::Model::GetObjectOutcome(DB::S3::Model::GetObjectResult(std::move(aws_result)));
        };
    }

    std::vector<size_t> getObjectOffsets() const
    {
        std::lock_guard lock(get_object_offsets_mutex);
        return get_object_offsets;
    }

    /// Answers with the size of the object given to `setGetObjectRanges`, so that nothing goes to the network.
    Aws::S3::Model::HeadObjectOutcome HeadObject(const Aws::S3::Model::HeadObjectRequest &) const override
    {
        if (!object_size)
            return Aws::S3::Model::HeadObjectOutcome(
                Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "No object is set in the fake", false));

        Aws::S3::Model::HeadObjectResult result;
        result.SetContentLength(*object_size);
        return result;
    }

    Aws::S3::Model::GetObjectOutcome GetObject([[maybe_unused]] const Aws::S3::Model::GetObjectRequest & request) const override
    {
        chassert(getObjectImpl);
        return (*getObjectImpl)(request);
    }

    Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(const Aws::S3::Model::ListObjectsV2Request & request) const override
    {
        last_start_after = request.GetStartAfter().c_str();
        list_requests.emplace_back(ListRequest{
            .start_after = request.GetStartAfter(),
            .continuation_token = request.GetContinuationToken(),
            .start_after_has_been_set = request.StartAfterHasBeenSet()});

        if (listObjectsV2Impl)
            return (*listObjectsV2Impl)(request);

        Aws::S3::Model::ListObjectsV2Result result;
        result.SetIsTruncated(false);
        return Aws::S3::Model::ListObjectsV2Outcome(std::move(result));
    }

private:
    std::optional<size_t> object_size;
    mutable std::mutex get_object_offsets_mutex;
    std::vector<size_t> get_object_offsets;
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
    read_settings.remote_fs_settings.buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), read_settings);

    auto session = std::make_shared<CountedSession>();
    client->setGetObjectSuccess(session, "123456789");

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
    read_settings.remote_fs_settings.buffer_size = 10;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), read_settings);

    auto session = std::make_shared<CountedSession>();
    client->setGetObjectSuccess(session, "1234");

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
    read_settings.remote_fs_settings.buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), read_settings);

    auto session = std::make_shared<CountedSession>();
    client->setGetObjectSuccess(session, "123456");

    subject.setReadUntilPosition(4);
    readAndAssert(subject, "1234");

    session.reset();
    ASSERT_EQ(CountedSession::OustandingObjects(), 0);

    ASSERT_TRUE(subject.eof());
    ASSERT_FALSE(subject.nextImpl());
}

TEST_F(ReadBufferFromS3Test, ReadBigAtReleasesSessionBeforeRetryBackoff)
{
    /// Contract: when a `readBigAt` request fails mid-read and is going to be retried, its pooled
    /// session must be given up before the back-off pause, not kept for its whole duration. The
    /// `supportsReadAt` readers (Parquet, ORC) issue many such requests in parallel, and holding a
    /// session per sleeping retry takes connections away from the rest of the same group.
    const int baseline = CountedSession::OustandingObjects();

    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings read_settings;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3::S3RequestSettings(), read_settings);

    std::atomic<bool> first_request_failed = false;
    std::atomic<bool> retry_request_sent = false;
    std::atomic<bool> released_before_retry = false;

    const std::string body = "1234567890";

    size_t requests = 0;
    client->getObjectImpl = [&](const Aws::S3::Model::GetObjectRequest &) -> Aws::S3::Model::GetObjectOutcome
    {
        const bool is_first = requests++ == 0;
        if (!is_first)
            retry_request_sent = true;

        /// Owned by the body of the response, exactly as a pooled session is.
        auto session = std::make_shared<CountedSession>();
        std::unique_ptr<DB::ReadBuffer> response_body;
        if (is_first)
            response_body = std::make_unique<ThrowingResponseBody>(first_request_failed, std::move(session));
        else
            response_body = std::make_unique<FakeResponseBody>(body, std::move(session));

        auto response_stream = Aws::Utils::Stream::ResponseStream(
            Aws::New<DB::StdStreamFromReadBuffer>("test response stream", std::move(response_body), body.size()));
        Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> aws_result(
            std::move(response_stream), Aws::Http::HeaderValueCollection{{"content-length", std::to_string(body.size())}});
        return DB::S3::Model::GetObjectOutcome(DB::S3::Model::GetObjectResult(std::move(aws_result)));
    };

    /// Watches the window between the failure and the retry request. Sampling cannot miss the
    /// release when it happens before the back-off, because the whole pause is inside the window.
    std::thread watcher([&]
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!first_request_failed.load() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
        while (!retry_request_sent.load() && std::chrono::steady_clock::now() < deadline)
        {
            if (CountedSession::OustandingObjects() == baseline)
            {
                released_before_retry = true;
                return;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    std::vector<char> data(10);
    const size_t read = subject.readBigAt(data.data(), data.size(), /*range_begin=*/0, /*progress_callback=*/{});
    watcher.join();

    ASSERT_EQ(read, data.size());
    ASSERT_EQ(std::string(data.data(), data.size()), "1234567890");
    ASSERT_EQ(requests, 2u);
    ASSERT_TRUE(released_before_retry.load());
    ASSERT_EQ(CountedSession::OustandingObjects(), baseline);
}

TEST_F(ReadBufferFromS3Test, MissingResponseETagIsNotRejected)
{
    /// Contract: a GET whose response omits the ETag header must NOT be rejected even when a non-empty
    /// expected_etag was requested. Some S3-compatible backends (and zero-byte reads) omit the header;
    /// the read must succeed rather than raise S3_OBJECT_CHANGED_DURING_READ. Guards the `!response_etag.empty()`
    /// clause in ReadBufferFromS3::initialize so this compatibility path cannot regress silently.
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = 10;
    auto subject = DB::ReadBufferFromS3(
        client,
        "test_bucket",
        "test_key",
        /*version_id_=*/"",
        DB::S3::S3RequestSettings(),
        read_settings,
        /*use_external_buffer=*/false,
        /*offset_=*/0,
        /*read_until_position_=*/0,
        /*restricted_seek_=*/false,
        /*file_size=*/std::nullopt,
        /*credentials_refresh_callback_=*/[] { return nullptr; },
        /*blob_storage_log_=*/{},
        /*expected_etag_=*/"expected-tag-1");

    auto session = std::make_shared<CountedSession>();
    /// setGetObjectSuccess builds the GetObjectResult without an ETag header.
    client->setGetObjectSuccess(session, "1234");

    readAndAssert(subject, "1234");
    ASSERT_TRUE(subject.eof());
}

TEST_F(ReadBufferFromS3Test, ResponseStreamServesIstreamReaders)
{
    /// Contract: the body of a response handed to the AWS SDK as `std::iostream` can be consumed
    /// through the `std::istream` interface itself - a character at a time through the get area
    /// of `underflow`, in blocks through `xsgetn`, or both interleaved - the way the parsers of the
    /// SDK read it, and not only by unwrapping it back into a `ReadBuffer`.
    std::string data(1000, '\0');
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<char>('a' + i % 26);

    {
        DB::StdStreamFromReadBuffer stream(std::make_unique<ChunkedResponseBody>(data, 7), data.size());
        std::string consumed{std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
        ASSERT_EQ(consumed, data);
    }

    {
        DB::StdStreamFromReadBuffer stream(std::make_unique<ChunkedResponseBody>(data, 7), data.size());
        std::string consumed;
        while (consumed.size() < data.size())
        {
            /// A single character puts a whole chunk into the get area, the block read that follows
            /// has to take the rest of the get area first and then continue from the buffer.
            const int c = stream.get();
            ASSERT_NE(c, std::char_traits<char>::eof());
            consumed.push_back(static_cast<char>(c));

            std::string block(std::min<size_t>(10, data.size() - consumed.size()), '\0');
            stream.read(block.data(), static_cast<std::streamsize>(block.size()));
            ASSERT_EQ(static_cast<size_t>(stream.gcount()), block.size());
            consumed += block;
        }
        ASSERT_EQ(consumed, data);
        ASSERT_EQ(stream.get(), std::char_traits<char>::eof());
    }

    {
        /// The SDK positions the output side of a stream it only reads from. That has to fail the
        /// way `std::streambuf` says - by returning -1 and setting `failbit` - and not throw, and
        /// it must not move the input side.
        DB::StdStreamFromReadBuffer stream(std::make_unique<ChunkedResponseBody>(data, 7), data.size());
        ASSERT_EQ(stream.get(), 'a');

        ASSERT_EQ(stream.rdbuf()->pubseekpos(0, std::ios_base::out), std::streampos(std::streamoff(-1)));
        ASSERT_EQ(stream.rdbuf()->pubseekoff(0, std::ios_base::end, std::ios_base::out), std::streampos(std::streamoff(-1)));

        ASSERT_NO_THROW(stream.seekp(0));
        ASSERT_TRUE(stream.fail());
        stream.clear();

        ASSERT_EQ(stream.get(), 'b');
    }
}

TEST_F(ReadBufferFromS3Test, GatherFillsWholeBufferAfterShortRead)
{
    /// Contract: a gather with its own buffer lends the whole buffer to the object reader on every fill,
    /// so a short fill at the end of one bounded range does not cap the fills of the next range.
    std::string data(1000, '\0');
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<char>('a' + i % 26);

    const auto client = std::make_shared<ClientFake>();
    client->setGetObjectRanges(data);

    DB::ReadSettings read_settings;
    DB::ReadBufferFromRemoteFSGather::ReadBufferCreator creator
        = [client, read_settings](bool restricted_seek, const DB::StoredObject & object) -> std::unique_ptr<DB::ReadBufferFromFileBase>
    {
        return std::make_unique<DB::ReadBufferFromS3>(
            client, "test_bucket", object.remote_path, /*version_id_=*/"", DB::S3::S3RequestSettings(), read_settings,
            /*use_external_buffer=*/true, /*offset_=*/0, /*read_until_position_=*/0, restricted_seek, object.bytes_size);
    };

    DB::ReadBufferFromRemoteFSGather gather(
        std::move(creator),
        DB::StoredObjects{DB::StoredObject("test_key", "", data.size())},
        read_settings.remote_fs_settings.min_bytes_for_seek,
        /*use_external_buffer_=*/false,
        /*buffer_size=*/100,
        /*query_status_=*/nullptr);

    auto read_and_check = [&](size_t n)
    {
        const size_t begin = gather.getPosition();
        std::string chunk(n, '\0');
        gather.readStrict(chunk.data(), n);
        ASSERT_EQ(chunk, data.substr(begin, n));
    };

    /// Fills of 100 and 50 bytes.
    gather.setReadUntilPosition(150);
    read_and_check(150);

    gather.setReadUntilPosition(630);
    ASSERT_TRUE(gather.next());
    EXPECT_EQ(gather.available(), 100);
    read_and_check(480);

    gather.setReadUntilPosition(1000);
    ASSERT_TRUE(gather.next());
    EXPECT_EQ(gather.available(), 100);
    read_and_check(370);
    ASSERT_TRUE(gather.eof());
}

TEST_F(ReadBufferFromS3Test, IterateUsesStartAfter)
{
    std::unique_ptr<DB::S3::Client> client = std::make_unique<ClientFake>();
    DB::S3::URI uri;
    uri.bucket = "test_bucket";
    DB::S3Capabilities cap;
    String disk_name = "s3";
    DB::ObjectStorageKeyGeneratorPtr gen;
    auto object_storage = std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(), std::move(uri), cap, gen, disk_name);

    const std::optional<std::string> start_after = "prefix/file_010";
    auto iterator = object_storage->iterate("prefix/", /*max_keys=*/1000, /*with_tags=*/false, start_after);
    iterator->getCurrentBatchAndScheduleNext();

    auto storage_client = object_storage->getS3StorageClient();
    auto * fake = dynamic_cast<ClientFake *>(const_cast<DB::S3::Client *>(storage_client.get()));
    ASSERT_TRUE(fake);
    ASSERT_TRUE(fake->last_start_after.has_value());
    ASSERT_EQ(*fake->last_start_after, "prefix/file_010");
}

TEST_F(ReadBufferFromS3Test, IterateUsesStartAfterOnlyForFirstPage)
{
    auto client = std::make_unique<ClientFake>();
    auto * fake = client.get();
    fake->listObjectsV2Impl = [call = 0](const Aws::S3::Model::ListObjectsV2Request &) mutable
    {
        Aws::S3::Model::ListObjectsV2Result result;
        if (call == 0)
        {
            Aws::S3::Model::Object object;
            object.SetKey("prefix/file_011");
            result.AddContents(object);
            result.SetIsTruncated(true);
            result.SetNextContinuationToken("next-page-token");
        }
        else
        {
            Aws::S3::Model::Object object;
            object.SetKey("prefix/file_012");
            result.AddContents(object);
            result.SetIsTruncated(false);
        }
        ++call;
        return Aws::S3::Model::ListObjectsV2Outcome(std::move(result));
    };

    DB::S3::URI uri;
    uri.bucket = "test_bucket";
    DB::S3Capabilities cap;
    String disk_name = "s3";
    DB::ObjectStorageKeyGeneratorPtr gen;
    auto object_storage = std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(), std::move(uri), cap, gen, disk_name);

    const std::optional<std::string> start_after = "prefix/file_010";
    auto iterator = object_storage->iterate("prefix/", /*max_keys=*/1, /*with_tags=*/false, start_after);
    while (iterator->getCurrentBatchAndScheduleNext())
    {
    }

    ASSERT_EQ(fake->list_requests.size(), 2);
    ASSERT_EQ(fake->list_requests[0].start_after, "prefix/file_010");
    ASSERT_TRUE(fake->list_requests[0].start_after_has_been_set);
    ASSERT_TRUE(fake->list_requests[0].continuation_token.empty());
    ASSERT_TRUE(fake->list_requests[1].start_after.empty());
    ASSERT_FALSE(fake->list_requests[1].start_after_has_been_set);
    ASSERT_EQ(fake->list_requests[1].continuation_token, "next-page-token");
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
    DB::ObjectStorageKeyGeneratorPtr gen;
    auto object_storage = std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(), std::move(uri), cap, gen, disk_name);

    auto log = getLogger("test");
    DB::ObjectMetadata object_metadata;
    std::string data = "12345678901234567890";
    object_metadata.size_bytes = data.size();
    object_metadata.etag = "tag1";
    DB::RelativePathWithMetadata relative_path_with_metadata("test_key", object_metadata);
    /// Configure the fake GET stub before createReadBuffer: for a small object it issues the
    /// initial prefetch eagerly (also over the filesystem cache), so the data must already be
    /// servable when that background read runs.
    auto storage_client = object_storage->getS3StorageClient();
    dynamic_cast<ClientFake *>(const_cast<DB::S3::Client *>(storage_client.get()))->setGetObjectRanges(data);

    auto buf = DB::createReadBuffer(relative_path_with_metadata, object_storage, query_context, log);

    auto * async_buf = dynamic_cast<DB::AsynchronousBoundedReadBuffer *>(buf.get());
    ASSERT_TRUE(async_buf);
    auto * cached_buf = dynamic_cast<DB::CachedOnDiskReadBufferFromFile *>(async_buf->getImpl().get());
    ASSERT_TRUE(cached_buf);

    /// The initial small-object prefetch is already in flight, so this manual prefetch is a no-op
    /// on the pending future; the driven read/seek sequence below is unchanged.
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
namespace
{

std::string makeObject(size_t size)
{
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>('a' + i % 26);
    return data;
}

struct BoundedReadResult
{
    std::string data;
    std::optional<int> error_code;
};

/// Reads the whole buffer and keeps the bytes handed out before an exception, if there is one.
BoundedReadResult readToEnd(DB::ReadBuffer & buf)
{
    BoundedReadResult result;
    try
    {
        while (!buf.eof())
        {
            result.data.append(buf.position(), buf.available());
            buf.position() = buf.buffer().end();
        }
    }
    catch (const DB::Exception & e)
    {
        result.error_code = e.code();
    }
    return result;
}

BoundedReadResult readWithRightBound(const std::shared_ptr<ClientFake> & client, size_t read_until_position, size_t buffer_size, size_t offset = 0)
{
    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;
    DB::ReadBufferFromS3 buf(client, "test_bucket", "test_key", "", DB::S3::S3RequestSettings(), read_settings);
    if (read_until_position)
        buf.setReadUntilPosition(read_until_position);
    if (offset)
        buf.seek(offset, SEEK_SET);
    return readToEnd(buf);
}

DB::ContextMutablePtr createQueryContext()
{
    DB::ServerUUID::setRandomForUnitTests();
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("query_id");
    return query_context;
}

void setSetting(const DB::ContextMutablePtr & context, const auto & setting, auto value)
{
    const_cast<DB::Settings &>(context->getSettingsRef())[setting] = value;
}

/// The read buffer of an `s3()` source for an object whose size from HEAD is `size_from_head`.
std::unique_ptr<DB::ReadBufferFromFileBase> createSourceReadBuffer(
    std::unique_ptr<ClientFake> client, size_t size_from_head, const DB::ContextMutablePtr & query_context)
{
    DB::S3::URI uri;
    uri.bucket = "test_bucket";
    auto object_storage = std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(), std::move(uri), DB::S3Capabilities{}, DB::ObjectStorageKeyGeneratorPtr{}, "s3");

    DB::ObjectMetadata object_metadata;
    object_metadata.size_bytes = size_from_head;
    object_metadata.etag = "tag1";
    DB::RelativePathWithMetadata relative_path_with_metadata("test_key", object_metadata);
    return DB::createReadBuffer(relative_path_with_metadata, object_storage, query_context, getLogger("test"));
}

UInt64 readErrors()
{
    return ProfileEvents::global_counters[ProfileEvents::ReadBufferFromS3RequestsErrors];
}

}

TEST_F(ReadBufferFromS3Test, CappedResponsesAreReassembled)
{
    const auto data = makeObject(100);
    const auto client = std::make_shared<ClientFake>();
    client->setGetObjectRanges(data, /*max_response_size=*/40);

    const auto errors_before = readErrors();
    const auto result = readWithRightBound(client, /*read_until_position=*/100, /*buffer_size=*/64);

    ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
    EXPECT_EQ(result.data, data);
    EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{0, 40, 80}));
    EXPECT_EQ(readErrors(), errors_before);
}

TEST_F(ReadBufferFromS3Test, CappedResponsesEndingOnAFillBoundaryAreReassembled)
{
    const auto data = makeObject(100);
    const auto client = std::make_shared<ClientFake>();
    client->setGetObjectRanges(data, /*max_response_size=*/40);

    const auto errors_before = readErrors();
    const auto result = readWithRightBound(client, /*read_until_position=*/100, /*buffer_size=*/20);

    ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
    EXPECT_EQ(result.data, data);
    EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{0, 40, 80}));
    EXPECT_EQ(readErrors(), errors_before);
}

TEST_F(ReadBufferFromS3Test, ResponsesThatStopBeforeTheRightBoundAreAnError)
{
    /// With a buffer of 64 the end of the response is seen with its last fill, with 20 only by the next read.
    for (const size_t buffer_size : {64, 20})
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(makeObject(40));

        const auto errors_before = readErrors();
        const auto result = readWithRightBound(client, /*read_until_position=*/100, buffer_size);

        ASSERT_TRUE(result.error_code.has_value()) << buffer_size;
        EXPECT_EQ(*result.error_code, DB::ErrorCodes::UNEXPECTED_END_OF_FILE) << buffer_size;
        /// The request that continues the read gets the whole `max_single_read_retries` budget of 4 attempts.
        EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{0, 40, 40, 40, 40})) << buffer_size;
        EXPECT_GT(readErrors(), errors_before) << buffer_size;
    }
}

TEST_F(ReadBufferFromS3Test, ExactResponseAndUnboundedReadAreNotReopened)
{
    const auto data = makeObject(100);
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data);
        const auto result = readWithRightBound(client, /*read_until_position=*/100, /*buffer_size=*/64);

        ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
        EXPECT_EQ(result.data, data);
        EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{0}));
    }
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data, /*max_response_size=*/40);
        const auto result = readWithRightBound(client, /*read_until_position=*/0, /*buffer_size=*/64);

        ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
        EXPECT_EQ(result.data, data.substr(0, 40));
        EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{0}));
    }
}

TEST_F(ReadBufferFromS3Test, RangeIgnoringEndpointIsRejectedOnReopen)
{
    const auto data = makeObject(100);
    const auto client = std::make_shared<ClientFake>();
    client->setGetObjectRanges(data, /*max_response_size=*/50, /*ignore_range=*/true);

    const auto result = readWithRightBound(client, /*read_until_position=*/100, /*buffer_size=*/64);

    ASSERT_TRUE(result.error_code.has_value());
    EXPECT_EQ(*result.error_code, DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
    EXPECT_EQ(result.data, data.substr(0, 50));
}

TEST_F(ReadBufferFromS3Test, RangeIgnoringEndpointIsRejectedAtANonzeroOffset)
{
    const auto data = makeObject(100);
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data, std::numeric_limits<size_t>::max(), /*ignore_range=*/true);
        const auto result = readWithRightBound(client, /*read_until_position=*/100, /*buffer_size=*/64, /*offset=*/40);

        ASSERT_TRUE(result.error_code.has_value());
        EXPECT_EQ(*result.error_code, DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
        EXPECT_TRUE(result.data.empty());
        EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{40, 40, 40, 40}));
    }
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data, /*max_response_size=*/40);
        const auto result = readWithRightBound(client, /*read_until_position=*/100, /*buffer_size=*/64, /*offset=*/40);

        ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
        EXPECT_EQ(result.data, data.substr(40));
        EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{40, 80}));
    }
}

TEST_F(ReadBufferFromS3Test, LongerResponseIsCutAtTheRightBound)
{
    const auto data = makeObject(100);
    const auto client = std::make_shared<ClientFake>();
    client->setGetObjectRanges(data, /*max_response_size=*/std::numeric_limits<size_t>::max(), /*ignore_range=*/true);

    const auto result = readWithRightBound(client, /*read_until_position=*/60, /*buffer_size=*/64);

    ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
    EXPECT_EQ(result.data, data.substr(0, 60));
    EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{0}));
}

TEST_F(ReadBufferFromS3Test, PositionedReadChecksTheReturnedRangeStart)
{
    const auto data = makeObject(100);
    auto read_at = [&](bool ignore_range, size_t range_begin) -> BoundedReadResult
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data, std::numeric_limits<size_t>::max(), ignore_range);
        DB::ReadBufferFromS3 buf(client, "test_bucket", "test_key", "", DB::S3::S3RequestSettings(), DB::ReadSettings{});

        BoundedReadResult result;
        result.data.resize(16);
        try
        {
            result.data.resize(buf.readBigAt(result.data.data(), result.data.size(), range_begin, {}));
        }
        catch (const DB::Exception & e)
        {
            result.error_code = e.code();
        }
        return result;
    };

    /// A `Range`-ignoring endpoint answers with the object from byte 0, which is a correct answer only at offset 0.
    const auto refused = read_at(/*ignore_range=*/true, /*range_begin=*/40);
    ASSERT_TRUE(refused.error_code.has_value());
    EXPECT_EQ(*refused.error_code, DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);

    const auto at_zero = read_at(/*ignore_range=*/true, /*range_begin=*/0);
    ASSERT_FALSE(at_zero.error_code.has_value()) << *at_zero.error_code;
    EXPECT_EQ(at_zero.data, data.substr(0, 16));

    const auto ranged = read_at(/*ignore_range=*/false, /*range_begin=*/40);
    ASSERT_FALSE(ranged.error_code.has_value()) << *ranged.error_code;
    EXPECT_EQ(ranged.data, data.substr(40, 16));
}

TEST_F(ReadBufferFromS3Test, UnboundedReadAfterASeekChecksTheReturnedRangeStart)
{
    const auto data = makeObject(100);
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data, std::numeric_limits<size_t>::max(), /*ignore_range=*/true);
        const auto result = readWithRightBound(client, /*read_until_position=*/0, /*buffer_size=*/64, /*offset=*/40);

        ASSERT_TRUE(result.error_code.has_value());
        EXPECT_EQ(*result.error_code, DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
        EXPECT_TRUE(result.data.empty());
    }
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data);
        const auto result = readWithRightBound(client, /*read_until_position=*/0, /*buffer_size=*/64, /*offset=*/40);

        ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
        EXPECT_EQ(result.data, data.substr(40));
        EXPECT_EQ(client->getObjectOffsets(), (std::vector<size_t>{40}));
    }
}

TEST_F(ReadBufferFromS3Test, ContentRangeHeaderIsParsed)
{
    const auto data = makeObject(100);
    /// Every request is answered with the 16 bytes at `body_begin` and the given `Content-Range`.
    auto read_at = [&](size_t range_begin, size_t body_begin, const std::string & content_range) -> BoundedReadResult
    {
        const auto client = std::make_shared<ClientFake>();
        client->getObjectImpl = [&](const Aws::S3::Model::GetObjectRequest &) -> Aws::S3::Model::GetObjectOutcome
        {
            std::string body = data.substr(body_begin, 16);
            Aws::Http::HeaderValueCollection headers{{"content-length", std::to_string(body.size())}, {"content-range", content_range}};
            const size_t size = body.size();
            auto response_stream = Aws::Utils::Stream::ResponseStream(Aws::New<DB::StdStreamFromReadBuffer>(
                "test response stream", std::make_unique<FakeResponseBody>(std::move(body), std::make_shared<CountedSession>()), size));
            Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> aws_result(std::move(response_stream), std::move(headers));
            return DB::S3::Model::GetObjectOutcome(DB::S3::Model::GetObjectResult(std::move(aws_result)));
        };
        DB::ReadBufferFromS3 buf(client, "test_bucket", "test_key", "", DB::S3::S3RequestSettings(), DB::ReadSettings{});

        BoundedReadResult result;
        result.data.resize(16);
        try
        {
            result.data.resize(buf.readBigAt(result.data.data(), result.data.size(), range_begin, {}));
        }
        catch (const DB::Exception & e)
        {
            result.error_code = e.code();
        }
        return result;
    };

    /// The range unit is case-insensitive.
    const auto mixed_case = read_at(/*range_begin=*/40, /*body_begin=*/40, "Bytes 40-55/100");
    ASSERT_FALSE(mixed_case.error_code.has_value()) << *mixed_case.error_code;
    EXPECT_EQ(mixed_case.data, data.substr(40, 16));

    /// A header that does not parse is not taken as a range starting at 0.
    const auto malformed = read_at(/*range_begin=*/0, /*body_begin=*/40, "bytes=40-55/100");
    ASSERT_TRUE(malformed.error_code.has_value()) << malformed.data;
    EXPECT_EQ(*malformed.error_code, DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
}

TEST_F(ReadBufferFromS3Test, PositionedReadOfEmptyResponsesIsAnError)
{
    const auto data = makeObject(100);
    auto read_at = [&](size_t max_response_size) -> std::pair<BoundedReadResult, std::vector<size_t>>
    {
        const auto client = std::make_shared<ClientFake>();
        client->setGetObjectRanges(data, max_response_size);
        /// Stops a reader that would not stop on its own.
        auto serve = *client->getObjectImpl;
        client->getObjectImpl = [serve, fake = client.get()](const Aws::S3::Model::GetObjectRequest & request) -> Aws::S3::Model::GetObjectOutcome
        {
            if (fake->getObjectOffsets().size() >= 10)
                return Aws::S3::Model::GetObjectOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "Too many requests", false));
            return serve(request);
        };
        DB::ReadBufferFromS3 buf(client, "test_bucket", "test_key", "", DB::S3::S3RequestSettings(), DB::ReadSettings{});

        BoundedReadResult result;
        result.data.resize(16);
        try
        {
            result.data.resize(buf.readBigAt(result.data.data(), result.data.size(), /*range_begin=*/40, {}));
        }
        catch (const DB::Exception & e)
        {
            result.error_code = e.code();
        }
        return {result, client->getObjectOffsets()};
    };

    const auto [empty, empty_offsets] = read_at(/*max_response_size=*/0);
    ASSERT_TRUE(empty.error_code.has_value());
    EXPECT_EQ(*empty.error_code, DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    EXPECT_EQ(empty_offsets, (std::vector<size_t>{40, 40, 40, 40}));

    const auto [capped, capped_offsets] = read_at(/*max_response_size=*/8);
    ASSERT_FALSE(capped.error_code.has_value()) << *capped.error_code;
    EXPECT_EQ(capped.data, data.substr(40, 16));
    EXPECT_EQ(capped_offsets, (std::vector<size_t>{40, 48}));
}

TEST_F(ReadBufferFromS3Test, CappedResponsesAreReassembledThroughFilesystemCache)
{
    auto query_context = createQueryContext();
    setSetting(query_context, DB::Setting::max_read_buffer_size_remote_fs, 64);
    setSetting(query_context, DB::Setting::filesystem_cache_name, "cache_capped_responses");
    setSetting(query_context, DB::Setting::filesystem_cache_prefer_bigger_buffer_size, false);

    DB::FileCacheSettings cache_settings;
    cache_settings[DB::FileCacheSetting::path] = fs::path(cache_base_path) / "capped_responses" / "";
    cache_settings[DB::FileCacheSetting::max_size] = 1000;
    cache_settings[DB::FileCacheSetting::max_elements] = 10;
    cache_settings[DB::FileCacheSetting::boundary_alignment] = 5;
    cache_settings[DB::FileCacheSetting::max_file_segment_size] = 100;
    cache_settings[DB::FileCacheSetting::background_download_threads] = 0;
    auto cache = DB::FileCacheFactory::instance().getOrCreate("cache_capped_responses", cache_settings, "");
    cache->initialize();
    cache.reset();

    const auto data = makeObject(100);
    auto client = std::make_unique<ClientFake>();
    client->setGetObjectRanges(data, /*max_response_size=*/40);

    auto buf = createSourceReadBuffer(std::move(client), data.size(), query_context);
    auto * async_buf = dynamic_cast<DB::AsynchronousBoundedReadBuffer *>(buf.get());
    ASSERT_TRUE(async_buf);
    ASSERT_TRUE(dynamic_cast<DB::CachedOnDiskReadBufferFromFile *>(async_buf->getImpl().get()));

    std::string read_data;
    DB::readStringUntilEOF(read_data, *buf);
    EXPECT_EQ(read_data, data);

    DB::FileCacheFactory::instance().clear();
}

/// The `s3()` source reads an object smaller than `2 * max_download_buffer_size` with a prefetch on the threadpool reader.
TEST_F(ReadBufferFromS3Test, CappedResponsesAreReassembledByThreadpoolPrefetch)
{
    /// With a buffer of 64 a response ends inside a fill, with 20 on a fill boundary.
    for (const size_t buffer_size : {64, 20})
    {
        auto query_context = createQueryContext();
        setSetting(query_context, DB::Setting::max_read_buffer_size_remote_fs, buffer_size);
        const auto data = makeObject(100);
        auto client = std::make_unique<ClientFake>();
        client->setGetObjectRanges(data, /*max_response_size=*/40);
        const auto * fake = client.get();

        auto buf = createSourceReadBuffer(std::move(client), data.size(), query_context);
        auto * async_buf = dynamic_cast<DB::AsynchronousBoundedReadBuffer *>(buf.get());
        ASSERT_TRUE(async_buf && dynamic_cast<DB::ReadBufferFromS3 *>(async_buf->getImpl().get())) << buffer_size;

        const auto result = readToEnd(*buf);
        ASSERT_FALSE(result.error_code.has_value()) << *result.error_code;
        EXPECT_EQ(result.data, data) << buffer_size;
        EXPECT_EQ(fake->getObjectOffsets(), (std::vector<size_t>{0, 40, 80})) << buffer_size;
    }
}

TEST_F(ReadBufferFromS3Test, ObjectShorterThanItsHeadIsAnErrorForThreadpoolPrefetch)
{
    auto query_context = createQueryContext();
    setSetting(query_context, DB::Setting::max_read_buffer_size_remote_fs, 64);
    const auto data = makeObject(100);
    auto client = std::make_unique<ClientFake>();
    client->setGetObjectRanges(data);
    const auto * fake = client.get();

    auto buf = createSourceReadBuffer(std::move(client), /*size_from_head=*/1000, query_context);
    ASSERT_TRUE(dynamic_cast<DB::AsynchronousBoundedReadBuffer *>(buf.get()));

    const auto result = readToEnd(*buf);
    ASSERT_TRUE(result.error_code.has_value()) << result.data.size();
    EXPECT_EQ(*result.error_code, DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    EXPECT_EQ(fake->getObjectOffsets(), (std::vector<size_t>{0, 100, 100, 100, 100}));
}

/// The `s3()` source reads an object of at least `2 * max_download_buffer_size` with `ParallelReadBuffer` (`readBigAt`).
TEST_F(ReadBufferFromS3Test, ObjectShorterThanItsHeadIsAnErrorForParallelRead)
{
    DB::getIOThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    auto query_context = createQueryContext();
    setSetting(query_context, DB::Setting::max_download_buffer_size, 20);
    const auto data = makeObject(100);
    auto client = std::make_unique<ClientFake>();
    client->setGetObjectRanges(data);
    /// Stops a reader that would not stop on its own.
    auto serve = *client->getObjectImpl;
    client->getObjectImpl = [serve, fake = client.get()](const Aws::S3::Model::GetObjectRequest & request) -> Aws::S3::Model::GetObjectOutcome
    {
        if (fake->getObjectOffsets().size() >= 50)
            return Aws::S3::Model::GetObjectOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "Too many requests", false));
        return serve(request);
    };
    const auto * fake = client.get();

    const size_t size_from_head = 120;
    auto buf = createSourceReadBuffer(std::move(client), size_from_head, query_context);
    ASSERT_TRUE(dynamic_cast<DB::ReadBufferFromS3 *>(buf.get()));
    auto parallel_buf = DB::wrapInParallelReadBufferIfSupported(
        *buf, DB::threadPoolCallbackRunnerUnsafe<void>(DB::getIOThreadPool().get(), DB::ThreadName::PARALLEL_READ),
        /*max_working_readers=*/4, /*range_step=*/20, size_from_head);
    ASSERT_TRUE(parallel_buf);

    const auto result = readToEnd(*parallel_buf);
    ASSERT_TRUE(result.error_code.has_value()) << result.data.size();
    EXPECT_EQ(*result.error_code, DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    auto offsets = fake->getObjectOffsets();
    std::ranges::sort(offsets);
    EXPECT_EQ(offsets, (std::vector<size_t>{0, 20, 40, 60, 80, 100, 100, 100, 100}));
}
#endif

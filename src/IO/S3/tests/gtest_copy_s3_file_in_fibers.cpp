#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3 && USE_SILK

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/RemoteHostFilter.h>
#include <Common/re2.h>
#include <Common/Silk/FiberScheduler.h>
#include <Common/Silk/tests/gtest_silk_scheduler.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadSettings.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/URI.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3Common.h>
#include <IO/S3RequestSettings.h>

#include <IO/S3/tests/TestPocoHTTPServer.h>

#include <Poco/StreamCopier.h>

#include <aws/s3/S3Errors.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace ProfileEvents
{
    extern const Event S3MultipartCopyInFibers;
}

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_operation_copy_size;
    extern const S3RequestSettingsUInt64 min_upload_part_size;
    extern const S3RequestSettingsUInt64 max_upload_part_size;
    extern const S3RequestSettingsUInt64 max_inflight_parts_for_one_file;
}

/// The S3 clients here rely on the `Poco::Util::ServerApplication` that `gtest_aws_s3_client.cpp` creates for the whole binary.

namespace
{

enum class RequestKind : uint8_t
{
    CreateMultipartUpload,
    UploadPartCopy,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    PutObject,
    UploadPart,
    CopyObject,
    Unknown,
};

struct RecordedRequest
{
    RequestKind kind;
    int part_number = 0;
};

struct ScriptedError
{
    Poco::Net::HTTPResponse::HTTPStatus status;
    std::string code;
    std::string message;
};

/// A minimal S3 that answers multipart copy and upload requests, records their order,
/// and fails the requests chosen by the test.
struct MockS3State
{
    std::mutex mutex;
    std::vector<RecordedRequest> requests;
    std::vector<int> completed_part_numbers;
    std::vector<std::string> completed_etags;
    std::optional<ScriptedError> create_multipart_upload_error;
    std::optional<ScriptedError> upload_part_copy_error;
    int failing_part_number = 0;
};

void sendError(Poco::Net::HTTPServerResponse & response, const ScriptedError & error)
{
    response.setStatus(error.status);
    response.setContentType("application/xml");
    auto & out = response.send();
    out << fmt::format(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message><RequestId>1</RequestId></Error>",
        error.code, error.message);
}

void sendXML(Poco::Net::HTTPServerResponse & response, const std::string & body)
{
    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/xml");
    auto & out = response.send();
    out << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << body;
}

class MockS3Handler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit MockS3Handler(MockS3State & state_) : state(state_) {}

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        std::string body;
        Poco::StreamCopier::copyToString(request.stream(), body);

        Poco::URI uri(request.getURI());
        std::optional<std::string> upload_id;
        std::optional<int> part_number;
        bool uploads = false;
        for (const auto & [name, value] : uri.getQueryParameters())
        {
            if (name == "uploads")
                uploads = true;
            else if (name == "uploadId")
                upload_id = value;
            else if (name == "partNumber")
                part_number = std::stoi(value);
        }
        const bool is_copy = request.has("x-amz-copy-source");
        const auto & method = request.getMethod();

        RecordedRequest recorded{RequestKind::Unknown, part_number.value_or(0)};
        if (method == "POST" && uploads)
            recorded.kind = RequestKind::CreateMultipartUpload;
        else if (method == "PUT" && part_number && is_copy)
            recorded.kind = RequestKind::UploadPartCopy;
        else if (method == "PUT" && part_number)
            recorded.kind = RequestKind::UploadPart;
        else if (method == "POST" && upload_id)
            recorded.kind = RequestKind::CompleteMultipartUpload;
        else if (method == "DELETE" && upload_id)
            recorded.kind = RequestKind::AbortMultipartUpload;
        else if (method == "PUT" && is_copy)
            recorded.kind = RequestKind::CopyObject;
        else if (method == "PUT")
            recorded.kind = RequestKind::PutObject;

        std::unique_lock lock(state.mutex);
        state.requests.push_back(recorded);

        switch (recorded.kind)
        {
            case RequestKind::CreateMultipartUpload:
                if (state.create_multipart_upload_error)
                    return sendError(response, *state.create_multipart_upload_error);
                return sendXML(response,
                    "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-1</UploadId>"
                    "</InitiateMultipartUploadResult>");
            case RequestKind::UploadPartCopy:
                if (state.upload_part_copy_error && recorded.part_number == state.failing_part_number)
                    return sendError(response, *state.upload_part_copy_error);
                return sendXML(response, fmt::format(
                    "<CopyPartResult><ETag>\"etag-{}\"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyPartResult>",
                    recorded.part_number));
            case RequestKind::CompleteMultipartUpload:
            {
                static const re2::RE2 part_regex("<PartNumber>([0-9]+)</PartNumber>");
                static const re2::RE2 etag_regex("<ETag>([^<]*)</ETag>");
                re2::StringPiece parts_input(body);
                int part_number_in_body = 0;
                while (re2::RE2::FindAndConsume(&parts_input, part_regex, &part_number_in_body))
                    state.completed_part_numbers.push_back(part_number_in_body);
                re2::StringPiece etags_input(body);
                std::string etag;
                while (re2::RE2::FindAndConsume(&etags_input, etag_regex, &etag))
                    state.completed_etags.push_back(etag);
                return sendXML(response,
                    "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><ETag>\"final\"</ETag>"
                    "</CompleteMultipartUploadResult>");
            }
            case RequestKind::AbortMultipartUpload:
                response.setStatus(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
                response.send();
                return;
            case RequestKind::PutObject:
            case RequestKind::UploadPart:
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                response.set("ETag", "\"put-etag\"");
                response.send();
                return;
            case RequestKind::CopyObject:
            case RequestKind::Unknown:
                return sendError(response, {Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "InvalidRequest", "unexpected request"});
        }
    }

private:
    MockS3State & state;
};

class MockS3HandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit MockS3HandlerFactory(MockS3State & state_) : state(state_) {}

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockS3Handler(state);
    }

private:
    MockS3State & state;
};

class MockS3Server
{
public:
    MockS3Server()
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , server(std::make_unique<Poco::Net::HTTPServer>(new MockS3HandlerFactory(state), *server_socket, makeMockServerParams()))
    {
        server->start();
    }

    ~MockS3Server() { server->stop(); }

    std::string getUrl() const { return "http://" + server_socket->address().toString(); }

    MockS3State state;

    std::vector<RecordedRequest> getRequests()
    {
        std::lock_guard lock(state.mutex);
        return state.requests;
    }

private:
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

std::shared_ptr<DB::S3::Client> createClient(const DB::S3::URI & uri)
{
    static const DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects= */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error= */ true,
        /* s3_slow_all_threads_after_retryable_error= */ true,
        /* enable_s3_requests_logging= */ false,
        /* group= */ DB::HTTPConnectionGroupType::STORAGE,
        /* opt_disk_name= */ {},
        /* request_throttler= */ {},
        uri.uri.getScheme());
    client_configuration.endpointOverride = uri.endpoint;

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };
    return DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        "ACCESS_KEY_ID",
        "SECRET_ACCESS_KEY",
        /* server_side_encryption_customer_key_base64= */ "",
        {},
        {},
        DB::S3::CredentialsConfiguration{.use_environment_credentials = false, .use_insecure_imds_request = false});
}

constexpr size_t PART_SIZE = 4;
constexpr size_t NUM_PARTS = 4;
constexpr size_t OBJECT_SIZE = PART_SIZE * NUM_PARTS;

class CopyS3FileInFibersTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }

    void SetUp() override
    {
        uri.emplace(server.getUrl() + "/bucket/key");
        client = createClient(*uri);

        /// Every copy is a multipart copy of `NUM_PARTS` parts, one part at a time.
        request_settings[DB::S3RequestSetting::max_single_operation_copy_size] = 1;
        request_settings[DB::S3RequestSetting::min_upload_part_size] = PART_SIZE;
        request_settings[DB::S3RequestSetting::max_upload_part_size] = PART_SIZE;
        request_settings[DB::S3RequestSetting::max_inflight_parts_for_one_file] = 1;
    }

    bool copy(const DB::CreateReadBuffer & fallback_file_reader = {})
    {
        return DB::copyS3File(
            client, uri->bucket, "source", OBJECT_SIZE,
            client, uri->bucket, "destination",
            request_settings, DB::ReadSettings{}, /* blob_storage_log= */ nullptr, /* schedule= */ {},
            fallback_file_reader, std::nullopt, DB::S3MultipartCopyInFibers{});
    }

    MockS3Server server;
    std::optional<DB::S3::URI> uri;
    std::shared_ptr<DB::S3::Client> client;
    DB::S3::S3RequestSettings request_settings;
};

std::vector<RequestKind> kindsOf(const std::vector<RecordedRequest> & requests)
{
    std::vector<RequestKind> kinds;
    for (const auto & request : requests)
        kinds.push_back(request.kind);
    return kinds;
}

std::vector<int> copiedParts(const std::vector<RecordedRequest> & requests)
{
    std::vector<int> parts;
    for (const auto & request : requests)
        if (request.kind == RequestKind::UploadPartCopy)
            parts.push_back(request.part_number);
    return parts;
}

}

TEST_F(CopyS3FileInFibersTest, CompletedCopyListsAllPartsInOrderAndCountsAsFiberCopy)
{
    const auto copies_in_fibers_before = ProfileEvents::global_counters[ProfileEvents::S3MultipartCopyInFibers];

    EXPECT_TRUE(copy());

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::S3MultipartCopyInFibers], copies_in_fibers_before + 1);

    const auto requests = server.getRequests();
    EXPECT_EQ(copiedParts(requests), (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(requests.front().kind, RequestKind::CreateMultipartUpload);
    EXPECT_EQ(requests.back().kind, RequestKind::CompleteMultipartUpload);

    std::lock_guard lock(server.state.mutex);
    EXPECT_EQ(server.state.completed_part_numbers, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(server.state.completed_etags, (std::vector<std::string>{"\"etag-1\"", "\"etag-2\"", "\"etag-3\"", "\"etag-4\""}));
}

TEST_F(CopyS3FileInFibersTest, FailedPartAbortsUploadAndRethrowsOriginalError)
{
    server.state.upload_part_copy_error = ScriptedError{Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "NoSuchUpload", "part 2 marker"};
    server.state.failing_part_number = 2;

    std::optional<Aws::S3::S3Errors> s3_error;
    std::string message;
    try
    {
        copy();
    }
    catch (const DB::S3Exception & e)
    {
        s3_error = e.getS3ErrorCode();
        message = e.message();
    }

    EXPECT_EQ(s3_error, Aws::S3::S3Errors::NO_SUCH_UPLOAD);
    EXPECT_TRUE(message.contains("part 2 marker")) << message;

    /// One part in flight: the part after the failed one is never started.
    const auto requests = server.getRequests();
    EXPECT_EQ(kindsOf(requests), (std::vector<RequestKind>{
        RequestKind::CreateMultipartUpload,
        RequestKind::UploadPartCopy,
        RequestKind::UploadPartCopy,
        RequestKind::AbortMultipartUpload}));
    EXPECT_EQ(copiedParts(requests), (std::vector<int>{1, 2}));
}

TEST_F(CopyS3FileInFibersTest, AccessDeniedPartAbortsUploadAndCopiesThroughBuffersOnCallingThread)
{
    server.state.upload_part_copy_error = ScriptedError{Poco::Net::HTTPResponse::HTTP_FORBIDDEN, "AccessDenied", "no copy access"};
    server.state.failing_part_number = 1;

    size_t fallback_calls = 0;
    size_t fallback_calls_elsewhere = 0;
    const auto calling_thread = std::this_thread::get_id();
    auto fallback_file_reader = [&]() -> std::unique_ptr<DB::SeekableReadBuffer>
    {
        ++fallback_calls;
        if (Silk::isInsideFiber() || std::this_thread::get_id() != calling_thread)
            ++fallback_calls_elsewhere;
        return std::make_unique<DB::ReadBufferFromOwnString>(std::string(OBJECT_SIZE, 'x'));
    };

    EXPECT_TRUE(copy(fallback_file_reader));

    EXPECT_GE(fallback_calls, 1u);
    EXPECT_EQ(fallback_calls_elsewhere, 0u);
    EXPECT_EQ(kindsOf(server.getRequests()), (std::vector<RequestKind>{
        RequestKind::CreateMultipartUpload,
        RequestKind::UploadPartCopy,
        RequestKind::AbortMultipartUpload,
        RequestKind::PutObject}));
}

TEST_F(CopyS3FileInFibersTest, FailedCreateMultipartUploadPropagatesWithoutAbort)
{
    server.state.create_multipart_upload_error = ScriptedError{Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "NoSuchBucket", "no bucket"};

    std::optional<Aws::S3::S3Errors> s3_error;
    try
    {
        copy();
    }
    catch (const DB::S3Exception & e)
    {
        s3_error = e.getS3ErrorCode();
    }

    EXPECT_EQ(s3_error, Aws::S3::S3Errors::NO_SUCH_BUCKET);
    EXPECT_EQ(kindsOf(server.getRequests()), (std::vector<RequestKind>{RequestKind::CreateMultipartUpload}));
}

#endif

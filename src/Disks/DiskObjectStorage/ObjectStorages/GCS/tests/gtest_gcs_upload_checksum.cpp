#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include <google/cloud/credentials.h>
#include <google/cloud/options.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/hashing_options.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/storage/retry_policy.h>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/StreamCopier.h>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <string>

namespace gcs = ::google::cloud::storage;
namespace gc = ::google::cloud;

namespace DB::ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
}

namespace
{

/// A minimal resumable-upload endpoint: the `POST` that opens the session answers with its location, and
/// the `PUT` that carries the data finalizes the object and reports `crc32c` as the stored object's hash.
class ResumableUploadHandler : public Poco::Net::HTTPRequestHandler
{
public:
    ResumableUploadHandler(std::string session_url_, std::string crc32c_) : session_url(std::move(session_url_)), crc32c(std::move(crc32c_)) { }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        std::string request_body;
        Poco::StreamCopier::copyToString(request.stream(), request_body);

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            response.set("Location", session_url);
            response.setContentLength(0);
            response.send();
            return;
        }

        const std::string body = fmt::format(
            R"({{"kind":"storage#object","bucket":"test-bucket","name":"key","generation":"1",)"
            R"("size":"{}","crc32c":"{}","updated":"2026-01-01T00:00:00Z"}})",
            request_body.size(), crc32c);
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentType("application/json");
        response.setContentLength(static_cast<std::streamsize>(body.size()));
        response.send() << body;
    }

private:
    const std::string session_url;
    const std::string crc32c;
};

class ResumableUploadHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    ResumableUploadHandlerFactory(std::string session_url_, std::string crc32c_) : session_url(std::move(session_url_)), crc32c(std::move(crc32c_)) { }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new ResumableUploadHandler(session_url, crc32c);
    }

private:
    const std::string session_url;
    const std::string crc32c;
};

/// Writes `data` through a resumable upload to a server that reports `crc32c` for the stored object.
void uploadReportingChecksum(const std::string & data, const std::string & crc32c)
{
    Poco::Net::ServerSocket server_socket(Poco::Net::SocketAddress("127.0.0.1", 0));
    const std::string endpoint = fmt::format("http://{}", server_socket.address().toString());
    Poco::Net::HTTPServer server(
        new ResumableUploadHandlerFactory(endpoint + "/upload/session", crc32c), server_socket, new Poco::Net::HTTPServerParams);
    server.start();

    gc::Options options;
    options.set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials());
    options.set<gcs::RestEndpointOption>(endpoint);
    options.set<gcs::RetryPolicyOption>(gcs::LimitedErrorCountRetryPolicy(0).clone());
    auto client = std::make_shared<gcs::Client>(std::move(options));

    try
    {
        /// A zero single-request limit makes the buffer switch to a resumable upload on the first flush.
        DB::WriteBufferFromGCS buffer(
            client, "test-bucket", "key", /* buf_size_ */ 1024, /* max_single_part_upload_size_ */ 0, DB::WriteSettings{}, /* blob_log_ */ nullptr);
        DB::writeString(data, buffer);
        buffer.finalize();
    }
    catch (...)
    {
        server.stop();
        throw;
    }
    server.stop();
}

}

/// `ObjectWriteStream::Close` keeps the server's metadata of a finished resumable upload even when the
/// hash the client computed over the uploaded bytes differs from the one the server reports; it only sets
/// `badbit`. The buffer must treat that as a failed write rather than commit an object with other data.
TEST(GCSUploadChecksum, ResumableUploadWithMismatchingChecksumFails)
{
    const std::string data = "some data of an object";
    try
    {
        uploadReportingChecksum(data, gcs::ComputeCrc32cChecksum("other data"));
        FAIL() << "the upload with a mismatching checksum succeeded";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CHECKSUM_DOESNT_MATCH) << e.message();
    }
}

TEST(GCSUploadChecksum, ResumableUploadWithMatchingChecksumSucceeds)
{
    const std::string data = "some data of an object";
    EXPECT_NO_THROW(uploadReportingChecksum(data, gcs::ComputeCrc32cChecksum(data)));
}

#endif

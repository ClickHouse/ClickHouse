#include "config.h"

#if USE_GOOGLE_CLOUD

#include <google/cloud/credentials.h>
#include <google/cloud/options.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/storage/retry_policy.h>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <iterator>
#include <mutex>
#include <string>
#include <vector>

namespace gcs = ::google::cloud::storage;
namespace gc = ::google::cloud;

namespace
{

/// Records the request target (the origin-form URI, exactly as it went over the wire) of every
/// request the client sends, and answers each one with a canned object representation.
class TargetRecorder
{
public:
    void add(std::string target)
    {
        std::lock_guard lock(mutex);
        targets.push_back(std::move(target));
    }

    std::vector<std::string> take()
    {
        std::lock_guard lock(mutex);
        auto result = std::move(targets);
        targets.clear();
        return result;
    }

private:
    std::mutex mutex;
    std::vector<std::string> targets;
};

class CapturingHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit CapturingHandler(TargetRecorder & recorder_) : recorder(recorder_) { }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        recorder.add(request.getURI());

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE)
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
            response.setContentLength(0);
            response.send();
            return;
        }

        /// A media download asks for `alt=media` and gets the object bytes; every other request in
        /// this test is a JSON API request and gets the object's metadata resource.
        const bool media = request.getURI().contains("alt=media");
        const std::string body = media
            ? std::string("test")
            : std::string(R"({"kind":"storage#object","bucket":"test-bucket",)"
                          R"("name":"mergetree/rsp/uaeuuzkbesnofuczqjvncbqvtijyg",)"
                          R"("generation":"1","size":"4","updated":"2026-01-01T00:00:00Z"})");

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentType(media ? "application/octet-stream" : "application/json");
        response.setContentLength(static_cast<std::streamsize>(body.size()));
        response.send() << body;
    }

private:
    TargetRecorder & recorder;
};

class CapturingHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit CapturingHandlerFactory(TargetRecorder & recorder_) : recorder(recorder_) { }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new CapturingHandler(recorder);
    }

private:
    TargetRecorder & recorder;
};

/// The path component of a request target, i.e. everything before the query string.
std::string pathOf(const std::string & target)
{
    return target.substr(0, target.find('?'));
}

}

/// A GCS object name goes into the *path* of the JSON API request, percent-encoded, so an object
/// key with slashes in it — which is every key the object-storage key generator produces, e.g.
/// `mergetree/rsp/uaeuuzkbesnofuczqjvncbqvtijyg` — is addressed as `.../o/mergetree%2Frsp%2F...`.
/// The Poco-based REST transport has to hand that target to the server byte for byte: `Poco::URI`
/// percent-decodes a path when it parses a URL and re-encodes it with a reserved set that does not
/// contain `/`, so a round trip through it turns `%2F` into a path separator and addresses a
/// different, non-existent resource. An upload puts the object name in the query string instead,
/// so the symptom of that bug is a write that reports success followed by `Not Found` on read-back.
TEST(GCSObjectNameEncoding, KeepsSlashesInObjectNamesEncoded)
{
    TargetRecorder recorder;

    Poco::Net::ServerSocket server_socket(Poco::Net::SocketAddress("127.0.0.1", 0));
    Poco::Net::HTTPServer server(new CapturingHandlerFactory(recorder), server_socket, new Poco::Net::HTTPServerParams);
    server.start();

    gc::Options options;
    options.set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials());
    options.set<gcs::RestEndpointOption>(fmt::format("http://{}", server_socket.address().toString()));
    /// Fail on the first error instead of retrying: every assertion below is about the very first
    /// request, and a retry loop would only make a failure slower to report.
    options.set<gcs::RetryPolicyOption>(gcs::LimitedErrorCountRetryPolicy(0).clone());
    gcs::Client client(std::move(options));

    const std::string bucket = "test-bucket";
    const std::string key = "mergetree/rsp/uaeuuzkbesnofuczqjvncbqvtijyg";
    const std::string expected_path = "/storage/v1/b/test-bucket/o/mergetree%2Frsp%2Fuaeuuzkbesnofuczqjvncbqvtijyg";

    {
        auto metadata = client.GetObjectMetadata(bucket, key);
        ASSERT_TRUE(metadata.ok()) << metadata.status().message();
        const auto targets = recorder.take();
        ASSERT_EQ(targets.size(), 1u);
        EXPECT_EQ(pathOf(targets.front()), expected_path);
    }

    {
        auto stream = client.ReadObject(bucket, key);
        const std::string contents(std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{});
        ASSERT_TRUE(stream.status().ok()) << stream.status().message();
        EXPECT_EQ(contents, "test");
        const auto targets = recorder.take();
        ASSERT_EQ(targets.size(), 1u);
        EXPECT_EQ(pathOf(targets.front()), expected_path);
        EXPECT_NE(targets.front().find("alt=media"), std::string::npos);
    }

    {
        auto status = client.DeleteObject(bucket, key);
        ASSERT_TRUE(status.ok()) << status.message();
        const auto targets = recorder.take();
        ASSERT_EQ(targets.size(), 1u);
        EXPECT_EQ(pathOf(targets.front()), expected_path);
    }

    server.stop();
}

#endif

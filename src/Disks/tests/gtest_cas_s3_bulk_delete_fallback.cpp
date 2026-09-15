#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/S3/Client.h>
#include <IO/S3/S3Capabilities.h>
#include <IO/S3/URI.h>
#include <IO/S3Settings.h>
#include <IO/S3Common.h>
#include <Common/RemoteHostFilter.h>
#include <Common/tests/gtest_global_context.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <istream>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/SharedPtr.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

/// `S3ObjectStorage::removeObjectsIfExistImpl` (the CAS bulk-delete path, reached through
/// `removeObjectsIfExistUnderProfile`) must honour `S3Capabilities::isBatchDeleteSupported()` the same
/// way the generic `deleteFilesFromS3` does, but WITHOUT looping over the objects itself: once the
/// capability is known false (a configured `false`, or one just learned from a `DeleteObjects` reply in
/// the "batch delete not implemented" error class), it throws `NOT_IMPLEMENTED` without sending anything
/// else, and leaves per-key retry to the caller (the CAS engine admits each such retry as its own
/// request -- see CasGc.cpp's `removeChunkWriteOnceOrOneByOne`). A request failure of any other class
/// must keep today's fail-close behaviour. The one exception to all of this is a batch of exactly one
/// object, which is always a plain `DeleteObject` -- never gated on the capability at all, since a
/// single physical request is never something the capability check exists to rule out.

namespace
{

/// A real local HTTP server standing in for S3. `DeleteObjects` arrives as a POST to the bucket root;
/// a per-key `DeleteObject` arrives as a plain HTTP DELETE to the key's path -- the two are
/// distinguished by HTTP method alone, with no need to parse the request body or query string.
class ScriptedS3Server
{
public:
    using Responder = std::function<void(const Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse &)>;

private:
    class Handler : public Poco::Net::HTTPRequestHandler
    {
        ScriptedS3Server & owner;

    public:
        explicit Handler(ScriptedS3Server & owner_) : owner(owner_) { }

        void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
        {
            {
                std::lock_guard lock(owner.mutex);
                owner.methods_seen.push_back(request.getMethod());
            }
            /// `DeleteObjects` carries a request body (the XML `<Delete>` payload); leaving it unread on a
            /// keep-alive connection makes Poco parse those leftover bytes as the start of the NEXT
            /// request once this handler returns, corrupting the very next `DeleteObject` this test expects.
            request.stream().ignore(std::numeric_limits<std::streamsize>::max());
            owner.responder(request, response);
        }
    };

    class Factory : public Poco::Net::HTTPRequestHandlerFactory
    {
        ScriptedS3Server & owner;

        Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
        {
            return new Handler(owner);
        }

    public:
        explicit Factory(ScriptedS3Server & owner_) : owner(owner_) { }
    };

    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<Factory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    Responder responder;
    mutable std::mutex mutex;
    std::vector<std::string> methods_seen;

public:
    explicit ScriptedS3Server(Responder responder_)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , handler_factory(new Factory(*this))
        , server_params(new Poco::Net::HTTPServerParams())
        , server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
        , responder(std::move(responder_))
    {
        server->start();
    }

    std::string getUrl() const { return "http://" + server_socket->address().toString(); }

    size_t countMethod(const std::string & method) const
    {
        std::lock_guard lock(mutex);
        return static_cast<size_t>(std::count(methods_seen.begin(), methods_seen.end(), method));
    }
};

void sendXml(Poco::Net::HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const std::string & body)
{
    response.setContentType("application/xml");
    response.setContentLength(body.size());
    response.setStatus(status);
    auto & out = response.send();
    out << body;
    out.flush();
}

/// A quiet-mode `DeleteObjects` success (HTTP 200) whose body lists only the failed keys, exactly as a
/// real S3 backend would report a mixed outcome.
void sendBatchSuccessWithErrors(Poco::Net::HTTPServerResponse & response, const std::string & not_found_key, const std::string & denied_key)
{
    const std::string body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Error><Key>" + not_found_key + "</Key><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"
        "<Error><Key>" + denied_key + "</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
        "</DeleteResult>";
    sendXml(response, Poco::Net::HTTPResponse::HTTP_OK, body);
}

/// A request-level `DeleteObjects` failure in the "batch delete is not implemented" class that
/// `deleteFileFromS3.cpp`'s `deleteFilesFromS3` also treats as "fall back to plain `DeleteObject`".
void sendBatchNotImplemented(Poco::Net::HTTPServerResponse & response)
{
    const std::string body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>NotImplemented</Code><Message>A header you provided implies functionality that is not implemented</Message></Error>";
    sendXml(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, body);
}

/// A request-level `DeleteObjects` failure in an ordinary (not "unsupported") class: this must keep
/// today's fail-close behaviour and never fall back to per-key deletes.
void sendBatchInternalError(Poco::Net::HTTPServerResponse & response)
{
    const std::string body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>InternalError</Code><Message>We encountered an internal error, please try again.</Message></Error>";
    sendXml(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, body);
}

void sendDeleteObjectSuccess(Poco::Net::HTTPServerResponse & response)
{
    response.setContentLength(0);
    response.setStatus(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
    response.send();
}

/// A single-key `DeleteObject` failure -- used to script the size-one path's own error handling, as
/// distinct from the batch response's per-key `<Error>` elements covered by the test above.
void sendSingleDeleteError(Poco::Net::HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const std::string & code, const std::string & message)
{
    const std::string body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>" + code + "</Code><Message>" + message + "</Message></Error>";
    sendXml(response, status, body);
}

std::shared_ptr<DB::S3ObjectStorage> makeStorageForTest(const std::string & endpoint, const DB::S3Capabilities & capabilities)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration cfg = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ false,
        /* s3_slow_all_threads_after_retryable_error = */ false,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ true,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});
    cfg.endpointOverride = endpoint;
    cfg.connectTimeoutMs = 10000;
    cfg.requestTimeoutMs = 10000;
    cfg.s3_use_adaptive_timeouts = false;
    /// Every test here starts its own server on an ephemeral port; with keep-alive on, the process-wide
    /// HTTP connection pool can hand a later test a connection to a port whose server is already gone
    /// (`Connection reset by peer` under `--gtest_repeat`). One connection per request is what a
    /// short-lived test server should get.
    cfg.http_keep_alive_timeout = 0;
    auto client = DB::S3::ClientFactory::instance().create(
        cfg,
        DB::S3::ClientSettings{
            .use_virtual_addressing = false,
            .disable_checksum = false,
            .gcs_issue_compose_request = false,
            .is_s3express_bucket = false,
        },
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{});
    return std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(),
        DB::S3::URI(endpoint + "/test-bucket/"), capabilities,
        DB::ObjectStorageKeyGeneratorPtr{}, "disk");
}

DB::ContextPtr contextForTest()
{
    return getContext().context;
}

/// The CAS-side fallback (CasGc.cpp's `removeChunkWriteOnceOrOneByOne`) keys specifically on
/// `NOT_IMPLEMENTED`; a capability-rejection test that only checks "threw a DB::Exception" would still
/// pass if this storage started throwing, say, BAD_ARGUMENTS instead -- which would silently break that
/// fallback while every assertion here kept passing.
void expectNotImplemented(const std::function<void()> & fn)
{
    try
    {
        fn();
        FAIL() << "expected a NOT_IMPLEMENTED exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED) << e.message();
    }
}

}

TEST(S3BulkDeleteFallback, PerKeyErrorsWithinASuccessfulBatchAreUnchanged)
{
    (void)contextForTest();

    ScriptedS3Server server([](const Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response)
    {
        sendBatchSuccessWithErrors(response, "notfound-key", "denied-key");
    });
    auto storage = makeStorageForTest(server.getUrl(), DB::S3Capabilities{});

    try
    {
        storage->removeObjectsIfExistUnderProfile(
            {DB::StoredObject("present-key"), DB::StoredObject("notfound-key"), DB::StoredObject("denied-key")},
            DB::ObjectStorageControlRequest{});
        FAIL() << "expected removeObjectsIfExistUnderProfile to throw on the AccessDenied key";
    }
    catch (const DB::S3Exception & e)
    {
        EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::ACCESS_DENIED);
        EXPECT_NE(e.message().find("denied-key"), std::string::npos) << e.message();
    }

    /// Exactly one DeleteObjects request; NoSuchKey and AccessDenied are both surfaced by the same
    /// batch response, no fallback is expected here.
    EXPECT_EQ(server.countMethod("POST"), 1u);
    EXPECT_EQ(server.countMethod("DELETE"), 0u);
}

TEST(S3BulkDeleteFallback, UnsupportedBatchReplyRecordsCapabilityFalseAndThrowsNotImplemented)
{
    (void)contextForTest();

    std::atomic<size_t> batch_attempts{0};
    ScriptedS3Server server([&](const Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
    {
        ASSERT_EQ(request.getMethod(), "POST") << "capability false must never send anything, batch or per-key";
        ++batch_attempts;
        sendBatchNotImplemented(response);
    });
    auto storage = makeStorageForTest(server.getUrl(), DB::S3Capabilities{});

    DB::StoredObjects objects{DB::StoredObject("key-a"), DB::StoredObject("key-b")};

    expectNotImplemented([&] { storage->removeObjectsIfExistUnderProfile(objects, DB::ObjectStorageControlRequest{}); });
    EXPECT_EQ(batch_attempts.load(), 1u);
    EXPECT_EQ(server.countMethod("DELETE"), 0u) << "this storage never loops over objects itself";

    /// The capability is now known false on this storage: a second call must throw at once, without
    /// even a `DeleteObjects` probe.
    expectNotImplemented([&] { storage->removeObjectsIfExistUnderProfile(objects, DB::ObjectStorageControlRequest{}); });
    EXPECT_EQ(batch_attempts.load(), 1u) << "a second DeleteObjects attempt means the learned capability was not honoured";
    EXPECT_EQ(server.countMethod("DELETE"), 0u);
}

/// A batch of exactly one object is always a plain `DeleteObject`: never sent as `DeleteObjects`, and
/// never gated on `s3_capabilities` at all -- proven here with the capability both explicitly false AND
/// left unknown (the default), since a single physical request is never something that check exists to
/// refuse. This is what makes the CAS engine's per-key fallback (CasGc.cpp) actually delete anything on
/// a backend that rejects `DeleteObjects` outright (GCS): a "batch" of one sent as `DeleteObjects` would
/// fail there identically to a bigger one.
TEST(S3BulkDeleteFallback, ExactlyOneObjectIsAlwaysAPlainDeleteObjectRegardlessOfCapability)
{
    (void)contextForTest();

    for (const bool explicit_false : {false, true})
    {
        ScriptedS3Server server([](const Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
        {
            ASSERT_EQ(request.getMethod(), "DELETE");
            sendDeleteObjectSuccess(response);
        });
        auto storage = makeStorageForTest(server.getUrl(), DB::S3Capabilities{explicit_false ? std::optional<bool>{false} : std::nullopt});

        EXPECT_NO_THROW(storage->removeObjectsIfExistUnderProfile({DB::StoredObject("solo-key")}, DB::ObjectStorageControlRequest{}));

        EXPECT_EQ(server.countMethod("POST"), 0u);
        EXPECT_EQ(server.countMethod("DELETE"), 1u);
    }
}

/// The size-one path's own error handling, exactly as thorough as the batch path's: an absence is
/// ignored, and a real error is reported with the object's path.
TEST(S3BulkDeleteFallback, ExactlyOneObjectIgnoresAbsenceAndThrowsOnARealError)
{
    (void)contextForTest();

    {
        ScriptedS3Server server([](const Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response)
        {
            sendSingleDeleteError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "NoSuchKey", "The specified key does not exist.");
        });
        auto storage = makeStorageForTest(server.getUrl(), DB::S3Capabilities{});
        EXPECT_NO_THROW(storage->removeObjectsIfExistUnderProfile({DB::StoredObject("absent-key")}, DB::ObjectStorageControlRequest{}));
    }
    {
        ScriptedS3Server server([](const Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response)
        {
            sendSingleDeleteError(response, Poco::Net::HTTPResponse::HTTP_FORBIDDEN, "AccessDenied", "Access Denied");
        });
        auto storage = makeStorageForTest(server.getUrl(), DB::S3Capabilities{});
        try
        {
            storage->removeObjectsIfExistUnderProfile({DB::StoredObject("denied-key")}, DB::ObjectStorageControlRequest{});
            FAIL() << "expected removeObjectsIfExistUnderProfile to throw on the AccessDenied key";
        }
        catch (const DB::S3Exception & e)
        {
            EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::ACCESS_DENIED);
            EXPECT_NE(e.message().find("denied-key"), std::string::npos) << e.message();
        }
    }
}

TEST(S3BulkDeleteFallback, OtherFailureClassesKeepFailingClosedWithNoFallback)
{
    (void)contextForTest();

    ScriptedS3Server server([](const Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response)
    {
        sendBatchInternalError(response);
    });
    auto storage = makeStorageForTest(server.getUrl(), DB::S3Capabilities{});

    EXPECT_THROW(
        storage->removeObjectsIfExistUnderProfile(
            {DB::StoredObject("key-a"), DB::StoredObject("key-b")}, DB::ObjectStorageControlRequest{}),
        DB::Exception);

    EXPECT_EQ(server.countMethod("POST"), 1u);
    EXPECT_EQ(server.countMethod("DELETE"), 0u) << "an ordinary batch failure must not fall back to per-key deletes";
}

TEST(S3BulkDeleteFallback, ExplicitlyDisabledCapabilityThrowsNotImplementedWithoutSendingAnything)
{
    (void)contextForTest();

    ScriptedS3Server server([](const Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse &)
    {
        FAIL() << "an explicit false capability must never send anything, batch or per-key";
    });
    /// `<support_batch_delete>false</support_batch_delete>` in a disk's config resolves to this.
    auto storage = makeStorageForTest(server.getUrl(), DB::S3Capabilities{/*support_batch_delete_=*/false});

    expectNotImplemented([&]
    {
        storage->removeObjectsIfExistUnderProfile(
            {DB::StoredObject("key-a"), DB::StoredObject("key-b")}, DB::ObjectStorageControlRequest{});
    });

    EXPECT_EQ(server.countMethod("POST"), 0u);
    EXPECT_EQ(server.countMethod("DELETE"), 0u);
}

#endif

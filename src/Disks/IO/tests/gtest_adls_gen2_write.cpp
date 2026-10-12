#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <sstream>

#include <base/EnumReflection.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#include <IO/WriteSettings.h>

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/StreamChannel.h>

#include <azure/core/http/policies/policy.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>

using namespace DB;

namespace
{

/// One HTTP request as seen by the SDK, plus the response the script returned for it.
struct RecordedRequest
{
    String method;
    String host;
    String path;
    String query;
    Azure::Core::CaseInsensitiveMap headers;

    String header(const String & name) const
    {
        auto it = headers.find(name);
        return it == headers.end() ? String{} : it->second;
    }

    bool hasHeader(const String & name) const { return headers.contains(name); }
};

/// What the fake service answers with. `status == Created/Ok/Accepted` (per operation) means success.
struct ScriptedResponse
{
    Azure::Core::Http::HttpStatusCode status;
    /// Azure error code, e.g. PathAlreadyExists. Only meaningful for failures.
    String error_code;
    /// Overrides the ETag the fake service reports for a successful create/flush.
    String etag;
};

/// Recognizes the DFS operation from an SDK request, so a script can key on it rather than on
/// method/query spelling.
enum class Op
{
    Create,
    Append,
    Flush,
    Rename,
    Delete,
    Other,
};

Op classifyRequest(const RecordedRequest & request)
{
    if (request.method == "PUT")
        return request.hasHeader("x-ms-rename-source") ? Op::Rename : Op::Create;
    if (request.method == "PATCH")
        return request.query.find("action=flush") != String::npos ? Op::Flush : Op::Append;
    if (request.method == "DELETE")
        return Op::Delete;
    return Op::Other;
}

class EmptyBodyStream : public Azure::Core::IO::BodyStream
{
public:
    int64_t Length() const override { return 0; }
    void Rewind() override { }
    size_t OnRead(uint8_t *, size_t, Azure::Core::Context const &) override { return 0; }
};

/// Serves scripted responses in-process and records every request. Per-operation queues let a test
/// script, say, "the first flush fails with 408" without caring about request ordering.
class FakeDfsTransport : public Azure::Core::Http::HttpTransport
{
public:
    /// Responses to serve for `op`, in order. When the queue runs dry, the operation succeeds.
    void script(Op op, std::vector<ScriptedResponse> responses) { scripted[op] = std::move(responses); }

    /// Fail every operation of this kind, however many times it is attempted.
    void scriptForever(Op op, ScriptedResponse response)
    {
        scripted[op] = {response};
        repeat_last.insert(op);
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
        Azure::Core::Http::Request & request, Azure::Core::Context const &) override
    {
        RecordedRequest recorded;
        recorded.method = request.GetMethod().ToString();
        recorded.host = request.GetUrl().GetHost();
        recorded.path = request.GetUrl().GetPath();
        recorded.query = request.GetUrl().GetRelativeUrl();
        recorded.headers = request.GetHeaders();
        const Op op = classifyRequest(recorded);
        requests.push_back(recorded);

        /// Drain the request body so a repeated attempt sees the stream from the start.
        if (auto * body = request.GetBodyStream())
            body->ReadToEnd(Azure::Core::Context{});

        ScriptedResponse response{successStatus(op), {}, {}};
        auto it = scripted.find(op);
        if (it != scripted.end() && !it->second.empty())
        {
            response = it->second.front();
            if (!repeat_last.contains(op))
                it->second.erase(it->second.begin());
        }

        return makeResponse(op, response);
    }

    std::vector<RecordedRequest> requests;

private:
    static Azure::Core::Http::HttpStatusCode successStatus(Op op)
    {
        using Azure::Core::Http::HttpStatusCode;
        switch (op)
        {
            case Op::Create:
            case Op::Rename:
                return HttpStatusCode::Created;
            case Op::Append:
                return HttpStatusCode::Accepted;
            case Op::Flush:
                return HttpStatusCode::Ok;
            case Op::Delete:
                return HttpStatusCode::Ok;
            case Op::Other:
                return HttpStatusCode::Ok;
        }
        return HttpStatusCode::Ok;
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> makeResponse(Op op, const ScriptedResponse & scripted_response)
    {
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, scripted_response.status, "scripted");
        const bool failed = scripted_response.status != successStatus(op);

        if (failed)
        {
            response->SetHeader("x-ms-error-code", scripted_response.error_code);
            response->SetBodyStream(std::make_unique<EmptyBodyStream>());
            response->SetBody({});
            return response;
        }

        /// Headers the SDK's response parsers require.
        response->SetHeader("ETag", scripted_response.etag.empty() ? defaultEtag(op) : scripted_response.etag);
        response->SetHeader("Last-Modified", "Tue, 28 Jul 2026 00:00:00 GMT");
        response->SetHeader("Content-Length", "0");
        response->SetHeader("x-ms-request-server-encrypted", "true");
        response->SetBodyStream(std::make_unique<EmptyBodyStream>());
        response->SetBody({});
        return response;
    }

    static String defaultEtag(Op op)
    {
        /// Distinct per operation so a test can tell which value a later request was pinned to.
        return op == Op::Flush ? "\"etag-after-flush\"" : "\"etag-after-create\"";
    }

    std::map<Op, std::vector<ScriptedResponse>> scripted;
    std::set<Op> repeat_last;
};

constexpr const char * FINAL_BLOB_PATH = "data/data-0.parquet";
/// `getContainerEndpoint` yields <host>/<container>, so this is the final object's URL path.
constexpr const char * FINAL_URL_PATH = "mycontainer/tables/mytable/data/data-0.parquet";

AzureBlobStorage::Endpoint makeOneLakeEndpoint()
{
    AzureBlobStorage::Endpoint endpoint;
    endpoint.storage_account_url = "https://onelake.dfs.fabric.microsoft.com";
    endpoint.container_name = "mycontainer";
    endpoint.prefix = "tables/mytable";
    return endpoint;
}

struct WriteFixture
{
    std::shared_ptr<FakeDfsTransport> transport = std::make_shared<FakeDfsTransport>();
    AzureBlobStorage::Endpoint endpoint = makeOneLakeEndpoint();
    AzureBlobStorage::AuthMethod auth_method
        = std::make_shared<AzureBlobStorage::StaticCredential>("token", std::chrono::system_clock::now() + std::chrono::hours(1));
    WriteSettings write_settings;
    AzureBlobStorage::RequestSettings request_settings;

    /// SDK-level retries the caller asks for. Production passes a non-zero value here, so the tests
    /// that pin single-shot behaviour must leave it non-zero: the writer is responsible for turning
    /// retries off where an operation cannot be repeated.
    Int32 sdk_max_retries = 3;

    Azure::Storage::Blobs::BlobClientOptions clientOptions() const
    {
        Azure::Storage::Blobs::BlobClientOptions options;
        options.Transport.Transport = transport;
        options.Retry.MaxRetries = sdk_max_retries;
        /// Keep the tests fast: the retry policy sleeps between attempts.
        options.Retry.RetryDelay = std::chrono::milliseconds(1);
        options.Retry.MaxRetryDelay = std::chrono::milliseconds(1);
        return options;
    }

    std::unique_ptr<WriteBufferFromAzureDataLakeStorage> makeBuffer()
    {
        return std::make_unique<WriteBufferFromAzureDataLakeStorage>(
            endpoint,
            auth_method,
            clientOptions(),
            FINAL_BLOB_PATH,
            /*buf_size_=*/ DBMS_DEFAULT_BUFFER_SIZE,
            write_settings,
            std::make_shared<AzureBlobStorage::RequestSettings>(request_settings));
    }

    /// Requests of a kind, in order.
    std::vector<RecordedRequest> requestsOf(Op op) const
    {
        std::vector<RecordedRequest> out;
        for (const auto & request : transport->requests)
            if (classifyRequest(request) == op)
                out.push_back(request);
        return out;
    }

    /// Every request whose target is the final object, i.e. the one holding already-committed rows.
    /// A rename's target is its destination URL, so a successful publish shows up here.
    std::vector<RecordedRequest> requestsTargetingFinalPath() const
    {
        std::vector<RecordedRequest> out;
        for (const auto & request : transport->requests)
            if (request.path == FINAL_URL_PATH)
                out.push_back(request);
        return out;
    }

    String stagingPath() const
    {
        for (const auto & request : transport->requests)
            if (classifyRequest(request) == Op::Create)
                return request.path;
        return {};
    }
};

/// A write of some rows, driven exactly as a sink would.
void writeSomeData(WriteBufferFromAzureDataLakeStorage & buffer)
{
    buffer.write("hello", 5);
    buffer.next();
}

/// Redirects the writer's own logger into a string for the duration of a test, then puts back exactly
/// what was there. The saved channel is held by an AutoPtr so replacing it cannot destroy it.
class CapturedWriterLog
{
public:
    /// Same name the writer passes to getLogger, so this is the very logger it uses.
    CapturedWriterLog()
        : logger(getLogger("WriteBufferFromAzureDataLakeStorage"))
        , saved_channel(logger->getChannel(), /*shared=*/ true)
        , saved_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("information");
    }

    ~CapturedWriterLog()
    {
        logger->setChannel(saved_channel.get());
        logger->setLevel(saved_level);
    }

    String text() const { return captured.str(); }

private:
    LoggerPtr logger;
    std::ostringstream captured; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(captured));
    Poco::AutoPtr<Poco::Channel> saved_channel;
    int saved_level;
};

}

/// The data-loss regression test: a failing write must leave the previously committed object alone.
TEST(AdlsGen2Write, FailedFlushLeavesTargetUntouched)
{
    WriteFixture fixture;
    fixture.transport->scriptForever(Op::Flush, {Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);

    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty())
        << "the final path was touched by " << fixture.requestsTargetingFinalPath().front().method;

    const auto deletes = fixture.requestsOf(Op::Delete);
    ASSERT_EQ(deletes.size(), 1u);
    EXPECT_EQ(deletes[0].path, fixture.stagingPath());
}

/// The happy path publishes by renaming staging onto the target, and leaves nothing behind.
TEST(AdlsGen2Write, SuccessPublishesWithRename)
{
    WriteFixture fixture;

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    const auto renames = fixture.requestsOf(Op::Rename);
    ASSERT_EQ(renames.size(), 1u);
    /// The destination must be exactly the final object, and the source exactly our staging object.
    EXPECT_EQ(renames[0].path, FINAL_URL_PATH);
    EXPECT_EQ(renames[0].header("x-ms-rename-source"), "/" + fixture.stagingPath());

    EXPECT_TRUE(fixture.requestsOf(Op::Delete).empty()) << "staging was renamed away, deleting it would 404";
    /// The rename is the only request that ever addresses the final object.
    ASSERT_EQ(fixture.requestsTargetingFinalPath().size(), 1u);
    EXPECT_EQ(classifyRequest(fixture.requestsTargetingFinalPath()[0]), Op::Rename);
}

TEST(AdlsGen2Write, CancelDeletesOnlyStaging)
{
    WriteFixture fixture;

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->cancel();

    const auto deletes = fixture.requestsOf(Op::Delete);
    ASSERT_EQ(deletes.size(), 1u);
    EXPECT_EQ(deletes[0].path, fixture.stagingPath());
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
}

/// A conditional write's precondition belongs on the publishing rename, which is the request that
/// actually creates the target; putting it on the staging create would silently vacate it.
TEST(AdlsGen2Write, IfNoneMatchTravelsOnRenameNotCreate)
{
    WriteFixture fixture;
    fixture.write_settings.object_storage_write_if_none_match = "*";

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    const auto creates = fixture.requestsOf(Op::Create);
    ASSERT_EQ(creates.size(), 1u);
    /// CreateIfNotExists sets If-None-Match itself; what matters is that the caller's condition is
    /// not what protects the staging object, and that the create is not a plain destructive Create.
    EXPECT_EQ(creates[0].header("If-None-Match"), "*");
    EXPECT_FALSE(creates[0].hasHeader("If-Match"));

    const auto renames = fixture.requestsOf(Op::Rename);
    ASSERT_EQ(renames.size(), 1u);
    EXPECT_EQ(renames[0].header("If-None-Match"), "*");
}

/// A precondition failure on publish must propagate, so conditional writers still see it.
TEST(AdlsGen2Write, PreconditionFailureOnPublishPropagates)
{
    WriteFixture fixture;
    fixture.write_settings.object_storage_write_if_none_match = "*";
    fixture.transport->scriptForever(Op::Rename, {Azure::Core::Http::HttpStatusCode::PreconditionFailed, "PreconditionFailed", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);

    try
    {
        buffer->finalize();
        FAIL() << "a precondition failure must not be swallowed";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(String(e.message()).find("commit status unknown"), String::npos) << e.message();
        EXPECT_NE(String(e.message()).find("412"), String::npos) << e.message();
    }
}

/// An If-Match overwrite (the shape Iceberg's version-hint write uses) must not destroy the target
/// when the write fails.
TEST(AdlsGen2Write, IfMatchTravelsOnRenameAndTargetSurvivesFailure)
{
    WriteFixture fixture;
    fixture.write_settings.object_storage_write_if_match = "\"previous-etag\"";
    fixture.transport->scriptForever(Op::Flush, {Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);

    const auto creates = fixture.requestsOf(Op::Create);
    ASSERT_EQ(creates.size(), 1u);
    EXPECT_FALSE(creates[0].hasHeader("If-Match")) << "the caller's condition must apply to the target, not to staging";
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
}

/// Now with the write succeeding: the caller's If-Match must reach the target.
TEST(AdlsGen2Write, IfMatchTravelsOnRename)
{
    WriteFixture fixture;
    fixture.write_settings.object_storage_write_if_match = "\"previous-etag\"";

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    const auto renames = fixture.requestsOf(Op::Rename);
    ASSERT_EQ(renames.size(), 1u);
    EXPECT_EQ(renames[0].header("If-Match"), "\"previous-etag\"");
}

/// An empty write is a legitimate result and must replace the target, not leave the old object.
TEST(AdlsGen2Write, ZeroByteWriteStillPublishes)
{
    WriteFixture fixture;

    auto buffer = fixture.makeBuffer();
    buffer->finalize();

    ASSERT_EQ(fixture.requestsOf(Op::Create).size(), 1u);
    EXPECT_TRUE(fixture.requestsOf(Op::Append).empty());
    const auto flushes = fixture.requestsOf(Op::Flush);
    ASSERT_EQ(flushes.size(), 1u);
    EXPECT_NE(flushes[0].query.find("position=0"), String::npos) << flushes[0].query;
    ASSERT_EQ(fixture.requestsOf(Op::Rename).size(), 1u);
}

/// The identity chain: create ETag guards the flush, and the flush's ETag guards the rename source
/// and the cleanup delete. So only the object this write created can ever be published or removed.
TEST(AdlsGen2Write, EtagChainPinsTheStagedObject)
{
    WriteFixture fixture;
    fixture.transport->script(Op::Create, {{Azure::Core::Http::HttpStatusCode::Created, {}, "\"e-create\""}});
    fixture.transport->script(Op::Flush, {{Azure::Core::Http::HttpStatusCode::Ok, {}, "\"e-flush\""}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    const auto flushes = fixture.requestsOf(Op::Flush);
    ASSERT_EQ(flushes.size(), 1u);
    EXPECT_EQ(flushes[0].header("If-Match"), "\"e-create\"");

    const auto renames = fixture.requestsOf(Op::Rename);
    ASSERT_EQ(renames.size(), 1u);
    /// The value returned by the flush, not the one from the create.
    EXPECT_EQ(renames[0].header("x-ms-source-if-match"), "\"e-flush\"");
    /// Appends need no condition: a substitution is caught at the flush, the single commit point.
    for (const auto & append : fixture.requestsOf(Op::Append))
        EXPECT_FALSE(append.hasHeader("If-Match"));
}

TEST(AdlsGen2Write, SourceEtagMismatchOnPublishDoesNotTouchTarget)
{
    WriteFixture fixture;
    fixture.transport->scriptForever(Op::Rename, {Azure::Core::Http::HttpStatusCode::PreconditionFailed, "SourceConditionNotMet", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);

    /// The failed rename is the only request that addressed the target, and it changed nothing.
    for (const auto & request : fixture.requestsTargetingFinalPath())
        EXPECT_EQ(classifyRequest(request), Op::Rename);
}

/// An ambiguous publish is reported honestly: the outcome is unknown, but the target holds a complete
/// object either way.
TEST(AdlsGen2Write, AmbiguousPublishYieldsCommitStatusUnknown)
{
    for (const auto status : {Azure::Core::Http::HttpStatusCode::RequestTimeout,
                              Azure::Core::Http::HttpStatusCode::ServiceUnavailable})
    {
        WriteFixture fixture;
        fixture.transport->scriptForever(Op::Rename, {status, "Timeout", {}});

        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);

        try
        {
            buffer->finalize();
            FAIL() << "an ambiguous publish must fail";
        }
        catch (const DB::Exception & e)
        {
            const String & message = e.message();
            EXPECT_NE(message.find("commit status unknown"), String::npos) << message;
            EXPECT_NE(message.find("never a partial object"), String::npos) << message;
        }

        /// Staging is still cleaned up, and only staging.
        const auto deletes = fixture.requestsOf(Op::Delete);
        ASSERT_EQ(deletes.size(), 1u);
        EXPECT_EQ(deletes[0].path, fixture.stagingPath());
    }
}

/// After an ambiguous rename the target may hold either object, so the buffer must not go on to report
/// it as unchanged: `publish` throws without setting `published`, `finalize` cancels, and the destructor
/// then has to describe the same outcome the exception did.
TEST(AdlsGen2Write, AmbiguousPublishDoesNotClaimTheTargetIsUnchanged)
{
    String log_text;
    {
        CapturedWriterLog captured;
        WriteFixture fixture;
        fixture.transport->scriptForever(Op::Rename, {Azure::Core::Http::HttpStatusCode::ServiceUnavailable, "Busy", {}});

        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        EXPECT_THROW(buffer->finalize(), DB::Exception);
        EXPECT_TRUE(buffer->isCanceled()) << "finalize() cancels on failure, which is what reaches the destructor";
        EXPECT_NO_THROW(buffer.reset());
        log_text = captured.text();
    }

    EXPECT_EQ(log_text.find("is unchanged"), String::npos) << log_text;
    EXPECT_NE(log_text.find("Outcome of publishing"), String::npos) << log_text;
    EXPECT_NE(log_text.find("never a partial object"), String::npos) << log_text;
}

/// `preFinalize` is public and publishes there, before `finalized` is set, so a caller may cancel a
/// buffer whose target has already been replaced. `MergedBlockOutputStream` has exactly that shape: it
/// preFinalizes every file and its `Finalizer::Impl::cancel` later cancels all of them, so one file's
/// failure cancels siblings that already published. Reporting those as unchanged would be backwards.
///
/// No fatal `ASSERT_` may run while the buffer is still live here: `WriteBuffer`'s own destructor
/// aborts the process when a buffer is neither finalized nor cancelled and no exception is in flight
/// (`WriteBuffer.cpp:24-35`), so a fatal assertion returning early from the test body would abort the
/// whole binary instead of failing one test. Every check is therefore either non-fatal or made after
/// the buffer has been disposed.
TEST(AdlsGen2Write, CancelAfterSuccessfulPreFinalizeReportsThePublishedFile)
{
    String log_text;
    size_t renames_before_cancel = 0;
    bool finalized_before_cancel = true;
    std::vector<RecordedRequest> deletes;
    std::vector<RecordedRequest> final_path_requests;
    {
        CapturedWriterLog captured;
        WriteFixture fixture;

        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        buffer->preFinalize();
        /// Published, yet neither finalized nor cancelled yet: exactly the window the caller cancels in.
        renames_before_cancel = fixture.requestsOf(Op::Rename).size();
        finalized_before_cancel = buffer->isFinalized();

        buffer->cancel();
        EXPECT_NO_THROW(buffer.reset());
        log_text = captured.text();
        deletes = fixture.requestsOf(Op::Delete);
        final_path_requests = fixture.requestsTargetingFinalPath();
    }

    ASSERT_EQ(renames_before_cancel, 1u) << "the file must be published by preFinalize for this to test anything";
    ASSERT_FALSE(finalized_before_cancel) << "published while not finalized is the state under test";

    /// The published guard holds: cancelling after publication must not delete the renamed-away staging
    /// object, and must certainly not touch the target.
    EXPECT_TRUE(deletes.empty());
    ASSERT_EQ(final_path_requests.size(), 1u);
    EXPECT_EQ(classifyRequest(final_path_requests[0]), Op::Rename);

    EXPECT_EQ(log_text.find("is unchanged"), String::npos) << log_text;
    EXPECT_NE(log_text.find("canceled after publishing"), String::npos) << log_text;
    EXPECT_NE(log_text.find("holds the new contents"), String::npos) << log_text;
}

/// Same published-but-not-finalized state reached by dropping the buffer instead of cancelling it. The
/// caller has to be failing for this to be legal: `WriteBuffer`'s own destructor asserts that a buffer
/// is finalized or cancelled unless an exception is in flight, which is why the throw is not decoration
/// and why, as above, nothing fatal may be asserted while the buffer is alive.
TEST(AdlsGen2Write, DroppingAfterSuccessfulPreFinalizeDoesNotClaimTheFileWasNotWritten)
{
    static constexpr auto caller_failure = "the caller failed after the file was published";

    String log_text;
    size_t renames_before_drop = 0;
    std::vector<RecordedRequest> deletes;
    {
        CapturedWriterLog captured;
        WriteFixture fixture;

        try
        {
            auto buffer = fixture.makeBuffer();
            writeSomeData(*buffer);
            buffer->preFinalize();
            renames_before_drop = fixture.requestsOf(Op::Rename).size();
            throw std::runtime_error(caller_failure);
        }
        catch (const std::runtime_error & e)
        {
            /// Only this test's own failure may be swallowed; anything else came from the writer.
            EXPECT_EQ(String(e.what()), caller_failure);
        }

        log_text = captured.text();
        deletes = fixture.requestsOf(Op::Delete);
    }

    ASSERT_EQ(renames_before_drop, 1u) << "the file must be published by preFinalize for this to test anything";
    EXPECT_TRUE(deletes.empty()) << log_text;

    EXPECT_EQ(log_text.find("was not written"), String::npos) << log_text;
    EXPECT_EQ(log_text.find("neither published nor cleaned up"), String::npos) << log_text;
    EXPECT_NE(log_text.find("dropped without being finalized"), String::npos) << log_text;
    EXPECT_NE(log_text.find("holds the new contents"), String::npos) << log_text;
}

/// The plain cancellation case keeps saying the target is unchanged, because there it provably is: no
/// rename was ever issued.
TEST(AdlsGen2Write, PlainCancellationStillReportsTheTargetUnchanged)
{
    String log_text;
    {
        CapturedWriterLog captured;
        WriteFixture fixture;

        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        buffer->cancel();
        EXPECT_NO_THROW(buffer.reset());
        log_text = captured.text();
    }

    EXPECT_NE(log_text.find("is unchanged"), String::npos) << log_text;
    EXPECT_EQ(log_text.find("Outcome of publishing"), String::npos) << log_text;
}

/// 408 is exactly what ClickHouse's Azure transport synthesizes for a timeout, and
/// isRetryableAzureException excludes it while treating 403 as retryable. So the publish classifier
/// cannot be that helper; this table pins its shape.
TEST(AdlsGen2Write, PublishOutcomeClassification)
{
    using Azure::Core::Http::HttpStatusCode;
    const std::vector<HttpStatusCode> ambiguous{
        HttpStatusCode::RequestTimeout,
        HttpStatusCode::InternalServerError,
        HttpStatusCode::BadGateway,
        HttpStatusCode::ServiceUnavailable,
        HttpStatusCode::GatewayTimeout};
    const std::vector<HttpStatusCode> definite{
        HttpStatusCode::BadRequest,
        HttpStatusCode::Forbidden,
        HttpStatusCode::NotFound,
        HttpStatusCode::Conflict,
        HttpStatusCode::PreconditionFailed};

    for (const auto status : ambiguous)
    {
        WriteFixture fixture;
        fixture.transport->scriptForever(Op::Rename, {status, "Err", {}});
        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        try
        {
            buffer->finalize();
            FAIL() << "expected a failure for HTTP " << static_cast<int>(status);
        }
        catch (const DB::Exception & e)
        {
            EXPECT_NE(String(e.message()).find("commit status unknown"), String::npos)
                << "HTTP " << static_cast<int>(status) << ": " << e.message();
        }
    }

    for (const auto status : definite)
    {
        WriteFixture fixture;
        fixture.transport->scriptForever(Op::Rename, {status, "Err", {}});
        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        try
        {
            buffer->finalize();
            FAIL() << "expected a failure for HTTP " << static_cast<int>(status);
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(String(e.message()).find("commit status unknown"), String::npos)
                << "HTTP " << static_cast<int>(status) << ": " << e.message();
        }
    }
}

/// The rename must be issued exactly once: it is a move, so a repeat would find no source and report
/// failure for a write that did get published. 500 is in the SDK's default retry set, so this also
/// proves the publish client turns SDK retries off rather than relying on the caller to.
TEST(AdlsGen2Write, PublishIsSingleShot)
{
    WriteFixture fixture;
    ASSERT_GT(fixture.sdk_max_retries, 0) << "the caller must be asking for retries for this to prove anything";
    fixture.transport->scriptForever(Op::Rename, {Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);

    EXPECT_EQ(fixture.requestsOf(Op::Rename).size(), 1u);
}

/// Same for the guarded flush: it changes the ETag, so a repeat under the same condition can only
/// fail. And because it runs before any rename, its failure is a plain staging failure, never
/// `commit status unknown`.
///
/// 500 is deliberately the status here: it is the one both retry layers would otherwise repeat (the
/// buffer's own loop via isRetryableAzureException, and the SDK's default retry set), so it is what
/// catches a flush left on a retrying path. Appends, which are idempotent, keep their retries.
TEST(AdlsGen2Write, GuardedFlushIsSingleShotAndFailsAsStagingFailure)
{
    WriteFixture fixture;
    ASSERT_GT(fixture.sdk_max_retries, 0) << "the caller must be asking for retries for this to prove anything";
    fixture.transport->scriptForever(Op::Flush, {Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);

    try
    {
        buffer->finalize();
        FAIL() << "a failed flush must fail the write";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(String(e.message()).find("commit status unknown"), String::npos) << e.message();
    }

    EXPECT_EQ(fixture.requestsOf(Op::Flush).size(), 1u);
    EXPECT_TRUE(fixture.requestsOf(Op::Rename).empty()) << "publication must not be attempted after a failed flush";
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
}

/// The staging append is idempotent, so it keeps the caller's retries: making the write single-shot
/// wholesale would turn transients into failed INSERTs.
TEST(AdlsGen2Write, StagingAppendKeepsRetries)
{
    WriteFixture fixture;
    fixture.transport->script(
        Op::Append,
        {{Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}},
         {Azure::Core::Http::HttpStatusCode::Accepted, {}, {}}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    EXPECT_GT(fixture.requestsOf(Op::Append).size(), 1u) << "a transient append failure must be retried";
    ASSERT_EQ(fixture.requestsOf(Op::Rename).size(), 1u);
}

/// `IDisk::checkAccess` writes through this buffer at startup and tolerates a 403 while Azure is
/// provisioning access; a 403 changes nothing, so retrying it is safe. An ambiguous status is not
/// retried even then.
TEST(AdlsGen2Write, ForbiddenRetriedDuringInitialAccessCheck)
{
    using Azure::Core::Http::HttpStatusCode;

    for (const auto op : {Op::Flush, Op::Rename})
    {
        WriteFixture fixture;
        fixture.write_settings.is_initial_access_check = true;
        fixture.transport->script(op, {{HttpStatusCode::Forbidden, "AuthorizationFailure", {}}});

        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        buffer->finalize();
        EXPECT_EQ(fixture.requestsOf(op).size(), 2u) << "expected one retry after 403";
    }

    for (const auto op : {Op::Flush, Op::Rename})
    {
        WriteFixture fixture;
        fixture.transport->script(op, {{HttpStatusCode::Forbidden, "AuthorizationFailure", {}}});

        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        try
        {
            buffer->finalize();
            FAIL() << "a 403 outside an access check must propagate";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(String(e.message()).find("commit status unknown"), String::npos) << e.message();
        }
        EXPECT_EQ(fixture.requestsOf(op).size(), 1u) << "a 403 outside an access check must not be retried";
    }

    /// Ambiguous statuses stay single-shot even during an access check.
    for (const auto op : {Op::Flush, Op::Rename})
    {
        WriteFixture fixture;
        fixture.write_settings.is_initial_access_check = true;
        fixture.transport->scriptForever(op, {HttpStatusCode::RequestTimeout, "Timeout", {}});

        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        EXPECT_THROW(buffer->finalize(), DB::Exception);
        EXPECT_EQ(fixture.requestsOf(op).size(), 1u);
    }
}

/// The staging name is unreserved, so a user object could already occupy it: never overwrite it,
/// pick another name instead.
TEST(AdlsGen2Write, StagingCollisionRegeneratesAndNeverOverwrites)
{
    WriteFixture fixture;
    fixture.transport->script(
        Op::Create,
        {{Azure::Core::Http::HttpStatusCode::Conflict, "PathAlreadyExists", {}},
         {Azure::Core::Http::HttpStatusCode::Created, {}, {}}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    const auto creates = fixture.requestsOf(Op::Create);
    ASSERT_EQ(creates.size(), 2u);
    EXPECT_NE(creates[0].path, creates[1].path) << "a taken name must not be reused";
    /// Nothing was written to or deleted from the occupied path.
    for (const auto & request : fixture.transport->requests)
        if (request.path == creates[0].path)
            EXPECT_EQ(classifyRequest(request), Op::Create);

    const auto renames = fixture.requestsOf(Op::Rename);
    ASSERT_EQ(renames.size(), 1u);
    EXPECT_EQ(renames[0].header("x-ms-rename-source"), "/" + creates[1].path);
}

TEST(AdlsGen2Write, StagingCreateFailureOtherThanCollisionPropagates)
{
    WriteFixture fixture;
    fixture.transport->scriptForever(Op::Create, {Azure::Core::Http::HttpStatusCode::Forbidden, "AuthorizationFailure", {}});

    auto buffer = fixture.makeBuffer();
    EXPECT_THROW({ writeSomeData(*buffer); buffer->finalize(); }, DB::Exception);

    /// One name, not eight: a non-collision error is not a reason to try another name.
    EXPECT_EQ(fixture.requestsOf(Op::Create).size(), 1u);
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
}

/// Regeneration must be bounded: a name that stays taken has to fail the write promptly rather than
/// spin against the service.
TEST(AdlsGen2Write, PersistentStagingCollisionThrowsWithoutTouchingTarget)
{
    WriteFixture fixture;
    fixture.transport->scriptForever(Op::Create, {Azure::Core::Http::HttpStatusCode::Conflict, "PathAlreadyExists", {}});

    auto buffer = fixture.makeBuffer();
    EXPECT_THROW({ writeSomeData(*buffer); buffer->finalize(); }, DB::Exception);

    const auto creates = fixture.requestsOf(Op::Create);
    EXPECT_GT(creates.size(), 1u) << "regeneration must be attempted";
    EXPECT_LE(creates.size(), 16u) << "regeneration must be bounded";
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
    EXPECT_TRUE(fixture.requestsOf(Op::Delete).empty()) << "nothing was created, so nothing may be deleted";
}

/// After an ambiguous rename the staging name is free again, so a concurrent writer could own an
/// object there. Deleting that would be the very bug being fixed.
TEST(AdlsGen2Write, StagingDeleteIsEtagConditionalAndLeavesForeignObjects)
{
    WriteFixture fixture;
    fixture.transport->script(Op::Flush, {{Azure::Core::Http::HttpStatusCode::Ok, {}, "\"e-flush\""}});
    fixture.transport->scriptForever(Op::Rename, {Azure::Core::Http::HttpStatusCode::ServiceUnavailable, "Busy", {}});
    fixture.transport->scriptForever(Op::Delete, {Azure::Core::Http::HttpStatusCode::PreconditionFailed, "ConditionNotMet", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);

    const auto deletes = fixture.requestsOf(Op::Delete);
    ASSERT_EQ(deletes.size(), 1u) << "a refused delete must not be retried or forced";
    EXPECT_EQ(deletes[0].header("If-Match"), "\"e-flush\"");
    /// The refusal must not escape: `cancelImpl` is `noexcept`.
    EXPECT_NO_THROW(buffer.reset());
}

/// A lost response to the staging create leaves an object behind while the process is still alive. The
/// sequence that produces it: the create is answered `500`, which the SDK retries against the same path,
/// and that retry is answered `PathAlreadyExists` because the first attempt had in fact taken effect. So
/// two requests address one staging path, this write never observed that object's ETag, and it cannot
/// prove it owns it. It is left in place deliberately, because deleting an object of unknown ownership is
/// the bug this whole change fixes. That residual is disclosed rather than papered over. The two-request
/// signature is what separates this from an ordinary name collision, which
/// `StagingCollisionRegeneratesAndNeverOverwrites` covers.
TEST(AdlsGen2Write, LostCreateResponseLeavesTheFirstStagingObjectAndNeverDeletesIt)
{
    WriteFixture fixture;
    ASSERT_GT(fixture.sdk_max_retries, 0) << "the lost response needs the SDK to repeat the create";
    /// The retry the SDK makes after the 500 lands on the same path, and the service reports the object
    /// the lost attempt created. `CreateIfNotExists` turns that into `Created = false` without throwing,
    /// so the writer regenerates the name rather than entering its own retry loop.
    fixture.transport->script(
        Op::Create,
        {{Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}},
         {Azure::Core::Http::HttpStatusCode::Conflict, "PathAlreadyExists", {}},
         {Azure::Core::Http::HttpStatusCode::Created, {}, "\"e-second\""}});
    fixture.transport->scriptForever(Op::Flush, {Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);
    EXPECT_NO_THROW(buffer.reset());

    const auto creates = fixture.requestsOf(Op::Create);
    ASSERT_EQ(creates.size(), 3u);
    /// The lost response: the retry went back to the SAME path and was told it already exists.
    EXPECT_EQ(creates[0].path, creates[1].path);
    EXPECT_NE(creates[1].path, creates[2].path);

    /// The orphan: the first staging object is never deleted, and never written to either.
    const auto deletes = fixture.requestsOf(Op::Delete);
    ASSERT_EQ(deletes.size(), 1u);
    EXPECT_EQ(deletes[0].path, creates[2].path) << "only the object whose ETag this write observed";
    EXPECT_EQ(deletes[0].header("If-Match"), "\"e-second\"");
    for (const auto & request : fixture.transport->requests)
        if (request.path == creates[0].path)
            EXPECT_EQ(classifyRequest(request), Op::Create) << request.method;

    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
}

/// A lost response to the flush is the other live-process orphan. The flush took effect, so the object's
/// ETag moved, but this write still holds the create ETag; the conditional cleanup delete is therefore
/// refused with 412 and the object stays. Same reasoning: an object we cannot identify is not ours to
/// remove. The refusal must not escape, since cleanup runs from a `noexcept` path.
TEST(AdlsGen2Write, LostFlushResponseLeavesStagingInPlaceRatherThanDeletingIt)
{
    WriteFixture fixture;
    fixture.transport->scriptForever(Op::Flush, {Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}});
    /// The staging object now holds post-flush content, so the create ETag no longer matches.
    fixture.transport->scriptForever(Op::Delete, {Azure::Core::Http::HttpStatusCode::PreconditionFailed, "ConditionNotMet", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);

    const auto deletes = fixture.requestsOf(Op::Delete);
    ASSERT_EQ(deletes.size(), 1u) << "a refused delete must not be retried or forced";
    EXPECT_EQ(deletes[0].path, fixture.stagingPath());
    EXPECT_EQ(deletes[0].header("If-Match"), "\"etag-after-create\"")
        << "the delete is pinned to the ETag this write observed, which the flush has moved past";

    EXPECT_NO_THROW(buffer.reset());
    /// A refusal is final: it clears `file_created`, so the destructor does not try again.
    EXPECT_EQ(fixture.requestsOf(Op::Delete).size(), 1u);
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
}

/// Cleanup is idempotent, so unlike the flush and the rename it keeps its retries: a transient failure
/// must not silently leave an orphan. Only a refusal (the ETag no longer matches) stops it, which
/// StagingDeleteIsEtagConditionalAndLeavesForeignObjects pins.
TEST(AdlsGen2Write, TransientCleanupFailureIsRetried)
{
    WriteFixture fixture;
    fixture.transport->scriptForever(Op::Flush, {Azure::Core::Http::HttpStatusCode::InternalServerError, "InternalError", {}});
    fixture.transport->script(Op::Delete, {{Azure::Core::Http::HttpStatusCode::ServiceUnavailable, "Busy", {}}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_THROW(buffer->finalize(), DB::Exception);

    const auto deletes = fixture.requestsOf(Op::Delete);
    ASSERT_GT(deletes.size(), 1u) << "a transient cleanup failure must be retried";
    EXPECT_EQ(deletes.back().path, fixture.stagingPath());
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());

    /// Staging was removed in the end, so the destructor has nothing left to do.
    const size_t deletes_before_destruction = deletes.size();
    EXPECT_NO_THROW(buffer.reset());
    EXPECT_EQ(fixture.requestsOf(Op::Delete).size(), deletes_before_destruction);
}

/// An expiry set for orphan hygiene would ride the rename onto the published object and delete the
/// user's freshly written data, because RenameFileOptions cannot clear it.
TEST(AdlsGen2Write, NoExpiryOptionOnStagingCreate)
{
    WriteFixture fixture;

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    for (const auto & request : fixture.transport->requests)
    {
        EXPECT_FALSE(request.hasHeader("x-ms-expiry-option")) << request.path;
        EXPECT_FALSE(request.hasHeader("x-ms-expiry-time")) << request.path;
    }
}

/// The cost claim: one extra request per object, the metadata-only rename. No existence probe and no
/// capability preflight on any path.
TEST(AdlsGen2Write, NoProbeOrPreflightRequests)
{
    WriteFixture fixture;

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    std::vector<String> methods;
    for (const auto & request : fixture.transport->requests)
        methods.push_back(request.method + " " + String{magic_enum::enum_name(classifyRequest(request))});

    const std::vector<String> expected{"PUT Create", "PATCH Append", "PATCH Flush", "PUT Rename"};
    EXPECT_EQ(methods, expected);
    for (const auto & request : fixture.transport->requests)
    {
        EXPECT_NE(request.method, "HEAD");
        EXPECT_NE(request.method, "GET");
    }
}

/// No read capability is required, so write-only credentials keep working.
TEST(AdlsGen2Write, WriteOnlyCredentialCompletesSuccessfully)
{
    WriteFixture fixture;
    fixture.transport->scriptForever(Op::Other, {Azure::Core::Http::HttpStatusCode::Forbidden, "AuthorizationFailure", {}});

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    EXPECT_NO_THROW(buffer->finalize());
}

/// A caller that throws between `next` and `finalize` never reaches either `finalize` or `cancel`, so
/// the destructor is the only place left to remove staging. `WriteBuffer`'s own destructor tolerates
/// this case precisely because an exception is in flight. As in the `preFinalize` tests above, the
/// staging path is only recorded while the buffer is alive and asserted once it is gone: a failing
/// `ASSERT_` returns without throwing, so the destructor would abort instead of failing this test.
TEST(AdlsGen2Write, DestructorCleansStagingWhenCallerThrows)
{
    static constexpr auto caller_failure = "the caller failed after writing";

    WriteFixture fixture;
    String staging;

    try
    {
        auto buffer = fixture.makeBuffer();
        writeSomeData(*buffer);
        staging = fixture.stagingPath();
        throw std::runtime_error(caller_failure);
    }
    catch (const std::runtime_error & e)
    {
        /// Only this test's own failure may be swallowed; anything else came from the writer.
        EXPECT_EQ(String(e.what()), caller_failure);
    }

    ASSERT_FALSE(staging.empty()) << "a staging object must have been created for this to test anything";

    const auto deletes = fixture.requestsOf(Op::Delete);
    ASSERT_EQ(deletes.size(), 1u);
    EXPECT_EQ(deletes[0].path, staging);
    EXPECT_TRUE(fixture.requestsTargetingFinalPath().empty());
}

/// RenameFile derives the destination's file system from the FIRST path segment of the client URL
/// unless told otherwise, so an endpoint whose URL also carries the account name (the
/// `endpoint_contains_account_name` shape) would publish into a path built from the account instead
/// of the container. Passing it explicitly is what keeps the destination equal to the target.
TEST(AdlsGen2Write, PublishTargetsTheContainerWhenTheUrlCarriesTheAccount)
{
    WriteFixture fixture;
    fixture.endpoint.account_name = "myaccount";
    fixture.endpoint.add_account_name_to_url = true;

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    const auto creates = fixture.requestsOf(Op::Create);
    ASSERT_EQ(creates.size(), 1u);
    ASSERT_TRUE(creates[0].path.starts_with("myaccount/mycontainer/")) << creates[0].path;

    const auto renames = fixture.requestsOf(Op::Rename);
    ASSERT_EQ(renames.size(), 1u);
    EXPECT_EQ(renames[0].path, "myaccount/mycontainer/tables/mytable/data/data-0.parquet");
    EXPECT_EQ(renames[0].header("x-ms-rename-source"), "/" + creates[0].path);
}

/// Production's default OneLake endpoint is the Blob host (`onelake_use_blob_endpoint` defaults to
/// true), which does not serve the DFS API. Both filesystem clients are built from the same container
/// URL, so every request the writer issues depends on that host being retargeted to DFS.
TEST(AdlsGen2Write, BlobHostEndpointIsRetargetedToDfsForEveryRequest)
{
    WriteFixture fixture;
    fixture.endpoint.storage_account_url = "https://onelake.blob.fabric.microsoft.com";

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    /// Pin the sequence too, so the host assertions cannot pass on a write that never happened.
    std::vector<String> ops;
    for (const auto & request : fixture.transport->requests)
        ops.push_back(String{magic_enum::enum_name(classifyRequest(request))});
    const std::vector<String> expected{"Create", "Append", "Flush", "Rename"};
    ASSERT_EQ(ops, expected);

    /// Per request, so a change that retargets only some of the clients is localized.
    for (const auto & request : fixture.transport->requests)
        EXPECT_EQ(request.host, "onelake.dfs.fabric.microsoft.com")
            << String{magic_enum::enum_name(classifyRequest(request))} << " " << request.path;
}

/// A regular Azure account is already on a .dfs host, so the retargeting has to stay keyed on the
/// Fabric Blob suffix instead of rewriting every host.
TEST(AdlsGen2Write, NonFabricDfsHostIsNotRetargeted)
{
    WriteFixture fixture;
    fixture.endpoint.storage_account_url = "https://myacct.dfs.core.windows.net";

    auto buffer = fixture.makeBuffer();
    writeSomeData(*buffer);
    buffer->finalize();

    ASSERT_FALSE(fixture.transport->requests.empty());
    for (const auto & request : fixture.transport->requests)
        EXPECT_EQ(request.host, "myacct.dfs.core.windows.net")
            << String{magic_enum::enum_name(classifyRequest(request))} << " " << request.path;
}

/// The error for a key that cannot be staged has to be actionable. Since only the file name may be
/// shortened, a key well inside the 1024-byte limit is still unwritable when its parent directory alone
/// leaves no room, and naming the byte limit alone would send the operator after the wrong thing.
TEST(AdlsGen2Write, KeyThatCannotBeStagedNamesTheParentDirectory)
{
    WriteFixture fixture;
    /// A parent long enough that no file name fits beside it, and a short base name so the message
    /// cannot be explained by the key as a whole being over the limit. The path the writer measures is
    /// the prefixed one, so both it and the key have to stay within the limit for the parent to be the
    /// only possible explanation.
    const String key = String(987, 'd') + "/b";
    ASSERT_LT(key.size(), 1024u) << "the key itself is within the limit; only staging does not fit";
    ASSERT_LE(String("tables/mytable/").size() + key.size(), 1024u) << "and so is the prefixed path";

    auto buffer = std::make_unique<WriteBufferFromAzureDataLakeStorage>(
        fixture.endpoint,
        fixture.auth_method,
        fixture.clientOptions(),
        key,
        DBMS_DEFAULT_BUFFER_SIZE,
        fixture.write_settings,
        std::make_shared<AzureBlobStorage::RequestSettings>(fixture.request_settings));

    try
    {
        writeSomeData(*buffer);
        buffer->finalize();
        FAIL() << "a key with no room for a staging sibling must fail the write";
    }
    catch (const DB::Exception & e)
    {
        const String & message = e.message();
        EXPECT_NE(message.find("parent directory"), String::npos) << message;
        EXPECT_NE(message.find("sibling"), String::npos) << message;
    }

    /// Nothing was attempted against the service, so certainly not against the target.
    EXPECT_TRUE(fixture.transport->requests.empty());
    EXPECT_NO_THROW(buffer.reset());
}

/// Shortening must never cut into the parent directory: a staging object in another directory breaks
/// both the sibling invariant and directory-scoped credentials. Where no basename fits beside the
/// parent, the only correct answer is to report it, which `ensureCreated` turns into a clear error.
TEST(AdlsGen2Write, StagingPathStaysASiblingForALongParentDirectory)
{
    const String random_suffix(16, 'q');
    const String suffix = ".tmp." + random_suffix;

    /// A parent so long that no basename fits beside it.
    const String long_parent(1010, 'd');
    EXPECT_TRUE(makeAdlsGen2StagingPath(long_parent + "/b", random_suffix).empty())
        << "shortening by byte offset alone would land before the separator and stage in the container root";

    /// A key that must be shortened but whose parent leaves room: still a sibling.
    const String parent(100, 'e');
    const String shortened = makeAdlsGen2StagingPath(parent + "/" + String(920, 'f'), random_suffix);
    ASSERT_FALSE(shortened.empty());
    EXPECT_LE(shortened.size(), 1024u);
    EXPECT_TRUE(shortened.ends_with(suffix)) << shortened;
    ASSERT_NE(shortened.rfind('/'), String::npos) << shortened;
    EXPECT_EQ(shortened.substr(0, shortened.rfind('/')), parent);

    /// The invariant across the whole reachable range of parent lengths: either a sibling of the
    /// target, or nothing.
    for (size_t parent_size = 1; parent_size + 2 <= 1024; ++parent_size)
    {
        const String key = String(parent_size, 'd') + "/" + String(std::min<size_t>(50, 1024 - parent_size - 1), 'b');
        const String staging = makeAdlsGen2StagingPath(key, random_suffix);
        if (staging.empty())
            continue;
        EXPECT_LE(staging.size(), 1024u) << staging.size();
        ASSERT_NE(staging.rfind('/'), String::npos) << "parent_size=" << parent_size << ": " << staging;
        EXPECT_EQ(staging.substr(0, staging.rfind('/')), key.substr(0, key.rfind('/')))
            << "parent_size=" << parent_size;
        EXPECT_TRUE(staging.ends_with(suffix)) << "parent_size=" << parent_size;
    }
}

/// A staging name is a sibling in the same directory, so directory-scoped credentials keep working.
TEST(AdlsGen2Write, StagingPathIsASiblingOfTheTarget)
{
    const String staging = makeAdlsGen2StagingPath("tables/mytable/data/data-0.parquet", "abcdef");
    EXPECT_TRUE(staging.starts_with("tables/mytable/data/data-0.parquet.")) << staging;
    EXPECT_EQ(staging.find('/', String("tables/mytable/data/").size()), String::npos) << staging;
}

/// A sibling suffix lengthens the key, so a key at the 1024-byte limit must be handled explicitly
/// rather than silently producing an over-long path.
TEST(AdlsGen2Write, StagingPathRespectsKeyLimit)
{
    const String at_limit(1024, 'a');
    const String staging = makeAdlsGen2StagingPath(at_limit, "abcdef");
    ASSERT_FALSE(staging.empty());
    EXPECT_LE(staging.size(), 1024u);
    EXPECT_TRUE(staging.ends_with(".tmp.abcdef")) << "the suffix is what makes the name unique";

    /// A suffix that cannot fit at all is reported rather than truncated into a collision.
    EXPECT_TRUE(makeAdlsGen2StagingPath("x", String(2048, 'b')).empty());
}

/// Keys must be valid UTF-8, so shortening the stem must stop at a character boundary.
TEST(AdlsGen2Write, StagingPathShortenedAtUtf8Boundary)
{
    /// 'é' is two bytes; repeat it so the truncation point falls inside a character.
    String at_limit;
    while (at_limit.size() < 1024)
        at_limit += "\xC3\xA9";
    ASSERT_EQ(at_limit.size(), 1024u);

    const String staging = makeAdlsGen2StagingPath(at_limit, "abcdef");
    ASSERT_FALSE(staging.empty());
    const String stem = staging.substr(0, staging.size() - String(".tmp.abcdef").size());
    EXPECT_EQ(stem.size() % 2, 0u) << "the stem must not end mid-character";
}

#endif

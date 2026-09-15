#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <gmock/gmock.h>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/config/AWSProfileConfigLoader.h>

#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>

#include <IO/WriteBufferFromS3.h>
#include <IO/S3Common.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/AsyncReadCounters.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/Client.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/Requests.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/SeekableReadBuffer.h>

#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>

#include <Common/filesystemHelpers.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>

#include <fmt/format.h>

#include <Poco/AutoPtr.h>
#include <Poco/StreamChannel.h>
#include <Poco/TemporaryFile.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool s3_check_objects_after_upload;
    extern const SettingsUInt64 s3_max_inflight_parts_for_one_file;
    extern const SettingsUInt64 s3_max_single_part_upload_size;
    extern const SettingsUInt64 s3_max_upload_part_size;
    extern const SettingsUInt64 s3_min_upload_part_size;
    extern const SettingsUInt64 s3_strict_upload_part_size;
    extern const SettingsUInt64 s3_upload_part_size_multiply_factor;
    extern const SettingsUInt64 s3_upload_part_size_multiply_parts_count_threshold;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool allow_native_copy;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int S3_ERROR;
    extern const int NOT_IMPLEMENTED;
}

}

using namespace DB;

namespace
{

/// A private copy of `gtest_writebuffer_s3.cpp`'s mock S3 client, extended with the attempt-seed
/// recorder and `PreconditionFailed` injection this file's tests need -- duplicated so an upstream
/// change to the mock never produces a conflict here on rebase. Anonymous namespace: both `.cpp`
/// files link into `unit_tests_dbms`, and without internal linkage the duplicate types below would
/// violate the One Definition Rule. A few members this file's own tests never call are kept
/// for parity with the upstream mock and marked `[[maybe_unused]]`, since the anonymous namespace
/// (unlike the external linkage of a shared header) makes an unused member function an error.
namespace MockS3
{

class Sequencer
{
public:
    size_t next() { return counter++; }
    std::string next_id()
    {
        std::stringstream ss;
        ss << "id-" << next();
        return ss.str();
    }

private:
    size_t counter = 0;
};

class BucketMemStore
{
public:
    using Key = std::string;
    using Data = std::string;
    using ETag = std::string;
    using MPU_ID = std::string;
    using MPUPartsInProgress = std::map<ETag, Data>;
    using MPUParts = std::vector<Data>;


    std::map<Key, Data> objects;
    std::map<MPU_ID, MPUPartsInProgress> multiPartUploads;
    std::vector<std::pair<MPU_ID, MPUParts>> CompletedPartUploads;

    Sequencer sequencer;

    std::string CreateMPU()
    {
        auto id = sequencer.next_id();
        multiPartUploads.emplace(id, MPUPartsInProgress{});
        return id;
    }

    std::string UploadPart(const std::string & upload_id, const std::string & part)
    {
        auto etag = sequencer.next_id();
        auto & parts = multiPartUploads.at(upload_id);
        parts.emplace(etag, part);
        return etag;
    }

    void PutObject(const std::string & key, const std::string & data)
    {
        objects[key] = data;
    }

    void CompleteMPU(const std::string & key, const std::string & upload_id, const std::vector<std::string> & etags)
    {
        MPUParts completedParts;
        completedParts.reserve(etags.size());

        auto & parts = multiPartUploads.at(upload_id);
        for (const auto & tag: etags) {
            completedParts.push_back(parts.at(tag));
        }

        std::stringstream file_data;
        for (const auto & part_data: completedParts) {
            file_data << part_data;
        }

        CompletedPartUploads.emplace_back(upload_id, std::move(completedParts));
        objects[key] = file_data.str();
        multiPartUploads.erase(upload_id);
    }

    void AbortMPU(const std::string & upload_id)
    {
        multiPartUploads.erase(upload_id);
    }


    const std::vector<std::pair<MPU_ID, MPUParts>> & GetCompletedPartUploads() const
    {
        return CompletedPartUploads;
    }

    [[maybe_unused]] static std::vector<size_t> GetPartSizes(const MPUParts & parts)
    {
        std::vector<size_t> result;
        result.reserve(parts.size());
        for (const auto & part_data : parts)
            result.push_back(part_data.size());

        return result;
    }

};

class S3MemStrore
{
public:
    void CreateBucket(const std::string & bucket)
    {
        chassert(!buckets.contains(bucket));
        buckets.emplace(bucket, BucketMemStore{});
    }

    BucketMemStore& GetBucketStore(const std::string & bucket) {
        return buckets.at(bucket);
    }

private:
    std::map<std::string, BucketMemStore> buckets;
};

struct EventCounts
{
    size_t headObject = 0;
    size_t getObject = 0;
    size_t putObject = 0;
    size_t multiUploadCreate = 0;
    size_t multiUploadComplete = 0;
    size_t multiUploadAbort = 0;
    size_t uploadParts = 0;
    size_t writtenSize = 0;
    size_t copyObject = 0;
    size_t deleteObject = 0;
    size_t getBucketVersioning = 0;

    [[maybe_unused]] size_t totalRequestsCount() const
    {
        return headObject + getObject + putObject + multiUploadCreate + multiUploadComplete + uploadParts;
    }
};

struct Client;

struct InjectionModel
{
    virtual ~InjectionModel() = default;

#define DeclareInjectCall(ObjectTypePart) \
    virtual std::optional<Aws::S3::Model::ObjectTypePart##Outcome> call(const Aws::S3::Model::ObjectTypePart##Request & /*request*/) \
    { \
        return std::nullopt; \
    }
    DeclareInjectCall(PutObject)
    DeclareInjectCall(HeadObject)
    DeclareInjectCall(CreateMultipartUpload)
    DeclareInjectCall(CompleteMultipartUpload)
    DeclareInjectCall(AbortMultipartUpload)
    DeclareInjectCall(UploadPart)
    DeclareInjectCall(CopyObject)
    DeclareInjectCall(DeleteObject)
    DeclareInjectCall(GetBucketVersioning)
#undef DeclareInjectCall
};

/// `DB::S3::getClickhouseAttemptNumber(const Aws::AmazonWebServiceRequest &)` reads `GetHeaders()`,
/// which for a plain S3 request never includes `SetAdditionalCustomHeaderValue`'s custom headers --
/// only `AWSClient::BuildHttpRequest` merges those into the wire-level `Aws::Http::HttpRequest` that
/// `PocoHTTPClient` actually inspects (the overload production code reads). This mock overrides the
/// `S3Client` virtuals directly, below that merge, so it reads the custom header collection itself.
/// `nullopt` means the `clickhouse-request` header is absent -- distinct from an explicit `attempt=1`,
/// since a seed of 0 leaves every verb but the read path unseeded (no header at all; see
/// `S3::seededAttemptNumber`'s callers).
std::optional<size_t> attemptNumberFromCustomHeaders(const Aws::AmazonWebServiceRequest & request)
{
    const auto & headers = request.GetAdditionalCustomHeaders();
    auto it = headers.find("clickhouse-request");
    if (it == headers.end())
        return std::nullopt;
    static const std::string key = "attempt=";
    auto pos = it->second.find(key);
    if (pos == std::string::npos)
        return std::nullopt;
    try
    {
        return static_cast<size_t>(std::stol(it->second.substr(pos + key.size())));
    }
    catch (const std::exception &)
    {
        return std::nullopt;
    }
}

struct Client : DB::S3::Client
{
    explicit Client(std::shared_ptr<S3MemStrore> mock_s3_store)
        : DB::S3::Client(
            100,
            DB::S3::ServerSideEncryptionKMSConfig(),
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("", ""),
            GetClientConfiguration(),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            DB::S3::ClientSettings{
                .use_virtual_addressing = true,
                .disable_checksum = false,
                .gcs_issue_compose_request = false,
                .is_s3express_bucket = false,
            })
        , store(mock_s3_store)
    {}

    static std::shared_ptr<Client> CreateClient(String bucket = "mock-s3-bucket")
    {
        auto s3store = std::make_shared<S3MemStrore>();
        s3store->CreateBucket(bucket);
        return std::make_shared<Client>(s3store);
    }

    static DB::S3::PocoHTTPClientConfiguration GetClientConfiguration()
    {
        DB::RemoteHostFilter remote_host_filter;
        return DB::S3::ClientFactory::instance().createClientConfiguration(
            "some-region",
            remote_host_filter,
            /* s3_max_redirects = */ 100,
            DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
            /* s3_slow_all_threads_after_network_error = */ true,
            /* s3_slow_all_threads_after_retryable_error = */ true,
            /* enable_s3_requests_logging = */ true,
            /* for_disk_s3 = */ false,
            /* opt_disk_name = */ {},
            /* request_throttler = */ {});
    }

    void setInjectionModel(std::shared_ptr<MockS3::InjectionModel> injections_)
    {
        injections = injections_;
    }

    /// `clickhouse-request` attempt of every verb, in order -- test-only recorder for the attempt-seed tests.
    mutable std::vector<std::optional<size_t>> attempts_seen;

    Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(const Aws::S3::Model::ListObjectsV2Request & request) const override
    {
        attempts_seen.push_back(attemptNumberFromCustomHeaders(request));
        auto & bStore = store->GetBucketStore(request.GetBucket());
        Aws::S3::Model::ListObjectsV2Result result;
        result.SetPrefix(request.GetPrefix());
        int emitted = 0;
        std::string last;
        const std::string after = request.ContinuationTokenHasBeenSet() ? request.GetContinuationToken()
                                : request.StartAfterHasBeenSet() ? request.GetStartAfter() : "";
        for (const auto & [key, data] : bStore.objects)
        {
            if (!key.starts_with(request.GetPrefix()) || key <= after)
                continue;
            if (emitted == request.GetMaxKeys())
            {
                result.SetIsTruncated(true);
                result.SetNextContinuationToken(last);
                break;
            }
            Aws::S3::Model::Object object;
            object.SetKey(key);
            object.SetSize(static_cast<long long>(data.size()));
            result.AddContents(std::move(object));
            last = key;
            ++emitted;
        }
        return Aws::S3::Model::ListObjectsV2Outcome(std::move(result));
    }

    Aws::S3::Model::DeleteObjectsOutcome DeleteObjects(const Aws::S3::Model::DeleteObjectsRequest & request) const override
    {
        attempts_seen.push_back(attemptNumberFromCustomHeaders(request));

        auto & bStore = store->GetBucketStore(request.GetBucket());
        for (const auto & identifier : request.GetDelete().GetObjects())
            bStore.objects.erase(identifier.GetKey());

        Aws::S3::Model::DeleteObjectsResult result;
        return Aws::S3::Model::DeleteObjectsOutcome(std::move(result));
    }

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest & request) const override
    {
        attempts_seen.push_back(attemptNumberFromCustomHeaders(request));
        ++counters.putObject;

        if (const auto * wrapper = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request))
            last_put_object_native_conditional = wrapper->isNativeConditional();

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return *opt_val;
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        std::stringstream data;
        data << request.GetBody()->rdbuf();
        bStore.PutObject(request.GetKey(), data.str());
        counters.writtenSize += data.str().length();

        Aws::S3::Model::PutObjectOutcome outcome;
        Aws::S3::Model::PutObjectResult result(outcome.GetResultWithOwnership());
        result.SetETag("etag-singlepart-" + request.GetKey());
        return result;
    }

    Aws::S3::Model::GetObjectOutcome GetObject(const Aws::S3::Model::GetObjectRequest & request) const override
    {
        ++counters.getObject;

        auto & bStore = store->GetBucketStore(request.GetBucket());
        const String data = bStore.objects[request.GetKey()];

        size_t begin = 0;
        size_t end = data.size() - 1;

        const String & range = request.GetRange();
        const String prefix = "bytes=";
        if (range.starts_with(prefix))
        {
            int ret = sscanf(range.c_str(), "bytes=%zu-%zu", &begin, &end); /// NOLINT
            chassert(ret == 2);
        }

        auto factory = request.GetResponseStreamFactory();
        Aws::Utils::Stream::ResponseStream responseStream(factory);
        responseStream.GetUnderlyingStream() << std::stringstream(data.substr(begin, end - begin + 1)).rdbuf();

        Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> awsStream(std::move(responseStream), Aws::Http::HeaderValueCollection());
        Aws::S3::Model::GetObjectResult getObjectResult(std::move(awsStream));
        return Aws::S3::Model::GetObjectOutcome(std::move(getObjectResult));
    }

    Aws::S3::Model::HeadObjectOutcome HeadObject(const Aws::S3::Model::HeadObjectRequest & request) const override
    {
        attempts_seen.push_back(attemptNumberFromCustomHeaders(request));
        ++counters.headObject;

        /// The request's DYNAMIC type is still the production `DB::S3::HeadObjectRequest` wrapper --
        /// this override only sees it through the SDK base-class reference. Mirrors the dynamic_cast
        /// `Client::BuildHttpRequest` itself does, so a test can observe the mark this mock never
        /// forwards through an HTTP layer.
        if (const auto * wrapper = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request))
            last_head_object_native_conditional = wrapper->isNativeConditional();

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        auto obj = bStore.objects[request.GetKey()];
        Aws::S3::Model::HeadObjectOutcome outcome;
        Aws::S3::Model::HeadObjectResult result(outcome.GetResultWithOwnership());
        result.SetContentLength(obj.length());
        return result;
    }

    Aws::S3::Model::CreateMultipartUploadOutcome CreateMultipartUpload(const Aws::S3::Model::CreateMultipartUploadRequest & request) const override
    {
        ++counters.multiUploadCreate;

        if (const auto * wrapper = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request))
            last_create_multipart_native_conditional = wrapper->isNativeConditional();

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        auto mpu_id = bStore.CreateMPU();

        Aws::S3::Model::CreateMultipartUploadResult result;
        result.SetUploadId(mpu_id.c_str());
        return Aws::S3::Model::CreateMultipartUploadOutcome(result);
    }

    Aws::S3::Model::UploadPartOutcome UploadPart(const Aws::S3::Model::UploadPartRequest & request) const override
    {
        ++counters.uploadParts;

        if (const auto * wrapper = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request))
            last_upload_part_native_conditional = wrapper->isNativeConditional();

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        std::stringstream data;
        data << request.GetBody()->rdbuf();
        counters.writtenSize += data.str().length();

        auto & bStore = store->GetBucketStore(request.GetBucket());
        auto etag = bStore.UploadPart(request.GetUploadId(), data.str());

        Aws::S3::Model::UploadPartResult result;
        result.SetETag(etag);
        return Aws::S3::Model::UploadPartOutcome(result);
    }

    Aws::S3::Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(const Aws::S3::Model::CompleteMultipartUploadRequest & request) const override
    {
        ++counters.multiUploadComplete;

        if (const auto * wrapper = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request))
            last_complete_multipart_native_conditional = wrapper->isNativeConditional();

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());

        std::vector<std::string> etags;
        for (const auto & x: request.GetMultipartUpload().GetParts()) {
            etags.push_back(x.GetETag());
        }
        bStore.CompleteMPU(request.GetKey(), request.GetUploadId(), etags);

        Aws::S3::Model::CompleteMultipartUploadResult result;
        result.SetETag("etag-multipart-" + request.GetKey());
        return Aws::S3::Model::CompleteMultipartUploadOutcome(result);
    }

    Aws::S3::Model::AbortMultipartUploadOutcome AbortMultipartUpload(const Aws::S3::Model::AbortMultipartUploadRequest & request) const override
    {
        ++counters.multiUploadAbort;

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        bStore.AbortMPU(request.GetUploadId());

        Aws::S3::Model::AbortMultipartUploadResult result;
        return Aws::S3::Model::AbortMultipartUploadOutcome(result);
    }

    Aws::S3::Model::CopyObjectOutcome CopyObject(const Aws::S3::Model::CopyObjectRequest & request) const override
    {
        ++counters.copyObject;

        if (const auto * wrapper = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request))
            last_copy_object_native_conditional = wrapper->isNativeConditional();

        last_copy_object_if_match = request.IfMatchHasBeenSet();
        last_copy_object_if_none_match = request.IfNoneMatchHasBeenSet();

        if (injections)
        {
            if (auto opt_val = injections->call(request))
                return std::move(*opt_val);
        }

        /// CopySource is "<bucket>/<key>"; parse it back apart to look the source object up
        /// (both source and destination live in the same S3MemStrore in these tests).
        const std::string & copy_source = request.GetCopySource();
        const size_t sep = copy_source.find('/');
        chassert(sep != std::string::npos);
        const std::string src_bucket_name = copy_source.substr(0, sep);
        const std::string src_key = copy_source.substr(sep + 1);

        auto & src_store = store->GetBucketStore(src_bucket_name);
        const std::string data = src_store.objects.at(src_key);

        auto & dst_store = store->GetBucketStore(request.GetBucket());
        dst_store.PutObject(request.GetKey(), data);

        Aws::S3::Model::CopyObjectResult result;
        Aws::S3::Model::CopyObjectResultDetails details;
        details.SetETag("etag-copy-" + request.GetKey());
        result.SetCopyObjectResultDetails(details);
        return Aws::S3::Model::CopyObjectOutcome(result);
    }

    Aws::S3::Model::DeleteObjectOutcome DeleteObject(const Aws::S3::Model::DeleteObjectRequest & request) const override
    {
        attempts_seen.push_back(attemptNumberFromCustomHeaders(request));
        ++counters.deleteObject;

        if (const auto * wrapper = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request))
            last_delete_object_native_conditional = wrapper->isNativeConditional();

        if (injections)
        {
            if (auto opt_val = injections->call(request))
                return std::move(*opt_val);
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        bStore.objects.erase(request.GetKey());

        Aws::S3::Model::DeleteObjectResult result;
        return Aws::S3::Model::DeleteObjectOutcome(result);
    }

    Aws::S3::Model::GetBucketVersioningOutcome GetBucketVersioning(const Aws::S3::Model::GetBucketVersioningRequest & request) const override
    {
        ++counters.getBucketVersioning;

        if (injections)
        {
            if (auto opt_val = injections->call(request))
                return std::move(*opt_val);
        }

        Aws::S3::Model::GetBucketVersioningResult result;
        result.SetStatus(Aws::S3::Model::BucketVersioningStatus::Enabled);
        return Aws::S3::Model::GetBucketVersioningOutcome(result);
    }

    std::shared_ptr<S3MemStrore> store;
    mutable EventCounts counters;
    mutable std::shared_ptr<InjectionModel> injections;
    mutable bool last_head_object_native_conditional = false;
    mutable bool last_delete_object_native_conditional = false;
    mutable bool last_put_object_native_conditional = false;
    mutable bool last_create_multipart_native_conditional = false;
    mutable bool last_upload_part_native_conditional = false;
    mutable bool last_complete_multipart_native_conditional = false;
    mutable bool last_copy_object_native_conditional = false;
    mutable bool last_copy_object_if_match = false;
    mutable bool last_copy_object_if_none_match = false;
    void resetCounters() const { counters = {}; }
};

struct PutObjectFailIngection: InjectionModel
{
    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::VALIDATION, "FailInjection", "PutObjectFailIngection", false);
    }
};

/// A conditional-write 412, matched by `S3::isPreconditionFailedError` on the canonical `<Code>` name.
struct PutObjectPreconditionFailedIngection: InjectionModel
{
    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::UNKNOWN, "PreconditionFailed", "precondition failed", false);
    }
};

struct HeadObjectFailIngection: InjectionModel
{
    std::optional<Aws::S3::Model::HeadObjectOutcome> call(const Aws::S3::Model::HeadObjectRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::VALIDATION, "FailInjection", "HeadObjectFailIngection", false);
    }
};

struct CreateMPUFailIngection: InjectionModel
{
    std::optional<Aws::S3::Model::CreateMultipartUploadOutcome> call(const Aws::S3::Model::CreateMultipartUploadRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::VALIDATION, "FailInjection", "CreateMPUFailIngection", false);
    }
};

struct CompleteMPUFailIngection: InjectionModel
{
    std::optional<Aws::S3::Model::CompleteMultipartUploadOutcome> call(const Aws::S3::Model::CompleteMultipartUploadRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::VALIDATION, "FailInjection", "CompleteMPUFailIngection", false);
    }
};

struct UploadPartFailIngection: InjectionModel
{
    std::optional<Aws::S3::Model::UploadPartOutcome> call(const Aws::S3::Model::UploadPartRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::VALIDATION, "FailInjection", "UploadPartFailIngection", false);
    }
};

/// Injects an arbitrary AWSError<S3Errors> on DeleteObject -- used to drive the conditional-remove
/// (`removeObjectIfTokenMatches`) outcome mapping: a 412-shaped error (exception name "PreconditionFailed",
/// matched by `S3::isPreconditionFailedError`) must map to `ConditionalRemoveOutcome::TokenMismatch`, and a
/// 404-shaped error (a `NO_SUCH_KEY`/`RESOURCE_NOT_FOUND`/`NO_SUCH_BUCKET` error type, matched by
/// `S3::isNotFoundError`) must map to `ConditionalRemoveOutcome::NotFound`.
struct DeleteObjectErrorInjection: InjectionModel
{
    [[maybe_unused]] explicit DeleteObjectErrorInjection(Aws::Client::AWSError<Aws::S3::S3Errors> error_) : error(std::move(error_)) {}

    std::optional<Aws::S3::Model::DeleteObjectOutcome> call(const Aws::S3::Model::DeleteObjectRequest & /*request*/) override
    {
        return error;
    }

    Aws::Client::AWSError<Aws::S3::S3Errors> error;
};

/// Injects an arbitrary `CopyObject` error to exercise ordinary-copy fallback and native-only
/// fail-close behavior.
struct CopyObjectErrorInjection: InjectionModel
{
    [[maybe_unused]] explicit CopyObjectErrorInjection(Aws::Client::AWSError<Aws::S3::S3Errors> error_) : error(std::move(error_)) {}

    std::optional<Aws::S3::Model::CopyObjectOutcome> call(const Aws::S3::Model::CopyObjectRequest & /*request*/) override
    {
        return error;
    }

    Aws::Client::AWSError<Aws::S3::S3Errors> error;
};

struct BaseSyncPolicy
{
    virtual ~BaseSyncPolicy() = default;
    virtual DB::ThreadPoolCallbackRunnerUnsafe<void> getScheduler() { return {}; }
    virtual void execute(size_t) {}
    virtual void setAutoExecute(bool) {}

    virtual size_t size() const { return 0; }
    virtual bool empty() const { return size() == 0; }
};

struct SimpleAsyncTasks : BaseSyncPolicy
{
    bool auto_execute = false;
    std::deque<std::packaged_task<void()>> queue;

    DB::ThreadPoolCallbackRunnerUnsafe<void> getScheduler() override
    {
        return [this] (std::function<void()> && operation, size_t /*priority*/)
        {
            if (auto_execute)
            {
                auto task = std::packaged_task<void()>(std::move(operation));
                task();
                return task.get_future();
            }

            queue.emplace_back(std::move(operation));
            return queue.back().get_future();
        };
    }

    void execute(size_t limit) override
    {
        if (limit == 0)
            limit = queue.size();

        while (!queue.empty() && limit)
        {
            auto & request = queue.front();
            request();

            queue.pop_front();
            --limit;
        }
    }

    void setAutoExecute(bool value) override
    {
        auto_execute = value;
        if (auto_execute)
            execute(0);
    }

    size_t size() const override { return queue.size(); }
};

}

static void writeAsOneBlock(WriteBuffer& buf, size_t size)
{
    std::vector<char> data(size, 'a');
    buf.write(data.data(), data.size());
}

static void writeAsPieces(WriteBuffer& buf, size_t size)
{
    size_t ceil = 15ull*1024*1024*1024;
    size_t piece = 1;
    size_t written = 0;
    while (written < size) {
        size_t len = std::min({piece, size-written, ceil});
        writeAsOneBlock(buf, len);
        written += len;
        piece *= 2;
    }
}

class CASWBS3Test : public ::testing::Test
{
public:
    const String bucket = "CASWBS3Test-bucket";

    Settings & getSettings()
    {
        return settings;
    }

    MockS3::BaseSyncPolicy & getAsyncPolicy()
    {
        return *async_policy;
    }

    std::unique_ptr<WriteBufferFromS3> getWriteBuffer(String file_name = "file", const WriteSettings & write_settings = {})
    {
        S3::S3RequestSettings request_settings;
        request_settings.updateFromSettings(settings, /* if_changed */true, /* validate_settings */false);

        client->resetCounters();

        getAsyncPolicy().setAutoExecute(false);

        return std::make_unique<WriteBufferFromS3>(
                    client,
                    bucket,
                    file_name,
                    DBMS_DEFAULT_BUFFER_SIZE,
                    request_settings,
                    nullptr,
                    std::nullopt,
                    getAsyncPolicy().getScheduler(),
                    write_settings);
    }

    void setInjectionModel(std::shared_ptr<MockS3::InjectionModel> injections_)
    {
        client->setInjectionModel(injections_);
    }

    [[maybe_unused]] void runSimpleScenario(MockS3::EventCounts expected_counters, size_t size)
    {
        auto scenario = [&] (std::function<void(WriteBuffer& buf, size_t size)> writeMethod) {
            auto buffer = getWriteBuffer("file");
            writeMethod(*buffer, size);

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();

            expected_counters.writtenSize = size;
            assertCountersEQ(expected_counters);

            auto & bStore = client->store->GetBucketStore(bucket);
            auto & data = bStore.objects["file"];
            ASSERT_EQ(size, data.size());
            for (char c : data)
               ASSERT_EQ('a', c);
        };

        scenario(writeAsOneBlock);
        scenario(writeAsPieces);
    }

    void assertCountersEQ(const MockS3::EventCounts & canonical) {
        const auto & actual = client->counters;
        ASSERT_EQ(canonical.headObject, actual.headObject);
        ASSERT_EQ(canonical.getObject, actual.getObject);
        ASSERT_EQ(canonical.putObject, actual.putObject);
        ASSERT_EQ(canonical.multiUploadCreate, actual.multiUploadCreate);
        ASSERT_EQ(canonical.multiUploadComplete, actual.multiUploadComplete);
        ASSERT_EQ(canonical.multiUploadAbort, actual.multiUploadAbort);
        ASSERT_EQ(canonical.uploadParts, actual.uploadParts);
        ASSERT_EQ(canonical.writtenSize, actual.writtenSize);
    }

    [[maybe_unused]] auto getCompletedPartUploads ()
    {
         return client->store->GetBucketStore(bucket).GetCompletedPartUploads();
    }

protected:
    Settings settings;

    std::shared_ptr<MockS3::Client> client;
    std::unique_ptr<MockS3::BaseSyncPolicy> async_policy;

    void SetUp() override
    {
        client = MockS3::Client::CreateClient(bucket);
        async_policy = std::make_unique<MockS3::BaseSyncPolicy>();
    }

    void TearDown() override
    {
        client.reset();
        async_policy.reset();
    }
};

class CASSyncAsync : public CASWBS3Test, public ::testing::WithParamInterface<bool>
{
protected:
    bool test_with_pool = false;

    void SetUp() override
    {
        test_with_pool = GetParam();
        client = MockS3::Client::CreateClient(bucket);
        if (test_with_pool)
        {
            /// Do not block the main thread awaiting the others task.
            /// This test use the only one thread at all
            getSettings()[Setting::s3_max_inflight_parts_for_one_file] = 0;
            async_policy = std::make_unique<MockS3::SimpleAsyncTasks>();
        }
        else
        {
            async_policy = std::make_unique<MockS3::BaseSyncPolicy>();
        }
    }
};

/// Captures what `WriteBufferFromS3` logs at `threshold` and above (default: Error). A message
/// logged below the threshold never reaches the channel, so an empty capture proves the site logged
/// below it rather than merely that this particular text was absent.
class ScopedWriteBufferS3ErrorLogCapture
{
public:
    explicit ScopedWriteBufferS3ErrorLogCapture(const std::string & threshold = "error")
        : logger(getLogger("WriteBufferFromS3"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel(threshold);
    }

    ~ScopedWriteBufferS3ErrorLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    std::string captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream;
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// `shared=true` is load-bearing: `AutoPtr(ptr)` would steal a reference the fixture never owned.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

}

INSTANTIATE_TEST_SUITE_P(CASWBS3
    , CASSyncAsync
    , ::testing::Values(true, false)
    , [] (const ::testing::TestParamInfo<CASSyncAsync::ParamType>& info_param) {
        std::string name = info_param.param ? "async" : "sync";
        return name;
  });

/// A non-412 `PutObject` failure on the ordinary (Default) retry profile is a genuine error: the
/// client's one attempt IS the final answer, so the site logs it at Error.
TEST_P(CASSyncAsync, PutObjectErrorLogsErrorForDefaultProfile)
{
    setInjectionModel(std::make_shared<MockS3::PutObjectFailIngection>());

    ScopedWriteBufferS3ErrorLogCapture log_capture;
    EXPECT_THROW({
        auto buffer = getWriteBuffer("put_object_error_default_profile");
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }, DB::S3Exception);

    EXPECT_THAT(log_capture.captured(), testing::HasSubstr("S3Exception name FailInjection"));
    EXPECT_THAT(log_capture.captured(), testing::HasSubstr("PutObjectFailIngection"));
}

/// The same failure on the SingleAttempt profile (the CAS conditional-write client) is owned by an
/// outer retry loop that resolves the outcome and reissues; the one failed attempt is not terminal,
/// so nothing here reaches Error.
TEST_P(CASSyncAsync, PutObjectErrorLogsDebugForSingleAttemptProfile)
{
    setInjectionModel(std::make_shared<MockS3::PutObjectFailIngection>());

    WriteSettings write_settings;
    write_settings.object_storage_retry_profile = ObjectStorageRetryProfile::SingleAttempt;

    ScopedWriteBufferS3ErrorLogCapture log_capture;
    EXPECT_THROW({
        auto buffer = getWriteBuffer("put_object_error_single_attempt_profile", write_settings);
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }, DB::S3Exception);

    EXPECT_TRUE(log_capture.captured().empty());
}

/// A conditional write losing its precondition (412) is the caller's expected answer, handled one
/// frame up -- it says nothing to the operator, so it must stay below Information, independent of the
/// retry profile. The capture threshold is Information so that an Info-level line from the site would
/// be caught; the cancel path logs its own Info lines, so the assertion is on the site's text, not on
/// an empty capture.
TEST_P(CASSyncAsync, PreconditionFailedNeverLogsAtError)
{
    setInjectionModel(std::make_shared<MockS3::PutObjectPreconditionFailedIngection>());

    ScopedWriteBufferS3ErrorLogCapture log_capture("information");
    EXPECT_THROW({
        auto buffer = getWriteBuffer("put_object_precondition_failed");
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }, DB::S3Exception);

    EXPECT_THAT(log_capture.captured(), testing::Not(testing::HasSubstr("S3Exception name")));
}

TEST_F(CASWBS3Test, S3RequestAttemptSeedPutHeadDeleteCarryTheSeed)
{
    WriteSettings write_settings;
    write_settings.object_storage_attempt_number = 3;
    client->attempts_seen.clear();
    {
        auto buffer = getWriteBuffer("seeded_put", write_settings);
        buffer->write('A');
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }
    ASSERT_FALSE(client->attempts_seen.empty());
    EXPECT_EQ(client->attempts_seen.front(), 3u);
    /// Seed 0 adds no header at all (the spec's rule for every verb but the read path).
    client->attempts_seen.clear();
    {
        auto buffer = getWriteBuffer("unseeded_put");
        buffer->write('A');
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }
    ASSERT_EQ(client->attempts_seen.size(), 1u);
    EXPECT_FALSE(client->attempts_seen.front().has_value());

    /// The native HEAD's seed: `S3ObjectStorage::tryGetObjectMetadataWithNativeToken`'s profile-aware
    /// overload now forwards `request.attempt_number`, like every other verb here; this exercises the
    /// seed-carrying layer directly -- `S3::getObjectInfoIfExists`, the same call
    /// `tryGetObjectMetadataImpl` makes.
    client->attempts_seen.clear();
    S3::getObjectInfoIfExists(*client, bucket, "seeded_head", /*version_id=*/{}, /*with_metadata=*/false,
                               /*with_tags=*/false, ObjectStorageRequestMode::Default, /*attempt_seed=*/4);
    ASSERT_EQ(client->attempts_seen.size(), 1u);
    EXPECT_EQ(client->attempts_seen.front(), 4u);
    client->attempts_seen.clear();
    S3::getObjectInfoIfExists(*client, bucket, "unseeded_head");
    ASSERT_EQ(client->attempts_seen.size(), 1u);
    EXPECT_FALSE(client->attempts_seen.front().has_value());

    /// Conditional (single) and bulk DELETE: reachable now through `S3ObjectStorage`'s
    /// `ObjectStorageControlRequest`-carrying overloads, which is what actually drives
    /// `removeObjectIfTokenMatchesImpl`/`removeObjectsIfExistImpl` with a real nonzero seed, through the
    /// object storage's own API rather than a lower-level free function.
    (void)getContext(); // BlobStorageLogWriter::create falls back to the global context
    auto delete_store = std::make_shared<MockS3::S3MemStrore>();
    delete_store->CreateBucket(bucket);
    auto owned_delete_client = std::make_unique<MockS3::Client>(delete_store);
    MockS3::Client * delete_client = owned_delete_client.get();
    S3::URI delete_uri;
    delete_uri.bucket = bucket;
    auto delete_object_storage = std::make_shared<S3ObjectStorage>(
        std::move(owned_delete_client),
        std::make_unique<S3Settings>(),
        delete_uri,
        S3Capabilities{},
        ObjectStorageKeyGeneratorPtr{},
        "seed-delete-disk");

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectIfTokenMatches(StoredObject("unseeded-delete-key"), "etag-1");
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_FALSE(delete_client->attempts_seen.front().has_value());

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectIfTokenMatches(
        StoredObject("seeded-delete-key"), "etag-1", ObjectStorageControlRequest{.attempt_number = 3});
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_EQ(delete_client->attempts_seen.front(), 3u);

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectsIfExistUnderProfile({StoredObject("unseeded-bulk-key")}, ObjectStorageControlRequest{});
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_FALSE(delete_client->attempts_seen.front().has_value());

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectsIfExistUnderProfile(
        {StoredObject("seeded-bulk-key")}, ObjectStorageControlRequest{.attempt_number = 3});
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_EQ(delete_client->attempts_seen.front(), 3u);
}

TEST_F(CASWBS3Test, S3RequestAttemptSeedListPagesCarryTheSeed)
{
    /// Drives the seed through the public `iterate` overload a real caller (the CAS backend's LIST
    /// primitive) uses, rather than the anonymous-namespace `S3IteratorAsync` directly -- that class is
    /// an implementation detail of `S3ObjectStorage.cpp` and not reachable from a test in this file.
    auto list_store = std::make_shared<MockS3::S3MemStrore>();
    list_store->CreateBucket(bucket);
    auto owned_list_client = std::make_unique<MockS3::Client>(list_store);
    MockS3::Client * list_client = owned_list_client.get();
    S3::URI list_uri;
    list_uri.bucket = bucket;
    auto list_object_storage = std::make_shared<S3ObjectStorage>(
        std::move(owned_list_client),
        std::make_unique<S3Settings>(),
        list_uri,
        S3Capabilities{},
        ObjectStorageKeyGeneratorPtr{},
        "seed-list-disk");

    auto & bucket_store = list_store->GetBucketStore(bucket);
    for (int i = 0; i < 5; ++i)
        bucket_store.PutObject(fmt::format("p/{}", i), "x");

    /// Profile is left at Default (not SingleAttempt): that would route through
    /// `clientForRetryProfile`'s single-attempt clone, whose `cloneWithConfigurationOverride` the mock
    /// client does not override, and the test would stop exercising the mock entirely.
    list_client->attempts_seen.clear();
    auto iterator = list_object_storage->iterate(
        "p/", /*max_keys=*/2, /*with_tags=*/false, std::optional<std::string>("p/0"),
        ObjectStorageControlRequest{.attempt_number = 2});
    size_t seen = 0;
    for (; iterator->isValid(); iterator->next())
        ++seen;
    EXPECT_EQ(seen, 4u);
    ASSERT_EQ(list_client->attempts_seen.size(), 2u);   /// the initial page and one rebuilt page
    EXPECT_EQ(list_client->attempts_seen[0], 2u);
    EXPECT_EQ(list_client->attempts_seen[1], 2u);

    /// Seed 0 adds no header on either page.
    list_client->attempts_seen.clear();
    auto unseeded_iterator = list_object_storage->iterate(
        "p/", /*max_keys=*/2, /*with_tags=*/false, std::optional<std::string>("p/0"), ObjectStorageControlRequest{});
    seen = 0;
    for (; unseeded_iterator->isValid(); unseeded_iterator->next())
        ++seen;
    EXPECT_EQ(seen, 4u);
    ASSERT_EQ(list_client->attempts_seen.size(), 2u);
    EXPECT_FALSE(list_client->attempts_seen[0].has_value());
    EXPECT_FALSE(list_client->attempts_seen[1].has_value());
}

#endif

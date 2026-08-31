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
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>

#include <IO/WriteBufferFromS3.h>
#include <IO/S3Common.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/AsyncReadCounters.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadSettings.h>
#include <IO/S3/Client.h>
#include <IO/S3/copyS3File.h>

#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>

#include <Common/filesystemHelpers.h>
#include <Core/Settings.h>


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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int S3_ERROR;
}

}

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
    using Metadata = std::map<std::string, std::string>;


    std::map<Key, Data> objects;
    /// Custom object metadata (`x-amz-meta-*`), stored alongside the object and served by HeadObject.
    std::map<Key, Metadata> object_metadata;
    std::map<MPU_ID, MPUPartsInProgress> multiPartUploads;
    /// Metadata of an in-flight upload, carried from CreateMultipartUpload onto the completed object
    /// the way real S3 does -- that is what makes a HEAD after completion see it.
    std::map<MPU_ID, Metadata> multiPartUploadMetadata;
    std::vector<std::pair<MPU_ID, MPUParts>> CompletedPartUploads;

    Sequencer sequencer;

    std::string CreateMPU(const Metadata & metadata = {})
    {
        auto id = sequencer.next_id();
        multiPartUploads.emplace(id, MPUPartsInProgress{});
        multiPartUploadMetadata.emplace(id, metadata);
        return id;
    }

    std::string UploadPart(const std::string & upload_id, const std::string & part)
    {
        auto etag = sequencer.next_id();
        auto & parts = multiPartUploads.at(upload_id);
        parts.emplace(etag, part);
        return etag;
    }

    void PutObject(const std::string & key, const std::string & data, const Metadata & metadata = {})
    {
        objects[key] = data;
        object_metadata[key] = metadata;
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
        if (auto it = multiPartUploadMetadata.find(upload_id); it != multiPartUploadMetadata.end())
            object_metadata[key] = it->second;
        multiPartUploads.erase(upload_id);
        multiPartUploadMetadata.erase(upload_id);
    }

    void AbortMPU(const std::string & upload_id)
    {
        multiPartUploads.erase(upload_id);
        multiPartUploadMetadata.erase(upload_id);
    }


    const std::vector<std::pair<MPU_ID, MPUParts>> & GetCompletedPartUploads() const
    {
        return CompletedPartUploads;
    }

    static std::vector<size_t> GetPartSizes(const MPUParts & parts)
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
    size_t copyObject = 0;
    size_t uploadPartCopy = 0;
    size_t writtenSize = 0;

    size_t totalRequestsCount() const
    {
        return headObject + getObject + putObject + multiUploadCreate + multiUploadComplete + uploadParts;
    }
};

struct Client;

/// Read a request body the way the AWS SDK does: block reads of `content_length` bytes via
/// istream::read (which routes to streambuf::xsgetn). `data << body->rdbuf()` instead reads
/// char-by-char through sbumpc/uflow, which needs a streambuf get area -- StdStreamBufFromReadBuffer
/// (used by the copyS3File body path) implements only xsgetn/underflow and leaves the get area empty,
/// so the rdbuf() form segfaults on it. Reading by content length works for every body stream.
inline std::string readRequestBody(const std::shared_ptr<Aws::IOStream> & body, size_t content_length)
{
    std::string data;
    data.resize(content_length);
    body->read(data.data(), static_cast<std::streamsize>(content_length));
    data.resize(static_cast<size_t>(body->gcount()));
    return data;
}

/// A CopyObject / UploadPartCopy `CopySource` has the form "bucket/key".
inline std::pair<std::string, std::string> splitCopySource(const std::string & copy_source)
{
    auto slash = copy_source.find('/');
    chassert(slash != std::string::npos);
    return {copy_source.substr(0, slash), copy_source.substr(slash + 1)};
}

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
#undef DeclareInjectCall
};

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
        auto configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
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
        /// createClientConfiguration leaves retryStrategy unset; ClientFactory::create() normally
        /// fills it in. This mock builds DB::S3::Client directly, bypassing the factory, so replicate
        /// that here -- otherwise chassert(client_configuration.retryStrategy) in Client::doRequest
        /// aborts every request in debug/sanitizer builds.
        configuration.retryStrategy = std::make_shared<DB::S3::Client::RetryStrategy>(configuration.retry_strategy);
        return configuration;
    }

    void setInjectionModel(std::shared_ptr<MockS3::InjectionModel> injections_)
    {
        injections = injections_;
    }

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest & request) const override
    {
        ++counters.putObject;

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return *opt_val;
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        const std::string data = readRequestBody(request.GetBody(), request.GetContentLength());
        BucketMemStore::Metadata metadata;
        for (const auto & [name, value] : request.GetMetadata())
            metadata[name] = value;
        bStore.PutObject(request.GetKey(), data, metadata);
        counters.writtenSize += data.length();

        Aws::S3::Model::PutObjectOutcome outcome;
        Aws::S3::Model::PutObjectResult result(outcome.GetResultWithOwnership());
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
        ++counters.headObject;

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
        if (auto it = bStore.object_metadata.find(request.GetKey()); it != bStore.object_metadata.end())
        {
            Aws::Map<Aws::String, Aws::String> metadata;
            for (const auto & [name, value] : it->second)
                metadata[name] = value;
            result.SetMetadata(std::move(metadata));
        }
        return result;
    }

    Aws::S3::Model::CreateMultipartUploadOutcome CreateMultipartUpload(const Aws::S3::Model::CreateMultipartUploadRequest & request) const override
    {
        ++counters.multiUploadCreate;

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        BucketMemStore::Metadata metadata;
        for (const auto & [name, value] : request.GetMetadata())
            metadata[name] = value;
        auto mpu_id = bStore.CreateMPU(metadata);

        Aws::S3::Model::CreateMultipartUploadResult result;
        result.SetUploadId(mpu_id.c_str());
        return Aws::S3::Model::CreateMultipartUploadOutcome(result);
    }

    Aws::S3::Model::UploadPartOutcome UploadPart(const Aws::S3::Model::UploadPartRequest & request) const override
    {
        ++counters.uploadParts;

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        const std::string data = readRequestBody(request.GetBody(), request.GetContentLength());
        counters.writtenSize += data.length();

        auto & bStore = store->GetBucketStore(request.GetBucket());
        auto etag = bStore.UploadPart(request.GetUploadId(), data);

        Aws::S3::Model::UploadPartResult result;
        result.SetETag(etag);
        return Aws::S3::Model::UploadPartOutcome(result);
    }

    Aws::S3::Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(const Aws::S3::Model::CompleteMultipartUploadRequest & request) const override
    {
        ++counters.multiUploadComplete;

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

    /// Whole-object server-side copy. A CopyObject request carries no byte range, so it always copies the
    /// entire source object -- modelling the real S3 behaviour that makes it unsafe for a partial range.
    Aws::S3::Model::CopyObjectOutcome CopyObject(const Aws::S3::Model::CopyObjectRequest & request) const override
    {
        ++counters.copyObject;

        const auto [src_bucket, src_key] = splitCopySource(request.GetCopySource());
        const String & src_data = store->GetBucketStore(src_bucket).objects[src_key];
        store->GetBucketStore(request.GetBucket()).PutObject(request.GetKey(), src_data);

        Aws::S3::Model::CopyObjectResult result;
        return Aws::S3::Model::CopyObjectOutcome(result);
    }

    /// Ranged server-side copy of one multipart part. Honours the `CopySourceRange` so only the requested
    /// bytes are copied -- this is the path a partial-range copy must take.
    Aws::S3::Model::UploadPartCopyOutcome UploadPartCopy(const Aws::S3::Model::UploadPartCopyRequest & request) const override
    {
        ++counters.uploadPartCopy;

        const auto [src_bucket, src_key] = splitCopySource(request.GetCopySource());
        const String & src_data = store->GetBucketStore(src_bucket).objects[src_key];

        size_t begin = 0;
        size_t end = src_data.size() - 1;
        const String & range = request.GetCopySourceRange();
        if (const String prefix = "bytes="; range.starts_with(prefix))
        {
            int ret = sscanf(range.c_str(), "bytes=%zu-%zu", &begin, &end); /// NOLINT
            chassert(ret == 2);
        }

        auto & dstStore = store->GetBucketStore(request.GetBucket());
        auto etag = dstStore.UploadPart(request.GetUploadId(), src_data.substr(begin, end - begin + 1));

        Aws::S3::Model::CopyPartResult copy_part_result;
        copy_part_result.SetETag(etag);
        Aws::S3::Model::UploadPartCopyResult result;
        result.SetCopyPartResult(copy_part_result);
        return Aws::S3::Model::UploadPartCopyOutcome(result);
    }

    std::shared_ptr<S3MemStrore> store;
    mutable EventCounts counters;
    mutable std::shared_ptr<InjectionModel> injections;
    void resetCounters() const { counters = {}; }
};

struct PutObjectFailIngection: InjectionModel
{
    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::VALIDATION, "FailInjection", "PutObjectFailIngection", false);
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

/// Fails the first `fail_times` CompleteMultipartUpload calls with the un-typed MinIO `InvalidPart`
/// eventual-consistency error, then lets the real mock store handle the rest. The AWS SDK cannot map
/// <Code>InvalidPart</Code> to a typed model error, so it produces UNKNOWN as the error type and keeps
/// the raw code only in the exception name -- exactly the shape WriteBufferFromS3 must recognise to
/// retry (see AWSErrorMarshaller::Marshall).
struct CompleteMPUInvalidPartOnceIngection : InjectionModel
{
    explicit CompleteMPUInvalidPartOnceIngection(size_t fail_times_) : fail_times(fail_times_) {}

    std::optional<Aws::S3::Model::CompleteMultipartUploadOutcome> call(const Aws::S3::Model::CompleteMultipartUploadRequest & /*request*/) override
    {
        if (calls++ >= fail_times)
            return std::nullopt;
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(
            Aws::Client::CoreErrors::UNKNOWN,
            "InvalidPart",
            "One or more of the specified parts could not be found. The part may not have been uploaded, "
            "or the specified entity tag may not match the part's entity tag.",
            false);
    }

    size_t fail_times;
    size_t calls = 0;
};

/// `PreconditionFailed` as the SDK actually produces it: 412 carries an <Code>PreconditionFailed</Code>
/// that has no typed S3 model error, so AWSErrorMarshaller yields UNKNOWN and keeps the raw code in the
/// exception name only -- the shape WriteBufferFromS3 must recognise.
inline Aws::Client::AWSError<Aws::Client::CoreErrors> makePreconditionFailedError()
{
    return Aws::Client::AWSError<Aws::Client::CoreErrors>(
        Aws::Client::CoreErrors::UNKNOWN,
        "PreconditionFailed",
        "At least one of the pre-conditions you specified did not hold",
        false);
}

/// Replays the lost-response scenario for a conditional (`If-None-Match: *`) PutObject: the first
/// attempt lands the object server-side but its response is lost, reported as the bogus MinIO
/// NO_SUCH_KEY that WriteBufferFromS3 retries; the replay then sees the object it just wrote and gets
/// 412. Records the metadata of every request so a test can assert what was stamped.
struct PutObjectLostResponseThenPreconditionFailed : InjectionModel
{
    PutObjectLostResponseThenPreconditionFailed(std::shared_ptr<S3MemStrore> store_, bool store_first_attempt_)
        : store(std::move(store_)), store_first_attempt(store_first_attempt_) {}

    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & request) override
    {
        EXPECT_FALSE(request.GetIfNoneMatch().empty());

        BucketMemStore::Metadata metadata;
        for (const auto & [name, value] : request.GetMetadata())
            metadata[name] = value;
        seen_metadata.push_back(metadata);

        if (calls++ > 0)
            return makePreconditionFailedError();

        if (store_first_attempt)
        {
            const std::string data = readRequestBody(request.GetBody(), request.GetContentLength());
            store->GetBucketStore(request.GetBucket()).PutObject(request.GetKey(), data, metadata);
        }

        return Aws::Client::AWSError<Aws::S3::S3Errors>(
            Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "The specified key does not exist.", false);
    }

    std::shared_ptr<S3MemStrore> store;
    bool store_first_attempt;
    size_t calls = 0;
    std::vector<BucketMemStore::Metadata> seen_metadata;
};

/// Every PutObject attempt fails with 412 -- a genuinely pre-existing object, written by somebody
/// else. Records the metadata and the `If-None-Match` of every request; this injection serves both
/// the conditional and the unconditional arms, so each asserts the header it expects.
struct PutObjectPreconditionFailedInjection : InjectionModel
{
    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & request) override
    {
        BucketMemStore::Metadata metadata;
        for (const auto & [name, value] : request.GetMetadata())
            metadata[name] = value;
        seen_metadata.push_back(metadata);
        seen_if_none_match.push_back(request.GetIfNoneMatch());
        return makePreconditionFailedError();
    }

    std::vector<BucketMemStore::Metadata> seen_metadata;
    std::vector<std::string> seen_if_none_match;
};

/// A conditional PutObject that gets 412 while the HEAD used to verify the write token also fails.
/// The write must report the original 412, never succeed on an unverifiable object.
struct PutObjectPreconditionFailedAndHeadFailsInjection : InjectionModel
{
    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & request) override
    {
        EXPECT_FALSE(request.GetIfNoneMatch().empty());
        return makePreconditionFailedError();
    }

    std::optional<Aws::S3::Model::HeadObjectOutcome> call(const Aws::S3::Model::HeadObjectRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(
            Aws::Client::CoreErrors::VALIDATION, "FailInjection", "HeadObjectFailIngection", false);
    }
};

/// Replays the lost-response scenario for a conditional CompleteMultipartUpload: the first attempt
/// completes the upload server-side but its response is lost (reported as the MinIO NO_SUCH_KEY that
/// WriteBufferFromS3 retries), so the replay sees the object it just wrote and gets 412.
struct CompleteMPULostResponseThenPreconditionFailed : InjectionModel
{
    explicit CompleteMPULostResponseThenPreconditionFailed(std::shared_ptr<S3MemStrore> store_)
        : store(std::move(store_)) {}

    std::optional<Aws::S3::Model::CompleteMultipartUploadOutcome> call(
        const Aws::S3::Model::CompleteMultipartUploadRequest & request) override
    {
        EXPECT_FALSE(request.GetIfNoneMatch().empty());

        if (calls++ > 0)
            return makePreconditionFailedError();

        std::vector<std::string> etags;
        for (const auto & part : request.GetMultipartUpload().GetParts())
            etags.push_back(part.GetETag());
        store->GetBucketStore(request.GetBucket()).CompleteMPU(request.GetKey(), request.GetUploadId(), etags);

        return Aws::Client::AWSError<Aws::S3::S3Errors>(
            Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "The specified key does not exist.", false);
    }

    std::shared_ptr<S3MemStrore> store;
    size_t calls = 0;
};

/// Every conditional CompleteMultipartUpload attempt fails with 412 -- a genuinely pre-existing
/// object. Records the metadata CreateMultipartUpload stamped so a test can assert the token.
struct CompleteMPUPreconditionFailedInjection : InjectionModel
{
    std::optional<Aws::S3::Model::CreateMultipartUploadOutcome> call(
        const Aws::S3::Model::CreateMultipartUploadRequest & request) override
    {
        BucketMemStore::Metadata metadata;
        for (const auto & [name, value] : request.GetMetadata())
            metadata[name] = value;
        seen_create_metadata.push_back(metadata);
        return std::nullopt;
    }

    std::optional<Aws::S3::Model::CompleteMultipartUploadOutcome> call(
        const Aws::S3::Model::CompleteMultipartUploadRequest & request) override
    {
        EXPECT_FALSE(request.GetIfNoneMatch().empty());
        return makePreconditionFailedError();
    }

    std::vector<BucketMemStore::Metadata> seen_create_metadata;
};

/// Reports `NO_SUCH_UPLOAD` on every CompleteMultipartUpload, optionally completing the upload
/// server-side first -- the shape of an upload id the server has already consumed. Records the
/// `If-None-Match` of every attempt because this injection serves conditional and unconditional arms.
struct CompleteMPUNoSuchUploadInjection : InjectionModel
{
    CompleteMPUNoSuchUploadInjection(std::shared_ptr<S3MemStrore> store_, bool complete_first_attempt_)
        : store(std::move(store_)), complete_first_attempt(complete_first_attempt_) {}

    std::optional<Aws::S3::Model::CompleteMultipartUploadOutcome> call(
        const Aws::S3::Model::CompleteMultipartUploadRequest & request) override
    {
        seen_if_none_match.push_back(request.GetIfNoneMatch());

        if (complete_first_attempt && calls == 0)
        {
            std::vector<std::string> etags;
            for (const auto & part : request.GetMultipartUpload().GetParts())
                etags.push_back(part.GetETag());
            store->GetBucketStore(request.GetBucket()).CompleteMPU(request.GetKey(), request.GetUploadId(), etags);
        }
        ++calls;

        return Aws::Client::AWSError<Aws::S3::S3Errors>(
            Aws::S3::S3Errors::NO_SUCH_UPLOAD,
            "NoSuchUpload",
            "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
            false);
    }

    std::shared_ptr<S3MemStrore> store;
    bool complete_first_attempt;
    size_t calls = 0;
    std::vector<std::string> seen_if_none_match;
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

using namespace DB;

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

class WBS3Test : public ::testing::Test
{
public:
    const String bucket = "WBS3Test-bucket";

    Settings & getSettings()
    {
        return settings;
    }

    MockS3::BaseSyncPolicy & getAsyncPolicy()
    {
        return *async_policy;
    }

    std::unique_ptr<WriteBufferFromS3> getWriteBuffer(
        String file_name = "file",
        const WriteSettings & write_settings = {},
        std::optional<ObjectAttributes> object_metadata = std::nullopt)
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
                    std::move(object_metadata),
                    getAsyncPolicy().getScheduler(),
                    write_settings);
    }

    /// The Iceberg conditional create-if-absent write: `If-None-Match: *`.
    static WriteSettings conditionalCreateWriteSettings()
    {
        WriteSettings write_settings;
        write_settings.object_storage_write_if_none_match = "*";
        return write_settings;
    }

    /// The Iceberg conditional replace-this-version write: `If-Match: <etag>`, no token minted.
    static WriteSettings conditionalReplaceWriteSettings()
    {
        WriteSettings write_settings;
        write_settings.object_storage_write_if_match = "some-etag";
        return write_settings;
    }

    void setInjectionModel(std::shared_ptr<MockS3::InjectionModel> injections_)
    {
        client->setInjectionModel(injections_);
    }

    void runSimpleScenario(MockS3::EventCounts expected_counters, size_t size)
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

    auto getCompletedPartUploads ()
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

class SyncAsync : public WBS3Test, public ::testing::WithParamInterface<bool>
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

INSTANTIATE_TEST_SUITE_P(WBS3
    , SyncAsync
    , ::testing::Values(true, false)
    , [] (const ::testing::TestParamInfo<SyncAsync::ParamType>& info_param) {
        std::string name = info_param.param ? "async" : "sync";
        return name;
  });

TEST_P(SyncAsync, ExceptionOnHead) {
    setInjectionModel(std::make_shared<MockS3::HeadObjectFailIngection>());

    getSettings()[Setting::s3_check_objects_after_upload] = true;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_head_1");
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("Immediately after upload:"));
            throw;
        }
    }, DB::S3Exception);
}

TEST_P(SyncAsync, ExceptionOnPut) {
    setInjectionModel(std::make_shared<MockS3::PutObjectFailIngection>());

    EXPECT_THROW({
        try
        {
            auto buffer = getWriteBuffer("exception_on_put_1");
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("PutObjectFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_put_2");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("PutObjectFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_put_3");
            buffer->write('A');
            getAsyncPolicy().setAutoExecute(true);
            buffer->preFinalize();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("PutObjectFailIngection"));
            throw;
        }
      }, DB::S3Exception);

}

TEST_P(SyncAsync, ExceptionOnCreateMPU) {
    setInjectionModel(std::make_shared<MockS3::CreateMPUFailIngection>());

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts ara ok

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_create_mpu_1");
            buffer->write('A');
            buffer->next();
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CreateMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_create_mpu_2");
            buffer->write('A');
            buffer->preFinalize();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CreateMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_create_mpu_2");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CreateMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);
}


TEST_P(SyncAsync, ExceptionOnCompleteMPU) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPUFailIngection>());

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts ara ok

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_complete_mpu_1");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CompleteMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);
}

/// A conditional (`If-None-Match: *`) PutObject replayed after it already succeeded must report
/// success, not the spurious `PreconditionFailed` the replay gets back.
TEST_P(SyncAsync, SinglepartConditionalPutRetryAfterLostResponse) {
    auto injection = std::make_shared<MockS3::PutObjectLostResponseThenPreconditionFailed>(
        client->store, /* store_first_attempt= */ true);
    setInjectionModel(injection);

    auto buffer = getWriteBuffer("conditional_put_lost_response", conditionalCreateWriteSettings());
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    /// Two PUT attempts (NO_SUCH_KEY then 412) and one HEAD that verified our own write token.
    EXPECT_EQ(client->counters.putObject, 2u);
    EXPECT_EQ(client->counters.headObject, 1u);

    auto & bStore = client->store->GetBucketStore(bucket);
    EXPECT_EQ(bStore.objects["conditional_put_lost_response"], "A");

    /// Both attempts carried the same token, and it is the one stored with the object.
    ASSERT_EQ(injection->seen_metadata.size(), 2u);
    const auto token = injection->seen_metadata[0].at("clickhouse-write-token");
    EXPECT_FALSE(token.empty());
    EXPECT_EQ(injection->seen_metadata[1].at("clickhouse-write-token"), token);
    EXPECT_EQ(bStore.object_metadata["conditional_put_lost_response"].at("clickhouse-write-token"), token);
}

/// A 412 caused by an object this request did NOT write must still fail. The pre-existing object is
/// byte-identical to the payload on purpose, so a byte or size comparison would wrongly accept it.
TEST_P(SyncAsync, SinglepartConditionalPutDoesNotMaskForeignObject) {
    auto & bStore = client->store->GetBucketStore(bucket);
    bStore.PutObject("conditional_put_foreign", "1", {{"clickhouse-write-token", "written-by-somebody-else"}});

    auto injection = std::make_shared<MockS3::PutObjectPreconditionFailedInjection>();
    setInjectionModel(injection);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("conditional_put_foreign", conditionalCreateWriteSettings());
            buffer->write('1');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            /// The thrown message carries the 412 text; the exception name is logged, not rethrown.
            EXPECT_THAT(e.what(), testing::HasSubstr("pre-conditions you specified did not hold"));
            throw;
        }
      }, DB::S3Exception);

    /// The foreign object is untouched, and the PUT really was conditional.
    EXPECT_EQ(bStore.objects["conditional_put_foreign"], "1");
    EXPECT_EQ(bStore.object_metadata["conditional_put_foreign"].at("clickhouse-write-token"), "written-by-somebody-else");
    ASSERT_FALSE(injection->seen_if_none_match.empty());
    EXPECT_EQ(injection->seen_if_none_match[0], "*");
}

/// An ordinary (unconditional) S3 write is untouched: no token is stamped on the request, and a 412 is
/// still thrown. Proves the `object_storage_write_if_none_match` guard is load-bearing.
TEST_P(SyncAsync, SinglepartPutWithoutIfNoneMatchStillThrows) {
    auto injection = std::make_shared<MockS3::PutObjectPreconditionFailedInjection>();
    setInjectionModel(injection);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("unconditional_put_412");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            /// The thrown message carries the 412 text; the exception name is logged, not rethrown.
            EXPECT_THAT(e.what(), testing::HasSubstr("pre-conditions you specified did not hold"));
            throw;
        }
      }, DB::S3Exception);

    /// The request was not conditional, no token was stamped, and no HEAD looked one up.
    ASSERT_FALSE(injection->seen_metadata.empty());
    for (const auto & metadata : injection->seen_metadata)
        EXPECT_FALSE(metadata.contains("clickhouse-write-token"));
    for (const auto & if_none_match : injection->seen_if_none_match)
        EXPECT_TRUE(if_none_match.empty());
    EXPECT_EQ(client->counters.headObject, 0u);
}

/// A caller-supplied `object_metadata` must survive next to the write token -- the token is merged in,
/// never substituted for the caller's map.
TEST_P(SyncAsync, SinglepartConditionalPutKeepsCallerMetadata) {
    auto injection = std::make_shared<MockS3::PutObjectLostResponseThenPreconditionFailed>(
        client->store, /* store_first_attempt= */ true);
    setInjectionModel(injection);

    auto buffer = getWriteBuffer(
        "conditional_put_caller_metadata", conditionalCreateWriteSettings(), ObjectAttributes{{"caller-key", "caller-value"}});
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    ASSERT_FALSE(injection->seen_metadata.empty());
    EXPECT_EQ(injection->seen_metadata[0].at("caller-key"), "caller-value");
    EXPECT_FALSE(injection->seen_metadata[0].at("clickhouse-write-token").empty());
}

/// A 412 must not be accepted on an object carrying no token at all -- a pre-Fix or non-ClickHouse
/// writer produced it, so this is a genuine conflict.
TEST_P(SyncAsync, SinglepartConditionalPutDoesNotMaskUntokenedObject) {
    auto & bStore = client->store->GetBucketStore(bucket);
    bStore.PutObject("conditional_put_untokened", "1", {});

    auto injection = std::make_shared<MockS3::PutObjectPreconditionFailedInjection>();
    setInjectionModel(injection);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("conditional_put_untokened", conditionalCreateWriteSettings());
            buffer->write('1');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("pre-conditions you specified did not hold"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_EQ(bStore.objects["conditional_put_untokened"], "1");
    EXPECT_TRUE(bStore.object_metadata["conditional_put_untokened"].empty());
    ASSERT_FALSE(injection->seen_if_none_match.empty());
    EXPECT_EQ(injection->seen_if_none_match[0], "*");
}

/// A 412 with no object stored at all (a pathological server) must throw: the guard proves our own
/// write, it never infers one from the status code.
TEST_P(SyncAsync, SinglepartConditionalPutDoesNotMaskAbsentObject) {
    auto injection = std::make_shared<MockS3::PutObjectPreconditionFailedInjection>();
    setInjectionModel(injection);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("conditional_put_absent", conditionalCreateWriteSettings());
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("pre-conditions you specified did not hold"));
            throw;
        }
      }, DB::S3Exception);

    /// The guard was consulted and found nothing to match against, so the payload never landed.
    EXPECT_EQ(client->counters.headObject, 1u);
    EXPECT_TRUE(client->store->GetBucketStore(bucket).objects["conditional_put_absent"].empty());
}

/// When the verifying HEAD itself fails the write must still report the original 412, not the HEAD
/// error and not success.
TEST_P(SyncAsync, SinglepartConditionalPutThrowsWhenHeadFails) {
    setInjectionModel(std::make_shared<MockS3::PutObjectPreconditionFailedAndHeadFailsInjection>());

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("conditional_put_head_fails", conditionalCreateWriteSettings());
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("pre-conditions you specified did not hold"));
            EXPECT_THAT(e.what(), testing::Not(testing::HasSubstr("HeadObjectFailIngection")));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_GE(client->counters.headObject, 1u);
}

/// The multipart carrier of the same defect: with `s3_max_single_part_upload_size = 0` a conditional
/// Iceberg create takes the multipart path, whose CompleteMultipartUpload sets the same
/// `If-None-Match` and is likewise replayed on a lost response. The token is stamped on
/// CreateMultipartUpload and lands on the completed object.
TEST_P(SyncAsync, MultipartConditionalCompleteRetryAfterLostResponse) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPULostResponseThenPreconditionFailed>(client->store));

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force the multipart path
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    auto buffer = getWriteBuffer("conditional_mpu_lost_response", conditionalCreateWriteSettings());
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    EXPECT_EQ(client->counters.multiUploadComplete, 2u);
    EXPECT_EQ(client->counters.headObject, 1u);
    EXPECT_EQ(client->counters.multiUploadAbort, 0u);

    auto & bStore = client->store->GetBucketStore(bucket);
    EXPECT_EQ(bStore.objects["conditional_mpu_lost_response"], "A");
    EXPECT_FALSE(bStore.object_metadata["conditional_mpu_lost_response"].at("clickhouse-write-token").empty());
}

/// The multipart twin of the foreign-object arm: a 412 on a completion whose object somebody else
/// wrote must still fail. The pre-existing object is byte-identical on purpose.
TEST_P(SyncAsync, MultipartConditionalCompleteDoesNotMaskForeignObject) {
    auto & bStore = client->store->GetBucketStore(bucket);
    bStore.PutObject("conditional_mpu_foreign", "A", {{"clickhouse-write-token", "written-by-somebody-else"}});

    auto injection = std::make_shared<MockS3::CompleteMPUPreconditionFailedInjection>();
    setInjectionModel(injection);

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force the multipart path
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("conditional_mpu_foreign", conditionalCreateWriteSettings());
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("pre-conditions you specified did not hold"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_EQ(bStore.objects["conditional_mpu_foreign"], "A");
    EXPECT_EQ(bStore.object_metadata["conditional_mpu_foreign"].at("clickhouse-write-token"), "written-by-somebody-else");

    /// CreateMultipartUpload carried a token, so the guard had something to compare and rejected it.
    ASSERT_FALSE(injection->seen_create_metadata.empty());
    EXPECT_FALSE(injection->seen_create_metadata[0].at("clickhouse-write-token").empty());
}

/// The other door into the same replay: a completion that already landed can come back as
/// `NO_SUCH_UPLOAD` (consumed upload id) instead of 412. On our own object that is still success.
TEST_P(SyncAsync, MultipartConditionalCompleteRecoversNoSuchUploadOnOwnObject) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPUNoSuchUploadInjection>(
        client->store, /* complete_first_attempt= */ true));

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force the multipart path
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    auto buffer = getWriteBuffer("conditional_mpu_no_such_upload", conditionalCreateWriteSettings());
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    /// The token was consulted rather than existence assumed, and the completed upload is not aborted.
    EXPECT_GE(client->counters.headObject, 1u);
    EXPECT_EQ(client->counters.multiUploadAbort, 0u);

    auto & bStore = client->store->GetBucketStore(bucket);
    EXPECT_EQ(bStore.objects["conditional_mpu_no_such_upload"], "A");
    EXPECT_FALSE(bStore.object_metadata["conditional_mpu_no_such_upload"].at("clickhouse-write-token").empty());
}

/// The same `NO_SUCH_UPLOAD` over an object somebody else wrote must still fail: existence at the key
/// is not authorship, and reporting success would let a conditional create silently lose its payload.
TEST_P(SyncAsync, MultipartConditionalCompleteDoesNotMaskForeignObjectOnNoSuchUpload) {
    auto & bStore = client->store->GetBucketStore(bucket);
    bStore.PutObject("conditional_mpu_no_such_upload_foreign", "A", {{"clickhouse-write-token", "written-by-somebody-else"}});

    auto injection = std::make_shared<MockS3::CompleteMPUNoSuchUploadInjection>(
        client->store, /* complete_first_attempt= */ false);
    setInjectionModel(injection);

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force the multipart path
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("conditional_mpu_no_such_upload_foreign", conditionalCreateWriteSettings());
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("The specified upload does not exist"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_EQ(bStore.objects["conditional_mpu_no_such_upload_foreign"], "A");
    EXPECT_EQ(
        bStore.object_metadata["conditional_mpu_no_such_upload_foreign"].at("clickhouse-write-token"),
        "written-by-somebody-else");
    ASSERT_FALSE(injection->seen_if_none_match.empty());
    EXPECT_EQ(injection->seen_if_none_match[0], "*");
}

/// `If-Match` is conditional too, and it mints no write token, so nothing can prove authorship: the
/// existence-only recovery must not fire there either.
TEST_P(SyncAsync, MultipartIfMatchCompleteDoesNotRecoverNoSuchUpload) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPUNoSuchUploadInjection>(
        client->store, /* complete_first_attempt= */ true));

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force the multipart path
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("conditional_mpu_if_match", conditionalReplaceWriteSettings());
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("The specified upload does not exist"));
            throw;
        }
      }, DB::S3Exception);
}

/// An unconditional completion keeps the existing recover-if-the-object-exists behaviour, which backs
/// copyS3File and the disk write paths: the conditional gate must not change them.
TEST_P(SyncAsync, MultipartUnconditionalCompleteStillRecoversNoSuchUpload) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPUNoSuchUploadInjection>(
        client->store, /* complete_first_attempt= */ true));

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force the multipart path
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    auto buffer = getWriteBuffer("unconditional_mpu_no_such_upload");
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    /// Recovered by the wrapper's own HEAD, with no token to look up.
    EXPECT_GE(client->counters.headObject, 1u);
    EXPECT_EQ(client->counters.multiUploadAbort, 0u);

    auto & bStore = client->store->GetBucketStore(bucket);
    EXPECT_EQ(bStore.objects["unconditional_mpu_no_such_upload"], "A");
    EXPECT_FALSE(bStore.object_metadata["unconditional_mpu_no_such_upload"].contains("clickhouse-write-token"));
}

/// A transient MinIO `InvalidPart` on CompleteMultipartUpload must be retried, not surfaced as a
/// hard failure. Regression test for the `Code: 499 ... InvalidPart` flake at hits_s3 fixture load.
/// The injection fails the first completion attempt with `InvalidPart` (UNKNOWN type, name only),
/// then succeeds; the write must finalize and store the object. Without the retry-predicate fix in
/// WriteBufferFromS3::completeMultipartUpload the first failure is thrown straight through and this
/// test fails.
TEST_P(SyncAsync, CompleteMPURetriesInvalidPart) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPUInvalidPartOnceIngection>(/* fail_times= */ 1));

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts are ok

    auto buffer = getWriteBuffer("complete_mpu_invalid_part_retry");
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    /// The completion was attempted twice: once failing with InvalidPart, once succeeding.
    EXPECT_EQ(client->counters.multiUploadComplete, 2u);
    EXPECT_EQ(client->counters.multiUploadAbort, 0u);

    auto & bStore = client->store->GetBucketStore(bucket);
    EXPECT_EQ(bStore.objects["complete_mpu_invalid_part_retry"].size(), 1u);
}

/// The same transient MinIO `InvalidPart` on CompleteMultipartUpload must also be retried by the
/// copyDataToS3File / copyS3File helper path (UploadHelper::completeMultipartUpload), which backs
/// MinIO-backed backups and DiskObjectStorage server-side copies. Injects `InvalidPart` on the first
/// completion attempt, then succeeds; the copy must finalize and store the object. Without the shared
/// retry predicate in UploadHelper::completeMultipartUpload the first failure is thrown straight
/// through and this test fails.
TEST_F(WBS3Test, CopyDataToS3FileRetriesInvalidPart) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPUInvalidPartOnceIngection>(/* fail_times= */ 1));

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force multipart
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts are ok
    getSettings()[Setting::s3_check_objects_after_upload] = false;

    S3::S3RequestSettings request_settings;
    request_settings.updateFromSettings(settings, /* if_changed */ true, /* validate_settings */ false);

    client->resetCounters();

    const String payload = "copy_invalid_part_payload";
    auto create_read_buffer = [&]() -> std::unique_ptr<SeekableReadBuffer>
    {
        return std::make_unique<ReadBufferFromOwnString>(payload);
    };

    /// Empty schedule => the multipart upload (and completion) runs synchronously on this thread.
    copyDataToS3File(
        create_read_buffer,
        /* offset= */ 0,
        /* size= */ payload.size(),
        client,
        bucket,
        "copy_data_invalid_part_retry",
        request_settings,
        /* blob_storage_log= */ nullptr,
        /* schedule= */ {},
        /* object_metadata= */ std::nullopt);

    /// The completion was attempted twice: once failing with InvalidPart, once succeeding.
    EXPECT_EQ(client->counters.multiUploadComplete, 2u);
    EXPECT_EQ(client->counters.multiUploadAbort, 0u);

    auto & bStore = client->store->GetBucketStore(bucket);
    EXPECT_EQ(bStore.objects["copy_data_invalid_part_retry"].size(), payload.size());
}

/// copyS3File routing between whole-object CopyObject and ranged UploadPartCopy. A small copy would take
/// CopyObject, which carries no byte range and copies the ENTIRE source; a partial-range copy must therefore
/// force UploadPartCopy, which sets a CopySourceRange per part -- but only when S3 would accept the source as
/// a byte-range copy source (it must be greater than 5 MB), otherwise the range is read through buffers.
class CopyS3FileRoutingTest : public WBS3Test
{
protected:
    /// S3 rejects a byte-range copy source of 5 MB or less, so tests need sources on both sides of it.
    static constexpr size_t min_source_size_for_range_copy = 5 * 1024 * 1024;

    /// A source object with position-dependent bytes, so a wrong (whole-object) copy is detectable both by
    /// size and by content.
    String putSource(const String & key, size_t size)
    {
        String data;
        data.reserve(size);
        for (size_t i = 0; i < size; ++i)
            data += static_cast<char>('0' + (i % 10));
        client->store->GetBucketStore(bucket).PutObject(key, data);
        return data;
    }

    S3::S3RequestSettings makeRequestSettings()
    {
        getSettings()[Setting::s3_check_objects_after_upload] = false;
        S3::S3RequestSettings request_settings;
        request_settings.updateFromSettings(settings, /* if_changed */ true, /* validate_settings */ false);
        return request_settings;
    }

    CreateReadBuffer wholeSourceReader(const String & src_key)
    {
        return [this, src_key]() -> std::unique_ptr<SeekableReadBuffer>
        {
            return std::make_unique<ReadBufferFromOwnString>(client->store->GetBucketStore(bucket).objects[src_key]);
        };
    }

    void runWholeCopy(const String & src_key, size_t size, const String & dst_key)
    {
        auto request_settings = makeRequestSettings();
        client->resetCounters();
        copyS3File(
            client, bucket, src_key, size,
            /* dest_s3_client= */ client, bucket, dst_key,
            request_settings, ReadSettings{},
            /* blob_storage_log= */ nullptr, /* schedule= */ {},
            wholeSourceReader(src_key));
    }

    void runRangeCopy(const String & src_key, size_t offset, size_t size, size_t src_object_size, const String & dst_key)
    {
        auto request_settings = makeRequestSettings();
        client->resetCounters();
        copyS3FileRange(
            client, bucket, src_key, offset, size, src_object_size,
            /* dest_s3_client= */ client, bucket, dst_key,
            request_settings, ReadSettings{},
            /* blob_storage_log= */ nullptr, /* schedule= */ {},
            wholeSourceReader(src_key));
    }
};

TEST_F(CopyS3FileRoutingTest, WholeObjectUsesCopyObject)
{
    const String source = putSource("src", /* size= */ 100);
    runWholeCopy("src", /* size= */ source.size(), "dst");

    EXPECT_EQ(client->counters.copyObject, 1u);
    EXPECT_EQ(client->counters.uploadPartCopy, 0u);
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], source);
}

/// A source above the 5 MB threshold can be range-copied server-side, so UploadPartCopy is used. The
/// sub-range content check discriminates: a wrong whole-object copy would copy the entire source.
TEST_F(CopyS3FileRoutingTest, RangedCopyOfLargeSourceUsesUploadPartCopy)
{
    const size_t source_size = min_source_size_for_range_copy + 1024;
    const String source = putSource("src", source_size);
    runRangeCopy("src", /* offset= */ 10, /* size= */ 20, source_size, "dst");

    EXPECT_EQ(client->counters.copyObject, 0u);
    EXPECT_GT(client->counters.uploadPartCopy, 0u);
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], source.substr(10, 20));
}

/// A prefix range [0, n) with n < full size is still a range: starting at offset 0 must NOT make it a
/// whole-object copy, which would copy the entire source instead of the first 20 bytes.
TEST_F(CopyS3FileRoutingTest, PrefixRangeOfLargeSourceUsesUploadPartCopy)
{
    const size_t source_size = min_source_size_for_range_copy + 1024;
    const String source = putSource("src", source_size);
    runRangeCopy("src", /* offset= */ 0, /* size= */ 20, source_size, "dst");

    EXPECT_EQ(client->counters.copyObject, 0u);
    EXPECT_GT(client->counters.uploadPartCopy, 0u);
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], source.substr(0, 20));
}

/// S3 rejects a byte-range copy source of 5 MB or less (InvalidRequest), so such a range must be read through
/// buffers up front -- no server-side copy of either kind may be issued.
TEST_F(CopyS3FileRoutingTest, RangedCopyOfSmallSourceUsesBuffers)
{
    const String source = putSource("src", /* size= */ 100);
    runRangeCopy("src", /* offset= */ 10, /* size= */ 20, source.size(), "dst");

    EXPECT_EQ(client->counters.uploadPartCopy, 0u);
    EXPECT_EQ(client->counters.copyObject, 0u);
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], source.substr(10, 20));
}

TEST_P(SyncAsync, ExceptionOnUploadPart) {
    setInjectionModel(std::make_shared<MockS3::UploadPartFailIngection>());

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts ara ok

    MockS3::EventCounts counters = {.multiUploadCreate = 1, .multiUploadAbort = 1};

    counters.uploadParts = 2;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_1");

            buffer->write('A');
            buffer->next();
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);

            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_2");
            getAsyncPolicy().setAutoExecute(true);

            buffer->write('A');
            buffer->next();

            buffer->write('A');
            buffer->next();

            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    counters.uploadParts = 1;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_3");
            buffer->write('A');

            buffer->preFinalize();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_4");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);
}


TEST_F(WBS3Test, PrefinalizeCalledMultipleTimes) {
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test trigger LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("prefinalize_called_multiple_times");
            buffer->write('A');
            buffer->next();
            buffer->preFinalize();
            buffer->write('A');
            buffer->next();
            buffer->preFinalize();
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::LOGICAL_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("write to prefinalized buffer for S3"));
            throw;
        }
    }, DB::Exception);
#endif
}

TEST_P(SyncAsync, EmptyFile) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};
    runSimpleScenario(counters, 0);
}

TEST_P(SyncAsync, ManualNextCalls) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        auto buffer = getWriteBuffer("manual_next_calls_1");
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
    }

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        auto buffer = getWriteBuffer("manual_next_calls_2");
        buffer->next();
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
    }

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1, .writtenSize = 1};

        auto buffer = getWriteBuffer("manual_next_calls_3");
        buffer->next();
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
    }

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1, .writtenSize = 2};

        auto buffer = getWriteBuffer("manual_next_calls_4");
        buffer->write('A');
        buffer->next();
        buffer->write('A');
        buffer->next();
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
     }
}

TEST_P(SyncAsync, SmallFileIsOnePutRequest) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 1000;
        getSettings()[Setting::s3_min_upload_part_size] = 10;

        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        runSimpleScenario(counters, 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] - 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size]);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] / 2);
    }

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 1000;

        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        runSimpleScenario(counters, 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] - 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size]);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] / 2);
    }
}

TEST_P(SyncAsync, LittleBiggerFileIsMultiPartUpload) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 1000;
        getSettings()[Setting::s3_min_upload_part_size] = 10;

        MockS3::EventCounts counters = {.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 2};
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 1);

        counters.uploadParts = 101;
        runSimpleScenario(counters, 2 * settings[Setting::s3_max_single_part_upload_size]);
    }

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 1000;

        MockS3::EventCounts counters = {.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 1};

        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 1);
        runSimpleScenario(counters, 2 * settings[Setting::s3_max_single_part_upload_size]);
        runSimpleScenario(counters, settings[Setting::s3_min_upload_part_size] - 1);
        runSimpleScenario(counters, settings[Setting::s3_min_upload_part_size]);
    }
}

TEST_P(SyncAsync, BiggerFileIsMultiPartUpload) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 1000;
        getSettings()[Setting::s3_min_upload_part_size] = 10;

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 2};
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size]);

        counters.uploadParts = 3;
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size] + 1);
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 2 * settings[Setting::s3_min_upload_part_size] - 1);
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 2 * settings[Setting::s3_min_upload_part_size]);
    }


    {
        // but not in that case, when s3_min_upload_part_size > s3_max_single_part_upload_size
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 1000;

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 2};
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size]);
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size] + 1);
        runSimpleScenario(counters, 2 * settings[Setting::s3_min_upload_part_size] - 1);
        runSimpleScenario(counters, 2 * settings[Setting::s3_min_upload_part_size]);

        counters.uploadParts = 3;
        runSimpleScenario(counters, 2 * settings[Setting::s3_min_upload_part_size] + 1);
    }
}

TEST_P(SyncAsync, IncreaseUploadBuffer) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 10;
        getSettings()[Setting::s3_upload_part_size_multiply_parts_count_threshold] = 1;
        // parts: 10 20 40 80  160
        // size:  10 30 70 150 310

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 6};
        runSimpleScenario(counters, 350);

        auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
        ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(10, 20, 40, 80, 160, 40));
    }

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 10;
        getSettings()[Setting::s3_upload_part_size_multiply_parts_count_threshold] = 2;
        getSettings()[Setting::s3_upload_part_size_multiply_factor] = 3;
        // parts: 10 10 30 30 90
        // size:  10 20 50 80 170

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 6};
        runSimpleScenario(counters, 190);

        auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
        ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(10, 10, 30, 30, 90, 20));
    }
}

TEST_P(SyncAsync, IncreaseLimited) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 10;
        getSettings()[Setting::s3_upload_part_size_multiply_parts_count_threshold] = 1;
        getSettings()[Setting::s3_max_upload_part_size] = 45;
        // parts: 10 20 40 45  45  45
        // size:  10 30 70 115 160 205

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 7};
        runSimpleScenario(counters, 220);

        auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
        ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(10, 20, 40, 45, 45, 45, 15));
    }
}

TEST_P(SyncAsync, StrictUploadPartSize) {
    getSettings()[Setting::s3_check_objects_after_upload] = false;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_strict_upload_part_size] = 11;

        {
            auto counters = MockS3::EventCounts{.multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 6};
            runSimpleScenario(counters, 66);

            auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
            ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(11, 11, 11, 11, 11, 11));

            // parts: 11 22 33 44 55 66
            // size:  11 11 11 11 11 11
        }

        {
            auto counters = MockS3::EventCounts{.multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 7};
            runSimpleScenario(counters, 67);

            auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
            ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(11, 11, 11, 11, 11, 11, 1));
        }
    }
}

[[maybe_unused]] static String fillStringWithPattern(String pattern, int n)
{
    String data;
    for (int i = 0; i < n; ++i)
    {
        data += pattern;
    }
    return data;
}

#endif

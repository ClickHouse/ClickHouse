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
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <Poco/Net/HTTPBasicStreamBuf.h>

#include <algorithm>
#include <array>

#include <IO/WriteBufferFromS3.h>
#include <IO/S3Common.h>
#include <IO/S3/Requests.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/AsyncReadCounters.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadSettings.h>
#include <IO/S3/Client.h>
#include <IO/S3/copyS3File.h>

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>

#include <Common/filesystemHelpers.h>
#include <Common/Crypto/OpenSSLInitializer.h>
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
    extern const SettingsString s3_upload_checksum_algorithm;
    extern const SettingsUInt64 s3_upload_part_size_multiply_factor;
    extern const SettingsUInt64 s3_upload_part_size_multiply_parts_count_threshold;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsString upload_checksum_algorithm;
}

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int LOGICAL_ERROR;
    extern const int S3_ERROR;
    extern const int S3_OBJECT_CHANGED_DURING_READ;
    extern const int FILE_CHANGED_DURING_READ;
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
    /// The `ETag` of the generation at each key, a new one for every write to the key, served by
    /// HeadObject and checked by the copies against `x-amz-copy-source-if-match`. Quoted, as S3 quotes it.
    std::map<Key, ETag> object_etags;
    /// The versions of a key a versioned bucket keeps, by version id: a copy that addresses the
    /// source as `key?versionId=...` copies the one it names, whatever is at the key now.
    std::map<Key, std::map<std::string, Data>> object_versions;
    /// The `If-None-Match` header of every PutObject that reached the store, empty for an
    /// unconditional one, so a test can assert that a write was a create-if-absent one.
    std::vector<std::string> put_if_none_match;
    /// The `If-Match` of every `DeleteObject` that reached the store (the `ETag` element of every
    /// object of a `DeleteObjects`), empty for an unconditional one, so a test can assert that a
    /// delete was pinned to a generation.
    std::vector<std::string> delete_if_match;
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

    /// Returns the `ETag` of the generation the write created, the way the response to the write
    /// reports it.
    ETag PutObject(const std::string & key, const std::string & data, const Metadata & metadata = {})
    {
        objects[key] = data;
        object_metadata[key] = metadata;
        return object_etags[key] = "\"" + sequencer.next_id() + "\"";
    }

    /// Keeps what is at `key` now as version `version_id`, the way a versioned bucket keeps every
    /// generation a write creates; the current version stays what it is.
    void RecordVersion(const std::string & key, const std::string & version_id)
    {
        object_versions[key][version_id] = objects.at(key);
    }

    /// A delete as S3 evaluates it on general purpose buckets: `If-Match` is compared with the
    /// generation that is at the key now, and nothing is deleted when it does not hold. Returns
    /// whether the precondition held (a delete without one always holds, of a missing key too).
    bool DeleteObject(const std::string & key, const std::string & if_match)
    {
        delete_if_match.push_back(if_match);
        if (!if_match.empty())
        {
            auto it = object_etags.find(key);
            if (it == object_etags.end() || it->second != if_match)
                return false;
        }
        objects.erase(key);
        object_metadata.erase(key);
        object_etags.erase(key);
        return true;
    }

    /// Returns the `ETag` of the completed object, the way the response to `CompleteMultipartUpload` reports it.
    ETag CompleteMPU(const std::string & key, const std::string & upload_id, const std::vector<std::string> & etags)
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
        const ETag etag = object_etags[key] = "\"" + sequencer.next_id() + "\"";
        if (auto it = multiPartUploadMetadata.find(upload_id); it != multiPartUploadMetadata.end())
            object_metadata[key] = it->second;
        multiPartUploads.erase(upload_id);
        multiPartUploadMetadata.erase(upload_id);
        return etag;
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
    size_t deleteObject = 0;
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

inline Aws::Client::AWSError<Aws::Client::CoreErrors> makePreconditionFailedError();

/// A CopyObject / UploadPartCopy `CopySource` has the form "bucket/key", or "bucket/key?versionId=..."
/// for a version of the source other than the current one. The version is not part of the key.
inline std::pair<std::string, std::string> splitCopySource(const std::string & copy_source)
{
    auto slash = copy_source.find('/');
    chassert(slash != std::string::npos);
    const auto version = copy_source.find("?versionId=", slash);
    const auto key_end = version == std::string::npos ? copy_source.size() : version;
    return {copy_source.substr(0, slash), copy_source.substr(slash + 1, key_end - slash - 1)};
}

/// The version a `CopySource` names, or empty for the current one.
inline std::string copySourceVersionId(const std::string & copy_source)
{
    static const std::string marker = "?versionId=";
    const auto version = copy_source.find(marker);
    return version == std::string::npos ? std::string{} : copy_source.substr(version + marker.size());
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
    DeclareInjectCall(DeleteObject)
    DeclareInjectCall(CopyObject)
#undef DeclareInjectCall
};

struct Client : DB::S3::Client
{
    /// `DB::S3::Client` derives the provider from the endpoint, so a test that depends on the provider
    /// selects it by passing an endpoint here. The default is empty, which deduces `ProviderType::UNKNOWN`.
    static constexpr std::string_view gcs_endpoint = "https://storage.googleapis.com";

    explicit Client(
        std::shared_ptr<S3MemStrore> mock_s3_store,
        bool is_s3express_bucket = false,
        std::string_view endpoint = {})
        : DB::S3::Client(
            100,
            DB::S3::ServerSideEncryptionKMSConfig(),
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("", ""),
            GetClientConfiguration(endpoint),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            DB::S3::ClientSettings{
                .use_virtual_addressing = true,
                .gcs_issue_compose_request = false,
                .is_s3express_bucket = is_s3express_bucket,
            })
        , store(mock_s3_store)
    {}

    static std::shared_ptr<Client> CreateClient(
        String bucket = "mock-s3-bucket",
        bool is_s3express_bucket = false,
        std::string_view endpoint = {})
    {
        auto s3store = std::make_shared<S3MemStrore>();
        s3store->CreateBucket(bucket);
        return std::make_shared<Client>(s3store, is_s3express_bucket, endpoint);
    }

    static DB::S3::PocoHTTPClientConfiguration GetClientConfiguration(std::string_view endpoint = {})
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
        if (!endpoint.empty())
            configuration.endpointOverride = String(endpoint);
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
        bStore.put_if_none_match.push_back(request.GetIfNoneMatch());
        /// `If-None-Match: *` as a real endpoint evaluates it: the write is refused with
        /// `412 Precondition Failed` when a generation is at the key.
        if (request.GetIfNoneMatch() == "*" && bStore.object_etags.contains(request.GetKey()))
            return makePreconditionFailedError();

        const std::string data = readRequestBody(request.GetBody(), request.GetContentLength());
        BucketMemStore::Metadata metadata;
        for (const auto & [name, value] : request.GetMetadata())
            metadata[name] = value;
        const auto etag = bStore.PutObject(request.GetKey(), data, metadata);
        counters.writtenSize += data.length();

        /// The `ETag` of the generation the write created, as S3 reports it in the response.
        Aws::S3::Model::PutObjectResult result;
        result.SetETag(etag);
        return Aws::S3::Model::PutObjectOutcome(std::move(result));
    }

    /// The body of a GetObject the way `ReadBufferFromIStream` reads it: it reads from the stream
    /// buffer of a real HTTP response directly, and requires it to be a `Poco::Net::HTTPBasicStreamBuf`,
    /// so a body served over a `std::stringstream` cannot be read by a `ReadBufferFromS3`.
    class HTTPBodyStreamBuf : public Poco::Net::HTTPBasicStreamBuf
    {
    public:
        explicit HTTPBodyStreamBuf(String data_)
            : Poco::Net::HTTPBasicStreamBuf(1024, std::ios::in)
            , data(std::move(data_))
        {
        }

    private:
        int readFromDevice(char * buffer, std::streamsize length) override
        {
            const size_t n = std::min<size_t>(static_cast<size_t>(length), data.size() - pos);
            memcpy(buffer, data.data() + pos, n);
            pos += n;
            return static_cast<int>(n);
        }

        String data;
        size_t pos = 0;
    };

    /// The stream buffer is a base rather than a member so that it is constructed before the stream
    /// that is initialised with it.
    struct HTTPBodyStreamBufHolder
    {
        explicit HTTPBodyStreamBufHolder(String data) : buf(std::move(data)) {}
        HTTPBodyStreamBuf buf;
    };

    class HTTPBodyStream : private HTTPBodyStreamBufHolder, public Aws::IOStream
    {
    public:
        explicit HTTPBodyStream(String data)
            : HTTPBodyStreamBufHolder(std::move(data))
            , Aws::IOStream(&buf)
        {
        }
    };

    /// `If-Match`, as a real endpoint evaluates it: against the generation that is at the key now, with
    /// `412 Precondition Failed` when it does not hold. `ReadBufferFromS3` tells that refusal apart by
    /// the response code, so it is set here.
    Aws::S3::Model::GetObjectOutcome GetObject(const Aws::S3::Model::GetObjectRequest & request) const override
    {
        ++counters.getObject;

        auto & bStore = store->GetBucketStore(request.GetBucket());
        if (const auto & if_match = request.GetIfMatch(); !if_match.empty())
        {
            auto it = bStore.object_etags.find(request.GetKey());
            if (it == bStore.object_etags.end() || it->second != if_match)
            {
                auto error = makePreconditionFailedError();
                error.SetResponseCode(Aws::Http::HttpResponseCode::PRECONDITION_FAILED);
                return error;
            }
        }
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

        const String body = data.substr(begin, end - begin + 1);
        Aws::Utils::Stream::ResponseStream responseStream(Aws::New<HTTPBodyStream>("MockS3::GetObject", body));

        Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> awsStream(std::move(responseStream), Aws::Http::HeaderValueCollection());
        Aws::S3::Model::GetObjectResult getObjectResult(std::move(awsStream));
        getObjectResult.SetContentLength(static_cast<Int64>(body.size()));
        if (auto it = bStore.object_etags.find(request.GetKey()); it != bStore.object_etags.end())
            getObjectResult.SetETag(it->second);
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
        if (auto it = bStore.object_etags.find(request.GetKey()); it != bStore.object_etags.end())
            result.SetETag(it->second);
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
        const auto etag = bStore.CompleteMPU(request.GetKey(), request.GetUploadId(), etags);

        Aws::S3::Model::CompleteMultipartUploadResult result;
        result.SetETag(etag);
        return Aws::S3::Model::CompleteMultipartUploadOutcome(std::move(result));
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
    /// `x-amz-copy-source-if-match`, as a real endpoint evaluates it: against the generation that is at
    /// the source key now, with `412 Precondition Failed` when it does not hold. The SDK produces that
    /// error without a typed model code, see `makePreconditionFailedError`.
    std::optional<Aws::Client::AWSError<Aws::Client::CoreErrors>> copySourcePreconditionFailure(
        const std::string & src_bucket, const std::string & src_key, const Aws::String & if_match) const
    {
        if (if_match.empty())
            return std::nullopt;
        copy_source_if_match_headers.push_back(if_match);
        const auto & etags = store->GetBucketStore(src_bucket).object_etags;
        if (auto it = etags.find(src_key); it != etags.end() && it->second == if_match)
            return std::nullopt;
        return makePreconditionFailedError();
    }

    /// `If-Match` on a `DELETE`, as S3 evaluates it on general purpose buckets: against the generation
    /// that is at the key now, with `412 Precondition Failed` and nothing deleted when it does not hold.
    /// `deleteFileFromS3` tells that refusal apart by the response code, so it is set here.
    Aws::S3::Model::DeleteObjectOutcome DeleteObject(const Aws::S3::Model::DeleteObjectRequest & request) const override
    {
        ++counters.deleteObject;

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        auto & bStore = store->GetBucketStore(request.GetBucket());
        if (!bStore.DeleteObject(request.GetKey(), request.GetIfMatch()))
        {
            auto error = makePreconditionFailedError();
            error.SetResponseCode(Aws::Http::HttpResponseCode::PRECONDITION_FAILED);
            return error;
        }

        Aws::S3::Model::DeleteObjectResult result;
        return Aws::S3::Model::DeleteObjectOutcome(result);
    }

    /// The same for a batch: every object is evaluated on its own, the ones whose precondition held
    /// are deleted and reported as such, the others are reported with the `PreconditionFailed` code
    /// in the `Error` element of the response, the way S3 does.
    Aws::S3::Model::DeleteObjectsOutcome DeleteObjects(const Aws::S3::Model::DeleteObjectsRequest & request) const override
    {
        auto & bStore = store->GetBucketStore(request.GetBucket());
        Aws::S3::Model::DeleteObjectsResult result;
        for (const auto & object : request.GetDelete().GetObjects())
        {
            ++counters.deleteObject;
            if (bStore.DeleteObject(object.GetKey(), object.GetETag()))
            {
                Aws::S3::Model::DeletedObject deleted;
                deleted.SetKey(object.GetKey());
                result.AddDeleted(std::move(deleted));
            }
            else
            {
                Aws::S3::Model::Error error;
                error.SetKey(object.GetKey());
                error.SetCode("PreconditionFailed");
                error.SetMessage("At least one of the pre-conditions you specified did not hold");
                result.AddErrors(std::move(error));
            }
        }
        return Aws::S3::Model::DeleteObjectsOutcome(std::move(result));
    }

    Aws::S3::Model::CopyObjectOutcome CopyObject(const Aws::S3::Model::CopyObjectRequest & request) const override
    {
        ++counters.copyObject;

        if (injections)
        {
            if (auto opt_val = injections->call(request))
            {
                return std::move(*opt_val);
            }
        }

        const auto [src_bucket, src_key] = splitCopySource(request.GetCopySource());
        if (auto refused = copySourcePreconditionFailure(src_bucket, src_key, request.GetCopySourceIfMatch()))
            return *refused;
        const String & src_data = copySourceData(src_bucket, src_key, copySourceVersionId(request.GetCopySource()));
        const auto etag = store->GetBucketStore(request.GetBucket()).PutObject(request.GetKey(), src_data);

        /// The `ETag` of the generation the copy created, in the `CopyObjectResult` element of the response.
        Aws::S3::Model::CopyObjectResultDetails details;
        details.SetETag(etag);
        Aws::S3::Model::CopyObjectResult result;
        result.SetCopyObjectResultDetails(std::move(details));
        return Aws::S3::Model::CopyObjectOutcome(std::move(result));
    }

    /// Ranged server-side copy of one multipart part. Honours the `CopySourceRange` so only the requested
    /// bytes are copied -- this is the path a partial-range copy must take.
    Aws::S3::Model::UploadPartCopyOutcome UploadPartCopy(const Aws::S3::Model::UploadPartCopyRequest & request) const override
    {
        ++counters.uploadPartCopy;

        const auto [src_bucket, src_key] = splitCopySource(request.GetCopySource());
        if (auto refused = copySourcePreconditionFailure(src_bucket, src_key, request.GetCopySourceIfMatch()))
            return *refused;
        const String & src_data = copySourceData(src_bucket, src_key, copySourceVersionId(request.GetCopySource()));

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

    /// The bytes a copy source names: the version it selects, as a versioned bucket serves it, or
    /// what is at the key now. Every version a copy names is recorded in `copy_source_version_ids`.
    const String & copySourceData(const std::string & src_bucket, const std::string & src_key, const std::string & version_id) const
    {
        auto & bucket_store = store->GetBucketStore(src_bucket);
        if (version_id.empty())
            return bucket_store.objects[src_key];
        copy_source_version_ids.push_back(version_id);
        return bucket_store.object_versions.at(src_key).at(version_id);
    }

    std::shared_ptr<S3MemStrore> store;
    mutable EventCounts counters;
    /// Every non-empty `x-amz-copy-source-if-match` a CopyObject or UploadPartCopy carried.
    mutable std::vector<std::string> copy_source_if_match_headers;
    /// Every `?versionId=` a CopyObject or UploadPartCopy source named.
    mutable std::vector<std::string> copy_source_version_ids;
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

struct ChecksumRecordingInjection : InjectionModel
{
    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & request) override
    {
        put_object_algorithm = request.GetChecksumAlgorithm();
        put_object_request_checksum_required = request.RequestChecksumRequired();
        put_object_should_compute_content_md5 = request.ShouldComputeContentMd5();
        return std::nullopt;
    }

    std::optional<Aws::S3::Model::CreateMultipartUploadOutcome> call(const Aws::S3::Model::CreateMultipartUploadRequest & request) override
    {
        create_multipart_upload_algorithm = request.GetChecksumAlgorithm();
        return std::nullopt;
    }

    std::optional<Aws::S3::Model::UploadPartOutcome> call(const Aws::S3::Model::UploadPartRequest & request) override
    {
        upload_part_algorithms.push_back(request.GetChecksumAlgorithm());
        upload_part_crc32_checksums.push_back(request.GetChecksumCRC32());
        upload_part_sha256_checksums.push_back(request.GetChecksumSHA256());
        return std::nullopt;
    }

    std::optional<Aws::S3::Model::CompleteMultipartUploadOutcome> call(const Aws::S3::Model::CompleteMultipartUploadRequest & request) override
    {
        for (const auto & part : request.GetMultipartUpload().GetParts())
        {
            complete_part_crc32_checksums.push_back(part.GetChecksumCRC32());
            complete_part_sha256_checksums.push_back(part.GetChecksumSHA256());
        }
        return std::nullopt;
    }

    Aws::S3::Model::ChecksumAlgorithm put_object_algorithm = Aws::S3::Model::ChecksumAlgorithm::NOT_SET;
    bool put_object_request_checksum_required = false;
    bool put_object_should_compute_content_md5 = true;
    Aws::S3::Model::ChecksumAlgorithm create_multipart_upload_algorithm = Aws::S3::Model::ChecksumAlgorithm::NOT_SET;
    std::vector<Aws::S3::Model::ChecksumAlgorithm> upload_part_algorithms;
    std::vector<String> upload_part_crc32_checksums;
    std::vector<String> upload_part_sha256_checksums;
    std::vector<String> complete_part_crc32_checksums;
    std::vector<String> complete_part_sha256_checksums;
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

TEST_P(SyncAsync, UploadChecksumAlgorithmSHA256Multipart)
{
    auto injection = std::make_shared<MockS3::ChecksumRecordingInjection>();
    setInjectionModel(injection);

    getSettings()[Setting::s3_upload_checksum_algorithm] = "SHA256";
    getSettings()[Setting::s3_max_single_part_upload_size] = 0;
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    auto buffer = getWriteBuffer("checksum_sha256_multipart");
    writeAsOneBlock(*buffer, 10);

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    ASSERT_EQ(Aws::S3::Model::ChecksumAlgorithm::SHA256, injection->create_multipart_upload_algorithm);
    ASSERT_THAT(injection->upload_part_algorithms, testing::Not(testing::IsEmpty()));
    ASSERT_THAT(injection->upload_part_algorithms, testing::Each(Aws::S3::Model::ChecksumAlgorithm::SHA256));
    ASSERT_EQ(injection->upload_part_sha256_checksums, injection->complete_part_sha256_checksums);
    ASSERT_THAT(injection->complete_part_sha256_checksums, testing::Each(testing::Not(testing::IsEmpty())));
}

TEST_P(SyncAsync, UploadChecksumAlgorithmCRC32Singlepart)
{
    auto injection = std::make_shared<MockS3::ChecksumRecordingInjection>();
    setInjectionModel(injection);

    getSettings()[Setting::s3_upload_checksum_algorithm] = "CRC32";

    auto buffer = getWriteBuffer("checksum_crc32_singlepart");
    writeAsOneBlock(*buffer, 10);

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    ASSERT_EQ(Aws::S3::Model::ChecksumAlgorithm::CRC32, injection->put_object_algorithm);
}

TEST_F(WBS3Test, CopyDataUploadChecksumAlgorithmCRC32Multipart)
{
    auto injection = std::make_shared<MockS3::ChecksumRecordingInjection>();
    setInjectionModel(injection);

    getSettings()[Setting::s3_upload_checksum_algorithm] = "CRC32";
    getSettings()[Setting::s3_max_single_part_upload_size] = 0;
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    const String data(10, 'a');
    CreateReadBuffer create_read_buffer = [data]() -> std::unique_ptr<SeekableReadBuffer>
    {
        return std::make_unique<ReadBufferFromString>(data);
    };

    S3::S3RequestSettings request_settings;
    request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ false);

    copyDataToS3File(
        create_read_buffer,
        0,
        data.size(),
        client,
        bucket,
        "copy_checksum_crc32_multipart",
        request_settings,
        nullptr,
        getAsyncPolicy().getScheduler(),
        std::nullopt);

    ASSERT_EQ(Aws::S3::Model::ChecksumAlgorithm::CRC32, injection->create_multipart_upload_algorithm);
    ASSERT_THAT(injection->upload_part_algorithms, testing::Not(testing::IsEmpty()));
    ASSERT_THAT(injection->upload_part_algorithms, testing::Each(Aws::S3::Model::ChecksumAlgorithm::CRC32));
    ASSERT_EQ(injection->upload_part_crc32_checksums, injection->complete_part_crc32_checksums);
    ASSERT_THAT(injection->complete_part_crc32_checksums, testing::Each(testing::Not(testing::IsEmpty())));
    ASSERT_EQ(data, client->store->GetBucketStore(bucket).objects["copy_checksum_crc32_multipart"]);
}

TEST_F(WBS3Test, UploadChecksumAlgorithmValidationAndNormalization)
{
    getSettings()[Setting::s3_upload_checksum_algorithm] = "crc32";

    S3::S3RequestSettings request_settings;
    request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ true);

    ASSERT_EQ("CRC32", request_settings[S3RequestSetting::upload_checksum_algorithm].value);

    /// `MD5` normalizes to upper case. The FIPS rejection is a runtime check, not a validation one.
    getSettings()[Setting::s3_upload_checksum_algorithm] = "md5";
    request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ true);
    ASSERT_EQ("MD5", request_settings[S3RequestSetting::upload_checksum_algorithm].value);

    getSettings()[Setting::s3_upload_checksum_algorithm] = "MD4";

    EXPECT_THROW({
        try
        {
            request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ true);
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::INVALID_SETTING_VALUE, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("only supports MD5, CRC32, SHA256"));
            throw;
        }
    }, DB::Exception);
}

TEST_F(WBS3Test, UploadChecksumAlgorithmDefaults)
{
    using Algorithm = S3::RequestChecksum::Algorithm;

    /// Default-constructed settings leave upload_checksum_algorithm empty.
    S3::S3RequestSettings request_settings;
    const bool fips = DB::OpenSSLInitializer::instance().isFIPSEnabled();

    /// Empty setting: always defer to the SDK's `Content-MD5`, including under FIPS where the SDK drops it.
    /// Attaching a flexible checksum is opt-in, because support for `x-amz-checksum-*` outside AWS is inconsistent.
    ASSERT_EQ(Algorithm::MD5,
        S3::RequestChecksum::getUploadChecksumAlgorithm(request_settings, /* is_s3express_bucket */ false));

    /// S3Express does not support `Content-MD5`, so the default upload checksum is `CRC32`.
    ASSERT_EQ(Algorithm::CRC32,
        S3::RequestChecksum::getUploadChecksumAlgorithm(request_settings, /* is_s3express_bucket */ true));

    /// S3Express honors an explicit flexible algorithm instead of forcing CRC32.
    getSettings()[Setting::s3_upload_checksum_algorithm] = "SHA256";
    request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ true);
    ASSERT_EQ(Algorithm::SHA256,
        S3::RequestChecksum::getUploadChecksumAlgorithm(request_settings, /* is_s3express_bucket */ true));

    /// S3Express cannot use MD5 (no Content-MD5), so an explicit MD5 is rejected rather than silently upgraded.
    if (!fips)
    {
        getSettings()[Setting::s3_upload_checksum_algorithm] = "MD5";
        request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ true);
        EXPECT_THROW({
            try
            {
                S3::RequestChecksum::getUploadChecksumAlgorithm(request_settings, /* is_s3express_bucket */ true);
            }
            catch (const DB::Exception & e)
            {
                ASSERT_EQ(ErrorCodes::INVALID_SETTING_VALUE, e.code());
                EXPECT_THAT(e.what(), testing::HasSubstr("cannot be MD5 for S3Express buckets"));
                throw;
            }
        }, DB::Exception);
    }
}

TEST_F(WBS3Test, UploadChecksumAlgorithmRuntimeValidation)
{
    S3::S3RequestSettings request_settings;

    getSettings()[Setting::s3_upload_checksum_algorithm] = "MD4";
    request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ false);

    EXPECT_THROW({
        try
        {
            S3::RequestChecksum::getUploadChecksumAlgorithm(request_settings, /* is_s3express_bucket */ false);
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::INVALID_SETTING_VALUE, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("only supports MD5, CRC32, SHA256"));
            throw;
        }
    }, DB::Exception);

    if (DB::OpenSSLInitializer::instance().isFIPSEnabled())
    {
        getSettings()[Setting::s3_upload_checksum_algorithm] = "MD5";
        request_settings.updateFromSettings(getSettings(), /* if_changed */ true, /* validate_settings */ false);

        EXPECT_THROW({
            try
            {
                S3::RequestChecksum::getUploadChecksumAlgorithm(request_settings, /* is_s3express_bucket */ false);
            }
            catch (const DB::Exception & e)
            {
                ASSERT_EQ(ErrorCodes::INVALID_SETTING_VALUE, e.code());
                EXPECT_THAT(e.what(), testing::HasSubstr("cannot be MD5 when FIPS mode is enabled"));
                throw;
            }
        }, DB::Exception);
    }
}

TEST_F(WBS3Test, UploadChecksumAlgorithmEmptyDefaultSinglepart)
{
    /// The empty setting keeps the SDK's `Content-MD5` path, including under FIPS where the SDK silently
    /// omits the header. Attaching a flexible checksum is opt-in.
    auto injection = std::make_shared<MockS3::ChecksumRecordingInjection>();
    setInjectionModel(injection);

    auto buffer = getWriteBuffer("checksum_empty_default_singlepart");
    writeAsOneBlock(*buffer, 10);

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    ASSERT_EQ(Aws::S3::Model::ChecksumAlgorithm::NOT_SET, injection->put_object_algorithm);
    ASSERT_FALSE(injection->put_object_request_checksum_required);
    ASSERT_TRUE(injection->put_object_should_compute_content_md5);
}

TEST_F(WBS3Test, UploadChecksumAlgorithmGCSIgnoresSetting)
{
    /// `GCS` requires `x-goog-*` and rejects `SigV4`-signed requests carrying `x-amz-checksum-*`, so even
    /// an explicit algorithm must not reach the request.
    client = MockS3::Client::CreateClient(bucket, /* is_s3express_bucket */ false, MockS3::Client::gcs_endpoint);
    ASSERT_TRUE(client->isClientForGCS());

    auto injection = std::make_shared<MockS3::ChecksumRecordingInjection>();
    setInjectionModel(injection);

    getSettings()[Setting::s3_upload_checksum_algorithm] = "SHA256";

    auto buffer = getWriteBuffer("checksum_gcs_ignores_setting");
    writeAsOneBlock(*buffer, 10);

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    ASSERT_EQ(Aws::S3::Model::ChecksumAlgorithm::NOT_SET, injection->put_object_algorithm);
    ASSERT_FALSE(injection->put_object_request_checksum_required);
    ASSERT_TRUE(injection->put_object_should_compute_content_md5);
}

TEST_F(WBS3Test, UploadChecksumAlgorithmMD5Singlepart)
{
    if (DB::OpenSSLInitializer::instance().isFIPSEnabled())
        return;

    auto injection = std::make_shared<MockS3::ChecksumRecordingInjection>();
    setInjectionModel(injection);

    getSettings()[Setting::s3_upload_checksum_algorithm] = "MD5";

    auto buffer = getWriteBuffer("checksum_md5_singlepart");
    writeAsOneBlock(*buffer, 10);

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    ASSERT_EQ(Aws::S3::Model::ChecksumAlgorithm::NOT_SET, injection->put_object_algorithm);
    ASSERT_FALSE(injection->put_object_request_checksum_required);
    ASSERT_TRUE(injection->put_object_should_compute_content_md5);
}

TEST_F(WBS3Test, S3ExpressHonorsExplicitUploadChecksumAlgorithm)
{
    /// S3Express forces CRC32 only as a default; an explicit SHA256 must survive `setIsS3ExpressBucket`,
    /// which is applied when the request is sent (after the upload algorithm has been chosen).
    client = MockS3::Client::CreateClient(bucket, /* is_s3express_bucket */ true);

    auto injection = std::make_shared<MockS3::ChecksumRecordingInjection>();
    setInjectionModel(injection);

    getSettings()[Setting::s3_upload_checksum_algorithm] = "SHA256";

    auto buffer = getWriteBuffer("s3express_explicit_sha256");
    writeAsOneBlock(*buffer, 10);

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    ASSERT_EQ(Aws::S3::Model::ChecksumAlgorithm::SHA256, injection->put_object_algorithm);
    ASSERT_TRUE(injection->put_object_request_checksum_required);
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

    void runWholeCopy(
        const String & src_key, size_t size, const String & dst_key, const String & src_etag = {}, const String & src_version_id = {})
    {
        auto request_settings = makeRequestSettings();
        client->resetCounters();
        copyS3File(
            client, bucket, src_key, size, src_etag, src_version_id,
            /* dest_s3_client= */ client, bucket, dst_key,
            request_settings, ReadSettings{},
            /* blob_storage_log= */ nullptr, /* schedule= */ {},
            wholeSourceReader(src_key));
    }

    void runRangeCopy(
        const String & src_key,
        size_t offset,
        size_t size,
        size_t src_object_size,
        const String & dst_key,
        const String & src_etag = {},
        const String & src_version_id = {})
    {
        auto request_settings = makeRequestSettings();
        client->resetCounters();
        copyS3FileRange(
            client, bucket, src_key, offset, size, src_object_size, src_etag, src_version_id,
            /* dest_s3_client= */ client, bucket, dst_key,
            request_settings, ReadSettings{},
            /* blob_storage_log= */ nullptr, /* schedule= */ {},
            wholeSourceReader(src_key));
    }

    /// The `ETag` of the generation at `key` now.
    String generationAt(const String & key) { return client->store->GetBucketStore(bucket).object_etags.at(key); }

    /// The error code a copy failed with, if it did.
    template <typename Copy>
    std::optional<int> errorCodeOf(Copy && copy)
    {
        try
        {
            copy();
            return std::nullopt;
        }
        catch (const Exception & e)
        {
            return e.code();
        }
    }
};

/// A copy pinned to the generation that is at the source key carries it as `x-amz-copy-source-if-match`
/// and goes through. This keeps the refusals below from passing by refusing every pinned copy.
TEST_F(CopyS3FileRoutingTest, WholeCopyPinnedToTheCurrentGenerationGoesThrough)
{
    const String source = putSource("src", /* size= */ 100);
    const String generation = generationAt("src");

    runWholeCopy("src", source.size(), "dst", generation);

    EXPECT_EQ(client->counters.copyObject, 1u);
    EXPECT_EQ(client->copy_source_if_match_headers, std::vector<std::string>{generation});
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], source);
}

/// The source is overwritten in place after the caller named its generation and before the copy: the
/// endpoint refuses the `CopyObject` with `412`, and the copy fails with `S3_OBJECT_CHANGED_DURING_READ`
/// rather than copying the newer generation - by another native route or through the read-and-write
/// fallback, which would read whatever is at the key by then.
TEST_F(CopyS3FileRoutingTest, WholeCopyPinnedToAReplacedGenerationIsRefused)
{
    const String source = putSource("src", /* size= */ 100);
    const String replaced_generation = generationAt("src");
    client->store->GetBucketStore(bucket).PutObject("src", String(source.size(), 'x'));
    ASSERT_NE(generationAt("src"), replaced_generation);

    const auto error_code = errorCodeOf([&] { runWholeCopy("src", source.size(), "dst", replaced_generation); });

    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::S3_OBJECT_CHANGED_DURING_READ);
    EXPECT_EQ(client->counters.copyObject, 1u);
    EXPECT_EQ(client->counters.uploadPartCopy, 0u);
    EXPECT_EQ(client->counters.putObject, 0u);
    EXPECT_EQ(client->counters.multiUploadCreate, 0u);
    EXPECT_FALSE(client->store->GetBucketStore(bucket).objects.contains("dst"));
}

/// The same for a ranged copy, which goes through `UploadPartCopy`: every part carries the generation,
/// the first refused part fails the copy, and the multipart upload it belonged to is aborted, so the
/// destination is not a splice of two generations of the source.
TEST_F(CopyS3FileRoutingTest, RangedCopyPinnedToAReplacedGenerationIsRefused)
{
    const size_t source_size = min_source_size_for_range_copy + 1024;
    putSource("src", source_size);
    const String replaced_generation = generationAt("src");
    client->store->GetBucketStore(bucket).PutObject("src", String(source_size, 'x'));

    const auto error_code
        = errorCodeOf([&] { runRangeCopy("src", /* offset= */ 10, /* size= */ 20, source_size, "dst", replaced_generation); });

    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::S3_OBJECT_CHANGED_DURING_READ);
    EXPECT_GE(client->counters.uploadPartCopy, 1u);
    EXPECT_EQ(client->counters.copyObject, 0u);
    EXPECT_EQ(client->counters.multiUploadComplete, 0u);
    EXPECT_EQ(client->counters.multiUploadAbort, 1u);
    EXPECT_FALSE(client->store->GetBucketStore(bucket).objects.contains("dst"));
}

/// A restore from `S3('...?versionId=...')` reads one version of every object of the backup, and its
/// native copy to an S3 disk has to transfer that same version: the `CopyObject` names it on the copy
/// source, so a newer version at the key - of the same size, which no size check would tell apart -
/// is not what lands on the disk.
TEST_F(CopyS3FileRoutingTest, WholeCopyOfAVersionedSourceCopiesThatVersion)
{
    const String selected_version = putSource("src", /* size= */ 100);
    client->store->GetBucketStore(bucket).RecordVersion("src", "v1");
    client->store->GetBucketStore(bucket).PutObject("src", String(selected_version.size(), 'x'));
    ASSERT_NE(client->store->GetBucketStore(bucket).objects["src"], selected_version);

    runWholeCopy("src", selected_version.size(), "dst", /* src_etag= */ {}, /* src_version_id= */ "v1");

    EXPECT_EQ(client->counters.copyObject, 1u);
    EXPECT_EQ(client->copy_source_version_ids, std::vector<std::string>{"v1"});
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], selected_version);
}

/// The same for a ranged copy: every `UploadPartCopy` names the version, so a restore of a range
/// of a versioned backup file is not a splice of the selected and the latest version.
TEST_F(CopyS3FileRoutingTest, RangedCopyOfAVersionedSourceCopiesThatVersion)
{
    const size_t source_size = min_source_size_for_range_copy + 1024;
    const String selected_version = putSource("src", source_size);
    client->store->GetBucketStore(bucket).RecordVersion("src", "v1");
    client->store->GetBucketStore(bucket).PutObject("src", String(source_size, 'x'));

    runRangeCopy("src", /* offset= */ 10, /* size= */ 20, source_size, "dst", /* src_etag= */ {}, /* src_version_id= */ "v1");

    EXPECT_EQ(client->counters.copyObject, 0u);
    EXPECT_GT(client->counters.uploadPartCopy, 0u);
    ASSERT_FALSE(client->copy_source_version_ids.empty());
    EXPECT_TRUE(std::all_of(
        client->copy_source_version_ids.begin(), client->copy_source_version_ids.end(), [](const auto & v) { return v == "v1"; }));
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], selected_version.substr(10, 20));
}

/// A copy that selects no version names none, as before: the current version of the key is copied.
TEST_F(CopyS3FileRoutingTest, UnversionedCopyNamesNoVersion)
{
    const String source = putSource("src", /* size= */ 100);
    runWholeCopy("src", source.size(), "dst");

    EXPECT_TRUE(client->copy_source_version_ids.empty());
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], source);
}

/// An unpinned copy carries no precondition, as before: a caller that names no generation gets a copy
/// of whatever is at the key.
TEST_F(CopyS3FileRoutingTest, UnpinnedCopyCarriesNoPrecondition)
{
    const String source = putSource("src", /* size= */ 100);
    runWholeCopy("src", source.size(), "dst");

    EXPECT_TRUE(client->copy_source_if_match_headers.empty());
    EXPECT_EQ(client->store->GetBucketStore(bucket).objects["dst"], source);
}

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

/// The `plain_rewritable` metadata operations name the generation of a source blob before they copy
/// it (`pinToTheGenerationThatIsThereNow`) and refuse one whose size is not the one the metadata of
/// the file records (`refuseAGenerationOfAnotherSize`). This is exercised here on an `S3ObjectStorage`
/// over the mock endpoint, which keeps a generation per key and evaluates
/// `x-amz-copy-source-if-match` on the copies (the Azure counterpart lives in `gtest_azure_read_buffer.cpp`).
class S3PlainRewritablePinningTest : public CopyS3FileRoutingTest
{
protected:
    std::shared_ptr<S3ObjectStorage> objectStorageOverTheSameStore(std::shared_ptr<MockS3::InjectionModel> injections = nullptr)
    {
        /// A client of its own over the very same in-memory store, so the objects put through `client`
        /// (and the generations `generationAt` reports) are the ones the object storage sees.
        auto storage_client = std::make_unique<MockS3::Client>(client->store);
        if (injections)
            storage_client->setInjectionModel(std::move(injections));
        S3::URI uri;
        uri.bucket = bucket;
        return std::make_shared<S3ObjectStorage>(
            std::move(storage_client), std::make_unique<S3Settings>(), std::move(uri), S3Capabilities{},
            ObjectStorageKeyGeneratorPtr{}, /* disk_name */ "s3_plain_rewritable");
    }

    const std::vector<String> & deleteIfMatchHeaders() { return client->store->GetBucketStore(bucket).delete_if_match; }

    String dataAt(const String & key) { return client->store->GetBucketStore(bucket).objects.at(key); }

    bool isThere(const String & key) { return client->store->GetBucketStore(bucket).object_etags.contains(key); }
};

/// One `HEAD` names the generation that is at the key together with its size, and a generation of the
/// recorded size passes the check.
TEST_F(S3PlainRewritablePinningTest, NamesTheGenerationAndItsSize)
{
    putSource("src", /* size= */ 100);
    auto object_storage = objectStorageOverTheSameStore();

    const StoredObject source = pinToTheGenerationThatIsThereNow(*object_storage, "src");

    EXPECT_EQ(source.remote_path, "src");
    EXPECT_EQ(source.etag, generationAt("src"));
    EXPECT_EQ(source.bytes_size, 100u);
    EXPECT_NO_THROW(refuseAGenerationOfAnotherSize(source, /* recorded_size= */ 100, "dir/file"));
}

/// The blob was written over out of band with one of another size after the file was recorded: the
/// generation that is at the key is not the file the metadata describes, and it is refused before
/// anything is copied.
TEST_F(S3PlainRewritablePinningTest, AGenerationOfAnotherSizeIsRefused)
{
    putSource("src", /* size= */ 100);
    client->store->GetBucketStore(bucket).PutObject("src", String(200, 'x'));
    auto object_storage = objectStorageOverTheSameStore();

    const StoredObject source = pinToTheGenerationThatIsThereNow(*object_storage, "src");
    EXPECT_EQ(source.bytes_size, 200u);

    const auto error_code = errorCodeOf([&] { refuseAGenerationOfAnotherSize(source, /* recorded_size= */ 100, "dir/file"); });
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::FILE_CHANGED_DURING_READ);
}

/// The blob is written over between the `HEAD` that named it and the copy: the copy carries the named
/// generation as `x-amz-copy-source-if-match`, the endpoint refuses it, and nothing is copied - the
/// target is neither recorded with the old size nor left holding the new generation.
TEST_F(S3PlainRewritablePinningTest, ACopyOfAReplacedGenerationIsRefused)
{
    putSource("src", /* size= */ 100);
    auto object_storage = objectStorageOverTheSameStore();
    const StoredObject source = pinToTheGenerationThatIsThereNow(*object_storage, "src");
    ASSERT_NO_THROW(refuseAGenerationOfAnotherSize(source, /* recorded_size= */ 100, "dir/file"));

    client->store->GetBucketStore(bucket).PutObject("src", String(200, 'x'));
    ASSERT_NE(generationAt("src"), source.etag);

    const auto error_code = errorCodeOf(
        [&] { object_storage->copyObject(source, StoredObject("dst"), ReadSettings{}, WriteSettings{}); });
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::S3_OBJECT_CHANGED_DURING_READ);
    EXPECT_FALSE(client->store->GetBucketStore(bucket).objects.contains("dst"));
}

/// An endpoint that reports no `ETag` for the blob cannot pin anything, and the move is refused rather
/// than made blind.
TEST_F(S3PlainRewritablePinningTest, AnObjectWithoutAnETagCannotBePinned)
{
    putSource("src", /* size= */ 100);
    client->store->GetBucketStore(bucket).object_etags.erase("src");
    auto object_storage = objectStorageOverTheSameStore();

    const auto error_code = errorCodeOf([&] { pinToTheGenerationThatIsThereNow(*object_storage, "src"); });
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::S3_ERROR);
}

/// Rolling back a `plain_rewritable` operation on S3 after its remote delete succeeded, the same way
/// `AzurePlainRewritableRollback` does on Azure. The key is free by then, so another writer can
/// recreate it, and the blob it puts there is a generation this transaction has never seen. The
/// restore is a create-if-absent write (`If-None-Match: *`) that the endpoint refuses while a
/// generation is at the key, so the newer blob stays and the saved one is left aside.
class S3PlainRewritableRollbackTest : public S3PlainRewritablePinningTest
{
protected:
    /// The write buffer of `S3ObjectStorage::writeObject` schedules its uploads on the writer pool
    /// of the global context, which the unit test does not have.
    static WriteSettings inlineWriteSettings()
    {
        WriteSettings write_settings;
        write_settings.s3_allow_parallel_part_upload = false;
        return write_settings;
    }

    const std::vector<String> & putIfNoneMatchHeaders() { return client->store->GetBucketStore(bucket).put_if_none_match; }
};

TEST_F(S3PlainRewritableRollbackTest, ARecreatedKeyIsNotRestoredOver)
{
    putSource("tmp_blob", /* size= */ 100);
    const String recreated = putSource("blob", /* size= */ 200);
    const String recreated_generation = generationAt("blob");
    auto object_storage = objectStorageOverTheSameStore();

    ASSERT_FALSE(restoreTheSavedBlobWithoutWritingOver(*object_storage, "tmp_blob", "blob", ReadSettings{}, inlineWriteSettings()));

    ASSERT_EQ(putIfNoneMatchHeaders(), std::vector<String>{"*"});
    /// The generation that was recreated stays, and so does the saved blob, for a recovery by hand.
    EXPECT_EQ(generationAt("blob"), recreated_generation);
    EXPECT_EQ(dataAt("blob"), recreated);
    EXPECT_TRUE(isThere("tmp_blob"));
}

/// The same rollback when the key really is free: nobody recreated the blob this transaction
/// deleted, so the copy it saved aside is put back. This keeps the test above from passing for the
/// wrong reason - by refusing every restore.
TEST_F(S3PlainRewritableRollbackTest, AFreeKeyIsRestored)
{
    const String saved = putSource("tmp_blob", /* size= */ 100);
    auto object_storage = objectStorageOverTheSameStore();

    ASSERT_TRUE(restoreTheSavedBlobWithoutWritingOver(*object_storage, "tmp_blob", "blob", ReadSettings{}, inlineWriteSettings()));

    ASSERT_EQ(putIfNoneMatchHeaders(), std::vector<String>{"*"});
    EXPECT_EQ(dataAt("blob"), saved);
}

/// The saved blob cannot be named - the endpoint reports no `ETag` for it - so its bytes cannot be
/// read pinned to a generation, and nothing is written: a restore that could stitch together the
/// saved blob with whatever was written over the temporary key since is not attempted.
TEST_F(S3PlainRewritableRollbackTest, ASavedBlobWithoutAGenerationIsNotRestored)
{
    putSource("tmp_blob", /* size= */ 100);
    client->store->GetBucketStore(bucket).object_etags.erase("tmp_blob");
    auto object_storage = objectStorageOverTheSameStore();

    ASSERT_FALSE(restoreTheSavedBlobWithoutWritingOver(*object_storage, "tmp_blob", "blob", ReadSettings{}, inlineWriteSettings()));

    EXPECT_TRUE(putIfNoneMatchHeaders().empty());
    EXPECT_FALSE(isThere("blob"));
}


/// The delete of a `StoredObject` that names a generation is pinned to it: `If-Match` on the
/// `DeleteObject`, which S3 evaluates on general purpose buckets, and the `ETag` element of every
/// object of a `DeleteObjects`. The `plain_rewritable` operations copy a generation and then delete
/// it, and the delete must not take away a generation that another writer has put at the key since.
class S3PlainRewritableDeleteTest : public S3PlainRewritablePinningTest
{
};

TEST_F(S3PlainRewritableDeleteTest, DeletesTheGenerationItNamed)
{
    putSource("src", /* size= */ 100);
    auto object_storage = objectStorageOverTheSameStore();
    const StoredObject source = pinToTheGenerationThatIsThereNow(*object_storage, "src");

    object_storage->removeObjectIfExists(source);

    EXPECT_FALSE(isThere("src"));
    EXPECT_EQ(deleteIfMatchHeaders(), std::vector<String>{source.etag});
}

/// The blob is written over between the `HEAD` that named it and the delete: the endpoint refuses the
/// delete, the newer generation stays, and the refusal is `FILE_CHANGED_DURING_READ` - not "does not
/// exist", which `removeObjectIfExists` would swallow.
TEST_F(S3PlainRewritableDeleteTest, RefusesAGenerationItDidNotName)
{
    putSource("src", /* size= */ 100);
    auto object_storage = objectStorageOverTheSameStore();
    const StoredObject source = pinToTheGenerationThatIsThereNow(*object_storage, "src");

    const String replaced = String(200, 'x');
    client->store->GetBucketStore(bucket).PutObject("src", replaced);
    const String replaced_generation = generationAt("src");
    ASSERT_NE(replaced_generation, source.etag);

    const auto error_code = errorCodeOf([&] { object_storage->removeObjectIfExists(source); });
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::FILE_CHANGED_DURING_READ);

    EXPECT_EQ(deleteIfMatchHeaders(), std::vector<String>{source.etag});
    EXPECT_EQ(generationAt("src"), replaced_generation);
    EXPECT_EQ(dataAt("src"), replaced);
}

/// A batch delete carries the generation of every object that names one, deletes the ones whose
/// precondition holds, and reports the replaced one once the rest are gone - it is not among the
/// successful objects, and it stays in the bucket.
TEST_F(S3PlainRewritableDeleteTest, ABatchDeletesWhatItCanAndReportsTheReplacedGeneration)
{
    putSource("a", /* size= */ 100);
    putSource("b", /* size= */ 100);
    putSource("c", /* size= */ 100);
    auto object_storage = objectStorageOverTheSameStore();
    const StoredObject a = pinToTheGenerationThatIsThereNow(*object_storage, "a");
    const StoredObject b = pinToTheGenerationThatIsThereNow(*object_storage, "b");
    const StoredObject c = pinToTheGenerationThatIsThereNow(*object_storage, "c");

    const String replaced = String(200, 'x');
    client->store->GetBucketStore(bucket).PutObject("b", replaced);

    StoredObjects successful;
    const auto error_code = errorCodeOf([&] { object_storage->removeObjectsIfExist({a, b, c}, &successful); });
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::FILE_CHANGED_DURING_READ);

    EXPECT_EQ(deleteIfMatchHeaders(), (std::vector<String>{a.etag, b.etag, c.etag}));
    EXPECT_FALSE(isThere("a"));
    EXPECT_FALSE(isThere("c"));
    EXPECT_EQ(dataAt("b"), replaced);

    std::vector<String> successful_paths;
    for (const auto & object : successful)
        successful_paths.push_back(object.remote_path);
    std::sort(successful_paths.begin(), successful_paths.end());
    EXPECT_EQ(successful_paths, (std::vector<String>{"a", "c"}));
}

/// An object without a generation is deleted by key, as it always was: the pinning costs nothing to
/// the callers that do not name one.
TEST_F(S3PlainRewritableDeleteTest, AnUnnamedObjectIsDeletedByKey)
{
    putSource("src", /* size= */ 100);
    auto object_storage = objectStorageOverTheSameStore();

    object_storage->removeObjectIfExists(StoredObject("src"));

    EXPECT_FALSE(isThere("src"));
    EXPECT_EQ(deleteIfMatchHeaders(), std::vector<String>{String{}});
}

namespace
{

/// Writes a new generation over `key` right before the delete of it runs, as another writer that gets
/// in between the pinned copy of a move and the delete that follows it would.
struct OverwriteBeforeDelete : MockS3::InjectionModel
{
    MockS3::BucketMemStore & store;
    String key;
    String data;

    OverwriteBeforeDelete(MockS3::BucketMemStore & store_, String key_, String data_)
        : store(store_), key(std::move(key_)), data(std::move(data_))
    {
    }

    std::optional<Aws::S3::Model::DeleteObjectOutcome> call(const Aws::S3::Model::DeleteObjectRequest & request) override
    {
        if (request.GetKey() == key)
            store.PutObject(key, data);
        return std::nullopt;
    }
};

/// Makes the `CopyObject` to `key` the way the store does, but answers it without an `ETag`, as an
/// endpoint that does not name the generations it writes does. Every `HeadObject` is recorded so a
/// test can assert that the destination was not named by one.
struct CopyWithoutETag : MockS3::InjectionModel
{
    MockS3::BucketMemStore & store;
    String key;
    std::vector<String> head_keys;

    CopyWithoutETag(MockS3::BucketMemStore & store_, String key_) : store(store_), key(std::move(key_)) { }

    std::optional<Aws::S3::Model::CopyObjectOutcome> call(const Aws::S3::Model::CopyObjectRequest & request) override
    {
        if (request.GetKey() != key)
            return std::nullopt;
        const auto [src_bucket, src_key] = MockS3::splitCopySource(request.GetCopySource());
        store.PutObject(key, store.objects.at(src_key));
        return Aws::S3::Model::CopyObjectOutcome(Aws::S3::Model::CopyObjectResult{});
    }

    std::optional<Aws::S3::Model::HeadObjectOutcome> call(const Aws::S3::Model::HeadObjectRequest & request) override
    {
        head_keys.push_back(request.GetKey());
        return std::nullopt;
    }
};

/// Makes the `CopyObject` to `key` the way the store does and reports the `ETag` of the generation
/// it created, but another writer replaces the key with `data` before the response is delivered:
/// by the time any request of the caller looks at the key again, the generation there is not the
/// one the copy wrote. Every `HeadObject` is recorded so a test can assert that none looked.
struct ReplaceRightAfterTheCopy : MockS3::InjectionModel
{
    MockS3::BucketMemStore & store;
    String key;
    String data;
    String written_generation;
    String replaced_generation;
    std::vector<String> head_keys;

    ReplaceRightAfterTheCopy(MockS3::BucketMemStore & store_, String key_, String data_)
        : store(store_), key(std::move(key_)), data(std::move(data_))
    {
    }

    std::optional<Aws::S3::Model::CopyObjectOutcome> call(const Aws::S3::Model::CopyObjectRequest & request) override
    {
        if (request.GetKey() != key)
            return std::nullopt;
        const auto [src_bucket, src_key] = MockS3::splitCopySource(request.GetCopySource());
        written_generation = store.PutObject(key, store.objects.at(src_key));
        replaced_generation = store.PutObject(key, data);

        Aws::S3::Model::CopyObjectResultDetails details;
        details.SetETag(written_generation);
        Aws::S3::Model::CopyObjectResult result;
        result.SetCopyObjectResultDetails(std::move(details));
        return Aws::S3::Model::CopyObjectOutcome(std::move(result));
    }

    std::optional<Aws::S3::Model::HeadObjectOutcome> call(const Aws::S3::Model::HeadObjectRequest & request) override
    {
        head_keys.push_back(request.GetKey());
        return std::nullopt;
    }
};

}

/// The `plain_rewritable` move and hard link operations of production, driven against the S3 mock
/// the way `AzurePlainRewritableMove` / `AzurePlainRewritableHardLinkRollback` drive them against
/// the fake Azure endpoint: the disk holds one file, `from`, in its root directory, whose blob is at
/// the key the layout builds for a root file.
class S3PlainRewritableOperationTest : public S3PlainRewritablePinningTest
{
protected:
    std::shared_ptr<FsSnapshot> fs_tree = std::make_shared<FsSnapshot>();
    std::shared_ptr<PlainRewritableLayout> layout = std::make_shared<PlainRewritableLayout>("");
    std::shared_ptr<PlainRewritableMetrics> metrics = createPlainRewritableMetrics(ObjectStorageType::S3);
    StoredObjects removed_objects;

    static constexpr size_t file_size = 100;

    void SetUp() override
    {
        S3PlainRewritablePinningTest::SetUp();
        fs_tree->recordDirectoryPath(
            "",
            DirectoryRemoteInfo{.remote_path = PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, .etag = {}, .last_modified = 0, .files = {}});
        fs_tree->recordFile("from", FileRemoteInfo{.bytes_size = file_size, .last_modified = 0});
        putSource(keyOf("from"), file_size);
    }

    String keyOf(const String & name) const { return layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, name); }
};

/// The happy path: one `HEAD` names the source, the copy carries that generation, and the delete is
/// pinned to it too. The file is recorded at `to` and gone from `from`.
TEST_F(S3PlainRewritableOperationTest, AMoveDeletesTheGenerationItCopied)
{
    const String data = dataAt(keyOf("from"));
    const String source_generation = generationAt(keyOf("from"));
    auto object_storage = objectStorageOverTheSameStore();

    MetadataStorageFromPlainObjectStorageMoveFileOperation operation(
        /* replaceable */ false, "from", "to", fs_tree, object_storage, layout, metrics, removed_objects);
    operation.execute();

    EXPECT_TRUE(fs_tree->existsFile("to"));
    EXPECT_FALSE(fs_tree->existsFile("from"));
    EXPECT_FALSE(isThere(keyOf("from")));
    EXPECT_EQ(dataAt(keyOf("to")), data);
    /// The one pinned delete so far is the one of the source, and it carried the generation the
    /// copy was pinned to.
    ASSERT_EQ(deleteIfMatchHeaders(), std::vector<String>{source_generation});
}

/// Another writer replaces the source between the pinned copy and the delete: the delete is refused,
/// the newer generation stays, the move fails with `FILE_CHANGED_DURING_READ` and the file is still
/// `from`. The rollback then removes the blob the copy wrote at `to` - pinned to the generation it
/// named right after the copy - and does not restore the saved copy of the old generation over the
/// source, which was never deleted.
TEST_F(S3PlainRewritableOperationTest, AMoveDoesNotDeleteAGenerationItDidNotCopy)
{
    const String source_generation = generationAt(keyOf("from"));
    const String replaced = String(200, 'y');
    auto object_storage = objectStorageOverTheSameStore(
        std::make_shared<OverwriteBeforeDelete>(client->store->GetBucketStore(bucket), keyOf("from"), replaced));

    MetadataStorageFromPlainObjectStorageMoveFileOperation operation(
        /* replaceable */ false, "from", "to", fs_tree, object_storage, layout, metrics, removed_objects);

    const auto error_code = errorCodeOf([&] { operation.execute(); });
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::FILE_CHANGED_DURING_READ);

    /// The delete of the source carried the generation the copy was pinned to, and was refused.
    ASSERT_EQ(deleteIfMatchHeaders(), std::vector<String>{source_generation});
    EXPECT_EQ(dataAt(keyOf("from")), replaced);
    EXPECT_TRUE(fs_tree->existsFile("from"));

    const String destination_generation = generationAt(keyOf("to"));
    operation.undo();

    /// The blob the copy wrote is taken back out of `to`, pinned to its generation; the source is
    /// left as the other writer made it.
    EXPECT_FALSE(isThere(keyOf("to")));
    EXPECT_EQ(dataAt(keyOf("from")), replaced);
    const auto & deletes = deleteIfMatchHeaders();
    ASSERT_GE(deletes.size(), 2u);
    EXPECT_EQ(deletes[1], destination_generation);
}

/// A rollback of a hard link deletes exactly the generation the copy wrote at the destination: a
/// generation another writer has put at the key since the copy is refused and stays.
TEST_F(S3PlainRewritableOperationTest, AHardLinkRollbackDeleteIsPinnedToWhatTheCopyWrote)
{
    auto object_storage = objectStorageOverTheSameStore();

    MetadataStorageFromPlainObjectStorageCopyFileOperation operation("from", "to", fs_tree, object_storage, layout, metrics);
    operation.execute();
    ASSERT_TRUE(fs_tree->existsFile("to"));
    const String written_generation = generationAt(keyOf("to"));

    const String replaced = String(200, 'z');
    client->store->GetBucketStore(bucket).PutObject(keyOf("to"), replaced);
    const String replaced_generation = generationAt(keyOf("to"));
    ASSERT_NE(replaced_generation, written_generation);

    /// The refusal is logged, not thrown: the rollback has done what it safely could.
    ASSERT_NO_THROW(operation.undo());

    ASSERT_EQ(deleteIfMatchHeaders(), std::vector<String>{written_generation});
    EXPECT_EQ(generationAt(keyOf("to")), replaced_generation);
    EXPECT_EQ(dataAt(keyOf("to")), replaced);
}

/// The same rollback when nobody touched the destination: the blob the copy wrote is removed. This
/// keeps the test above from passing by refusing every delete.
TEST_F(S3PlainRewritableOperationTest, AHardLinkRollbackRemovesAnUntouchedDestination)
{
    auto object_storage = objectStorageOverTheSameStore();

    MetadataStorageFromPlainObjectStorageCopyFileOperation operation("from", "to", fs_tree, object_storage, layout, metrics);
    operation.execute();
    const String written_generation = generationAt(keyOf("to"));

    operation.undo();

    EXPECT_FALSE(isThere(keyOf("to")));
    ASSERT_EQ(deleteIfMatchHeaders(), std::vector<String>{written_generation});
}

/// The endpoint answers the copy without the `ETag` of the object it wrote: a rollback could then only
/// delete the destination by key, which is the cross-generation loss the pinning exists to prevent, so
/// the hard link is refused before the file is recorded - with `S3_ERROR` on S3, as the Azure
/// counterpart is with `AZURE_BLOB_STORAGE_ERROR` - and the rollback leaves the blob the copy wrote
/// at its key rather than deleting by the key alone whatever is there, although `load` brings the
/// uncommitted file back on the next start. The store would name the generation on a `HeadObject`,
/// and the operation does not ask: the response to the copy is the only thing that names what the
/// copy wrote.
TEST_F(S3PlainRewritableOperationTest, AHardLinkWhoseDestinationGenerationCannotBeNamedIsRefused)
{
    auto injection = std::make_shared<CopyWithoutETag>(client->store->GetBucketStore(bucket), keyOf("to"));
    auto object_storage = objectStorageOverTheSameStore(injection);

    MetadataStorageFromPlainObjectStorageCopyFileOperation operation("from", "to", fs_tree, object_storage, layout, metrics);

    const auto error_code = errorCodeOf([&] { operation.execute(); });
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, ErrorCodes::S3_ERROR);
    EXPECT_FALSE(fs_tree->existsFile("to"));
    EXPECT_TRUE(deleteIfMatchHeaders().empty());
    /// The `HeadObject`s are of the source only (the one that names it, and the one `copyObject`
    /// sizes it with): the destination is not asked about.
    EXPECT_FALSE(injection->head_keys.empty());
    EXPECT_TRUE(std::ranges::all_of(injection->head_keys, [&](const String & key) { return key == keyOf("from"); }));

    operation.undo();

    EXPECT_TRUE(isThere(keyOf("to")));
    EXPECT_EQ(dataAt(keyOf("to")), dataAt(keyOf("from")));
    EXPECT_TRUE(deleteIfMatchHeaders().empty());
}

/// Another writer replaces the destination right after the copy, before any request of the operation
/// could look at the key again. The generation the rollback deletes is still the one the copy wrote,
/// because it is named by the response to the copy and not by a `HeadObject` of the key afterwards,
/// which would have named the newer generation and bound it to the operation: the delete carries the
/// copy's `ETag`, the endpoint refuses it, and the newer generation stays.
TEST_F(S3PlainRewritableOperationTest, AHardLinkRollbackDeleteIsPinnedToWhatTheCopyWroteWhenTheKeyIsReplacedRightAfterTheCopy)
{
    const String replaced = String(200, 'z');
    auto injection = std::make_shared<ReplaceRightAfterTheCopy>(client->store->GetBucketStore(bucket), keyOf("to"), replaced);
    auto object_storage = objectStorageOverTheSameStore(injection);

    MetadataStorageFromPlainObjectStorageCopyFileOperation operation("from", "to", fs_tree, object_storage, layout, metrics);
    operation.execute();
    ASSERT_TRUE(fs_tree->existsFile("to"));
    ASSERT_NE(injection->written_generation, injection->replaced_generation);
    ASSERT_EQ(generationAt(keyOf("to")), injection->replaced_generation);
    /// The destination is named without a request of its own: every `HeadObject` is of the source.
    EXPECT_FALSE(injection->head_keys.empty());
    EXPECT_TRUE(std::ranges::all_of(injection->head_keys, [&](const String & key) { return key == keyOf("from"); }));

    ASSERT_NO_THROW(operation.undo());

    ASSERT_EQ(deleteIfMatchHeaders(), std::vector<String>{injection->written_generation});
    EXPECT_EQ(generationAt(keyOf("to")), injection->replaced_generation);
    EXPECT_EQ(dataAt(keyOf("to")), replaced);
}

/// The same for a move: its rollback deletes the destination by the generation the copy reported.
TEST_F(S3PlainRewritableOperationTest, AMoveRollbackDeleteIsPinnedToWhatTheCopyWroteWhenTheKeyIsReplacedRightAfterTheCopy)
{
    const String replaced = String(200, 'z');
    auto injection = std::make_shared<ReplaceRightAfterTheCopy>(client->store->GetBucketStore(bucket), keyOf("to"), replaced);
    auto object_storage = objectStorageOverTheSameStore(injection);

    MetadataStorageFromPlainObjectStorageMoveFileOperation operation(
        /* replaceable */ false, "from", "to", fs_tree, object_storage, layout, metrics, removed_objects);
    operation.execute();
    ASSERT_TRUE(fs_tree->existsFile("to"));
    ASSERT_FALSE(fs_tree->existsFile("from"));
    ASSERT_EQ(generationAt(keyOf("to")), injection->replaced_generation);
    /// The destination is named without a request of its own: every `HeadObject` is of the source.
    EXPECT_FALSE(injection->head_keys.empty());
    EXPECT_TRUE(std::ranges::all_of(injection->head_keys, [&](const String & key) { return key == keyOf("from"); }));

    ASSERT_NO_THROW(operation.undo());

    /// The rollback deleted the destination pinned to what the copy wrote (the delete of the blob
    /// copied aside, by its scratch key, follows it); that precondition did not hold, so the newer
    /// generation stays.
    const auto & if_match = deleteIfMatchHeaders();
    EXPECT_TRUE(std::ranges::find(if_match, injection->written_generation) != if_match.end());
    EXPECT_EQ(generationAt(keyOf("to")), injection->replaced_generation);
    EXPECT_EQ(dataAt(keyOf("to")), replaced);
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

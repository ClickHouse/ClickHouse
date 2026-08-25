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
#include <IO/SeekableReadBuffer.h>

#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>

#include <Common/filesystemHelpers.h>
#include <Common/tests/gtest_global_context.h>
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
    extern const int NOT_IMPLEMENTED;
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
    size_t writtenSize = 0;
    size_t copyObject = 0;
    size_t deleteObject = 0;
    size_t getBucketVersioning = 0;

    size_t totalRequestsCount() const
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

struct Client : DB::S3::Client
{
    /// `simulate_gcs_native_conditional_` fakes a GCS-dialect client purely at the config-selection
    /// level (`http_client`), which is ALL `Client::supportsGcsNativeConditionalRequests` /
    /// `S3ObjectStorage::conditionalOpsUseGenerationTokens` consult -- it does not enable the actual
    /// GCS wire dialect (that stays authoritative on the real HTTP path until a later task). Just
    /// enough for a test to drive the generation-dialect BRANCH of code that only checks that
    /// predicate (e.g. the copy single-operation cap in `S3ObjectStorage::copyObjectConditional`).
    explicit Client(std::shared_ptr<S3MemStrore> mock_s3_store, bool simulate_gcs_native_conditional_ = false)
        : DB::S3::Client(
            100,
            DB::S3::ServerSideEncryptionKMSConfig(),
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("", ""),
            [simulate_gcs_native_conditional_]
            {
                auto cfg = GetClientConfiguration();
                if (simulate_gcs_native_conditional_)
                    cfg.http_client = "gcp_oauth";
                return cfg;
            }(),
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

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest & request) const override
    {
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

/// Injects an arbitrary AWSError<S3Errors> on DeleteObject -- used to drive the conditional-remove
/// (`removeObjectIfTokenMatches`) outcome mapping: a 412-shaped error (exception name "PreconditionFailed",
/// matched by `S3::isPreconditionFailedError`) must map to `ConditionalRemoveOutcome::TokenMismatch`, and a
/// 404-shaped error (a `NO_SUCH_KEY`/`RESOURCE_NOT_FOUND`/`NO_SUCH_BUCKET` error type, matched by
/// `S3::isNotFoundError`) must map to `ConditionalRemoveOutcome::NotFound`.
struct DeleteObjectErrorInjection: InjectionModel
{
    explicit DeleteObjectErrorInjection(Aws::Client::AWSError<Aws::S3::S3Errors> error_) : error(std::move(error_)) {}

    std::optional<Aws::S3::Model::DeleteObjectOutcome> call(const Aws::S3::Model::DeleteObjectRequest & /*request*/) override
    {
        return error;
    }

    Aws::Client::AWSError<Aws::S3::S3Errors> error;
};

/// Injects an arbitrary AWSError<S3Errors> on CopyObject -- used to drive `copyObjectConditional` /
/// `copyS3File`'s `If-None-Match` handling: a "PreconditionFailed" error is the expected "lost the
/// race" signal, while an "AccessDenied" error must propagate as a genuine failure rather than being
/// swallowed into the unconditional-copy fallback (see `copyS3File.cpp`'s `processCopyRequest`).
struct CopyObjectErrorInjection: InjectionModel
{
    explicit CopyObjectErrorInjection(Aws::Client::AWSError<Aws::S3::S3Errors> error_) : error(std::move(error_)) {}

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

// The object ETag from the PutObject / CompleteMultipartUpload response is surfaced via
// getResultObjectETag() after a successful finalize() — lets content-addressed callers record the
// just-written incarnation's token WITHOUT a follow-up HEAD (CA head-after-put elimination).
TEST_F(WBS3Test, ResultObjectETagIsCaptured) {
    // Singlepart upload: the PutObject response ETag.
    {
        auto buffer = getWriteBuffer("singlepart-file");
        writeAsOneBlock(*buffer, 10);
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
        ASSERT_TRUE(buffer->getResultObjectETag().has_value());
        ASSERT_EQ(*buffer->getResultObjectETag(), "etag-singlepart-singlepart-file");
    }

    // Multipart upload: the final object ETag comes from CompleteMultipartUpload, NOT a per-part tag.
    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part — force multipart
        getSettings()[Setting::s3_min_upload_part_size] = 1;
        auto buffer = getWriteBuffer("multipart-file");
        writeAsOneBlock(*buffer, 10);
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
        ASSERT_TRUE(buffer->getResultObjectETag().has_value());
        ASSERT_EQ(*buffer->getResultObjectETag(), "etag-multipart-multipart-file");
    }
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

/// Task 3: the actual PutObject request a single-part upload issues must carry the typed
/// NativeConditional mode exactly when the caller's WriteSettings asked for it -- the old blanket GCS
/// dialect stays authoritative over the wire until a later task; this only proves the mark reaches
/// the production request object (mirrors the HEAD/DELETE marking tests in
/// S3ObjectStorageConditionalOpsTest below).
TEST_F(WBS3Test, PutObjectNativeConditionalModePropagates)
{
    WriteSettings ws;
    ws.object_storage_request_mode = ObjectStorageRequestMode::NativeConditional;

    auto buffer = getWriteBuffer("native_conditional_put", ws);
    buffer->write('A');
    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    EXPECT_EQ(client->counters.putObject, 1);
    EXPECT_TRUE(client->last_put_object_native_conditional);
}

/// The control: an ordinary (Default-mode) single-part upload must NOT pick up the mark.
TEST_F(WBS3Test, PutObjectOrdinaryWriteRemainsDefault)
{
    auto buffer = getWriteBuffer("ordinary_put");
    buffer->write('A');
    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    EXPECT_EQ(client->counters.putObject, 1);
    EXPECT_FALSE(client->last_put_object_native_conditional);
}

/// A multipart upload's CompleteMultipartUpload request must carry the mode too (Task 4's native
/// adapter consumes it as a defense-in-depth guard against a conditional multipart completion), while
/// CreateMultipartUpload and UploadPart -- which no consumer needs marked -- must NOT.
TEST_F(WBS3Test, CompleteMultipartUploadNativeConditionalModePropagatesButCreateAndUploadPartDoNot)
{
    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force multipart
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    WriteSettings ws;
    ws.object_storage_request_mode = ObjectStorageRequestMode::NativeConditional;

    auto buffer = getWriteBuffer("native_conditional_multipart", ws);
    buffer->write('A');
    buffer->next();
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    EXPECT_EQ(client->counters.multiUploadComplete, 1);
    EXPECT_TRUE(client->last_complete_multipart_native_conditional);
    EXPECT_FALSE(client->last_create_multipart_native_conditional);
    EXPECT_FALSE(client->last_upload_part_native_conditional);
}

/// Mock-S3 coverage for the content-addressed conditional-write primitives: `removeObjectIfTokenMatches`
/// (`If-Match` `DeleteObject`) and `copyObjectConditional` (`If-None-Match: *` `CopyObject`), plus the
/// fallback-disable guarantee in `copyS3File` when a conditional copy is requested.
class S3ObjectStorageConditionalOpsTest : public ::testing::Test
{
public:
    const String bucket = "cond-ops-bucket";
    const String disk_name = "cond-ops-disk";

    std::shared_ptr<S3ObjectStorage> object_storage;
    MockS3::Client * mock_client = nullptr;
    std::shared_ptr<MockS3::S3MemStrore> store;

protected:
    void SetUp() override
    {
        /// removeObjectIfTokenMatches()/copyObjectConditional() unconditionally call
        /// BlobStorageLogWriter::create(), which falls back to Context::getGlobalContextInstance()
        /// when there is no query context. Force that global context to exist (harmless -- blob
        /// storage logging stays off by default) regardless of which other gtest TU ran first.
        (void)getContext();

        store = std::make_shared<MockS3::S3MemStrore>();
        store->CreateBucket(bucket);

        auto owned_client = std::make_unique<MockS3::Client>(store);
        mock_client = owned_client.get();

        S3::URI uri;
        uri.bucket = bucket;
        S3Capabilities capabilities;
        ObjectStorageKeyGeneratorPtr key_generator;

        object_storage = std::make_shared<S3ObjectStorage>(
            std::move(owned_client), std::make_unique<S3Settings>(), std::move(uri), capabilities, key_generator, disk_name);
    }

    void TearDown() override
    {
        object_storage.reset();
        mock_client = nullptr;
        store.reset();
    }
};

TEST_F(S3ObjectStorageConditionalOpsTest, RemoveObjectIfTokenMatchesSuccess)
{
    store->GetBucketStore(bucket).PutObject("key1", "data");

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("key1"), "etag-1");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::Removed);
    ASSERT_EQ(mock_client->counters.deleteObject, 1);
}

TEST_F(S3ObjectStorageConditionalOpsTest, RemoveObjectIfTokenMatchesPreconditionFailedIsTokenMismatch)
{
    mock_client->setInjectionModel(std::make_shared<MockS3::DeleteObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed", "precondition failed", false)));

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("key1"), "stale-etag");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::TokenMismatch);
}

TEST_F(S3ObjectStorageConditionalOpsTest, RemoveObjectIfTokenMatchesNotFoundIsNotFound)
{
    mock_client->setInjectionModel(std::make_shared<MockS3::DeleteObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "not found", false)));

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("missing-key"), "any-etag");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::NotFound);
}

/// `tryGetObjectMetadataWithNativeToken` must mark its HEAD wrapper eligible for the typed
/// NativeConditional mode — the mark is what makes a GCS-mode client apply generation semantics to
/// this HEAD — and it must keep tryGetObjectMetadata's existing missing-object contract of returning
/// nullopt.
TEST_F(S3ObjectStorageConditionalOpsTest, NativeTokenHeadIsMarkedAndMissingIsNullopt)
{
    store->GetBucketStore(bucket).PutObject("existing-key", "some-body");

    auto found = object_storage->tryGetObjectMetadataWithNativeToken("existing-key", /*with_tags=*/false);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->size_bytes, 9u);
    EXPECT_TRUE(mock_client->last_head_object_native_conditional);

    auto missing = object_storage->tryGetObjectMetadataWithNativeToken("missing-key", /*with_tags=*/false);
    EXPECT_FALSE(missing.has_value());
    EXPECT_TRUE(mock_client->last_head_object_native_conditional);
}

/// The token-exact DELETE removeObjectIfTokenMatches issues (CAS's `If-Match` reclaim) must be marked
/// eligible for the typed NativeConditional mode -- it is the exact-delete path a GCS generation token
/// belongs on.
TEST_F(S3ObjectStorageConditionalOpsTest, GenerationDeleteUsesNativeConditionalMode)
{
    store->GetBucketStore(bucket).PutObject("key1", "data");

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("key1"), "etag-1");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::Removed);
    EXPECT_TRUE(mock_client->last_delete_object_native_conditional);
}

/// An ordinary (non-conditional) delete must NOT pick up the native mark -- only the exact-token
/// delete path is content-addressed-storage-owned.
TEST_F(S3ObjectStorageConditionalOpsTest, OrdinaryDeleteRemainsDefault)
{
    store->GetBucketStore(bucket).PutObject("key1", "data");

    object_storage->removeObjectIfExists(StoredObject("key1"));

    /// Pin that the ordinary delete actually reached the singular DeleteObject hook this test reads --
    /// otherwise a future refactor onto the batch DeleteObjects path would silently stop exercising
    /// this assertion (the field would sit unwritten at its `false` initializer) and this test would
    /// keep passing while proving nothing.
    ASSERT_EQ(mock_client->counters.deleteObject, 1);
    EXPECT_FALSE(mock_client->last_delete_object_native_conditional);
}

TEST_F(S3ObjectStorageConditionalOpsTest, CopyObjectConditionalSuccess)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    auto result = object_storage->copyObjectConditional(
        StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, WriteSettings{}, std::nullopt);

    ASSERT_TRUE(result.created);
    ASSERT_FALSE(result.dest_etag.empty());
    ASSERT_EQ(store->GetBucketStore(bucket).objects.at("dst-key"), "hello-world");
}

TEST_F(S3ObjectStorageConditionalOpsTest, CopyObjectConditionalPreconditionFailedIsNotCreated)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    mock_client->setInjectionModel(std::make_shared<MockS3::CopyObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed", "precondition failed", false)));

    auto result = object_storage->copyObjectConditional(
        StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, WriteSettings{}, std::nullopt);

    /// "Lost the race": not an error, just created == false and no destination etag.
    ASSERT_FALSE(result.created);
    ASSERT_TRUE(result.dest_etag.empty());
}

/// Task 3: the actual CopyObjectRequest a conditional (write-once) copy issues must carry the typed
/// NativeConditional mode exactly when the caller's WriteSettings asked for it.
TEST_F(S3ObjectStorageConditionalOpsTest, CopyObjectConditionalNativeConditionalModePropagates)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    WriteSettings ws;
    ws.object_storage_request_mode = ObjectStorageRequestMode::NativeConditional;
    auto result = object_storage->copyObjectConditional(
        StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, ws, std::nullopt);

    ASSERT_TRUE(result.created);
    EXPECT_TRUE(mock_client->last_copy_object_native_conditional);
}

/// The control: an ordinary (Default-mode) conditional copy must NOT pick up the mark -- this is the
/// exact shape `CopyObjectConditionalSuccess` above already exercises with `WriteSettings{}`.
TEST_F(S3ObjectStorageConditionalOpsTest, CopyObjectConditionalOrdinaryModeRemainsDefault)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    auto result = object_storage->copyObjectConditional(
        StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, WriteSettings{}, std::nullopt);

    ASSERT_TRUE(result.created);
    EXPECT_FALSE(mock_client->last_copy_object_native_conditional);
}

/// A token-producing conditional copy on a GENERATION-dialect store must fail BEFORE issuing any
/// request when the source exceeds the single-operation cap (see the "Unconditional token-producing
/// write" / "Write-settings decomposition" design sections and
/// S3ObjectStorage::copyObjectConditional): GCS drops preconditions on multipart completion, so a
/// lost race on a multipart-completed conditional copy would silently overwrite instead of failing.
TEST_F(S3ObjectStorageConditionalOpsTest, CopyObjectConditionalAboveSinglePutCapThrowsNotImplementedBeforeAnyRequest)
{
    /// Deliberately builds its OWN store/client/object_storage instead of the fixture's -- this test
    /// needs a client that reports the generation dialect (conditionalOpsUseGenerationTokens()==true),
    /// which the fixture's plain client does not.
    auto cap_store = std::make_shared<MockS3::S3MemStrore>();
    const String cap_bucket = "cap-cond-bucket";
    cap_store->CreateBucket(cap_bucket);
    /// simulate_gcs_native_conditional makes conditionalOpsUseGenerationTokens() report true, so the
    /// cap-enforcement branch this test targets actually runs -- it does not enable the real wire
    /// dialect (that stays gated on Task 4).
    auto owned_client = std::make_unique<MockS3::Client>(cap_store, /*simulate_gcs_native_conditional_=*/true);
    auto * cap_client = owned_client.get();
    cap_store->GetBucketStore(cap_bucket).PutObject("src-key", String(65, 'a'));

    S3::URI uri;
    uri.bucket = cap_bucket;
    S3Capabilities capabilities;
    ObjectStorageKeyGeneratorPtr key_generator;
    auto cap_object_storage = std::make_shared<S3ObjectStorage>(
        std::move(owned_client), std::make_unique<S3Settings>(), std::move(uri), capabilities, key_generator, "cap-cond-disk");

    WriteSettings ws;
    ws.object_storage_request_mode = ObjectStorageRequestMode::NativeConditional;
    ws.s3_single_part_upload_max_bytes_override = 64;   /// one byte below the 65-byte source

    EXPECT_THROW({
        try
        {
            cap_object_storage->copyObjectConditional(StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, ws, std::nullopt);
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
            throw;
        }
    }, DB::Exception);
    EXPECT_EQ(cap_client->counters.copyObject, 0);
}

/// B166-adjacent: `copyS3File`'s `If-None-Match` conditional copy must not silently fall back to an
/// unconditional read-write copy on an `AccessDenied` `CopyObject` response -- that would defeat the
/// write-once guarantee the CA promote path relies on. The exception must propagate, and the fallback
/// (an unconditional `PutObject` upload of the source data) must never run.
TEST_F(WBS3Test, CopyS3FileConditionalAccessDeniedPropagatesWithoutFallback)
{
    client->store->GetBucketStore(bucket).PutObject("src-key", "hello");

    setInjectionModel(std::make_shared<MockS3::CopyObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied", "access denied", false)));

    client->resetCounters();

    S3::S3RequestSettings request_settings;
    ReadSettings read_settings;
    bool fallback_called = false;
    auto fallback_reader = [&]() -> std::unique_ptr<SeekableReadBuffer>
    {
        fallback_called = true;
        return nullptr;
    };

    EXPECT_THROW({
        try
        {
            String dest_etag;
            copyS3File(client, bucket, "src-key", 0, 5, client, bucket, "dst-key",
                       request_settings, read_settings, nullptr, getAsyncPolicy().getScheduler(),
                       fallback_reader, std::nullopt, String("*"), &dest_etag);
        }
        catch (const DB::S3Exception & e)
        {
            EXPECT_FALSE(e.isPreconditionFailed());
            EXPECT_EQ(e.getExceptionName(), "AccessDenied");
            throw;
        }
    }, DB::S3Exception);

    EXPECT_FALSE(fallback_called);
    EXPECT_EQ(client->counters.copyObject, 1);
    EXPECT_EQ(client->counters.putObject, 0);
}

/// The other half of the same guard: a losing conditional copy (412) is a distinct, recognizable
/// outcome (`S3Exception::isPreconditionFailed()`), still without ever running the fallback.
TEST_F(WBS3Test, CopyS3FileConditionalPreconditionFailedSurfacesAsException)
{
    client->store->GetBucketStore(bucket).PutObject("src-key", "world");

    setInjectionModel(std::make_shared<MockS3::CopyObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed", "precondition failed", false)));

    client->resetCounters();

    S3::S3RequestSettings request_settings;
    ReadSettings read_settings;
    auto fallback_reader = []() -> std::unique_ptr<SeekableReadBuffer>
    {
        ADD_FAILURE() << "fallback must not run on a losing conditional copy";
        return nullptr;
    };

    EXPECT_THROW({
        try
        {
            String dest_etag;
            copyS3File(client, bucket, "src-key", 0, 5, client, bucket, "dst-key",
                       request_settings, read_settings, nullptr, getAsyncPolicy().getScheduler(),
                       fallback_reader, std::nullopt, String("*"), &dest_etag);
        }
        catch (const DB::S3Exception & e)
        {
            EXPECT_TRUE(e.isPreconditionFailed());
            throw;
        }
    }, DB::S3Exception);

    EXPECT_EQ(client->counters.putObject, 0);
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

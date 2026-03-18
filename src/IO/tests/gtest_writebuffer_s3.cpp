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
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>

#include <IO/WriteBufferFromS3.h>
#include <IO/S3Common.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/AsyncReadCounters.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/Client.h>

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
        assert(!buckets.contains(bucket));
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
        return DB::S3::ClientFactory::instance().createClientConfiguration(
            "some-region",
            remote_host_filter,
            /* s3_max_redirects = */ 100,
            /* s3_retry_attempts = */ 0,
            /* enable_s3_requests_logging = */ true,
            /* for_disk_s3 = */ false,
            /* get_request_throttler = */ {},
            /* put_request_throttler = */ {}
        );
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
        std::stringstream data;
        data << request.GetBody()->rdbuf();
        bStore.PutObject(request.GetKey(), data.str());
        counters.writtenSize += data.str().length();

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
        auto mpu_id = bStore.CreateMPU();

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

void writeAsOneBlock(WriteBuffer& buf, size_t size)
{
    std::vector<char> data(size, 'a');
    buf.write(data.data(), data.size());
}

void writeAsPieces(WriteBuffer& buf, size_t size)
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

    std::unique_ptr<WriteBufferFromS3> getWriteBuffer(String file_name = "file")
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
                    getAsyncPolicy().getScheduler());
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

String fillStringWithPattern(String pattern, int n)
{
    String data;
    for (int i = 0; i < n; ++i)
    {
        data += pattern;
    }
    return data;
}

#endif

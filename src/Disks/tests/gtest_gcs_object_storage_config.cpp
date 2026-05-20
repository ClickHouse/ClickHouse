#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>
#include <IO/GCS/GCSXMLClient.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/registerDisks.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>
#include <Poco/TemporaryFile.h>
#include <Poco/Util/MapConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>
#include <Common/tests/gtest_global_context.h>

#if USE_GOOGLE_CLOUD
#    include <absl/strings/cord.h>
#    include <gmock/gmock.h>
#    include <google/cloud/storage/internal/object_read_source.h>
#    include <google/cloud/storage/internal/object_requests.h>
#    include <google/cloud/storage/testing/mock_client.h>
#    include <google/storage/v2/storage.pb.h>
#    include <grpcpp/support/status.h>
#endif
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

#if USE_AWS_S3
namespace DB::S3RequestSetting
{
extern const S3RequestSettingsUInt64 min_upload_part_size;
extern const S3RequestSettingsUInt64 max_inflight_parts_for_one_file;
}
#endif

namespace ProfileEvents
{
extern const Event GCSGetObject;
extern const Event GCSListObjects;
extern const Event GCSDeleteObject;
extern const Event GCSReadObject;
extern const Event GCSWriteObject;
extern const Event DiskGCSGetObject;
extern const Event DiskGCSListObjects;
extern const Event DiskGCSDeleteObject;
extern const Event DiskGCSReadObject;
extern const Event DiskGCSWriteObject;
extern const Event GCSReadRequestsCount;
extern const Event GCSReadRequestsErrors;
extern const Event GCSReadRequestsThrottling;
extern const Event GCSReadRequestAttempts;
extern const Event GCSReadRequestRetryableErrors;
extern const Event GCSWriteRequestsCount;
extern const Event GCSWriteRequestsErrors;
extern const Event GCSWriteRequestsThrottling;
extern const Event GCSWriteRequestAttempts;
extern const Event GCSWriteRequestRetryableErrors;
extern const Event DiskGCSReadRequestsCount;
extern const Event DiskGCSReadRequestsErrors;
extern const Event DiskGCSReadRequestsThrottling;
extern const Event DiskGCSReadRequestAttempts;
extern const Event DiskGCSReadRequestRetryableErrors;
extern const Event DiskGCSWriteRequestsCount;
extern const Event DiskGCSWriteRequestsErrors;
extern const Event DiskGCSWriteRequestsThrottling;
extern const Event DiskGCSWriteRequestAttempts;
extern const Event DiskGCSWriteRequestRetryableErrors;
extern const Event GCSGetRequestThrottlerCount;
extern const Event GCSGetRequestThrottlerBlocked;
extern const Event GCSGetRequestThrottlerSleepMicroseconds;
extern const Event GCSPutRequestThrottlerCount;
extern const Event GCSPutRequestThrottlerBlocked;
extern const Event GCSPutRequestThrottlerSleepMicroseconds;
extern const Event DiskGCSGetRequestThrottlerCount;
extern const Event DiskGCSGetRequestThrottlerBlocked;
extern const Event DiskGCSGetRequestThrottlerSleepMicroseconds;
extern const Event DiskGCSPutRequestThrottlerCount;
extern const Event DiskGCSPutRequestThrottlerBlocked;
extern const Event DiskGCSPutRequestThrottlerSleepMicroseconds;
extern const Event ReadBufferFromGCSMicroseconds;
extern const Event ReadBufferFromGCSInitMicroseconds;
extern const Event ReadBufferFromGCSBytes;
extern const Event ReadBufferFromGCSRequestsErrors;
extern const Event WriteBufferFromGCSMicroseconds;
extern const Event WriteBufferFromGCSBytes;
extern const Event WriteBufferFromGCSRequestsErrors;
extern const Event RemoteReadThrottlerBytes;
extern const Event RemoteReadThrottlerSleepMicroseconds;
extern const Event RemoteWriteThrottlerBytes;
extern const Event RemoteWriteThrottlerSleepMicroseconds;
}

using namespace DB;

namespace
{


#if USE_GOOGLE_CLOUD
String readAll(ReadBuffer & in)
{
    String result;
    readStringUntilEOF(result, in);
    return result;
}

String readBytes(ReadBuffer & in, size_t size)
{
    String result(size, '\0');
    in.readStrict(result.data(), size);
    return result;
}

String cordToString(const absl::Cord & cord)
{
    String result;
    absl::CopyCordToString(cord, &result);
    return result;
}

ReadSettings readSettings(size_t buffer_size)
{
    ReadSettings settings;
    settings.remote_fs_buffer_size = buffer_size;
    return settings;
}

UInt64 profileEventValue(ProfileEvents::Event event)
{
    return CurrentThread::getProfileEvents()[event];
}

void resetProfileEvents()
{
    CurrentThread::getProfileEvents().reset();
}

std::shared_ptr<Throttler> blockingThrottler(ProfileEvents::Event amount, ProfileEvents::Event sleep)
{
    return std::make_shared<Throttler>(1000, 0, nullptr, amount, sleep);
}


class CapturedBlobStorageLog
{
public:
    CapturedBlobStorageLog()
    {
        SystemLogSettings settings;
        settings.queue_settings.database = "system";
        settings.queue_settings.table = "blob_storage_log_test";
        settings.queue_settings.reserved_size_rows = 128;
        settings.queue_settings.max_size_rows = 1024;
        settings.queue_settings.buffer_size_rows_flush_threshold = 1024;
        settings.queue_settings.flush_interval_milliseconds = 100000;
        settings.queue_settings.notify_flush_on_crash = false;
        settings.queue_settings.turn_off_logger = true;
        settings.engine = "ENGINE = Null";

        queue = std::make_shared<SystemLogQueue<BlobStorageLogElement>>(settings.queue_settings);
        log = std::make_shared<BlobStorageLog>(getContext().context, settings, queue);
    }

    BlobStorageLogWriterPtr createWriter(const String & disk_name)
    {
        auto writer = std::make_shared<BlobStorageLogWriter>(log);
        writer->disk_name = disk_name;
        return writer;
    }

    std::vector<BlobStorageLogElement> drain()
    {
        queue->notifyFlush(queue->getLastLogIndex(), /* should_prepare_tables_anyway */ false);
        auto result = queue->pop();
        queue->confirm(result.last_log_index);
        return std::move(result.logs);
    }

private:
    std::shared_ptr<SystemLogQueue<BlobStorageLogElement>> queue;
    BlobStorageLogPtr log;
};


struct FakeGCSClient
{
    struct FakeObject
    {
        std::string data;
        google::storage::v2::Object metadata;
    };

    grpc::Status get_object_status;
    std::vector<grpc::Status> get_object_statuses;
    google::storage::v2::Object get_object_response;
    grpc::Status list_objects_status;
    std::vector<grpc::Status> list_objects_statuses;
    google::storage::v2::ListObjectsResponse list_objects_response;
    grpc::Status delete_object_status;
    std::vector<grpc::Status> delete_object_statuses;
    grpc::Status compose_object_status;
    grpc::Status rewrite_object_status;
    std::vector<google::storage::v2::RewriteResponse> rewrite_object_responses;
    std::vector<google::storage::v2::ReadObjectResponse> read_object_responses;
    grpc::Status read_object_finish_status;
    size_t read_object_null_streams = 0;
    grpc::Status write_object_finish_status;
    bool write_object_write_returns_false = false;
    bool write_object_writes_done_returns_false = false;
    std::atomic_int write_object_finish_calls = 0;
    std::atomic_int write_object_stream_creations = 0;
    bool use_object_map = false;
    std::map<std::string, FakeObject> objects;

    std::vector<google::storage::v2::GetObjectRequest> get_object_requests;
    std::vector<google::storage::v2::ListObjectsRequest> list_objects_requests;
    std::vector<google::storage::v2::DeleteObjectRequest> delete_object_requests;
    std::vector<google::storage::v2::ComposeObjectRequest> compose_object_requests;
    std::vector<google::storage::v2::RewriteObjectRequest> rewrite_object_requests;
    std::vector<google::storage::v2::ReadObjectRequest> read_object_requests;
    std::vector<google::storage::v2::WriteObjectRequest> write_object_requests;
};

google::cloud::Status cloudStatusFromGrpcStatus(const grpc::Status & status)
{
    if (status.ok())
        return {};

    switch (status.error_code())
    {
        case grpc::StatusCode::NOT_FOUND:
            return google::cloud::Status(google::cloud::StatusCode::kNotFound, status.error_message());
        case grpc::StatusCode::PERMISSION_DENIED:
        case grpc::StatusCode::UNAUTHENTICATED:
            return google::cloud::Status(google::cloud::StatusCode::kPermissionDenied, status.error_message());
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            return google::cloud::Status(google::cloud::StatusCode::kDeadlineExceeded, status.error_message());
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
            return google::cloud::Status(google::cloud::StatusCode::kResourceExhausted, status.error_message());
        case grpc::StatusCode::UNAVAILABLE:
            return google::cloud::Status(google::cloud::StatusCode::kUnavailable, status.error_message());
        case grpc::StatusCode::INVALID_ARGUMENT:
        case grpc::StatusCode::FAILED_PRECONDITION:
        case grpc::StatusCode::OUT_OF_RANGE:
            return google::cloud::Status(google::cloud::StatusCode::kInvalidArgument, status.error_message());
        case grpc::StatusCode::UNIMPLEMENTED:
            return google::cloud::Status(google::cloud::StatusCode::kUnimplemented, status.error_message());
        default:
            return google::cloud::Status(google::cloud::StatusCode::kUnknown, status.error_message());
    }
}

class FakeHighLevelObjectReadSource final : public google::cloud::storage::internal::ObjectReadSource
{
public:
    FakeHighLevelObjectReadSource(String data_, google::cloud::Status finish_status_)
        : data(std::move(data_))
        , finish_status(std::move(finish_status_))
    {
    }

    bool IsOpen() const override { return open; }

    google::cloud::StatusOr<google::cloud::storage::internal::HttpResponse> Close() override
    {
        open = false;
        return google::cloud::storage::internal::HttpResponse{google::cloud::storage::internal::kOk, {}, {}};
    }

    google::cloud::StatusOr<google::cloud::storage::internal::ReadSourceResult> Read(char * buffer, std::size_t size) override
    {
        if (!open)
            return google::cloud::storage::internal::ReadSourceResult{
                0, google::cloud::storage::internal::HttpResponse{google::cloud::storage::internal::kOk, {}, {}}};

        if (position >= data.size())
        {
            open = false;
            if (!finish_status.ok())
                return finish_status;
            return google::cloud::storage::internal::ReadSourceResult{
                0, google::cloud::storage::internal::HttpResponse{google::cloud::storage::internal::kOk, {}, {}}};
        }

        const size_t bytes_to_copy = std::min(size, data.size() - position);
        memcpy(buffer, data.data() + position, bytes_to_copy);
        position += bytes_to_copy;

        google::cloud::storage::internal::ReadSourceResult result{
            bytes_to_copy, google::cloud::storage::internal::HttpResponse{google::cloud::storage::internal::kContinue, {}, {}}};
        result.size = data.size();
        return result;
    }

private:
    String data;
    google::cloud::Status finish_status;
    size_t position = 0;
    bool open = true;
};

std::pair<size_t, std::optional<size_t>> highLevelReadRange(
    const google::cloud::storage::internal::ReadObjectRangeRequest & request)
{
    if (request.HasOption<google::cloud::storage::ReadRange>())
    {
        const auto range = request.GetOption<google::cloud::storage::ReadRange>().value();
        const auto begin = std::max<std::int64_t>(0, range.begin);
        const auto end = std::max(begin, range.end);
        return {static_cast<size_t>(begin), static_cast<size_t>(end - begin)};
    }

    if (request.HasOption<google::cloud::storage::ReadFromOffset>())
    {
        const auto offset = std::max<std::int64_t>(0, request.GetOption<google::cloud::storage::ReadFromOffset>().value());
        return {static_cast<size_t>(offset), std::nullopt};
    }

    return {0, std::nullopt};
}

String applyReadRange(String data, size_t offset, std::optional<size_t> limit)
{
    if (offset >= data.size())
        return {};

    const size_t bytes_to_keep = limit ? std::min(*limit, data.size() - offset) : data.size() - offset;
    return data.substr(offset, bytes_to_keep);
}

String bucketResourceNameForFake(const String & bucket)
{
    if (bucket.starts_with("projects/"))
        return bucket;
    return "projects/_/buckets/" + bucket;
}

String plainBucketNameForFake(const String & bucket)
{
    static constexpr std::string_view prefix = "projects/_/buckets/";
    if (bucket.starts_with(prefix))
        return bucket.substr(prefix.size());
    return bucket;
}

google::cloud::storage::ObjectMetadata cloudMetadataFromFakeObject(const FakeGCSClient::FakeObject & object)
{
    google::cloud::storage::ObjectMetadata metadata;
    metadata.set_bucket(plainBucketNameForFake(object.metadata.bucket()));
    metadata.set_name(object.metadata.name());
    metadata.set_size(static_cast<std::uint64_t>(std::max<int64_t>(0, object.metadata.size())));
    metadata.set_etag(object.metadata.etag());
    for (const auto & [key, value] : object.metadata.metadata())
        metadata.upsert_metadata(key, value);
    return metadata;
}

google::storage::v2::Object protoMetadataFromCloudMetadata(
    const google::cloud::storage::ObjectMetadata & metadata, const String & bucket, const String & object_name)
{
    google::storage::v2::Object object;
    object.set_bucket(bucketResourceNameForFake(bucket));
    object.set_name(object_name);
    object.set_size(static_cast<int64_t>(metadata.size()));
    object.set_etag(metadata.etag());
    for (const auto & [key, value] : metadata.metadata())
        (*object.mutable_metadata())[key] = value;
    return object;
}

template <typename Request>
google::storage::v2::Object destinationFromOptions(const Request & request, const String & bucket, const String & object_name)
{
    google::storage::v2::Object object;
    object.set_bucket(bucketResourceNameForFake(bucket));
    object.set_name(object_name);
    if (request.template HasOption<google::cloud::storage::WithObjectMetadata>())
    {
        const auto metadata = request.template GetOption<google::cloud::storage::WithObjectMetadata>().value();
        object = protoMetadataFromCloudMetadata(metadata, bucket, object_name);
    }
    return object;
}

template <typename Request>
bool hasGenerationMatchZero(const Request & request)
{
    return request.template HasOption<google::cloud::storage::IfGenerationMatch>()
        && request.template GetOption<google::cloud::storage::IfGenerationMatch>().value() == 0;
}

google::cloud::Status objectAlreadyExistsStatus()
{
    return google::cloud::Status(google::cloud::StatusCode::kAlreadyExists, "fake destination already exists");
}

google::cloud::Status objectNotFoundStatus(std::string message = "fake object not found")
{
    return google::cloud::Status(google::cloud::StatusCode::kNotFound, std::move(message));
}

std::shared_ptr<GCS::HighLevelClient> makeFakeHighLevelClient(
    const std::shared_ptr<FakeGCSClient> & fake_stub, const GCS::ClientSettings & settings)
{
    auto mock = std::make_shared<::testing::NiceMock<google::cloud::storage::testing::MockClient>>();
    ON_CALL(*mock, ReadObject(::testing::_))
        .WillByDefault(::testing::Invoke(
            [fake_stub](const google::cloud::storage::internal::ReadObjectRangeRequest & request)
                -> google::cloud::StatusOr<std::unique_ptr<google::cloud::storage::internal::ObjectReadSource>>
            {
                const auto [offset, limit] = highLevelReadRange(request);

                google::storage::v2::ReadObjectRequest captured;
                captured.set_bucket(bucketResourceNameForFake(request.bucket_name()));
                captured.set_object(request.object_name());
                captured.set_read_offset(static_cast<int64_t>(offset));
                if (limit)
                    captured.set_read_limit(static_cast<int64_t>(*limit));
                fake_stub->read_object_requests.push_back(captured);

                if (fake_stub->read_object_null_streams)
                {
                    --fake_stub->read_object_null_streams;
                    return google::cloud::Status(google::cloud::StatusCode::kUnavailable, "fake ReadObject did not create a stream");
                }

                String data;
                if (fake_stub->use_object_map)
                {
                    const auto key = captured.bucket() + "\n" + captured.object();
                    auto it = fake_stub->objects.find(key);
                    if (it == fake_stub->objects.end())
                        return objectNotFoundStatus();
                    data = it->second.data;
                }
                else
                {
                    for (const auto & response : fake_stub->read_object_responses)
                    {
                        if (response.has_checksummed_data())
                            data += cordToString(response.checksummed_data().content());
                    }
                }

                data = applyReadRange(std::move(data), offset, limit);
                std::unique_ptr<google::cloud::storage::internal::ObjectReadSource> source
                    = std::make_unique<FakeHighLevelObjectReadSource>(
                        std::move(data), cloudStatusFromGrpcStatus(fake_stub->read_object_finish_status));
                return source;
            }));

    ON_CALL(*mock, InsertObjectMedia(::testing::_))
        .WillByDefault(::testing::Invoke(
            [fake_stub](const google::cloud::storage::internal::InsertObjectMediaRequest & request)
                -> google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
            {
                ++fake_stub->write_object_stream_creations;
                ++fake_stub->write_object_finish_calls;

                google::storage::v2::WriteObjectRequest captured;
                auto & spec = *captured.mutable_write_object_spec();
                *spec.mutable_resource() = destinationFromOptions(request, request.bucket_name(), request.object_name());
                if (hasGenerationMatchZero(request))
                    spec.set_if_generation_match(0);
                captured.set_write_offset(0);
                captured.set_finish_write(true);
                if (!request.payload().empty())
                    captured.mutable_checksummed_data()->set_content(request.payload());
                fake_stub->write_object_requests.push_back(captured);

                if (fake_stub->write_object_write_returns_false || fake_stub->write_object_writes_done_returns_false)
                    return google::cloud::Status(google::cloud::StatusCode::kUnavailable, "fake high-level write stream closed");
                if (!fake_stub->write_object_finish_status.ok())
                    return cloudStatusFromGrpcStatus(fake_stub->write_object_finish_status);

                auto metadata = cloudMetadataFromFakeObject(FakeGCSClient::FakeObject{String(request.payload()), spec.resource()});
                if (!fake_stub->use_object_map)
                    return metadata;

                const auto object_key = bucketResourceNameForFake(request.bucket_name()) + "\n" + request.object_name();
                if (hasGenerationMatchZero(request) && fake_stub->objects.contains(object_key))
                    return objectAlreadyExistsStatus();

                FakeGCSClient::FakeObject object;
                object.data = String(request.payload());
                object.metadata = spec.resource();
                object.metadata.set_size(static_cast<int64_t>(object.data.size()));
                fake_stub->objects[object_key] = object;
                return cloudMetadataFromFakeObject(object);
            }));

    ON_CALL(*mock, GetObjectMetadata(::testing::_))
        .WillByDefault(::testing::Invoke(
            [fake_stub](const google::cloud::storage::internal::GetObjectMetadataRequest & request)
                -> google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
            {
                google::storage::v2::GetObjectRequest captured;
                captured.set_bucket(bucketResourceNameForFake(request.bucket_name()));
                captured.set_object(request.object_name());
                fake_stub->get_object_requests.push_back(captured);

                auto status = fake_stub->get_object_statuses.empty() ? fake_stub->get_object_status : fake_stub->get_object_statuses.front();
                if (!fake_stub->get_object_statuses.empty())
                    fake_stub->get_object_statuses.erase(fake_stub->get_object_statuses.begin());
                if (!status.ok())
                    return cloudStatusFromGrpcStatus(status);

                if (fake_stub->use_object_map)
                {
                    auto it = fake_stub->objects.find(captured.bucket() + "\n" + captured.object());
                    if (it == fake_stub->objects.end())
                        return objectNotFoundStatus();
                    return cloudMetadataFromFakeObject(it->second);
                }
                return cloudMetadataFromFakeObject(FakeGCSClient::FakeObject{{}, fake_stub->get_object_response});
            }));

    ON_CALL(*mock, ListObjects(::testing::_))
        .WillByDefault(::testing::Invoke(
            [fake_stub](const google::cloud::storage::internal::ListObjectsRequest & request)
                -> google::cloud::StatusOr<google::cloud::storage::internal::ListObjectsResponse>
            {
                google::storage::v2::ListObjectsRequest captured;
                captured.set_parent(bucketResourceNameForFake(request.bucket_name()));
                if (request.HasOption<google::cloud::storage::Prefix>())
                    captured.set_prefix(request.GetOption<google::cloud::storage::Prefix>().value());
                if (request.HasOption<google::cloud::storage::StartOffset>())
                    captured.set_lexicographic_start(request.GetOption<google::cloud::storage::StartOffset>().value());
                if (request.HasOption<google::cloud::storage::MaxResults>())
                    captured.set_page_size(static_cast<int32_t>(request.GetOption<google::cloud::storage::MaxResults>().value()));
                captured.set_page_token(request.page_token());
                fake_stub->list_objects_requests.push_back(captured);

                auto status = fake_stub->list_objects_statuses.empty() ? fake_stub->list_objects_status : fake_stub->list_objects_statuses.front();
                if (!fake_stub->list_objects_statuses.empty())
                    fake_stub->list_objects_statuses.erase(fake_stub->list_objects_statuses.begin());
                if (!status.ok())
                    return cloudStatusFromGrpcStatus(status);

                google::cloud::storage::internal::ListObjectsResponse response;
                if (!fake_stub->use_object_map)
                {
                    for (const auto & object : fake_stub->list_objects_response.objects())
                        response.items.push_back(cloudMetadataFromFakeObject(FakeGCSClient::FakeObject{{}, object}));
                    response.next_page_token = fake_stub->list_objects_response.next_page_token();
                    return response;
                }

                std::vector<const FakeGCSClient::FakeObject *> matched;
                for (const auto & [key, object] : fake_stub->objects)
                {
                    (void)key;
                    if (object.metadata.bucket() != captured.parent())
                        continue;
                    if (!captured.prefix().empty() && !object.metadata.name().starts_with(captured.prefix()))
                        continue;
                    if (!captured.lexicographic_start().empty() && object.metadata.name() < captured.lexicographic_start())
                        continue;
                    matched.push_back(&object);
                }

                const size_t start = captured.page_token().empty() ? 0 : std::stoull(captured.page_token());
                const size_t limit = captured.page_size() > 0 ? static_cast<size_t>(captured.page_size()) : std::numeric_limits<size_t>::max();
                for (size_t i = start; i < matched.size() && response.items.size() < limit; ++i)
                    response.items.push_back(cloudMetadataFromFakeObject(*matched[i]));

                const size_t next = start + response.items.size();
                if (next < matched.size())
                    response.next_page_token = std::to_string(next);
                return response;
            }));

    ON_CALL(*mock, DeleteObject(::testing::_))
        .WillByDefault(::testing::Invoke(
            [fake_stub](const google::cloud::storage::internal::DeleteObjectRequest & request)
                -> google::cloud::StatusOr<google::cloud::storage::internal::EmptyResponse>
            {
                google::storage::v2::DeleteObjectRequest captured;
                captured.set_bucket(bucketResourceNameForFake(request.bucket_name()));
                captured.set_object(request.object_name());
                fake_stub->delete_object_requests.push_back(captured);

                auto status = fake_stub->delete_object_statuses.empty() ? fake_stub->delete_object_status : fake_stub->delete_object_statuses.front();
                if (!fake_stub->delete_object_statuses.empty())
                    fake_stub->delete_object_statuses.erase(fake_stub->delete_object_statuses.begin());
                if (!status.ok())
                    return cloudStatusFromGrpcStatus(status);

                if (fake_stub->use_object_map)
                {
                    auto it = fake_stub->objects.find(captured.bucket() + "\n" + captured.object());
                    if (it == fake_stub->objects.end())
                        return objectNotFoundStatus();
                    fake_stub->objects.erase(it);
                }
                return google::cloud::storage::internal::EmptyResponse{};
            }));

    ON_CALL(*mock, ComposeObject(::testing::_))
        .WillByDefault(::testing::Invoke(
            [fake_stub](const google::cloud::storage::internal::ComposeObjectRequest & request)
                -> google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
            {
                google::storage::v2::ComposeObjectRequest captured;
                *captured.mutable_destination() = destinationFromOptions(request, request.bucket_name(), request.object_name());
                if (hasGenerationMatchZero(request))
                    captured.set_if_generation_match(0);
                for (const auto & source : request.source_objects())
                    captured.add_source_objects()->set_name(source.object_name);
                fake_stub->compose_object_requests.push_back(captured);

                if (!fake_stub->compose_object_status.ok())
                    return cloudStatusFromGrpcStatus(fake_stub->compose_object_status);

                if (!fake_stub->use_object_map)
                    return cloudMetadataFromFakeObject(FakeGCSClient::FakeObject{{}, captured.destination()});

                std::string data;
                for (const auto & source : captured.source_objects())
                {
                    auto it = fake_stub->objects.find(captured.destination().bucket() + "\n" + source.name());
                    if (it == fake_stub->objects.end())
                        return objectNotFoundStatus("fake compose source object not found");
                    data += it->second.data;
                }

                const auto destination_key = captured.destination().bucket() + "\n" + captured.destination().name();
                if (captured.has_if_generation_match() && captured.if_generation_match() == 0 && fake_stub->objects.contains(destination_key))
                    return objectAlreadyExistsStatus();

                FakeGCSClient::FakeObject object;
                object.data = std::move(data);
                object.metadata = captured.destination();
                object.metadata.set_size(static_cast<int64_t>(object.data.size()));
                fake_stub->objects[destination_key] = object;
                return cloudMetadataFromFakeObject(object);
            }));

    ON_CALL(*mock, RewriteObject(::testing::_))
        .WillByDefault(::testing::Invoke(
            [fake_stub](const google::cloud::storage::internal::RewriteObjectRequest & request)
                -> google::cloud::StatusOr<google::cloud::storage::internal::RewriteObjectResponse>
            {
                google::storage::v2::RewriteObjectRequest captured;
                captured.set_source_bucket(bucketResourceNameForFake(request.source_bucket()));
                captured.set_source_object(request.source_object());
                captured.set_destination_bucket(bucketResourceNameForFake(request.destination_bucket()));
                captured.set_destination_name(request.destination_object());
                captured.set_rewrite_token(request.rewrite_token());
                if (request.HasOption<google::cloud::storage::WithObjectMetadata>())
                    *captured.mutable_destination() = protoMetadataFromCloudMetadata(
                        request.GetOption<google::cloud::storage::WithObjectMetadata>().value(),
                        request.destination_bucket(),
                        request.destination_object());
                if (hasGenerationMatchZero(request))
                    captured.set_if_generation_match(0);
                fake_stub->rewrite_object_requests.push_back(captured);

                if (!fake_stub->rewrite_object_status.ok())
                    return cloudStatusFromGrpcStatus(fake_stub->rewrite_object_status);

                if (!fake_stub->rewrite_object_responses.empty())
                {
                    auto raw = fake_stub->rewrite_object_responses.front();
                    fake_stub->rewrite_object_responses.erase(fake_stub->rewrite_object_responses.begin());
                    google::cloud::storage::internal::RewriteObjectResponse response{};
                    response.done = raw.done();
                    response.rewrite_token = raw.rewrite_token();
                    response.object_size = raw.object_size();
                    response.total_bytes_rewritten = raw.total_bytes_rewritten();
                    response.resource = cloudMetadataFromFakeObject(FakeGCSClient::FakeObject{{}, raw.resource()});
                    return response;
                }

                if (!fake_stub->use_object_map)
                    return google::cloud::storage::internal::RewriteObjectResponse{0, 0, true, {}, {}};

                auto it = fake_stub->objects.find(captured.source_bucket() + "\n" + captured.source_object());
                if (it == fake_stub->objects.end())
                    return objectNotFoundStatus("fake rewrite source object not found");

                const auto destination_key = captured.destination_bucket() + "\n" + captured.destination_name();
                if (captured.has_if_generation_match() && captured.if_generation_match() == 0 && fake_stub->objects.contains(destination_key))
                    return objectAlreadyExistsStatus();

                FakeGCSClient::FakeObject object;
                object.data = it->second.data;
                object.metadata = captured.has_destination() ? captured.destination() : it->second.metadata;
                object.metadata.set_bucket(captured.destination_bucket());
                object.metadata.set_name(captured.destination_name());
                object.metadata.set_size(static_cast<int64_t>(object.data.size()));
                fake_stub->objects[destination_key] = object;

                google::cloud::storage::internal::RewriteObjectResponse response{};
                response.done = true;
                response.object_size = object.data.size();
                response.total_bytes_rewritten = object.data.size();
                response.resource = cloudMetadataFromFakeObject(object);
                return response;
            }));

    auto options = GCS::makeGrpcClientOptions(settings);
    auto storage_client = google::cloud::storage::testing::UndecoratedClientFromMock(mock);
    return std::make_shared<GCS::HighLevelClient>(settings, std::move(options), std::move(storage_client));
}

std::shared_ptr<GCSObjectStorage> makeFakeGCSObjectStorage(
    const std::shared_ptr<FakeGCSClient> & fake_stub,
    bool read_only = false,
    GCS::ClientSettings client_settings = {},
    GCSObjectStorageSettings::BlobStorageLogWriterFactory blob_storage_log_writer_factory = {},
    String bucket = "native-bucket",
    String endpoint = "storage.googleapis.com",
    GCS::WriteTransport write_transport = GCS::WriteTransport::Grpc)
{
    fake_stub->use_object_map = true;

    GCSObjectStorageSettings settings;
    settings.disk_name = "native_gcs_disk";
    settings.bucket = std::move(bucket);
    settings.key_prefix = "clickhouse-data/";
    settings.description = "fake/" + settings.bucket;
    settings.read_only = read_only;
    settings.client_settings = std::move(client_settings);
    settings.client_settings.endpoint = std::move(endpoint);
    settings.client_settings.use_insecure_credentials_for_tests = true;
    settings.write_transport = write_transport;
    settings.xml_client_settings = GCS::makeXMLMultipartClientSettings(settings.client_settings, settings.xml_endpoint);
    settings.client_settings.for_disk = true;
    settings.blob_storage_log_writer_factory = std::move(blob_storage_log_writer_factory);

    auto high_level_client = makeFakeHighLevelClient(fake_stub, settings.client_settings);
    return std::make_shared<GCSObjectStorage>(settings, std::move(high_level_client));
}


String fakeObjectMapKey(const String & path, const String & bucket = "native-bucket")
{
    return "projects/_/buckets/" + bucket + "\n" + path;
}

void addFakeObject(
    const std::shared_ptr<FakeGCSClient> & fake_stub,
    const String & path,
    const String & data = {},
    const String & bucket = "native-bucket")
{
    FakeGCSClient::FakeObject object;
    object.data = data;
    object.metadata.set_bucket("projects/_/buckets/" + bucket);
    object.metadata.set_name(path);
    object.metadata.set_size(static_cast<int64_t>(data.size()));
    fake_stub->objects[fakeObjectMapKey(path, bucket)] = std::move(object);
}

StoredObject writeFakeObject(
    const std::shared_ptr<GCSObjectStorage> & storage, const String & path, const String & data, const ObjectAttributes & attributes = {})
{
    StoredObject object(path, path, data.size());
    std::optional<ObjectAttributes> object_attributes;
    if (!attributes.empty())
        object_attributes = attributes;
    auto out = storage->writeObject(object, WriteMode::Rewrite, std::move(object_attributes), 4, {});
    writeString(data, *out);
    out->finalize();
    return object;
}
#endif

Poco::AutoPtr<Poco::Util::MapConfiguration> makeNativeGCSConfig()
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;
    config->setString("disk.object_storage_type", "gcs");
    config->setString("disk.bucket", "native-bucket");
    config->setString("disk.key_prefix", "clickhouse-data");
    config->setString("disk.endpoint", "storage.googleapis.com");
    config->setBool("disk.use_insecure_credentials_for_tests", true);
    config->setUInt64("disk.request_timeout_ms", 1000);
    return config;
}

class GCSObjectStorageConfigTest : public testing::Test
{
public:
    void SetUp() override
    {
        clearDiskRegistry();
        registerDisks(/* global_skip_access_check */ true);
    }

    void TearDown() override { clearDiskRegistry(); }
};

}

TEST(GCSDiskType, NativeGCSIdentity)
{
    DataSourceDescription native_gcs;
    native_gcs.type = DataSourceType::ObjectStorage;
    native_gcs.object_storage_type = ObjectStorageType::GCS;
    native_gcs.description = "storage.googleapis.com/native-bucket";

    DataSourceDescription native_gcs_with_trailing_slash;
    native_gcs_with_trailing_slash.type = DataSourceType::ObjectStorage;
    native_gcs_with_trailing_slash.object_storage_type = ObjectStorageType::GCS;
    native_gcs_with_trailing_slash.description = "storage.googleapis.com/native-bucket/";

    DataSourceDescription s3_compatible_gcs;
    s3_compatible_gcs.type = DataSourceType::ObjectStorage;
    s3_compatible_gcs.object_storage_type = ObjectStorageType::S3;
    s3_compatible_gcs.description = "storage.googleapis.com/native-bucket";

    EXPECT_EQ("gcs", native_gcs.name());
    EXPECT_TRUE(native_gcs.sameKind(native_gcs_with_trailing_slash));
    EXPECT_FALSE(native_gcs.sameKind(s3_compatible_gcs));
}

TEST_F(GCSObjectStorageConfigTest, NativeGCSConfigUsesNativeFactoryEntry)
{
    auto config = makeNativeGCSConfig();

#if USE_GOOGLE_CLOUD
    auto storage
        = ObjectStorageFactory::instance().create("native_gcs_disk", *config, "disk", getContext().context, /* skip_access_check */ true);

    EXPECT_EQ(ObjectStorageType::GCS, storage->getType());
    EXPECT_NE(ObjectStorageType::S3, storage->getType());
    EXPECT_EQ("GCS", storage->getName());
    EXPECT_EQ("native_gcs_disk", storage->getDiskName());
    EXPECT_EQ("native-bucket", storage->getObjectsNamespace());
    EXPECT_EQ("native-bucket", storage->getRootPrefix());
    EXPECT_EQ("clickhouse-data/", storage->getCommonKeyPrefix());
    EXPECT_EQ("storage.googleapis.com/native-bucket", storage->getDescription());
#else
    try
    {
        (void)ObjectStorageFactory::instance().create(
            "native_gcs_disk", *config, "disk", getContext().context, /* skip_access_check */ true);
        FAIL() << "Native GCS object storage unexpectedly constructed without Google Cloud support";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::NOT_IMPLEMENTED, e.code());
        EXPECT_NE(std::string(e.message()).find("Native GCS gRPC support is not available"), std::string::npos);
    }
#endif
}

TEST_F(GCSObjectStorageConfigTest, NativeGCSConfigKeepsTableFunctionGCSDefinitionSeparate)
{
    EXPECT_STREQ("gcs", GCSDefinition::name);
    EXPECT_STREQ("GCS", GCSDefinition::storage_engine_name);
    EXPECT_STREQ("gcs", GCSDefinition::object_storage_type);

    auto config = makeNativeGCSConfig();
#if USE_GOOGLE_CLOUD
    auto storage
        = ObjectStorageFactory::instance().create("native_gcs_disk", *config, "disk", getContext().context, /* skip_access_check */ true);
    EXPECT_EQ(ObjectStorageType::GCS, storage->getType());
#else
    EXPECT_THROW(
        ObjectStorageFactory::instance().create("native_gcs_disk", *config, "disk", getContext().context, /* skip_access_check */ true),
        Exception);
#endif
}

TEST_F(GCSObjectStorageConfigTest, NativeGCSConfigRequiresBucket)
{
    auto config = makeNativeGCSConfig();
    config->remove("disk.bucket");

    EXPECT_THROW(
        ObjectStorageFactory::instance().create("native_gcs_disk", *config, "disk", getContext().context, /* skip_access_check */ true),
        Exception);
}

TEST(GCSXMLMultipartEndpoint, NormalizesDefaultDirectPathAndCustomEndpoint)
{
    EXPECT_EQ("https://storage.googleapis.com", GCS::normalizeXMLMultipartEndpoint("storage.googleapis.com", ""));
    EXPECT_EQ("https://storage.googleapis.com", GCS::normalizeXMLMultipartEndpoint("https://storage.googleapis.com", ""));
    EXPECT_EQ("https://storage.googleapis.com", GCS::normalizeXMLMultipartEndpoint("google-c2p:///storage.googleapis.com", ""));
    EXPECT_EQ("https://custom.googleapis.com", GCS::normalizeXMLMultipartEndpoint("storage.googleapis.com", "custom.googleapis.com"));
    EXPECT_EQ("https://custom.googleapis.com", GCS::normalizeXMLMultipartEndpoint("storage.googleapis.com", "https://custom.googleapis.com"));
    EXPECT_THROW(GCS::normalizeXMLMultipartEndpoint("storage.googleapis.com", "http://custom.googleapis.com"), Exception);
    EXPECT_THROW(GCS::normalizeXMLMultipartEndpoint("custom.googleapis.com", ""), Exception);
    EXPECT_THROW(GCS::normalizeXMLMultipartEndpoint("google-c2p:///other.googleapis.com", ""), Exception);
}

TEST(GCSXMLMultipartCredentialSeam, RepresentsCredentialModesHeadersAndRefresh)
{
    int token_requests = 0;
    GCS::CallbackXMLBearerTokenProvider provider(
        GCS::CredentialMode::ServiceAccountKey,
        [&token_requests]
        {
            ++token_requests;
            return GCS::XMLBearerToken{
                .token = "token-" + std::to_string(token_requests),
                .expiration = std::chrono::system_clock::now() + std::chrono::hours(1),
            };
        });

    EXPECT_EQ(GCS::CredentialMode::ServiceAccountKey, provider.getCredentialMode());
    EXPECT_EQ("token-1", provider.getToken().token);
    EXPECT_EQ("token-2", provider.getToken().token);

    GCS::ClientSettings default_settings;
    default_settings.user_project = "billing-project";
    auto default_xml_settings = GCS::makeXMLMultipartClientSettings(default_settings, "");
    EXPECT_EQ(GCS::CredentialMode::GoogleDefault, default_xml_settings.credential_mode);
    EXPECT_EQ("billing-project", default_xml_settings.user_project);

    auto headers = GCS::makeXMLMultipartHeaders(default_settings);
    ASSERT_EQ(1, headers.size());
    EXPECT_EQ("x-goog-user-project", headers.front().name);
    EXPECT_EQ("billing-project", headers.front().value);

    GCS::ClientSettings service_account_settings;
    service_account_settings.service_account_json = "{}";
    auto service_account_xml_settings = GCS::makeXMLMultipartClientSettings(service_account_settings, "");
    EXPECT_EQ(GCS::CredentialMode::ServiceAccountKey, service_account_xml_settings.credential_mode);

    GCS::CallbackXMLBearerTokenProvider empty_provider(
        GCS::CredentialMode::GoogleDefault,
        []
        {
            return GCS::XMLBearerToken{};
        });
    EXPECT_THROW(empty_provider.getToken(), Exception);
}

#if USE_AWS_S3
TEST(GCSXMLMultipartCredentialSeam, CreatesXMLMultipartClientWithFakeTokenProvider)
{
    GCS::ClientSettings settings;
    settings.user_project = "billing-project";
    settings.max_retry_attempts = 1;

    auto provider = std::make_shared<GCS::CallbackXMLBearerTokenProvider>(
        GCS::CredentialMode::GoogleDefault,
        []
        {
            return GCS::XMLBearerToken{
                .token = "fake-token",
                .expiration = std::chrono::system_clock::now() + std::chrono::hours(1),
            };
        });

    auto client = GCS::createXMLMultipartClient(settings, "", getContext().context, "native_gcs_disk", provider);

    ASSERT_NE(nullptr, client);
    EXPECT_EQ("https://storage.googleapis.com", client->getInitialEndpoint());
    EXPECT_TRUE(client->getCredentials().IsEmpty());
    EXPECT_EQ("fake-token", client->getGCSOAuthToken());

    auto empty_provider = std::make_shared<GCS::CallbackXMLBearerTokenProvider>(
        GCS::CredentialMode::GoogleDefault,
        []
        {
            return GCS::XMLBearerToken{};
        });
    auto empty_client = GCS::createXMLMultipartClient(settings, "", getContext().context, "native_gcs_disk", empty_provider);
    EXPECT_THROW(empty_client->getGCSOAuthToken(), Exception);
}
#endif

TEST_F(GCSObjectStorageConfigTest, NativeGCSWriteTransportDefaultsToGrpc)
{
    auto config = makeNativeGCSConfig();
    auto settings = getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context);

    EXPECT_EQ(GCS::WriteTransport::Grpc, settings.write_transport);
    EXPECT_TRUE(settings.xml_client_settings.endpoint.empty());
}

TEST_F(GCSObjectStorageConfigTest, NativeGCSGrpcTransportDoesNotValidateXMLEndpoint)
{
    auto config = makeNativeGCSConfig();
    config->setString("disk.endpoint", "google-c2p:///other.googleapis.com");

    auto settings = getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context);

    EXPECT_EQ(GCS::WriteTransport::Grpc, settings.write_transport);
    EXPECT_EQ("google-c2p:///other.googleapis.com", settings.client_settings.endpoint);
    EXPECT_TRUE(settings.xml_client_settings.endpoint.empty());
}

TEST_F(GCSObjectStorageConfigTest, NativeGCSXMLMultipartWriteTransportConfig)
{
    auto config = makeNativeGCSConfig();
    config->setString("disk.write_transport", "xml_multipart");
    config->setString("disk.xml_endpoint", "xml.storage.test");

#if USE_AWS_S3
    auto settings = getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context);
    EXPECT_EQ(GCS::WriteTransport::XMLMultipart, settings.write_transport);
    EXPECT_EQ("xml.storage.test", settings.xml_endpoint);
    EXPECT_EQ("https://xml.storage.test", settings.xml_client_settings.endpoint);
#else
    EXPECT_THROW(getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context), Exception);
#endif
}

TEST_F(GCSObjectStorageConfigTest, NativeGCSXMLMultipartRequestSettingsConfig)
{
    auto config = makeNativeGCSConfig();
    config->setString("disk.write_transport", "xml_multipart");
    config->setString("disk.xml_endpoint", "xml.storage.test");
    config->setUInt64("disk.xml_multipart.min_upload_part_size", 64 * 1024 * 1024);
    config->setUInt64("disk.xml_multipart.max_inflight_parts_for_one_file", 20);

#if USE_AWS_S3
    auto settings = getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context);
    EXPECT_EQ(64 * 1024 * 1024, settings.xml_request_settings[S3RequestSetting::min_upload_part_size].value);
    EXPECT_EQ(20, settings.xml_request_settings[S3RequestSetting::max_inflight_parts_for_one_file].value);
#else
    EXPECT_THROW(getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context), Exception);
#endif
}



TEST_F(GCSObjectStorageConfigTest, NativeGCSRejectsInvalidWriteTransport)
{
    auto config = makeNativeGCSConfig();
    config->setString("disk.write_transport", "obviously_not_a_transport");

    EXPECT_THROW(getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context), Exception);
}


#if USE_GOOGLE_CLOUD
TEST(GCSObjectStorageCore, FakeReadWriteListDeleteAndCopy)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    StoredObject object("clickhouse-data/object", "object");
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, ObjectAttributes{{"owner", "clickhouse"}}, 4, {});
        writeString("abcdef", *out);
        out->finalize();
    }

    ASSERT_TRUE(storage->exists(object));
    auto metadata = storage->getObjectMetadata(object.remote_path, /* with_tags */ true);
    EXPECT_EQ(6, metadata.size_bytes);
    EXPECT_EQ("clickhouse", metadata.attributes["owner"]);

    auto in = storage->readObject(object, {}, {});
    EXPECT_EQ("abcdef", readAll(*in));

    StoredObject bounded_object("clickhouse-data/object", "object", 3);
    auto bounded_in = storage->readObject(bounded_object, {}, {});
    EXPECT_EQ("abc", readAll(*bounded_in));
    ASSERT_FALSE(fake_stub->read_object_requests.empty());
    EXPECT_EQ(3, fake_stub->read_object_requests.back().read_limit());

    RelativePathsWithMetadata children;
    storage->listObjects("clickhouse-data/", children, 10);
    ASSERT_EQ(1, children.size());
    EXPECT_EQ("clickhouse-data/object", children.front()->relative_path);
    ASSERT_TRUE(children.front()->metadata.has_value());
    EXPECT_EQ(6, children.front()->metadata->size_bytes);

    StoredObject copied("clickhouse-data/copied", "copied");
    storage->copyObject(object, copied, {}, {}, {});
    auto copied_in = storage->readObject(copied, {}, {});
    EXPECT_EQ("abcdef", readAll(*copied_in));

    fake_stub->write_object_requests.clear();
    const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
    String large_payload(max_chunk + 7, 'x');
    StoredObject large_object("clickhouse-data/large", "large");
    {
        auto out = storage->writeObject(large_object, WriteMode::Rewrite, {}, large_payload.size(), {});
        out->write(large_payload.data(), large_payload.size());
        out->finalize();
    }
    ASSERT_EQ(1, fake_stub->write_object_requests.size());
    EXPECT_EQ(0, fake_stub->write_object_requests.front().write_offset());
    EXPECT_TRUE(fake_stub->write_object_requests.front().finish_write());
    ASSERT_TRUE(fake_stub->write_object_requests.front().has_checksummed_data());
    EXPECT_EQ(large_payload, cordToString(fake_stub->write_object_requests.front().checksummed_data().content()));

    EXPECT_THROW(storage->writeObject(object, WriteMode::Append, {}, 4, {}), Exception);

    storage->removeObjectIfExists(object);
    EXPECT_FALSE(storage->exists(object));
    storage->removeObjectIfExists(object);
    EXPECT_TRUE(storage->exists(copied));
}

TEST(GCSObjectStorageCore, XMLMultipartWriteTransportFailsClosedWithoutXMLClient)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(
        fake_stub,
        /* read_only */ false,
        {},
        {},
        "native-bucket",
        "storage.googleapis.com",
        GCS::WriteTransport::XMLMultipart);

    StoredObject object("clickhouse-data/xml-mode-object", "xml-mode-object");
    EXPECT_THROW(storage->writeObject(object, WriteMode::Rewrite, {}, 4, {}), Exception);
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
}

TEST(GCSObjectStorageCore, XMLMultipartWriteTransportRejectsIfMatchBeforeUpload)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(
        fake_stub,
        /* read_only */ false,
        {},
        {},
        "native-bucket",
        "storage.googleapis.com",
        GCS::WriteTransport::XMLMultipart);

    WriteSettings write_settings;
    write_settings.object_storage_write_if_match = "anything";
    StoredObject object("clickhouse-data/xml-mode-object", "xml-mode-object");
    EXPECT_THROW(storage->writeObject(object, WriteMode::Rewrite, {}, 4, write_settings), Exception);
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
}

TEST(GCSObjectStorageCore, XMLMultipartWriteTransportRejectsIfNoneMatchBeforeUpload)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(
        fake_stub,
        /* read_only */ false,
        {},
        {},
        "native-bucket",
        "storage.googleapis.com",
        GCS::WriteTransport::XMLMultipart);

    WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "*";
    StoredObject object("clickhouse-data/xml-mode-object", "xml-mode-object");
    EXPECT_THROW(storage->writeObject(object, WriteMode::Rewrite, {}, 4, write_settings), Exception);
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
}


#if USE_AWS_S3
TEST(GCSObjectStorageCore, XMLMultipartWriteTransportConstructsS3WriteBufferWithNativeKey)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();

    GCSObjectStorageSettings settings;
    settings.disk_name = "native_gcs_disk";
    settings.bucket = "native-bucket";
    settings.key_prefix = "clickhouse-data/";
    settings.description = "fake/native-bucket";
    settings.client_settings.endpoint = "storage.googleapis.com";
    settings.client_settings.user_project = "billing-project";
    settings.client_settings.use_insecure_credentials_for_tests = true;
    settings.client_settings.for_disk = true;
    settings.write_transport = GCS::WriteTransport::XMLMultipart;
    settings.xml_client_settings = GCS::makeXMLMultipartClientSettings(settings.client_settings, settings.xml_endpoint);

    auto high_level_client = makeFakeHighLevelClient(fake_stub, settings.client_settings);
    auto provider = std::make_shared<GCS::CallbackXMLBearerTokenProvider>(
        GCS::CredentialMode::GoogleDefault,
        []
        {
            return GCS::XMLBearerToken{
                .token = "fake-token",
                .expiration = std::chrono::system_clock::now() + std::chrono::hours(1),
            };
        });
    auto unique_xml_client = GCS::createXMLMultipartClient(
        settings.client_settings,
        settings.xml_endpoint,
        getContext().context,
        settings.disk_name,
        provider);
    std::shared_ptr<const S3::Client> xml_client = std::shared_ptr<S3::Client>(std::move(unique_xml_client));

    auto storage = std::make_shared<GCSObjectStorage>(std::move(settings), std::move(high_level_client), std::move(xml_client));
    StoredObject object("clickhouse-data/xml-mode-object", "xml-mode-object");
    auto out = storage->writeObject(object, WriteMode::Rewrite, ObjectAttributes{{"owner", "clickhouse"}}, 4, {});

    ASSERT_NE(nullptr, out);
    EXPECT_EQ("clickhouse-data/xml-mode-object", out->getFileName());
    out->cancel();
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
}
#endif


TEST(GCSObjectStorageRewriteCopy, SameStorageCopyUsesRewriteObject)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    auto source = writeFakeObject(storage, "clickhouse-data/rewrite-source", "abcdef", {{"owner", "source"}});

    fake_stub->read_object_requests.clear();
    fake_stub->write_object_requests.clear();
    fake_stub->rewrite_object_requests.clear();

    StoredObject destination("clickhouse-data/rewrite-destination", "rewrite-destination");
    storage->copyObject(source, destination, {}, {}, ObjectAttributes{{"owner", "copy"}});

    ASSERT_EQ(1, fake_stub->rewrite_object_requests.size());
    EXPECT_TRUE(fake_stub->read_object_requests.empty());
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
    const auto & request = fake_stub->rewrite_object_requests.front();
    EXPECT_EQ("projects/_/buckets/native-bucket", request.source_bucket());
    EXPECT_EQ(source.remote_path, request.source_object());
    EXPECT_EQ("projects/_/buckets/native-bucket", request.destination_bucket());
    EXPECT_EQ(destination.remote_path, request.destination_name());
    ASSERT_TRUE(request.has_destination());
    EXPECT_EQ("copy", request.destination().metadata().at("owner"));

    ASSERT_TRUE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    const auto & copied = fake_stub->objects.at(fakeObjectMapKey(destination.remote_path));
    EXPECT_EQ("abcdef", copied.data);
    EXPECT_EQ("copy", copied.metadata.metadata().at("owner"));
}

TEST(GCSObjectStorageRewriteCopy, RewriteMetadataAndPreconditions)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    addFakeObject(fake_stub, "clickhouse-data/precondition-source", "precondition-data");

    WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "*";
    StoredObject source("clickhouse-data/precondition-source", "precondition-source");
    StoredObject destination("clickhouse-data/precondition-destination", "precondition-destination");
    storage->copyObject(source, destination, {}, write_settings, ObjectAttributes{{"owner", "rewrite"}});

    ASSERT_EQ(1, fake_stub->rewrite_object_requests.size());
    const auto & request = fake_stub->rewrite_object_requests.front();
    EXPECT_TRUE(request.has_if_generation_match());
    EXPECT_EQ(0, request.if_generation_match());
    ASSERT_TRUE(request.has_destination());
    EXPECT_EQ("rewrite", request.destination().metadata().at("owner"));

    EXPECT_THROW(storage->copyObject(source, destination, {}, write_settings, {}), Exception);

    WriteSettings if_match_settings;
    if_match_settings.object_storage_write_if_match = "etag";
    const auto requests_before_if_match = fake_stub->rewrite_object_requests.size();
    EXPECT_THROW(storage->copyObject(source, StoredObject("clickhouse-data/if-match-copy", "if-match-copy"), {}, if_match_settings, {}), Exception);
    EXPECT_EQ(requests_before_if_match, fake_stub->rewrite_object_requests.size());

    WriteSettings unsupported_none_match;
    unsupported_none_match.object_storage_write_if_none_match = "etag";
    const auto requests_before_none_match = fake_stub->rewrite_object_requests.size();
    EXPECT_THROW(
        storage->copyObject(source, StoredObject("clickhouse-data/if-none-match-copy", "if-none-match-copy"), {}, unsupported_none_match, {}),
        Exception);
    EXPECT_EQ(requests_before_none_match, fake_stub->rewrite_object_requests.size());
}

TEST(GCSObjectStorageRewriteCopy, CompatibleGcsToGcsCopyUsesDestinationRewrite)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto source_storage = makeFakeGCSObjectStorage(fake_stub, false, {}, {}, "source-bucket");
    auto destination_storage = makeFakeGCSObjectStorage(fake_stub, false, {}, {}, "destination-bucket");
    addFakeObject(fake_stub, "clickhouse-data/cross-source", "cross-data", "source-bucket");

    StoredObject source("clickhouse-data/cross-source", "cross-source");
    StoredObject destination("clickhouse-data/cross-destination", "cross-destination");
    source_storage->copyObjectToAnotherObjectStorage(
        source, destination, {}, {}, *destination_storage, ObjectAttributes{{"owner", "destination"}});

    ASSERT_EQ(1, fake_stub->rewrite_object_requests.size());
    EXPECT_TRUE(fake_stub->read_object_requests.empty());
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
    const auto & request = fake_stub->rewrite_object_requests.front();
    EXPECT_EQ("projects/_/buckets/source-bucket", request.source_bucket());
    EXPECT_EQ("projects/_/buckets/destination-bucket", request.destination_bucket());

    ASSERT_TRUE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path, "destination-bucket")));
    const auto & copied = fake_stub->objects.at(fakeObjectMapKey(destination.remote_path, "destination-bucket"));
    EXPECT_EQ("cross-data", copied.data);
    EXPECT_EQ("destination", copied.metadata.metadata().at("owner"));
}

TEST(GCSObjectStorageRewriteCopy, SameEndpointDifferentAuthorityGcsCopyUsesGenericReadWrite)
{
    auto source_stub = std::make_shared<FakeGCSClient>();
    auto destination_stub = std::make_shared<FakeGCSClient>();
    GCS::ClientSettings source_settings;
    source_settings.user_project = "source-project";
    GCS::ClientSettings destination_settings;
    destination_settings.user_project = "destination-project";
    auto source_storage = makeFakeGCSObjectStorage(source_stub, false, source_settings, {}, "source-bucket");
    auto destination_storage = makeFakeGCSObjectStorage(destination_stub, false, destination_settings, {}, "destination-bucket");
    addFakeObject(source_stub, "clickhouse-data/generic-source", "generic-data", "source-bucket");

    StoredObject source("clickhouse-data/generic-source", "generic-source", 12);
    StoredObject destination("clickhouse-data/generic-destination", "generic-destination");
    source_storage->copyObjectToAnotherObjectStorage(source, destination, {}, {}, *destination_storage, {});

    EXPECT_TRUE(source_stub->rewrite_object_requests.empty());
    EXPECT_FALSE(source_stub->read_object_requests.empty());
    EXPECT_TRUE(destination_stub->rewrite_object_requests.empty());
    EXPECT_FALSE(destination_stub->write_object_requests.empty());
    ASSERT_TRUE(destination_stub->objects.contains(fakeObjectMapKey(destination.remote_path, "destination-bucket")));
    EXPECT_EQ("generic-data", destination_stub->objects.at(fakeObjectMapKey(destination.remote_path, "destination-bucket")).data);
}

TEST(GCSObjectStorageRewriteCopy, NonGcsDestinationUsesGenericReadWrite)
{
    auto source_stub = std::make_shared<FakeGCSClient>();
    auto source_storage = makeFakeGCSObjectStorage(source_stub);
    addFakeObject(source_stub, "clickhouse-data/local-source", "local-data");

    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    LocalObjectStorage local_storage(LocalObjectStorageSettings("local_disk", temp_dir.path(), false));
    const auto destination_path = (std::filesystem::path(temp_dir.path()) / "copied.bin").string();

    StoredObject source("clickhouse-data/local-source", "local-source", 10);
    StoredObject destination(destination_path, destination_path);
    source_storage->copyObjectToAnotherObjectStorage(source, destination, {}, {}, local_storage, {});

    EXPECT_TRUE(source_stub->rewrite_object_requests.empty());
    EXPECT_FALSE(source_stub->read_object_requests.empty());
    auto in = local_storage.readObject(destination, {}, {});
    EXPECT_EQ("local-data", readAll(*in));
}

TEST(GCSObjectStorageRewriteCopy, CrossGcsRewriteFailuresDoNotFallback)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto source_storage = makeFakeGCSObjectStorage(fake_stub, false, {}, {}, "source-bucket");
    auto destination_storage = makeFakeGCSObjectStorage(fake_stub, false, {}, {}, "destination-bucket");
    addFakeObject(fake_stub, "clickhouse-data/cross-failure-source", "cross-failure-data", "source-bucket");
    fake_stub->rewrite_object_status = grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "cross rewrite denied");

    StoredObject source("clickhouse-data/cross-failure-source", "cross-failure-source");
    StoredObject destination("clickhouse-data/cross-failure-destination", "cross-failure-destination");
    EXPECT_THROW(source_storage->copyObjectToAnotherObjectStorage(source, destination, {}, {}, *destination_storage, {}), Exception);

    ASSERT_EQ(1, fake_stub->rewrite_object_requests.size());
    EXPECT_TRUE(fake_stub->read_object_requests.empty());
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
    EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path, "destination-bucket")));
}

TEST(GCSObjectStorageRewriteCopy, RewriteFailuresDoNotFallback)
{
    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        StoredObject source("clickhouse-data/missing-source", "missing-source");
        StoredObject destination("clickhouse-data/missing-destination", "missing-destination");

        EXPECT_THROW(storage->copyObject(source, destination, {}, {}, {}), Exception);
        ASSERT_EQ(1, fake_stub->rewrite_object_requests.size());
        EXPECT_TRUE(fake_stub->read_object_requests.empty());
        EXPECT_TRUE(fake_stub->write_object_requests.empty());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    }

    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        addFakeObject(fake_stub, "clickhouse-data/permission-source", "permission-data");
        fake_stub->rewrite_object_status = grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "rewrite denied");
        StoredObject source("clickhouse-data/permission-source", "permission-source");
        StoredObject destination("clickhouse-data/permission-destination", "permission-destination");

        EXPECT_THROW(storage->copyObject(source, destination, {}, {}, {}), Exception);
        ASSERT_EQ(1, fake_stub->rewrite_object_requests.size());
        EXPECT_TRUE(fake_stub->read_object_requests.empty());
        EXPECT_TRUE(fake_stub->write_object_requests.empty());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    }

    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        google::storage::v2::RewriteResponse incomplete_response;
        incomplete_response.set_done(false);
        fake_stub->rewrite_object_responses = {incomplete_response};
        StoredObject source("clickhouse-data/incomplete-source", "incomplete-source");
        StoredObject destination("clickhouse-data/incomplete-destination", "incomplete-destination");

        EXPECT_THROW(storage->copyObject(source, destination, {}, {}, {}), Exception);
        ASSERT_GE(fake_stub->rewrite_object_requests.size(), 1);
        EXPECT_TRUE(fake_stub->read_object_requests.empty());
        EXPECT_TRUE(fake_stub->write_object_requests.empty());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    }
}

TEST(GCSObjectStorageCore, FakeIteratorStatusAndDeleteFailures)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    StoredObject first("clickhouse-data/a", "a");
    StoredObject second("clickhouse-data/b", "b");
    {
        auto out = storage->writeObject(first, WriteMode::Rewrite, {}, 4, {});
        writeString("a", *out);
        out->finalize();
    }
    {
        auto out = storage->writeObject(second, WriteMode::Rewrite, {}, 4, {});
        writeString("b", *out);
        out->finalize();
    }

    auto iterator = storage->iterate("clickhouse-data/", 1, /* with_tags */ false, {});
    ASSERT_TRUE(iterator->isValid());
    EXPECT_EQ("clickhouse-data/a", iterator->current()->relative_path);
    EXPECT_EQ(1, iterator->getAccumulatedSize());

    auto resumed_iterator = storage->iterate("clickhouse-data/", 10, /* with_tags */ false, first.remote_path);
    ASSERT_TRUE(resumed_iterator->isValid());
    EXPECT_EQ("clickhouse-data/b", resumed_iterator->current()->relative_path);

    fake_stub->get_object_status = grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "denied");
    EXPECT_THROW(storage->getObjectMetadata(first.remote_path, false), Exception);
    fake_stub->get_object_status = grpc::Status::OK;

    fake_stub->delete_object_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "unavailable");
    EXPECT_THROW(storage->removeObjectIfExists(first), Exception);
}

TEST(GCSObjectStorageReadBuffer, SequentialReadsUseSingleStreamAcrossBufferRefills)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    const String payload = "abcdefghijklmnopqrstuvwxyz";
    auto object = writeFakeObject(storage, "clickhouse-data/sequential", payload);
    auto settings = readSettings(4);

    fake_stub->read_object_requests.clear();
    auto in = storage->readObject(object, settings, {});
    EXPECT_EQ(payload, readAll(*in));

    ASSERT_EQ(1, fake_stub->read_object_requests.size());
    EXPECT_EQ(0, fake_stub->read_object_requests.front().read_offset());
    EXPECT_EQ(static_cast<int64_t>(payload.size()), fake_stub->read_object_requests.front().read_limit());


    const String multi_window_payload(100, 'm');
    auto multi_window_object = writeFakeObject(storage, "clickhouse-data/sequential-multi-window", multi_window_payload);
    settings.prefetch_buffer_size = 8;
    fake_stub->read_object_requests.clear();
    auto multi_window_in = storage->readObject(multi_window_object, settings, {});
    EXPECT_EQ(multi_window_payload, readAll(*multi_window_in));
    ASSERT_EQ(4, fake_stub->read_object_requests.size());
    EXPECT_EQ(0, fake_stub->read_object_requests[0].read_offset());
    EXPECT_EQ(32, fake_stub->read_object_requests[0].read_limit());
    EXPECT_EQ(32, fake_stub->read_object_requests[1].read_offset());
    EXPECT_EQ(32, fake_stub->read_object_requests[1].read_limit());
    EXPECT_EQ(64, fake_stub->read_object_requests[2].read_offset());
    EXPECT_EQ(32, fake_stub->read_object_requests[2].read_limit());
    EXPECT_EQ(96, fake_stub->read_object_requests[3].read_offset());
    EXPECT_EQ(4, fake_stub->read_object_requests[3].read_limit());
}

TEST(GCSObjectStorageReadBuffer, ReadHintAndPrefetchBoundSequentialWindows)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    const String payload = "abcdefghij";
    auto object = writeFakeObject(storage, "clickhouse-data/read-hint", payload);
    StoredObject unknown_size_object(object.remote_path, object.local_path);
    auto settings = readSettings(4);

    fake_stub->read_object_requests.clear();
    auto hinted_in = storage->readObject(unknown_size_object, settings, 6);
    EXPECT_EQ(payload, readAll(*hinted_in));
    ASSERT_EQ(2, fake_stub->read_object_requests.size());
    EXPECT_EQ(0, fake_stub->read_object_requests[0].read_offset());
    EXPECT_EQ(6, fake_stub->read_object_requests[0].read_limit());
    EXPECT_EQ(6, fake_stub->read_object_requests[1].read_offset());
    EXPECT_EQ(6, fake_stub->read_object_requests[1].read_limit());

    settings.remote_fs_prefetch = true;
    settings.prefetch_buffer_size = 6;
    fake_stub->read_object_requests.clear();
    auto prefetch_in = storage->readObject(unknown_size_object, settings, {});
    EXPECT_EQ(payload, readAll(*prefetch_in));
    ASSERT_EQ(2, fake_stub->read_object_requests.size());
    EXPECT_EQ(0, fake_stub->read_object_requests[0].read_offset());
    EXPECT_EQ(6, fake_stub->read_object_requests[0].read_limit());
    EXPECT_EQ(6, fake_stub->read_object_requests[1].read_offset());
    EXPECT_EQ(6, fake_stub->read_object_requests[1].read_limit());
}

TEST(GCSObjectStorageReadBuffer, SeekPositionAndOffsetContracts)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    auto object = writeFakeObject(storage, "clickhouse-data/seek", "0123456789");
    auto settings = readSettings(4);
    settings.remote_read_min_bytes_for_seek = 0;

    fake_stub->read_object_requests.clear();
    auto in = storage->readObject(object, settings, {});
    EXPECT_EQ(0, in->getPosition());
    EXPECT_EQ("012", readBytes(*in, 3));
    EXPECT_EQ(3, in->getPosition());
    EXPECT_EQ(4, in->getFileOffsetOfBufferEnd());
    ASSERT_EQ(1, fake_stub->read_object_requests.size());
    EXPECT_EQ(0, fake_stub->read_object_requests.back().read_offset());

    EXPECT_EQ(5, in->seek(5, SEEK_SET));
    EXPECT_EQ("56", readBytes(*in, 2));
    ASSERT_EQ(2, fake_stub->read_object_requests.size());
    EXPECT_EQ(5, fake_stub->read_object_requests.back().read_offset());

    EXPECT_EQ(4, in->seek(-3, SEEK_CUR));
    EXPECT_EQ("45", readBytes(*in, 2));
    ASSERT_EQ(3, fake_stub->read_object_requests.size());
    EXPECT_EQ(4, fake_stub->read_object_requests.back().read_offset());

    EXPECT_THROW(in->seek(-100, SEEK_CUR), Exception);
    EXPECT_THROW(in->seek(11, SEEK_SET), Exception);
}

TEST(GCSObjectStorageReadBuffer, RangeReadsAndEOF)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    auto settings = readSettings(4);

    auto empty = writeFakeObject(storage, "clickhouse-data/empty", "");
    EXPECT_EQ("", readAll(*storage->readObject(empty, settings, {})));

    auto exact = writeFakeObject(storage, "clickhouse-data/exact", "abcd");
    auto exact_in = storage->readObject(exact, settings, {});
    EXPECT_TRUE(exact_in->supportsRightBoundedReads());
    ASSERT_TRUE(exact_in->getRemoteFileSize().has_value());
    EXPECT_EQ(4, *exact_in->getRemoteFileSize());
    EXPECT_EQ("abcd", readAll(*exact_in));

    auto object = writeFakeObject(storage, "clickhouse-data/range", "abcdefghij");
    fake_stub->read_object_requests.clear();
    auto in = storage->readObject(object, settings, {});
    String buffer(4, '\0');
    EXPECT_EQ(4, in->readBigAt(buffer.data(), buffer.size(), 2, {}));
    EXPECT_EQ("cdef", buffer);
    ASSERT_FALSE(fake_stub->read_object_requests.empty());
    EXPECT_EQ(2, fake_stub->read_object_requests.back().read_offset());
    EXPECT_EQ(4, fake_stub->read_object_requests.back().read_limit());

    StoredObject bounded(object.remote_path, object.local_path, 3);
    fake_stub->read_object_requests.clear();
    EXPECT_EQ("abc", readAll(*storage->readObject(bounded, settings, {})));
    ASSERT_FALSE(fake_stub->read_object_requests.empty());
    EXPECT_EQ(3, fake_stub->read_object_requests.front().read_limit());

    auto bounded_in = storage->readObject(bounded, settings, {});
    String bounded_buffer(4, '\0');
    EXPECT_EQ(1, bounded_in->readBigAt(bounded_buffer.data(), bounded_buffer.size(), 2, {}));
    EXPECT_EQ('c', bounded_buffer[0]);
    EXPECT_EQ(0, bounded_in->readBigAt(bounded_buffer.data(), bounded_buffer.size(), 3, {}));


    String cancellable_buffer(6, '\0');
    size_t callback_calls = 0;
    auto cancellable_in = storage->readObject(object, settings, {});
    EXPECT_EQ(
        4,
        cancellable_in->readBigAt(
            cancellable_buffer.data(),
            cancellable_buffer.size(),
            0,
            [&](size_t copied)
            {
                ++callback_calls;
                return copied >= 4;
            }));
    EXPECT_EQ("abcd", cancellable_buffer.substr(0, 4));
    EXPECT_EQ(1, callback_calls);


    auto cancel_stub = std::make_shared<FakeGCSClient>();
    auto cancel_storage = makeFakeGCSObjectStorage(cancel_stub);
    cancel_stub->use_object_map = false;
    google::storage::v2::ReadObjectResponse cancel_response;
    cancel_response.mutable_checksummed_data()->set_content("abcdef");
    cancel_stub->read_object_responses = {cancel_response};
    cancel_stub->read_object_finish_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "cancelled");
    String cancel_buffer(6, '\0');
    auto cancel_in = cancel_storage->readObject(StoredObject("clickhouse-data/cancel", "cancel", 6), settings, {});
    EXPECT_EQ(
        4,
        cancel_in->readBigAt(
            cancel_buffer.data(),
            cancel_buffer.size(),
            0,
            [](size_t copied) { return copied >= 4; }));
}
TEST(GCSObjectStorageReadBuffer, ReadFailuresAndAccounting)
{
    {
        resetProfileEvents();
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);

        auto in = storage->readObject(StoredObject("clickhouse-data/auth", "auth", 3), readSettings(4), {});
        EXPECT_THROW(readAll(*in), Exception);
        ASSERT_EQ(1, fake_stub->read_object_requests.size());
        EXPECT_EQ(1, profileEventValue(ProfileEvents::ReadBufferFromGCSRequestsErrors));
    }

    {
        resetProfileEvents();
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        fake_stub->use_object_map = false;
        fake_stub->read_object_null_streams = 1;

        auto in = storage->readObject(StoredObject("clickhouse-data/null-stream", "null-stream", 3), readSettings(4), {});
        EXPECT_THROW(readAll(*in), Exception);
        ASSERT_EQ(1, fake_stub->read_object_requests.size());
        EXPECT_EQ(1, profileEventValue(ProfileEvents::ReadBufferFromGCSRequestsErrors));
    }

    {
        resetProfileEvents();
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        fake_stub->use_object_map = false;
        google::storage::v2::ReadObjectResponse response;
        response.mutable_checksummed_data()->set_content("ab");
        fake_stub->read_object_responses = {response};

        auto in = storage->readObject(StoredObject("clickhouse-data/short", "short", 4), readSettings(4), {});
        EXPECT_THROW(readAll(*in), Exception);
        ASSERT_EQ(1, fake_stub->read_object_requests.size());
        EXPECT_EQ(1, profileEventValue(ProfileEvents::ReadBufferFromGCSRequestsErrors));
    }

    {
        resetProfileEvents();
        auto fake_stub = std::make_shared<FakeGCSClient>();
        CapturedBlobStorageLog captured_log;
        auto storage = makeFakeGCSObjectStorage(
            fake_stub,
            /* read_only */ false,
            {},
            [&](const String & disk_name) { return captured_log.createWriter(disk_name); });
        addFakeObject(fake_stub, "clickhouse-data/accounting", "abcd");

        auto settings = readSettings(4);
        settings.enable_blob_storage_log_for_read_operations = true;
        EXPECT_EQ("abcd", readAll(*storage->readObject(StoredObject("clickhouse-data/accounting", "accounting", 4), settings, {})));
        EXPECT_EQ(4, profileEventValue(ProfileEvents::ReadBufferFromGCSBytes));

        auto logs = captured_log.drain();
        ASSERT_EQ(1, logs.size());
        EXPECT_EQ(BlobStorageLogElement::EventType::Read, logs.front().event_type);
        EXPECT_EQ(4, logs.front().data_size);
        EXPECT_EQ(0, logs.front().error_code);
    }
}

TEST(GCSObjectStorageWriteBuffer, EmptySmallAndChunkedWrites)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    StoredObject empty("clickhouse-data/empty-write", "empty-write");
    {
        auto out = storage->writeObject(empty, WriteMode::Rewrite, {}, 4, {});
        out->finalize();
    }
    ASSERT_EQ(1, fake_stub->write_object_requests.size());
    EXPECT_TRUE(fake_stub->write_object_requests.front().finish_write());
    EXPECT_EQ(0, fake_stub->write_object_requests.front().write_offset());
    EXPECT_FALSE(fake_stub->write_object_requests.front().has_checksummed_data());

    fake_stub->write_object_requests.clear();
    StoredObject small("clickhouse-data/small-write", "small-write");
    {
        auto out = storage->writeObject(small, WriteMode::Rewrite, ObjectAttributes{{"owner", "clickhouse"}}, 8, {});
        writeString("abc", *out);
        out->finalize();
    }
    ASSERT_EQ(1, fake_stub->write_object_requests.size());
    const auto & small_request = fake_stub->write_object_requests.front();
    EXPECT_EQ(0, small_request.write_offset());
    EXPECT_TRUE(small_request.finish_write());
    EXPECT_EQ("abc", cordToString(small_request.checksummed_data().content()));
    EXPECT_EQ("clickhouse", small_request.write_object_spec().resource().metadata().at("owner"));

    fake_stub->write_object_requests.clear();
    const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
    String exact_payload(max_chunk, 'x');
    StoredObject exact("clickhouse-data/exact-chunk", "exact-chunk");
    {
        auto out = storage->writeObject(exact, WriteMode::Rewrite, {}, max_chunk * 2, {});
        out->write(exact_payload.data(), exact_payload.size());
        out->finalize();
    }
    ASSERT_EQ(1, fake_stub->write_object_requests.size());
    EXPECT_EQ(0, fake_stub->write_object_requests.front().write_offset());
    EXPECT_EQ(max_chunk, fake_stub->write_object_requests.front().checksummed_data().content().size());
    EXPECT_TRUE(fake_stub->write_object_requests.front().finish_write());
}

TEST(GCSObjectStorageWriteBuffer, RepeatedSyncAndFinalize)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    StoredObject object("clickhouse-data/sync", "sync");
    auto out = storage->writeObject(object, WriteMode::Rewrite, {}, 4, {});
    writeString("abc", *out);
    out->sync();
    writeString("def", *out);
    out->finalize();

    EXPECT_EQ(1, fake_stub->write_object_finish_calls.load());
    ASSERT_EQ(1, fake_stub->write_object_requests.size());
    EXPECT_EQ(0, fake_stub->write_object_requests.front().write_offset());
    EXPECT_TRUE(fake_stub->write_object_requests.front().finish_write());
    ASSERT_TRUE(fake_stub->write_object_requests.front().has_checksummed_data());
    EXPECT_EQ("abcdef", cordToString(fake_stub->write_object_requests.front().checksummed_data().content()));
}

TEST(GCSObjectStorageWriteBuffer, ParallelComposeWritesLargeObjects)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    EXPECT_TRUE(storage->supportParallelWrite());

    const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
    String payload(max_chunk * 2 + 1, 'x');
    StoredObject object("clickhouse-data/parallel-write", "parallel-write", payload.size());
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, ObjectAttributes{{"owner", "clickhouse"}}, max_chunk, {});
        writeString(payload, *out);
        out->finalize();
    }

    ASSERT_TRUE(fake_stub->objects.contains(fakeObjectMapKey(object.remote_path)));
    EXPECT_EQ(payload, fake_stub->objects.at(fakeObjectMapKey(object.remote_path)).data);
    ASSERT_EQ(1, fake_stub->compose_object_requests.size());
    const auto & compose_request = fake_stub->compose_object_requests.front();
    EXPECT_EQ(object.remote_path, compose_request.destination().name());
    EXPECT_EQ("clickhouse", compose_request.destination().metadata().at("owner"));
    EXPECT_GE(compose_request.source_objects_size(), 2);
    EXPECT_LE(compose_request.source_objects_size(), 32);
    EXPECT_GE(fake_stub->write_object_stream_creations.load(), 2);
    EXPECT_GE(fake_stub->delete_object_requests.size(), 2);
    for (const auto & source : compose_request.source_objects())
    {
        EXPECT_TRUE(source.name().starts_with(object.remote_path + ".clickhouse-gcs-compose-tmp/"));
        EXPECT_NE(object.remote_path, source.name());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(source.name())));
    }
}

TEST(GCSObjectStorageWriteBuffer, SyncAfterParallelModeKeepsComposedData)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
    String payload(max_chunk * 3, 's');
    payload += "tail";

    StoredObject object("clickhouse-data/sync-after-parallel", "sync-after-parallel", payload.size());
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, {}, max_chunk, {});
        out->write(payload.data(), max_chunk * 3);
        writeString("tail", *out);
        out->sync();
        out->finalize();
    }

    ASSERT_TRUE(fake_stub->objects.contains(fakeObjectMapKey(object.remote_path)));
    EXPECT_EQ(payload, fake_stub->objects.at(fakeObjectMapKey(object.remote_path)).data);
    EXPECT_FALSE(fake_stub->compose_object_requests.empty());
    EXPECT_GE(fake_stub->delete_object_requests.size(), 3);
}

TEST(GCSObjectStorageWriteBuffer, ParallelComposeTreeHandlesMoreThanThirtyTwoSources)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
    String payload(max_chunk * 33 + 1, 't');

    StoredObject object("clickhouse-data/compose-tree", "compose-tree", payload.size());
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, {}, max_chunk, {});
        writeString(payload, *out);
        out->finalize();
    }

    ASSERT_TRUE(fake_stub->objects.contains(fakeObjectMapKey(object.remote_path)));
    EXPECT_EQ(payload, fake_stub->objects.at(fakeObjectMapKey(object.remote_path)).data);
    ASSERT_GE(fake_stub->compose_object_requests.size(), 2);
    for (const auto & compose_request : fake_stub->compose_object_requests)
        EXPECT_LE(compose_request.source_objects_size(), 32);
    EXPECT_EQ(object.remote_path, fake_stub->compose_object_requests.back().destination().name());
    EXPECT_GT(fake_stub->delete_object_requests.size(), 32);
}

TEST(GCSObjectStorageWriteBuffer, ParallelUploadCanBeDisabled)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    WriteSettings write_settings;
    write_settings.s3_allow_parallel_part_upload = false;
    String payload = "abcdefghijkl";
    StoredObject object("clickhouse-data/single-stream-disabled", "single-stream-disabled", payload.size());
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, {}, 4, write_settings);
        writeString(payload, *out);
        out->finalize();
    }

    EXPECT_TRUE(fake_stub->compose_object_requests.empty());
    EXPECT_EQ(1, fake_stub->write_object_stream_creations.load());
    ASSERT_TRUE(fake_stub->objects.contains(fakeObjectMapKey(object.remote_path)));
    EXPECT_EQ(payload, fake_stub->objects.at(fakeObjectMapKey(object.remote_path)).data);
}

TEST(GCSObjectStorageWriteBuffer, ParallelComposeMetadataAndPreconditions)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
    String payload(max_chunk * 2 + 1, 'p');
    WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "*";
    StoredObject object("clickhouse-data/precondition-compose", "precondition-compose", payload.size());
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, ObjectAttributes{{"owner", "clickhouse"}}, max_chunk, write_settings);
        writeString(payload, *out);
        out->finalize();
    }

    ASSERT_FALSE(fake_stub->compose_object_requests.empty());
    const auto & final_compose = fake_stub->compose_object_requests.back();
    EXPECT_EQ(object.remote_path, final_compose.destination().name());
    EXPECT_EQ("clickhouse", final_compose.destination().metadata().at("owner"));
    EXPECT_TRUE(final_compose.has_if_generation_match());
    EXPECT_EQ(0, final_compose.if_generation_match());

    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, {}, max_chunk, write_settings);
        writeString(payload, *out);
        EXPECT_THROW(out->finalize(), Exception);
    }

    WriteSettings if_match_settings;
    if_match_settings.object_storage_write_if_match = "etag";
    EXPECT_THROW(
        storage->writeObject(StoredObject("clickhouse-data/if-match", "if-match"), WriteMode::Rewrite, {}, 4, if_match_settings),
        Exception);

    WriteSettings unsupported_none_match;
    unsupported_none_match.object_storage_write_if_none_match = "etag";
    EXPECT_THROW(
        storage->writeObject(
            StoredObject("clickhouse-data/if-none-match-etag", "if-none-match-etag"),
            WriteMode::Rewrite,
            {},
            4,
            unsupported_none_match),
        Exception);
}

TEST(GCSObjectStorageWriteBuffer, ParallelComposeFailuresCleanupTemps)
{
    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        fake_stub->compose_object_status = grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "compose failed");

        const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
        String payload(max_chunk * 2 + 1, 'c');
        StoredObject object("clickhouse-data/compose-fails", "compose-fails", payload.size());
        {
            auto out = storage->writeObject(object, WriteMode::Rewrite, {}, max_chunk, {});
            writeString(payload, *out);
            EXPECT_THROW(out->finalize(), Exception);
        }

        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(object.remote_path)));
        EXPECT_FALSE(fake_stub->delete_object_requests.empty());
    }

    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        fake_stub->write_object_finish_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "finish failed");

        const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
        String payload(max_chunk * 2 + 1, 'u');
        StoredObject object("clickhouse-data/chunk-fails", "chunk-fails", payload.size());
        {
            auto out = storage->writeObject(object, WriteMode::Rewrite, {}, max_chunk, {});
            writeString(payload, *out);
            EXPECT_THROW(out->finalize(), Exception);
        }

        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(object.remote_path)));
        EXPECT_TRUE(fake_stub->delete_object_requests.empty());
    }
}

TEST(GCSObjectStorageCore, ListPaginationAndExclusiveStartAfter)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);

    for (size_t i = 0; i != 1002; ++i)
        addFakeObject(fake_stub, "clickhouse-data/page-" + std::to_string(100000 + i));

    RelativePathsWithMetadata children;
    storage->listObjects("clickhouse-data/page-", children, 0);
    EXPECT_EQ(1002, children.size());
    ASSERT_GE(fake_stub->list_objects_requests.size(), 2);
    EXPECT_EQ("1000", fake_stub->list_objects_requests[1].page_token());

    auto iterator = storage->iterate("clickhouse-data/page-", 3, /* with_tags */ false, "clickhouse-data/page-100000");
    ASSERT_TRUE(iterator->isValid());
    EXPECT_EQ("clickhouse-data/page-100001", iterator->current()->relative_path);
    ASSERT_FALSE(fake_stub->list_objects_requests.empty());
    EXPECT_EQ("clickhouse-data/page-100000", fake_stub->list_objects_requests.back().lexicographic_start());
}

TEST(GCSObjectStorageCore, MetadataAndNotFoundSemantics)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    auto object = writeFakeObject(storage, "clickhouse-data/metadata", "payload", ObjectAttributes{{"owner", "clickhouse"}});

    EXPECT_TRUE(storage->exists(object));
    auto metadata = storage->getObjectMetadata(object.remote_path, /* with_tags */ true);
    EXPECT_EQ(7, metadata.size_bytes);
    EXPECT_EQ("clickhouse", metadata.attributes["owner"]);
    EXPECT_TRUE(metadata.tags.empty());

    EXPECT_FALSE(storage->tryGetObjectMetadata("clickhouse-data/missing", false).has_value());
    EXPECT_THROW(storage->getObjectMetadata("clickhouse-data/missing", false), Exception);

    fake_stub->get_object_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "unavailable");
    EXPECT_THROW(storage->exists(object), Exception);
}

TEST(GCSObjectStorageCore, ReadOnlyRejectsWritesAndInvalidNames)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto read_only_storage = makeFakeGCSObjectStorage(fake_stub, true);
    EXPECT_THROW(
        read_only_storage->writeObject(StoredObject("clickhouse-data/read-only", "read-only"), WriteMode::Rewrite, {}, 4, {}), Exception);

    auto storage = makeFakeGCSObjectStorage(fake_stub);
    EXPECT_THROW(storage->writeObject(StoredObject("/absolute", "absolute"), WriteMode::Rewrite, {}, 4, {}), Exception);
    EXPECT_THROW(storage->readObject(StoredObject("/absolute", "absolute"), {}, {}), Exception);
    EXPECT_THROW(storage->getObjectMetadata("/absolute", false), Exception);
    RelativePathsWithMetadata children;
    EXPECT_THROW(storage->listObjects("/absolute", children, 1), Exception);

    auto config = makeNativeGCSConfig();
    config->setString("disk.key_prefix", "/absolute");
    EXPECT_THROW(getGCSObjectStorageSettings("native_gcs_disk", *config, "disk", getContext().context), Exception);
}
TEST(GCSObjectStorageCore, ReadOnlyAndPreconditionsRejectBeforeFinalObjects)
{
    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto read_only_storage = makeFakeGCSObjectStorage(fake_stub, true);
        addFakeObject(fake_stub, "clickhouse-data/read-only-source", "read-only-data");
        StoredObject source("clickhouse-data/read-only-source", "read-only-source");
        StoredObject destination("clickhouse-data/read-only-destination", "read-only-destination");

        EXPECT_THROW(read_only_storage->writeObject(destination, WriteMode::Rewrite, {}, 4, {}), Exception);
        EXPECT_THROW(read_only_storage->copyObject(source, destination, {}, {}, {}), Exception);
        EXPECT_TRUE(fake_stub->write_object_requests.empty());
        EXPECT_TRUE(fake_stub->rewrite_object_requests.empty());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    }

    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto source_storage = makeFakeGCSObjectStorage(fake_stub, false, {}, {}, "source-bucket");
        auto read_only_destination = makeFakeGCSObjectStorage(fake_stub, true, {}, {}, "destination-bucket");
        addFakeObject(fake_stub, "clickhouse-data/cross-read-only-source", "cross-read-only-data", "source-bucket");
        StoredObject source("clickhouse-data/cross-read-only-source", "cross-read-only-source");
        StoredObject destination("clickhouse-data/cross-read-only-destination", "cross-read-only-destination");

        EXPECT_THROW(source_storage->copyObjectToAnotherObjectStorage(source, destination, {}, {}, *read_only_destination, {}), Exception);
        EXPECT_TRUE(fake_stub->rewrite_object_requests.empty());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path, "destination-bucket")));
    }

    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        const size_t max_chunk = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
        String payload(max_chunk * 2 + 1, 'w');
        WriteSettings if_match_settings;
        if_match_settings.object_storage_write_if_match = "etag";
        StoredObject destination("clickhouse-data/compose-if-match", "compose-if-match", payload.size());

        EXPECT_THROW(storage->writeObject(destination, WriteMode::Rewrite, {}, max_chunk, if_match_settings), Exception);
        EXPECT_TRUE(fake_stub->write_object_requests.empty());
        EXPECT_TRUE(fake_stub->compose_object_requests.empty());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    }

    {
        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        addFakeObject(fake_stub, "clickhouse-data/rewrite-if-none-match-source", "rewrite-data");
        WriteSettings unsupported_none_match;
        unsupported_none_match.object_storage_write_if_none_match = "etag";
        StoredObject source("clickhouse-data/rewrite-if-none-match-source", "rewrite-if-none-match-source");
        StoredObject destination("clickhouse-data/rewrite-if-none-match-destination", "rewrite-if-none-match-destination");

        EXPECT_THROW(storage->copyObject(source, destination, {}, unsupported_none_match, {}), Exception);
        EXPECT_TRUE(fake_stub->rewrite_object_requests.empty());
        EXPECT_FALSE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    }
}

TEST(GCSObjectStorageCore, RepresentativeSettingsRemainCompatible)
{
    resetProfileEvents();
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    const String payload = "abcdefghijklmnopqrstuvwxyz";
    auto object = writeFakeObject(storage, "clickhouse-data/settings-source", payload);

    auto read_settings = readSettings(4);
    read_settings.remote_fs_method = RemoteFSReadMethod::read;
    read_settings.remote_fs_prefetch = true;
    read_settings.prefetch_buffer_size = 12;
    read_settings.remote_read_min_bytes_for_seek = 2;
    read_settings.enable_filesystem_cache = false;
    read_settings.use_page_cache_for_object_storage = true;
    read_settings.read_from_page_cache_if_exists_otherwise_bypass_cache = true;
    read_settings.remote_throttler
        = blockingThrottler(ProfileEvents::RemoteReadThrottlerBytes, ProfileEvents::RemoteReadThrottlerSleepMicroseconds);

    fake_stub->read_object_requests.clear();
    auto in = storage->readObject(object, read_settings, {});
    EXPECT_EQ(payload, readAll(*in));
    ASSERT_EQ(1, fake_stub->read_object_requests.size());
    EXPECT_EQ(0, fake_stub->read_object_requests.front().read_offset());
    EXPECT_EQ(static_cast<int64_t>(payload.size()), fake_stub->read_object_requests.front().read_limit());
    EXPECT_EQ(payload.size(), profileEventValue(ProfileEvents::RemoteReadThrottlerBytes));

    resetProfileEvents();
    WriteSettings write_settings;
    write_settings.enable_filesystem_cache_on_write_operations = true;
    write_settings.use_adaptive_write_buffer = true;
    write_settings.adaptive_write_buffer_initial_size = 5;
    write_settings.s3_allow_parallel_part_upload = false;
    write_settings.remote_throttler
        = blockingThrottler(ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
    StoredObject destination("clickhouse-data/settings-destination", "settings-destination", payload.size());
    {
        auto out = storage->writeObject(destination, WriteMode::Rewrite, {}, 4, write_settings);
        writeString(payload, *out);
        out->finalize();
    }

    ASSERT_TRUE(fake_stub->objects.contains(fakeObjectMapKey(destination.remote_path)));
    EXPECT_EQ(payload, fake_stub->objects.at(fakeObjectMapKey(destination.remote_path)).data);
    EXPECT_EQ(payload.size(), profileEventValue(ProfileEvents::RemoteWriteThrottlerBytes));
}


TEST(GCSObjectStorageCore, DeleteNotFoundAndMultiDelete)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    auto first = writeFakeObject(storage, "clickhouse-data/delete-a", "a");
    auto second = writeFakeObject(storage, "clickhouse-data/delete-b", "b");
    StoredObject missing("clickhouse-data/delete-missing", "delete-missing");

    storage->removeObjectIfExists(missing);
    storage->removeObjectsIfExist({first, missing, second});

    EXPECT_FALSE(storage->exists(first));
    EXPECT_FALSE(storage->exists(second));
    EXPECT_FALSE(storage->exists(missing));
}
TEST(GCSObjectStorageObservability, ProfileEventsForDiskOperationsAndBuffers)
{
    resetProfileEvents();

    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage = makeFakeGCSObjectStorage(fake_stub);
    StoredObject object("clickhouse-data/observability", "clickhouse-data/observability", 7);

    WriteSettings write_settings;
    write_settings.remote_throttler
        = blockingThrottler(ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, {}, 4, write_settings);
        writeString("payload", *out);
        out->finalize();
    }

    EXPECT_TRUE(storage->exists(object));

    RelativePathsWithMetadata children;
    storage->listObjects("clickhouse-data/", children, 10);
    EXPECT_EQ(1, children.size());

    auto read_settings = readSettings(16);
    read_settings.remote_throttler
        = blockingThrottler(ProfileEvents::RemoteReadThrottlerBytes, ProfileEvents::RemoteReadThrottlerSleepMicroseconds);
    auto in = storage->readObject(object, read_settings, {});
    EXPECT_EQ("payload", readAll(*in));

    storage->removeObjectIfExists(object);

    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSGetObject));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSGetObject));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSListObjects));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSListObjects));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSDeleteObject));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSDeleteObject));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSReadObject));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSReadObject));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSWriteObject));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSWriteObject));
    EXPECT_EQ(7, profileEventValue(ProfileEvents::ReadBufferFromGCSBytes));
    EXPECT_EQ(7, profileEventValue(ProfileEvents::WriteBufferFromGCSBytes));
    EXPECT_EQ(7, profileEventValue(ProfileEvents::RemoteReadThrottlerBytes));
    EXPECT_GT(profileEventValue(ProfileEvents::RemoteReadThrottlerSleepMicroseconds), 0);
    EXPECT_EQ(7, profileEventValue(ProfileEvents::RemoteWriteThrottlerBytes));
    EXPECT_GT(profileEventValue(ProfileEvents::RemoteWriteThrottlerSleepMicroseconds), 0);
    EXPECT_GT(profileEventValue(ProfileEvents::ReadBufferFromGCSInitMicroseconds), 0);
    EXPECT_GT(profileEventValue(ProfileEvents::ReadBufferFromGCSMicroseconds), 0);
    EXPECT_GT(profileEventValue(ProfileEvents::WriteBufferFromGCSMicroseconds), 0);
}

TEST(GCSObjectStorageObservability, ReadAndWriteBufferFailuresAccountErrors)
{
    {
        resetProfileEvents();

        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        fake_stub->use_object_map = false;

        google::storage::v2::ReadObjectResponse response;
        response.mutable_checksummed_data()->set_content("abc");
        fake_stub->read_object_responses = {response};
        fake_stub->read_object_finish_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "unavailable");

        auto in = storage->readObject(StoredObject("clickhouse-data/read-fails", "read-fails", 3), readSettings(4), {});
        EXPECT_THROW(readAll(*in), Exception);
        EXPECT_EQ(1, profileEventValue(ProfileEvents::ReadBufferFromGCSRequestsErrors));
        EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSReadRequestsErrors));
        EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSReadRequestsErrors));
    }

    {
        resetProfileEvents();

        auto fake_stub = std::make_shared<FakeGCSClient>();
        auto storage = makeFakeGCSObjectStorage(fake_stub);
        fake_stub->use_object_map = false;
        fake_stub->write_object_finish_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "finish failed");

        auto out = storage->writeObject(StoredObject("clickhouse-data/write-fails", "write-fails"), WriteMode::Rewrite, {}, 4, {});
        writeString("abc", *out);
        EXPECT_THROW(out->finalize(), Exception);
        EXPECT_EQ(1, profileEventValue(ProfileEvents::WriteBufferFromGCSRequestsErrors));
        EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSWriteRequestsErrors));
        EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSWriteRequestsErrors));
    }
}

TEST(GCSObjectStorageObservability, RetryThrottleAndRequestThrottlerEventsReachDiskPath)
{
    resetProfileEvents();

    auto fake_stub = std::make_shared<FakeGCSClient>();
    addFakeObject(fake_stub, "clickhouse-data/retry", "payload");
    fake_stub->get_object_statuses = {grpc::Status(grpc::StatusCode::UNAVAILABLE, "temporarily unavailable")};
    fake_stub->list_objects_statuses = {grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "quota exhausted")};

    GCS::ClientSettings client_settings;
    client_settings.max_retry_attempts = 2;
    client_settings.request_throttler.get_throttler
        = blockingThrottler(ProfileEvents::GCSGetRequestThrottlerCount, ProfileEvents::GCSGetRequestThrottlerSleepMicroseconds);
    client_settings.request_throttler.put_throttler
        = blockingThrottler(ProfileEvents::GCSPutRequestThrottlerCount, ProfileEvents::GCSPutRequestThrottlerSleepMicroseconds);

    auto storage = makeFakeGCSObjectStorage(fake_stub, false, std::move(client_settings));
    EXPECT_TRUE(storage->exists(StoredObject("clickhouse-data/retry", "retry", 7)));

    RelativePathsWithMetadata children;
    storage->listObjects("clickhouse-data/", children, 10);
    EXPECT_EQ(1, children.size());

    EXPECT_EQ(2, fake_stub->get_object_requests.size());
    EXPECT_EQ(2, fake_stub->list_objects_requests.size());
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSReadRequestRetryableErrors));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSReadRequestRetryableErrors));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSReadRequestsErrors));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSReadRequestsErrors));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSWriteRequestRetryableErrors));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSWriteRequestRetryableErrors));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::GCSWriteRequestsThrottling));
    EXPECT_EQ(1, profileEventValue(ProfileEvents::DiskGCSWriteRequestsThrottling));
    EXPECT_EQ(2, profileEventValue(ProfileEvents::GCSGetRequestThrottlerCount));
    EXPECT_EQ(2, profileEventValue(ProfileEvents::DiskGCSGetRequestThrottlerCount));
    EXPECT_EQ(2, profileEventValue(ProfileEvents::GCSPutRequestThrottlerCount));
    EXPECT_EQ(2, profileEventValue(ProfileEvents::DiskGCSPutRequestThrottlerCount));
    EXPECT_GT(profileEventValue(ProfileEvents::GCSGetRequestThrottlerSleepMicroseconds), 0);
    EXPECT_GT(profileEventValue(ProfileEvents::DiskGCSGetRequestThrottlerSleepMicroseconds), 0);
    EXPECT_GT(profileEventValue(ProfileEvents::GCSPutRequestThrottlerSleepMicroseconds), 0);
    EXPECT_GT(profileEventValue(ProfileEvents::DiskGCSPutRequestThrottlerSleepMicroseconds), 0);
}

TEST(GCSObjectStorageObservability, BlobStorageLogRowsCaptureReadUploadDeleteAndErrors)
{
    CapturedBlobStorageLog captured_log;
    auto fake_stub = std::make_shared<FakeGCSClient>();
    auto storage
        = makeFakeGCSObjectStorage(fake_stub, false, {}, [&](const String & disk_name) { return captured_log.createWriter(disk_name); });

    StoredObject object("clickhouse-data/blob-log", "local/blob-log", 7);
    {
        auto out = storage->writeObject(object, WriteMode::Rewrite, {}, 4, {});
        writeString("payload", *out);
        out->finalize();
    }

    auto read_settings = readSettings(16);
    read_settings.enable_blob_storage_log_for_read_operations = true;
    auto in = storage->readObject(object, read_settings, {});
    EXPECT_EQ("payload", readAll(*in));

    storage->removeObjectIfExists(object);

    fake_stub->use_object_map = false;
    fake_stub->read_object_responses.clear();
    fake_stub->read_object_finish_status = grpc::Status(grpc::StatusCode::NOT_FOUND, "missing");
    auto failed_in = storage->readObject(StoredObject("clickhouse-data/missing", "local/missing", 3), read_settings, {});
    EXPECT_THROW(readAll(*failed_in), Exception);

    auto logs = captured_log.drain();
    ASSERT_GE(logs.size(), 4);

    const auto find_log = [&](BlobStorageLogElement::EventType event_type, const String & remote_path) -> const BlobStorageLogElement *
    {
        for (const auto & log : logs)
        {
            if (log.event_type == event_type && log.remote_path == remote_path)
                return &log;
        }
        return nullptr;
    };

    const auto * upload = find_log(BlobStorageLogElement::EventType::Upload, "clickhouse-data/blob-log");
    ASSERT_NE(nullptr, upload);
    EXPECT_EQ("native_gcs_disk", upload->disk_name);
    EXPECT_EQ("native-bucket", upload->bucket);
    EXPECT_EQ("local/blob-log", upload->local_path);
    EXPECT_EQ(7, upload->data_size);
    EXPECT_EQ(0, upload->error_code);
    EXPECT_GT(upload->elapsed_microseconds, 0);

    const auto * read = find_log(BlobStorageLogElement::EventType::Read, "clickhouse-data/blob-log");
    ASSERT_NE(nullptr, read);
    EXPECT_EQ("native_gcs_disk", read->disk_name);
    EXPECT_EQ("native-bucket", read->bucket);
    EXPECT_EQ("local/blob-log", read->local_path);
    EXPECT_EQ(7, read->data_size);
    EXPECT_EQ(0, read->error_code);
    EXPECT_GT(read->elapsed_microseconds, 0);

    const auto * delete_log = find_log(BlobStorageLogElement::EventType::Delete, "clickhouse-data/blob-log");
    ASSERT_NE(nullptr, delete_log);
    EXPECT_EQ("native_gcs_disk", delete_log->disk_name);
    EXPECT_EQ("native-bucket", delete_log->bucket);
    EXPECT_EQ("local/blob-log", delete_log->local_path);
    EXPECT_EQ(7, delete_log->data_size);
    EXPECT_EQ(0, delete_log->error_code);
    EXPECT_GT(delete_log->elapsed_microseconds, 0);

    const auto * failed_read = find_log(BlobStorageLogElement::EventType::Read, "clickhouse-data/missing");
    ASSERT_NE(nullptr, failed_read);
    EXPECT_EQ("native_gcs_disk", failed_read->disk_name);
    EXPECT_EQ("native-bucket", failed_read->bucket);
    EXPECT_EQ("local/missing", failed_read->local_path);
    EXPECT_EQ(0, failed_read->data_size);
    EXPECT_NE(0, failed_read->error_code);
    EXPECT_NE(std::string::npos, failed_read->error_message.find("NotFound"));
}


TEST(GCSObjectStorageCore, FakeDiskObjectStorageLocalMetadataScenario)
{
    auto fake_stub = std::make_shared<FakeGCSClient>();
    ObjectStoragePtr object_storage = makeFakeGCSObjectStorage(fake_stub);

    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    const auto metadata_path = std::filesystem::path(temp_dir.path()) / "metadata";
    std::filesystem::create_directories(metadata_path);
    auto metadata_disk = std::make_shared<DiskLocal>("metadata_disk", metadata_path);
    MetadataStoragePtr metadata_storage = std::make_shared<MetadataStorageFromDisk>(
        metadata_disk,
        "/",
        object_storage->createKeyGenerator(),
        /* persist_removal_queue_ */ true,
        /* removal_log_compaction_threshold_ */ 1000);

    std::unordered_map<Location, LocationInfo> cluster_registry = {{"main", {true, true, ""}}};
    std::unordered_map<Location, ObjectStoragePtr> object_storage_registry = {{"main", object_storage}};
    auto cluster = std::make_shared<ClusterConfiguration>("native_gcs", std::move(cluster_registry));
    auto object_storages = std::make_shared<ObjectStorageRouter>(std::move(object_storage_registry));
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    auto disk = std::make_shared<DiskObjectStorage>(
        "native_gcs", std::move(cluster), std::move(metadata_storage), std::move(object_storages), nullptr, *config, "");

    disk->createDirectory("dir");
    {
        auto out = disk->writeFile("dir/file.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString("payload", *out);
        out->finalize();
    }

    EXPECT_TRUE(disk->existsFile("dir/file.txt"));
    EXPECT_EQ(7, disk->getFileSize("dir/file.txt"));
    auto in = disk->readFile("dir/file.txt", {}, {});
    EXPECT_EQ("payload", readAll(*in));

    std::vector<String> files;
    disk->listFiles("dir", files);
    EXPECT_EQ(std::vector<String>{"file.txt"}, files);
    fake_stub->read_object_requests.clear();
    fake_stub->write_object_requests.clear();
    fake_stub->rewrite_object_requests.clear();
    disk->copyFile("dir/file.txt", *disk, "dir/copied.txt", {}, {}, {});
    ASSERT_EQ(1, fake_stub->rewrite_object_requests.size());
    EXPECT_TRUE(fake_stub->read_object_requests.empty());
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
    auto copied = disk->readFile("dir/copied.txt", {}, {});
    EXPECT_EQ("payload", readAll(*copied));

    fake_stub->read_object_requests.clear();
    fake_stub->write_object_requests.clear();
    fake_stub->rewrite_object_requests.clear();
    disk->moveFile("dir/copied.txt", "dir/moved.txt");
    EXPECT_FALSE(disk->existsFile("dir/copied.txt"));
    EXPECT_TRUE(disk->existsFile("dir/moved.txt"));
    EXPECT_TRUE(fake_stub->rewrite_object_requests.empty());
    EXPECT_TRUE(fake_stub->write_object_requests.empty());
    auto moved = disk->readFile("dir/moved.txt", {}, {});
    EXPECT_EQ("payload", readAll(*moved));

    {
        auto out = disk->writeFile("dir/file.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString("rewritten", *out);
        out->finalize();
    }
    auto rewritten = disk->readFile("dir/file.txt", {}, {});
    EXPECT_EQ("rewritten", readAll(*rewritten));
    disk->removeFile("dir/file.txt");
    EXPECT_FALSE(disk->existsFile("dir/file.txt"));
    disk->removeFile("dir/moved.txt");
    EXPECT_FALSE(disk->existsFile("dir/moved.txt"));
    disk->shutdown();
}
#endif

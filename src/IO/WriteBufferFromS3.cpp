#include "config.h"
#include <Common/CurrentThread.h>

#if USE_AWS_S3

#include <IO/StdIStreamFromMemory.h>
#include <IO/WriteBufferFromS3.h>

#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>
#include <Interpreters/FileCache/FileCache.h>

#include <IO/WriteHelpers.h>
#include <IO/S3Common.h>
#include <IO/S3/Requests.h>
#include <IO/S3/getObjectInfo.h>
#include <Common/BlobStorageLogWriter.h>

#include <utility>


namespace ProfileEvents
{
    extern const Event WriteBufferFromS3Bytes;
    extern const Event WriteBufferFromS3Microseconds;
    extern const Event WriteBufferFromS3RequestsErrors;
    extern const Event WriteBufferFromS3WaitInflightLimitMicroseconds;

    extern const Event S3CreateMultipartUpload;
    extern const Event S3CompleteMultipartUpload;
    extern const Event S3AbortMultipartUpload;
    extern const Event S3UploadPart;
    extern const Event S3PutObject;

    extern const Event DiskS3CreateMultipartUpload;
    extern const Event DiskS3CompleteMultipartUpload;
    extern const Event DiskS3AbortMultipartUpload;
    extern const Event DiskS3UploadPart;
    extern const Event DiskS3PutObject;

    extern const Event WriteBufferFromGCSBytes;
    extern const Event WriteBufferFromGCSMicroseconds;
    extern const Event WriteBufferFromGCSRequestsErrors;
    extern const Event WriteBufferFromGCSWaitInflightLimitMicroseconds;
    extern const Event GCSWriteObject;
    extern const Event DiskGCSWriteObject;
}

namespace DB
{

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool check_objects_after_upload;
    extern const S3RequestSettingsUInt64 max_inflight_parts_for_one_file;
    extern const S3RequestSettingsUInt64 max_part_number;
    extern const S3RequestSettingsUInt64 max_single_part_upload_size;
    extern const S3RequestSettingsUInt64 max_unexpected_write_error_retries;
    extern const S3RequestSettingsUInt64 max_upload_part_size;
    extern const S3RequestSettingsUInt64 min_upload_part_size;
    extern const S3RequestSettingsString storage_class_name;
    extern const S3RequestSettingsUInt64 strict_upload_part_size;
    extern const S3RequestSettingsUInt64 upload_part_size_multiply_factor;
    extern const S3RequestSettingsUInt64 upload_part_size_multiply_parts_count_threshold;
}

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

struct WriteBufferFromS3::PartData
{
    Memory<> memory;
    size_t data_size = 0;

    std::shared_ptr<std::iostream> createAwsBuffer()
    {
        auto buffer = std::make_shared<StdIStreamFromMemory>(memory.data(), data_size);
        buffer->exceptions(std::ios::badbit);
        return buffer;
    }

    bool isEmpty() const
    {
        return data_size == 0;
    }
};

BufferAllocationPolicyPtr createBufferAllocationPolicy(const S3::S3RequestSettings & settings)
{
    BufferAllocationPolicy::Settings allocation_settings;
    allocation_settings.strict_size = settings[S3RequestSetting::strict_upload_part_size];
    allocation_settings.min_size = settings[S3RequestSetting::min_upload_part_size];
    allocation_settings.max_size = settings[S3RequestSetting::max_upload_part_size];
    allocation_settings.multiply_factor = settings[S3RequestSetting::upload_part_size_multiply_factor];
    allocation_settings.multiply_parts_count_threshold = settings[S3RequestSetting::upload_part_size_multiply_parts_count_threshold];
    allocation_settings.max_single_size = settings[S3RequestSetting::max_single_part_upload_size];

    return BufferAllocationPolicy::create(allocation_settings);
}

struct WriteBufferProfileEvents
{
    ProfileEvents::Event bytes;
    ProfileEvents::Event microseconds;
    ProfileEvents::Event errors;
    ProfileEvents::Event wait_inflight_microseconds;
    ProfileEvents::Event put_object;
    ProfileEvents::Event create_multipart_upload;
    ProfileEvents::Event upload_part;
    ProfileEvents::Event complete_multipart_upload;
    ProfileEvents::Event abort_multipart_upload;
    ProfileEvents::Event disk_put_object;
    ProfileEvents::Event disk_create_multipart_upload;
    ProfileEvents::Event disk_upload_part;
    ProfileEvents::Event disk_complete_multipart_upload;
    ProfileEvents::Event disk_abort_multipart_upload;
};

const WriteBufferProfileEvents & writeBufferProfileEvents(S3::ProfileEventsNamespace profile_events_namespace)
{
    static const WriteBufferProfileEvents s3_events{
        ProfileEvents::WriteBufferFromS3Bytes,
        ProfileEvents::WriteBufferFromS3Microseconds,
        ProfileEvents::WriteBufferFromS3RequestsErrors,
        ProfileEvents::WriteBufferFromS3WaitInflightLimitMicroseconds,
        ProfileEvents::S3PutObject,
        ProfileEvents::S3CreateMultipartUpload,
        ProfileEvents::S3UploadPart,
        ProfileEvents::S3CompleteMultipartUpload,
        ProfileEvents::S3AbortMultipartUpload,
        ProfileEvents::DiskS3PutObject,
        ProfileEvents::DiskS3CreateMultipartUpload,
        ProfileEvents::DiskS3UploadPart,
        ProfileEvents::DiskS3CompleteMultipartUpload,
        ProfileEvents::DiskS3AbortMultipartUpload,
    };
    static const WriteBufferProfileEvents gcs_events{
        ProfileEvents::WriteBufferFromGCSBytes,
        ProfileEvents::WriteBufferFromGCSMicroseconds,
        ProfileEvents::WriteBufferFromGCSRequestsErrors,
        ProfileEvents::WriteBufferFromGCSWaitInflightLimitMicroseconds,
        ProfileEvents::GCSWriteObject,
        ProfileEvents::GCSWriteObject,
        ProfileEvents::GCSWriteObject,
        ProfileEvents::GCSWriteObject,
        ProfileEvents::GCSWriteObject,
        ProfileEvents::DiskGCSWriteObject,
        ProfileEvents::DiskGCSWriteObject,
        ProfileEvents::DiskGCSWriteObject,
        ProfileEvents::DiskGCSWriteObject,
        ProfileEvents::DiskGCSWriteObject,
    };
    return profile_events_namespace == S3::ProfileEventsNamespace::GCS ? gcs_events : s3_events;
}

String writeBufferLogName(S3::ProfileEventsNamespace profile_events_namespace)
{
    return profile_events_namespace == S3::ProfileEventsNamespace::GCS ? "WriteBufferFromGCS" : "WriteBufferFromS3";
}

String writeBufferObjectStorageName(S3::ProfileEventsNamespace profile_events_namespace)
{
    return profile_events_namespace == S3::ProfileEventsNamespace::GCS ? "GCS" : "S3";
}


WriteBufferFromS3::WriteBufferFromS3(
    std::shared_ptr<const S3::Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    size_t buf_size_,
    const S3::S3RequestSettings & request_settings_,
    BlobStorageLogWriterPtr blob_log_,
    std::optional<std::map<String, String>> object_metadata_,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_,
    const WriteSettings & write_settings_,
    S3::ProfileEventsNamespace profile_events_namespace_)
    : WriteBufferFromFileBase(std::min(buf_size_, static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE)), nullptr, 0)
    , bucket(bucket_)
    , key(key_)
    , request_settings(request_settings_)
    , write_settings(write_settings_)
    , client_ptr(std::move(client_ptr_))
    , object_metadata(std::move(object_metadata_))
    , profile_events_namespace(profile_events_namespace_)
    , log_name(writeBufferLogName(profile_events_namespace))
    , object_storage_name(writeBufferObjectStorageName(profile_events_namespace))
    , log(getLogger(log_name))
    , limited_log(std::make_shared<LogSeriesLimiter>(log, 1, 5))
    , buffer_allocation_policy(createBufferAllocationPolicy(request_settings))
    , task_tracker(
          std::make_unique<TaskTracker>(
              std::move(schedule_),
              request_settings[S3RequestSetting::max_inflight_parts_for_one_file],
              limited_log,
              writeBufferProfileEvents(profile_events_namespace).wait_inflight_microseconds))
    , blob_log(std::move(blob_log_))
{
    LOG_TRACE(limited_log, "Create {}, {}", log_name, getShortLogDetails());
    allocateBuffer();
}

void WriteBufferFromS3::nextImpl()
{
    if (is_prefinalized)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot write to prefinalized buffer for {}, the file could have been created with PutObjectRequest",
            object_storage_name);

    /// Make sense to call waitIfAny before adding new async task to check if there is an exception
    /// The faster the exception is propagated the lesser time is spent for cancellation
    /// Despite the fact that `task_tracker->add()` collects tasks statuses and propagates their exceptions
    /// that call is necessary for the case when the is no in-flight limitation and therefore `task_tracker->add()` doesn't wait anything
    task_tracker->waitIfAny();

    hidePartialData();

    reallocateFirstBuffer();

    if (available() > 0)
        return;

    detachBuffer();

    if (!multipart_upload_id.empty() || detached_part_data.size() > 1)
        writeMultipartUpload();

    allocateBuffer();
}

void WriteBufferFromS3::preFinalize()
{
    if (is_prefinalized)
        return;

    LOG_TEST(limited_log, "preFinalize {}. {}", log_name, getShortLogDetails());
    /// This function should not be run again if an exception has occurred
    is_prefinalized = true;

    hidePartialData();

    if (hidden_size > 0)
        detachBuffer();
    setFakeBufferWhenPreFinalized();

    bool do_single_part_upload = false;

    if (multipart_upload_id.empty() && detached_part_data.size() <= 1)
    {
        if (detached_part_data.empty() || detached_part_data.front().data_size <= request_settings[S3RequestSetting::max_single_part_upload_size])
            do_single_part_upload = true;
    }

    if (do_single_part_upload)
    {
        if (detached_part_data.empty())
        {
            makeSinglepartUpload({});
        }
        else
        {
            makeSinglepartUpload(std::move(detached_part_data.front()));
            detached_part_data.pop_front();
        }
    }
    else
    {
        writeMultipartUpload();
    }
}

void WriteBufferFromS3::finalizeImpl()
{
    OpenTelemetry::SpanHolder span(fmt::format("{}::finalizeImpl", log_name));
    span.addAttribute("clickhouse.object_storage_bucket", bucket);
    span.addAttribute("clickhouse.object_storage_key", key);
    span.addAttribute("clickhouse.total_size", total_size);

    LOG_TRACE(limited_log, "finalizeImpl {}. {}.", log_name, getShortLogDetails());
    WriteBufferFromFileBase::finalizeImpl();

    if (!is_prefinalized)
        preFinalize();

    chassert(offset() == 0);
    chassert(hidden_size == 0);

    task_tracker->waitAll();

    span.addAttributeIfNotZero("clickhouse.multipart_upload_parts", multipart_tags.size());

    if (!multipart_upload_id.empty())
    {
        completeMultipartUpload();
        multipart_upload_finished = true;
    }

    if (request_settings[S3RequestSetting::check_objects_after_upload])
    {
        S3::checkObjectExists(*client_ptr, bucket, key, {}, "Immediately after upload");

        size_t actual_size = S3::getObjectSize(*client_ptr, bucket, key, {});
        if (actual_size != total_size)
            throw Exception(
                    ErrorCodes::S3_ERROR,
                    "Object {} from bucket {} has unexpected size {} after upload, expected size {}, it's a bug in {} or its API.",
                    key, bucket, actual_size, total_size, object_storage_name);
    }
}

void WriteBufferFromS3::cancelImpl() noexcept
{
    WriteBufferFromFileBase::cancelImpl();
    tryToAbortMultipartUpload();
}

String WriteBufferFromS3::getVerboseLogDetails() const
{
    String multipart_upload_details;
    if (!multipart_upload_id.empty())
        multipart_upload_details = fmt::format(", upload id {}, upload has finished {}"
                                       , multipart_upload_id, multipart_upload_finished);

    return fmt::format("Details: bucket {}, key {}, total size {}, count {}, hidden_size {}, offset {}, with pool: {}, prefinalized: {}, finalized: {}{}",
                       bucket, key, total_size, count(), hidden_size, offset(), task_tracker->isAsync(), is_prefinalized, finalized, multipart_upload_details);
}

String WriteBufferFromS3::getShortLogDetails() const
{
    String multipart_upload_details;
    if (!multipart_upload_id.empty())
        multipart_upload_details = fmt::format(", upload id {}"
                                               , multipart_upload_id);

    return fmt::format("Details: bucket {}, key {}{}",
                       bucket, key, multipart_upload_details);
}

void WriteBufferFromS3::tryToAbortMultipartUpload() noexcept
{
    try
    {
        task_tracker->safeWaitAll();
        abortMultipartUpload();
    }
    catch (...)
    {
        LOG_ERROR(log, "Multipart upload hasn't been aborted. {}", getVerboseLogDetails());
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

WriteBufferFromS3::~WriteBufferFromS3()
{
    LOG_TRACE(limited_log, "Close {}. {}.", log_name, getShortLogDetails());

    if (canceled)
    {
        if (!isEmpty())
        {
            LOG_INFO(
                log,
                "{} was canceled."
                "The file might not be written to {}. "
                "{}.",
                log_name, object_storage_name, getVerboseLogDetails());
        }
    }
    else if (!finalized)
    {
        LOG_INFO(
            log,
            "{} is not finalized in destructor. "
            "The file might not be written to {}. "
            "{}.",
            log_name, object_storage_name, getVerboseLogDetails());
    }

    task_tracker->safeWaitAll();

    if (!canceled && !multipart_upload_id.empty() && !multipart_upload_finished)
    {
        LOG_WARNING(log, "{} was neither finished nor aborted, try to abort upload in destructor. {}.", log_name, getVerboseLogDetails());
        tryToAbortMultipartUpload();
    }
}

void WriteBufferFromS3::hidePartialData()
{
    chassert(memory.size() >= hidden_size + offset());

    hidden_size += offset();
    chassert(memory.data() + hidden_size == working_buffer.begin() + offset());
    chassert(memory.data() + hidden_size == position());

    WriteBuffer::set(memory.data() + hidden_size, memory.size() - hidden_size);
    chassert(offset() == 0);
}

void WriteBufferFromS3::reallocateFirstBuffer()
{
    chassert(offset() == 0);

    if (buffer_allocation_policy->getBufferNumber() > 1 || available() > 0)
        return;

    const size_t max_first_buffer = buffer_allocation_policy->getBufferSize();
    if (memory.size() == max_first_buffer)
        return;

    size_t size = std::min(memory.size() * 2, max_first_buffer);
    memory.resize(size);

    WriteBuffer::set(memory.data() + hidden_size, memory.size() - hidden_size);

    chassert(offset() == 0);
}

void WriteBufferFromS3::detachBuffer()
{
    size_t data_size = size_t(position() - memory.data());
    chassert(data_size == hidden_size);

    auto buf = std::move(memory);

    WriteBuffer::set(nullptr, 0);
    total_size += hidden_size;
    hidden_size = 0;

    detached_part_data.push_back({std::move(buf), data_size});
}

void WriteBufferFromS3::allocateBuffer()
{
    buffer_allocation_policy->nextBuffer();
    chassert(0 == hidden_size);

    /// First buffer was already allocated in BufferWithOwnMemory constructor with provided in constructor buffer size.
    /// It will be reallocated in subsequent nextImpl calls up to the desired buffer size from buffer_allocation_policy.
    if (buffer_allocation_policy->getBufferNumber() == 1)
    {
        /// Reduce memory size if initial size was larger then desired size from buffer_allocation_policy.
        /// Usually it doesn't happen but we have it in unit tests.
        if (memory.size() > buffer_allocation_policy->getBufferSize())
        {
            memory.resize(buffer_allocation_policy->getBufferSize());
            WriteBuffer::set(memory.data(), memory.size());
        }
        return;
    }

    memory = Memory<>(buffer_allocation_policy->getBufferSize());
    WriteBuffer::set(memory.data(), memory.size());
}

void WriteBufferFromS3::setFakeBufferWhenPreFinalized()
{
    WriteBuffer::set(fake_buffer_when_prefinalized, sizeof(fake_buffer_when_prefinalized));
}

void WriteBufferFromS3::writeMultipartUpload()
{
    if (multipart_upload_id.empty())
    {
        createMultipartUpload();
    }

    while (!detached_part_data.empty())
    {
        writePart(std::move(detached_part_data.front()));
        detached_part_data.pop_front();
    }
}

void WriteBufferFromS3::createMultipartUpload()
{
    LOG_TEST(limited_log, "Create multipart upload. {}", getShortLogDetails());

    S3::CreateMultipartUploadRequest req;

    req.SetBucket(bucket);
    req.SetKey(key);

    req.SetContentType("binary/octet-stream");

    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());

    client_ptr->setKMSHeaders(req);

    const auto & events = writeBufferProfileEvents(profile_events_namespace);
    ProfileEvents::increment(events.create_multipart_upload);
    if (client_ptr->isClientForDisk())
        ProfileEvents::increment(events.disk_create_multipart_upload);

    Stopwatch watch;
    auto outcome = client_ptr->CreateMultipartUpload(req);
    auto elapsed = watch.elapsedMicroseconds();

    ProfileEvents::increment(events.microseconds, elapsed);
    if (blob_log)
        blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadCreate, bucket, key, {}, 0, elapsed,
                           outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                           outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

    if (!outcome.IsSuccess())
    {
        ProfileEvents::increment(events.errors, 1);
        throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
    }

    multipart_upload_id = outcome.GetResult().GetUploadId();

    if (multipart_upload_id.empty())
    {
        ProfileEvents::increment(events.errors, 1);
        throw Exception(ErrorCodes::S3_ERROR, "Invalid CreateMultipartUpload result: missing UploadId.");
    }

    LOG_TRACE(limited_log, "Multipart upload was created. {}", getShortLogDetails());
}

void WriteBufferFromS3::abortMultipartUpload()
{
    if (multipart_upload_id.empty())
    {
        if (!isEmpty())
        {
            LOG_INFO(log, "Nothing to abort. {}", getVerboseLogDetails());
        }
        return;
    }

    LOG_INFO(log, "Abort multipart upload. {}", getVerboseLogDetails());

    S3::AbortMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetUploadId(multipart_upload_id);

    const auto & events = writeBufferProfileEvents(profile_events_namespace);
    ProfileEvents::increment(events.abort_multipart_upload);
    if (client_ptr->isClientForDisk())
        ProfileEvents::increment(events.disk_abort_multipart_upload);

    Stopwatch watch;
    auto outcome = client_ptr->AbortMultipartUpload(req);
    auto elapsed = watch.elapsedMicroseconds();

    ProfileEvents::increment(events.microseconds, elapsed);

    if (blob_log)
        blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadAbort, bucket, key, {}, 0, elapsed,
                           outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                           outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

    if (!outcome.IsSuccess())
    {
        ProfileEvents::increment(events.errors, 1);
        throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
    }

    LOG_INFO(log, "Multipart upload has been aborted successfully. {}", getVerboseLogDetails());
}

S3::UploadPartRequest WriteBufferFromS3::getUploadRequest(size_t part_number, PartData & data)
{
    ProfileEvents::increment(writeBufferProfileEvents(profile_events_namespace).bytes, data.data_size);

    S3::UploadPartRequest req;

    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetPartNumber(static_cast<int>(part_number));
    req.SetUploadId(multipart_upload_id);
    req.SetContentLength(data.data_size);
    req.SetBody(data.createAwsBuffer());
    req.SetContentType("binary/octet-stream");

    if (client_ptr->isS3ExpressBucket())
    {
        auto checksum = S3::RequestChecksum::calculateChecksum(req);
        S3::RequestChecksum::setRequestChecksum(req, checksum);
        multipart_checksums.push_back(std::move(checksum));
    }

    return req;
}

void WriteBufferFromS3::writePart(WriteBufferFromS3::PartData && data)
{
    if (data.data_size == 0)
    {
        LOG_TEST(limited_log, "Skipping writing part as empty {}", getShortLogDetails());
        return;
    }

    multipart_tags.push_back({});
    auto & part_tag = multipart_tags.back();
    size_t part_number = multipart_tags.size();
    LOG_TEST(limited_log, "writePart {}, part size {}, part number {}", getShortLogDetails(), data.data_size, part_number);

    if (multipart_upload_id.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unable to write a part without multipart_upload_id, details: {} created for bucket {}, key {}",
            log_name, bucket, key);

    if (part_number > request_settings[S3RequestSetting::max_part_number])
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Part number exceeded {} while writing {} bytes to {}. Check min_upload_part_size = {}, max_upload_part_size = {}, "
            "upload_part_size_multiply_factor = {}, upload_part_size_multiply_parts_count_threshold = {}, max_single_part_upload_size = {}",
            request_settings[S3RequestSetting::max_part_number].value, count(), object_storage_name, request_settings[S3RequestSetting::min_upload_part_size].value, request_settings[S3RequestSetting::max_upload_part_size].value,
            request_settings[S3RequestSetting::upload_part_size_multiply_factor].value, request_settings[S3RequestSetting::upload_part_size_multiply_parts_count_threshold].value,
            request_settings[S3RequestSetting::max_single_part_upload_size].value);
    }

    if (data.data_size > request_settings[S3RequestSetting::max_upload_part_size])
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Part size exceeded max_upload_part_size. {}, part number {}, part size {}, max_upload_part_size {}",
            getShortLogDetails(), part_number, data.data_size, request_settings[S3RequestSetting::max_upload_part_size].value);
    }

    auto req = getUploadRequest(part_number, data);
    auto worker_data = std::make_shared<std::tuple<S3::UploadPartRequest, WriteBufferFromS3::PartData>>(std::move(req), std::move(data));

    auto upload_worker = [&, worker_data, part_number] ()
    {
        auto & data_size = std::get<1>(*worker_data).data_size;

        LOG_TEST(limited_log, "Write part started {}, part size {}, part number {}", getShortLogDetails(), data_size, part_number);

        const auto & events = writeBufferProfileEvents(profile_events_namespace);
        ProfileEvents::increment(events.upload_part);
        if (client_ptr->isClientForDisk())
            ProfileEvents::increment(events.disk_upload_part);

        auto & request = std::get<0>(*worker_data);

        CurrentThread::IOSchedulingScope io_scope(write_settings.io_scheduling);
        CurrentThread::WriteThrottlingScope write_throttling_scope(write_settings.remote_throttler);

        Stopwatch watch;
        auto outcome = client_ptr->UploadPart(request);
        auto elapsed = watch.elapsedMicroseconds();

        ProfileEvents::increment(events.microseconds, elapsed);

        if (blob_log)
        {
            blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadWrite,
                /* bucket = */ bucket, /* remote_path = */ key, /* local_path = */ {}, /* data_size */ data_size, elapsed,
                               outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                               outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());
        }

        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(events.errors, 1);
            throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
        }

        part_tag = outcome.GetResult().GetETag();

        LOG_TEST(limited_log, "Write part succeeded {}, part size {}, part number {}, etag {}",
                 getShortLogDetails(), data_size, part_number, part_tag);
    };

    task_tracker->add(std::move(upload_worker));
}

void WriteBufferFromS3::completeMultipartUpload()
{
    LOG_TEST(limited_log, "Completing multipart upload. {}, Parts: {}", getShortLogDetails(), multipart_tags.size());

    if (multipart_tags.empty())
        throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Failed to complete multipart upload. No parts have uploaded");

    for (size_t i = 0; i < multipart_tags.size(); ++i)
    {
        const auto tag = multipart_tags.at(i);
        if (tag.empty())
            throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Failed to complete multipart upload. Part {} haven't been uploaded.", i);
    }

    S3::CompleteMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetUploadId(multipart_upload_id);

    if (!write_settings.object_storage_write_if_none_match.empty())
        req.SetIfNoneMatch(write_settings.object_storage_write_if_none_match);

    if (!write_settings.object_storage_write_if_match.empty())
        req.SetIfMatch(write_settings.object_storage_write_if_match);

    Aws::S3::Model::CompletedMultipartUpload multipart_upload;
    for (size_t i = 0; i < multipart_tags.size(); ++i)
    {
        Aws::S3::Model::CompletedPart part;
        part.WithETag(multipart_tags[i]).WithPartNumber(static_cast<int>(i + 1));
        if (!multipart_checksums.empty())
            S3::RequestChecksum::setPartChecksum(part, multipart_checksums.at(i));
        multipart_upload.AddParts(part);
    }

    req.SetMultipartUpload(multipart_upload);

    size_t max_retry = std::max<UInt64>(request_settings[S3RequestSetting::max_unexpected_write_error_retries].value, 1UL);
    const auto & events = writeBufferProfileEvents(profile_events_namespace);
    for (size_t i = 0; i < max_retry; ++i)
    {
        ProfileEvents::increment(events.complete_multipart_upload);
        if (client_ptr->isClientForDisk())
            ProfileEvents::increment(events.disk_complete_multipart_upload);

        Stopwatch watch;
        auto outcome = client_ptr->CompleteMultipartUpload(req);
        auto elapsed = watch.elapsedMicroseconds();

        ProfileEvents::increment(events.microseconds, elapsed);

        if (blob_log)
            blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadComplete, bucket, key, {}, 0, elapsed,
                               outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                               outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

        if (outcome.IsSuccess())
        {
            LOG_TRACE(limited_log, "Multipart upload has completed. {}, Parts: {}", getShortLogDetails(), multipart_tags.size());
            return;
        }

        ProfileEvents::increment(events.errors, 1);

        if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY)
        {
            LOG_INFO(log, "Multipart upload failed with NO_SUCH_KEY error, will retry. {}, Parts: {}", getVerboseLogDetails(), multipart_tags.size());
        }
        else
        {
            throw S3Exception(
                outcome.GetError().GetErrorType(),
                "Message: {}, Key: {}, Bucket: {}, Tags: {}",
                outcome.GetError().GetMessage(), key, bucket, fmt::join(multipart_tags.begin(), multipart_tags.end(), " "));
        }
    }

    throw S3Exception(
        Aws::S3::S3Errors::NO_SUCH_KEY,
        "Message: Multipart upload failed with NO_SUCH_KEY error, retries {}, Key: {}, Bucket: {}",
        max_retry, key, bucket);
}

S3::PutObjectRequest WriteBufferFromS3::getPutRequest(PartData & data)
{
    ProfileEvents::increment(writeBufferProfileEvents(profile_events_namespace).bytes, data.data_size);

    S3::PutObjectRequest req;

    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetContentLength(data.data_size);
    req.SetBody(data.createAwsBuffer());
    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());
    if (!request_settings[S3RequestSetting::storage_class_name].value.empty())
        req.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(request_settings[S3RequestSetting::storage_class_name]));

    if (!write_settings.object_storage_write_if_none_match.empty())
        req.SetIfNoneMatch(write_settings.object_storage_write_if_none_match);

    if (!write_settings.object_storage_write_if_match.empty())
        req.SetIfMatch(write_settings.object_storage_write_if_match);

    req.SetContentType("binary/octet-stream");

    client_ptr->setKMSHeaders(req);

    return req;
}

void WriteBufferFromS3::makeSinglepartUpload(WriteBufferFromS3::PartData && data)
{
    LOG_TEST(limited_log, "Making single part upload. {}, size {}", getShortLogDetails(), data.data_size);

    auto req = getPutRequest(data);
    auto worker_data = std::make_shared<std::tuple<S3::PutObjectRequest, WriteBufferFromS3::PartData>>(std::move(req), std::move(data));

    auto upload_worker = [&, worker_data] ()
    {
        LOG_TEST(limited_log, "writing single part upload started. {}", getShortLogDetails());

        auto & request = std::get<0>(*worker_data);
        size_t content_length = request.GetContentLength();

        size_t max_retry = std::max<UInt64>(request_settings[S3RequestSetting::max_unexpected_write_error_retries].value, 1UL);
        const auto & events = writeBufferProfileEvents(profile_events_namespace);
        for (size_t i = 0; i < max_retry; ++i)
        {
            ProfileEvents::increment(events.put_object);
            if (client_ptr->isClientForDisk())
                ProfileEvents::increment(events.disk_put_object);

            CurrentThread::IOSchedulingScope io_scope(write_settings.io_scheduling);
            CurrentThread::WriteThrottlingScope write_throttling_scope(write_settings.remote_throttler);

            Stopwatch watch;
            auto outcome = client_ptr->PutObject(request);
            auto elapsed = watch.elapsedMicroseconds();

            ProfileEvents::increment(events.microseconds, elapsed);
            if (blob_log)
                blob_log->addEvent(BlobStorageLogElement::EventType::Upload, bucket, key, {}, request.GetContentLength(), elapsed,
                                   outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                                   outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

            if (outcome.IsSuccess())
            {
                LOG_TRACE(limited_log, "Single part upload has completed. {}, size {}", getShortLogDetails(), content_length);
                return;
            }

            ProfileEvents::increment(events.errors, 1);

            if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY)
            {
                LOG_INFO(log, "Single part upload failed with NO_SUCH_KEY error. {}, size {}, will retry", getShortLogDetails(), content_length);
            }
            else
            {
                if (outcome.GetError().GetExceptionName() == "PreconditionFailed")
                    LOG_INFO(log, "S3Exception name {}, Message: {}, bucket {}, key {}, object size {}",
                              outcome.GetError().GetExceptionName(), outcome.GetError().GetMessage(), bucket, key, content_length);
                else
                    LOG_ERROR(log, "S3Exception name {}, Message: {}, bucket {}, key {}, object size {}",
                              outcome.GetError().GetExceptionName(), outcome.GetError().GetMessage(), bucket, key, content_length);
                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Message: {}, bucket {}, key {}, object size {}",
                    outcome.GetError().GetMessage(), bucket, key, content_length);
            }
        }

        throw S3Exception(
            Aws::S3::S3Errors::NO_SUCH_KEY,
            "Message: Single part upload failed with NO_SUCH_KEY error, retries {}, Key: {}, Bucket: {}",
            max_retry, key, bucket);
    };

    task_tracker->add(std::move(upload_worker));
}

}

#endif

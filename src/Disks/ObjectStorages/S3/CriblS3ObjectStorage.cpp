#include <Disks/ObjectStorages/S3/CriblS3ObjectStorage.h>

#if USE_AWS_S3


#    include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>
#    include <IO/S3Common.h>

#    include <Core/Settings.h>
#    include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#    include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#    include <Disks/IO/ThreadPoolRemoteFSReader.h>
#    include <Disks/IO/getThreadPoolReader.h>
#    include <IO/ReadBufferFromS3.h>
#    include <IO/S3/BlobStorageLogWriter.h>
#    include <IO/S3/copyS3File.h>
#    include <IO/S3/deleteFileFromS3.h>
#    include <IO/S3/getObjectInfo.h>
#    include <IO/WriteBufferFromS3.h>
#    include <Interpreters/Context.h>
#    include <Common/quoteString.h>
#    include <Common/threadPoolCallbackRunner.h>

#    include <Disks/ObjectStorages/S3/diskSettings.h>

#    include <Common/Macros.h>
#    include <Common/MultiVersion.h>
#    include <Common/ProfileEvents.h>
#    include <Common/StringUtils.h>
#    include <Common/logger_useful.h>


namespace ProfileEvents
{
extern const Event S3ListObjects;
extern const Event DiskS3DeleteObjects;
extern const Event DiskS3ListObjects;
}

namespace CurrentMetrics
{
extern const Metric ObjectStorageS3Threads;
extern const Metric ObjectStorageS3ThreadsActive;
extern const Metric ObjectStorageS3ThreadsScheduled;
}

namespace DB
{

namespace Setting
{
extern const SettingsBool s3_validate_request_settings;
}

namespace ErrorCodes
{
extern const int S3_ERROR;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}
namespace
{

//this is the class to hack
class CriblS3IteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    CriblS3IteratorAsync(
        const std::string & bucket_, const std::string & path_prefix, std::shared_ptr<const S3::Client> client_, size_t max_list_size)
        : IObjectStorageIteratorAsync(
              CurrentMetrics::ObjectStorageS3Threads,
              CurrentMetrics::ObjectStorageS3ThreadsActive,
              CurrentMetrics::ObjectStorageS3ThreadsScheduled,
              "ListObjectS3")
        , client(client_)
        , request(std::make_unique<S3::ListObjectsV2Request>())
    {
        request->SetBucket(bucket_);
        request->SetPrefix(path_prefix);
        request->SetMaxKeys(static_cast<int>(max_list_size));
    }

    ~CriblS3IteratorAsync() override
    {
        /// Deactivate background threads before resetting the request to avoid data race.
        deactivate();
        request.reset();
        client.reset();
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);

        auto outcome = client->ListObjectsV2(*request);

        /// Outcome failure will be handled on the caller side.
        if (outcome.IsSuccess())
        {
            request->SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

            auto objects = outcome.GetResult().GetContents();
            for (const auto & object : objects)
            {
                ObjectMetadata metadata{
                    static_cast<uint64_t>(object.GetSize()),
                    Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()),
                    object.GetETag(),
                    {}};
                batch.emplace_back(std::make_shared<RelativePathWithMetadata>(object.GetKey(), std::move(metadata)));
            }

            /// It returns false when all objects were returned
            return outcome.GetResult().GetIsTruncated();
        }

        throw S3Exception(
            outcome.GetError().GetErrorType(),
            "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
            quoteString(request->GetBucket()),
            quoteString(request->GetPrefix()),
            backQuote(outcome.GetError().GetExceptionName()),
            quoteString(outcome.GetError().GetMessage()));
    }

    std::shared_ptr<const S3::Client> client;
    std::unique_ptr<S3::ListObjectsV2Request> request;
};

}

CriblS3ObjectStorage::CriblS3ObjectStorage(
    const char * logger_name,
    std::unique_ptr<S3::Client> && client_,
    std::unique_ptr<S3ObjectStorageSettings> && s3_settings_,
    S3::URI uri_,
    const S3Capabilities & s3_capabilities_,
    ObjectStorageKeysGeneratorPtr key_generator_,
    const String & disk_name_,
    bool for_disk_s3_)
    : S3ObjectStorage(
          logger_name, std::move(client_), std::move(s3_settings_), uri_, s3_capabilities_, key_generator_, disk_name_, for_disk_s3_)
{
}

CriblS3ObjectStorage::~CriblS3ObjectStorage()
{
}


ObjectStorageIteratorPtr CriblS3ObjectStorage::iterate(const std::string & path_prefix, size_t max_keys) const
{
    auto settings_ptr = s3_settings.get();
    if (!max_keys)
        max_keys = settings_ptr->list_object_keys_size;
    return std::make_shared<CriblS3IteratorAsync>(uri.bucket, path_prefix, client.get(), max_keys);
}
}
#endif

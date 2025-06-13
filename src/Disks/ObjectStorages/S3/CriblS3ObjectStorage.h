#pragma once
#include "config.h"

#if USE_AWS_S3
#    include <Disks/ObjectStorages/S3/S3ObjectStorage.h>

namespace DB
{
class CriblS3ObjectStorage : public S3ObjectStorage
{
public:
    CriblS3ObjectStorage(
        const char * logger_name,
        std::unique_ptr<S3::Client> && client_,
        std::unique_ptr<S3ObjectStorageSettings> && s3_settings_,
        S3::URI uri_,
        const S3Capabilities & s3_capabilities_,
        ObjectStorageKeysGeneratorPtr key_generator_,
        const String & disk_name_,
        bool for_disk_s3_ = true);
    ~CriblS3ObjectStorage() override;

    ObjectStorageIteratorPtr iterate(const std::string & path_prefix, size_t max_keys) const override;

public:
    template <typename... Args>
    explicit CriblS3ObjectStorage(std::unique_ptr<S3::Client> && client_, Args &&... args)
        : CriblS3ObjectStorage("CriblS3ObjectStorage", std::move(client_), std::forward<Args>(args)...)
    {
    }
};

}
#endif

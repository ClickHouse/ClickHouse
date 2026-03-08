#include <Databases/DataLake/StaticStorageCredentials.h>

namespace DB::DatabaseDataLakeSetting
{
    extern const DatabaseDataLakeSettingsString aws_access_key_id;
    extern const DatabaseDataLakeSettingsString aws_secret_access_key;
    extern const DatabaseDataLakeSettingsString storage_aws_access_key_id;
    extern const DatabaseDataLakeSettingsString storage_aws_secret_access_key;
}

namespace DataLake
{

std::shared_ptr<IStorageCredentials> tryGetStaticStorageCredentials(
    DB::DatabaseDataLakeStorageType storage_type,
    const DB::DatabaseDataLakeSettings & settings)
{
    if (storage_type != DB::DatabaseDataLakeStorageType::S3)
        return nullptr;

    String access_key_id = settings[DB::DatabaseDataLakeSetting::aws_access_key_id].value;
    String secret_access_key = settings[DB::DatabaseDataLakeSetting::aws_secret_access_key].value;

    /// Keep backward compatibility with storage_* names.
    if (access_key_id.empty())
        access_key_id = settings[DB::DatabaseDataLakeSetting::storage_aws_access_key_id].value;
    if (secret_access_key.empty())
        secret_access_key = settings[DB::DatabaseDataLakeSetting::storage_aws_secret_access_key].value;

    if (access_key_id.empty() || secret_access_key.empty())
        return nullptr;

    return std::make_shared<S3Credentials>(access_key_id, secret_access_key, /* session_token */ "");
}

}


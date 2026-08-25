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

namespace
{
std::shared_ptr<IStorageCredentials> tryMakeStaticS3Credentials(const String & access_key_id, const String & secret_access_key)
{
    if (access_key_id.empty() || secret_access_key.empty())
        return nullptr;

    return std::make_shared<S3Credentials>(access_key_id, secret_access_key, /* session_token */ "");
}
}

std::shared_ptr<IStorageCredentials> tryGetStaticStorageCredentials(
    DB::DatabaseDataLakeStorageType storage_type,
    const DB::DatabaseDataLakeSettings & settings)
{
    if (storage_type != DB::DatabaseDataLakeStorageType::S3)
        return nullptr;

    if (auto credentials = tryMakeStaticS3Credentials(
            settings[DB::DatabaseDataLakeSetting::aws_access_key_id].value,
            settings[DB::DatabaseDataLakeSetting::aws_secret_access_key].value))
    {
        return credentials;
    }

    /// Keep backward compatibility with storage_* names, but do not mix the two setting namespaces.
    return tryMakeStaticS3Credentials(
        settings[DB::DatabaseDataLakeSetting::storage_aws_access_key_id].value,
        settings[DB::DatabaseDataLakeSetting::storage_aws_secret_access_key].value);
}

}


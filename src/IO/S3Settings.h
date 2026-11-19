#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/IThrottler.h>

#include <IO/S3Common.h>
#include <IO/S3AuthSettings.h>
#include <IO/S3Defines.h>
#include <IO/S3RequestSettings.h>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

struct Settings;

struct S3Settings
{
    S3::S3AuthSettings auth_settings;
    S3::S3RequestSettings request_settings;

    /// Read all settings with from specified prefix in config. Expect settings name to start with "s3_".
    /// This method is useful when you need to get S3Settings for standalone S3 Client.
    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings);

    /// Read all settings with from specified prefix in config. Expect settings name to start with "s3_".
    /// Override 4 settings: readonly, min_bytes_for_seek, list_object_keys_size, objects_chunk_size_to_delete from
    /// settings values from config without "s3_" prefix. This method exists for historical reasons. Initially we added
    /// some settings which don't directly affect S3 requests in terms of S3 API, but they affect ClickHouse logic
    /// when it reads from ObjectStorage.
    ///
    /// This method is useful when you need to get S3Settings for Disk or ObjectStorage, not a standalone S3Client.
    void loadFromConfigForObjectStorage(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings,
        const std::string & scheme,
        bool validate_settings);

    void updateIfChanged(const S3Settings & settings);

    void serialize(WriteBuffer & os, ContextPtr) const;
    static S3Settings deserialize(ReadBuffer & is, ContextPtr context);
};

class S3SettingsByEndpoint
{
public:
    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings);

    std::optional<S3Settings> getSettings(
        const std::string & endpoint,
        const std::string & user,
        bool ignore_user = false) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const S3Settings> s3_settings;
};


}

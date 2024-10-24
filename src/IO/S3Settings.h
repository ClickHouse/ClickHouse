#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Throttler_fwd.h>

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

    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings);

    void updateIfChanged(const S3Settings & settings);
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

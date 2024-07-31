#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context_fwd.h>

#include <IO/S3/Client.h>

namespace DB
{

struct S3ObjectStorageSettings;

std::unique_ptr<S3ObjectStorageSettings> getSettings(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const std::pair<std::string, int> &endpoint,
    bool validate_settings);

std::unique_ptr<S3::Client> getClient(
    const std::pair<std::string, int> &endpoint,
    const S3ObjectStorageSettings & settings,
    ContextPtr context,
    bool for_disk_s3);

std::unique_ptr<S3::Client> getClient(
    const S3::URI & url_,
    const S3ObjectStorageSettings & settings,
    ContextPtr context,
    bool for_disk_s3);

}

#endif

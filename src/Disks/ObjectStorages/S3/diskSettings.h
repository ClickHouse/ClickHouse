#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context_fwd.h>

#include <IO/S3/Client.h>

namespace DB
{

struct S3ObjectStorageSettings;

std::unique_ptr<S3ObjectStorageSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);

std::unique_ptr<S3::Client> getClient(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context, const S3ObjectStorageSettings & settings);

}

#endif

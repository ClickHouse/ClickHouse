#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context_fwd.h>
#include <IO/S3Settings.h>

#include <IO/S3/Client.h>

namespace DB
{

std::unique_ptr<S3::Client> getClient(
    const std::string & endpoint,
    const S3Settings & settings,
    ContextPtr context,
    bool for_disk_s3,
    std::optional<std::string> opt_disk_name = {});

std::unique_ptr<S3::Client> getClient(
    const S3::URI & url_, const S3Settings & settings, ContextPtr context, bool for_disk_s3, std::optional<std::string> opt_disk_name = {});
}

#endif

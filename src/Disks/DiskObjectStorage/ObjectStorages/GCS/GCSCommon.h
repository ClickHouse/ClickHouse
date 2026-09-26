#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <base/types.h>
#include <google/cloud/status.h>

namespace DB
{

/// True if the status denotes a missing object (HTTP 404 / gRPC NOT_FOUND).
bool isGCSNotFoundError(const google::cloud::Status & status);

/// Translate a failed google-cloud-cpp Status into a ClickHouse exception (GOOGLE_CLOUD_ERROR).
/// `context` is appended for diagnostics (e.g. "while reading 'key' in bucket 'b'").
[[noreturn]] void throwFromGCSStatus(const google::cloud::Status & status, const String & context);

}

#endif

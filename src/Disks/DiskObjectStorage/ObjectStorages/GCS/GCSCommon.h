#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <base/types.h>
#include <google/cloud/status.h>

#include <optional>

namespace DB
{

/// True if the status denotes a missing object (HTTP 404 / gRPC NOT_FOUND).
bool isGCSNotFoundError(const google::cloud::Status & status);

/// Translate a failed google-cloud-cpp Status into a ClickHouse exception (GOOGLE_CLOUD_ERROR).
/// `context` is appended for diagnostics (e.g. "while reading 'key' in bucket 'b'").
[[noreturn]] void throwFromGCSStatus(const google::cloud::Status & status, const String & context);

/// Same for a failed write (an upload or a server-side copy). `expected_generation` is the `IfGenerationMatch`
/// condition of the write, if it has one. A conditional write that lost to another writer is a conflict the caller may
/// expect and handle, like the `PreconditionFailed` of S3 (e.g. Iceberg's `CREATE TABLE IF NOT EXISTS` and its metadata
/// commits), so it keeps the code of the condition: `FILE_ALREADY_EXISTS` for a create-only write (generation 0),
/// `STALE_VERSION` for a write of an expected generation.
[[noreturn]] void throwFromGCSWriteStatus(
    const google::cloud::Status & status, std::optional<Int64> expected_generation, const String & context);

}

#endif

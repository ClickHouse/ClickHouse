#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>

#if USE_GOOGLE_CLOUD

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int GOOGLE_CLOUD_ERROR;
    extern const int FILE_ALREADY_EXISTS;
    extern const int STALE_VERSION;
}

bool isGCSNotFoundError(const google::cloud::Status & status)
{
    return status.code() == google::cloud::StatusCode::kNotFound;
}

void throwFromGCSStatus(const google::cloud::Status & status, const String & context)
{
    throw Exception(
        ErrorCodes::GOOGLE_CLOUD_ERROR,
        "Google Cloud Storage error (code {}): {}{}",
        static_cast<int>(status.code()),
        status.message(),
        context.empty() ? "" : (", " + context));
}

void throwFromGCSWriteStatus(const google::cloud::Status & status, std::optional<Int64> expected_generation, const String & context)
{
    if (status.code() == google::cloud::StatusCode::kFailedPrecondition && expected_generation.has_value())
        throw Exception(
            *expected_generation == 0 ? ErrorCodes::FILE_ALREADY_EXISTS : ErrorCodes::STALE_VERSION,
            "Conditional write failed: {} (Google Cloud Storage: {}){}",
            *expected_generation == 0 ? "the object already exists" : "the object is not of the expected generation",
            status.message(),
            context.empty() ? "" : (", " + context));
    throwFromGCSStatus(status, context);
}

}

#endif

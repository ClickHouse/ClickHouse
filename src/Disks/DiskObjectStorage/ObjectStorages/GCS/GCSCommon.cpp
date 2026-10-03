#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>

#if USE_GOOGLE_CLOUD

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int GOOGLE_CLOUD_ERROR;
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

}

#endif

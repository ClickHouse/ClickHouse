#pragma once

#include <config.h>
#include <base/types.h>

#if USE_GOOGLE_CLOUD
#    include <grpcpp/support/status.h>
#endif

namespace DB::GCS
{

enum class StatusCode
{
    OK,
    NotFound,
    PermissionDenied,
    DeadlineExceeded,
    ResourceExhausted,
    Unavailable,
    InvalidArgument,
    Unsupported,
    Unknown,
};

struct Status
{
    StatusCode code = StatusCode::OK;
    String message;

    bool ok() const { return code == StatusCode::OK; }
};

Status makeStatus(StatusCode code, String message = {});
const char * statusCodeName(StatusCode code);
bool isRetryableStatus(StatusCode code);
bool isThrottlingStatus(StatusCode code);
int errorCodeForStatus(StatusCode code);
void throwIfError(const Status & status, const String & operation);

#if USE_GOOGLE_CLOUD
Status fromGrpcStatus(const grpc::Status & status);
#endif

}

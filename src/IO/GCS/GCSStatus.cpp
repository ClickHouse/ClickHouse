#include <IO/GCS/GCSStatus.h>

#include <Common/Exception.h>

#include <fmt/format.h>

namespace DB::ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
extern const int NETWORK_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int S3_ERROR;
extern const int TIMEOUT_EXCEEDED;
}

namespace DB::GCS
{

Status makeStatus(StatusCode code, String message)
{
    return Status{code, std::move(message)};
}

const char * statusCodeName(StatusCode code)
{
    switch (code)
    {
        case StatusCode::OK:
            return "OK";
        case StatusCode::NotFound:
            return "NotFound";
        case StatusCode::PermissionDenied:
            return "PermissionDenied";
        case StatusCode::DeadlineExceeded:
            return "DeadlineExceeded";
        case StatusCode::ResourceExhausted:
            return "ResourceExhausted";
        case StatusCode::Unavailable:
            return "Unavailable";
        case StatusCode::InvalidArgument:
            return "InvalidArgument";
        case StatusCode::Unsupported:
            return "Unsupported";
        case StatusCode::Unknown:
            return "Unknown";
    }
}

bool isRetryableStatus(StatusCode code)
{
    return code == StatusCode::ResourceExhausted || code == StatusCode::Unavailable || code == StatusCode::DeadlineExceeded;
}

bool isThrottlingStatus(StatusCode code)
{
    return code == StatusCode::ResourceExhausted;
}

int errorCodeForStatus(StatusCode code)
{
    switch (code)
    {
        case StatusCode::OK:
            return 0;
        case StatusCode::NotFound:
            return ErrorCodes::FILE_DOESNT_EXIST;
        case StatusCode::PermissionDenied:
            return ErrorCodes::ACCESS_DENIED;
        case StatusCode::DeadlineExceeded:
            return ErrorCodes::TIMEOUT_EXCEEDED;
        case StatusCode::ResourceExhausted:
        case StatusCode::Unavailable:
            return ErrorCodes::NETWORK_ERROR;
        case StatusCode::InvalidArgument:
            return ErrorCodes::BAD_ARGUMENTS;
        case StatusCode::Unsupported:
            return ErrorCodes::NOT_IMPLEMENTED;
        case StatusCode::Unknown:
            return ErrorCodes::S3_ERROR;
    }
}

void throwIfError(const Status & status, const String & operation)
{
    if (status.ok())
        return;

    throw Exception(
        errorCodeForStatus(status.code), "GCS gRPC {} failed with {}: {}", operation, statusCodeName(status.code), status.message);
}

#if USE_GOOGLE_CLOUD
Status fromGrpcStatus(const grpc::Status & status)
{
    if (status.ok())
        return {};

    String message = status.error_message();
    if (!status.error_details().empty())
        message += fmt::format("; details: {}", status.error_details());
    message += fmt::format("; grpc_status_code: {}", static_cast<int>(status.error_code()));

    switch (status.error_code())
    {
        case grpc::StatusCode::NOT_FOUND:
            return makeStatus(StatusCode::NotFound, std::move(message));
        case grpc::StatusCode::PERMISSION_DENIED:
        case grpc::StatusCode::UNAUTHENTICATED:
            return makeStatus(StatusCode::PermissionDenied, std::move(message));
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            return makeStatus(StatusCode::DeadlineExceeded, std::move(message));
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
            return makeStatus(StatusCode::ResourceExhausted, std::move(message));
        case grpc::StatusCode::UNAVAILABLE:
            return makeStatus(StatusCode::Unavailable, std::move(message));
        case grpc::StatusCode::INVALID_ARGUMENT:
        case grpc::StatusCode::FAILED_PRECONDITION:
        case grpc::StatusCode::OUT_OF_RANGE:
            return makeStatus(StatusCode::InvalidArgument, std::move(message));
        case grpc::StatusCode::UNIMPLEMENTED:
            return makeStatus(StatusCode::Unsupported, std::move(message));
        default:
            return makeStatus(StatusCode::Unknown, std::move(message));
    }
}
#endif

}

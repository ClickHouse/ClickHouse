#include <Server/ArrowFlight/FlightStatus.h>

#if USE_ARROWFLIGHT

namespace DB::ArrowFlight
{

String Status::codeAsString() const
{
    switch (code)
    {
        case Code::OK: return "OK";
        case Code::KeyError: return "Key error";
        case Code::TypeError: return "Type error";
        case Code::Invalid: return "Invalid";
        case Code::IOError: return "IOError";
        case Code::CapacityError: return "Capacity error";
        case Code::Cancelled: return "Cancelled";
        case Code::UnknownError: return "Unknown error";
        case Code::NotImplemented: return "NotImplemented";
        case Code::SerializationError: return "Serialization error";
        case Code::ExecutionError: return "ExecutionError in Gandiva";
        case Code::AlreadyExists: return "Already exists";
    }
    return "Unknown";
}

std::optional<String> Status::detail() const
{
    if (!flight_code)
        return std::nullopt;
    /// As `arrow::flight::FlightStatusDetail::CodeAsString`.
    switch (*flight_code)
    {
        case FlightCode::Internal: return "Internal";
        case FlightCode::TimedOut: return "TimedOut";
        case FlightCode::Cancelled: return "Cancelled";
        case FlightCode::Unauthenticated: return "Unauthenticated";
        case FlightCode::Unauthorized: return "Unauthorized";
        case FlightCode::Unavailable: return "Unavailable";
        case FlightCode::Failed: return "Failed";
    }
    return "Unknown";
}

String Status::ToString() const
{
    if (ok())
        return "OK";
    String result = codeAsString() + ": " + message_text;
    if (auto status_detail = detail())
        result += ". Detail: " + *status_detail;
    return result;
}

grpc::Status Status::toGrpcStatus(grpc::ServerContext * context) const
{
    if (ok())
        return grpc::Status::OK;

    /// As `arrow::flight::TransportStatus::FromStatus` and `ToRawGrpcStatus` in Arrow's gRPC transport.
    grpc::StatusCode grpc_code = grpc::StatusCode::UNKNOWN;
    if (flight_code)
    {
        switch (*flight_code)
        {
            case FlightCode::Internal: grpc_code = grpc::StatusCode::INTERNAL; break;
            case FlightCode::TimedOut: grpc_code = grpc::StatusCode::DEADLINE_EXCEEDED; break;
            case FlightCode::Cancelled: grpc_code = grpc::StatusCode::CANCELLED; break;
            case FlightCode::Unauthenticated: grpc_code = grpc::StatusCode::UNAUTHENTICATED; break;
            case FlightCode::Unauthorized: grpc_code = grpc::StatusCode::PERMISSION_DENIED; break;
            case FlightCode::Unavailable: grpc_code = grpc::StatusCode::UNAVAILABLE; break;
            case FlightCode::Failed: grpc_code = grpc::StatusCode::UNKNOWN; break;
        }
    }
    else
    {
        switch (code)
        {
            case Code::KeyError: grpc_code = grpc::StatusCode::NOT_FOUND; break;
            case Code::Invalid: grpc_code = grpc::StatusCode::INVALID_ARGUMENT; break;
            case Code::Cancelled: grpc_code = grpc::StatusCode::CANCELLED; break;
            case Code::NotImplemented: grpc_code = grpc::StatusCode::UNIMPLEMENTED; break;
            case Code::AlreadyExists: grpc_code = grpc::StatusCode::ALREADY_EXISTS; break;
            default: break;
        }
    }

    String message = message_text;
    const auto status_detail = detail();
    if (status_detail)
        message += ". Detail: " + *status_detail;

    /// The original status, which the Arrow Flight client of Arrow restores instead of the gRPC one.
    if (context)
    {
        context->AddTrailingMetadata("x-arrow-status", std::to_string(static_cast<int>(code)));
        context->AddTrailingMetadata("x-arrow-status-message-bin", message_text);
        if (status_detail)
            context->AddTrailingMetadata("x-arrow-status-detail-bin", *status_detail);
    }
    return {grpc_code, message};
}

}

#endif

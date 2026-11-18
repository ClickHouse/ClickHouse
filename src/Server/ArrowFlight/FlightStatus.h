#pragma once

#include "config.h"

#if USE_ARROWFLIGHT

#include <base/types.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <optional>
#include <variant>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::ArrowFlight
{

/// The outcome of an Arrow Flight call, as the Apache Arrow library (`arrow::Status`) expressed it, with the same
/// codes: clients built on that library (pyarrow, ADBC, the Flight SQL JDBC driver) rebuild the error from the
/// codes and the message, which `toGrpcStatus` transmits the way Arrow's gRPC transport does.
class Status
{
public:
    /// The codes of `arrow::StatusCode` that the server uses.
    enum class Code : int
    {
        OK = 0,
        KeyError = 2,
        TypeError = 3,
        Invalid = 4,
        IOError = 5,
        CapacityError = 6,
        Cancelled = 8,
        UnknownError = 9,
        NotImplemented = 10,
        SerializationError = 11,
        ExecutionError = 42,
        AlreadyExists = 45,
    };

    /// The codes of `arrow::flight::FlightStatusCode`, for the errors specific to Arrow Flight.
    enum class FlightCode : int
    {
        Internal,
        TimedOut,
        Cancelled,
        Unauthenticated,
        Unauthorized,
        Unavailable,
        Failed,
    };

    Status() = default;
    Status(Code code_, String message_) : code(code_), message_text(std::move(message_)) {}

    static Status OK() { return {}; }

    template <typename... Args>
    static Status Invalid(Args &&... args) { return {Code::Invalid, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status NotImplemented(Args &&... args) { return {Code::NotImplemented, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status KeyError(Args &&... args) { return {Code::KeyError, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status TypeError(Args &&... args) { return {Code::TypeError, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status ExecutionError(Args &&... args) { return {Code::ExecutionError, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status SerializationError(Args &&... args) { return {Code::SerializationError, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status IOError(Args &&... args) { return {Code::IOError, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status UnknownError(Args &&... args) { return {Code::UnknownError, concat(std::forward<Args>(args)...)}; }
    template <typename... Args>
    static Status CapacityError(Args &&... args) { return {Code::CapacityError, concat(std::forward<Args>(args)...)}; }

    /// An error specific to Arrow Flight (`arrow::flight::MakeFlightError`).
    static Status FlightError(FlightCode flight_code_, String message_)
    {
        Status status(Code::IOError, std::move(message_));
        status.flight_code = flight_code_;
        return status;
    }

    [[nodiscard]] bool ok() const { return code == Code::OK; }
    [[nodiscard]] bool IsIOError() const { return code == Code::IOError; }
    [[nodiscard]] const String & message() const { return message_text; }

    /// As `arrow::Status::ToString`, e.g. "Invalid: message".
    [[nodiscard]] String ToString() const;

    /// The gRPC status, and the trailing metadata that encodes the original status for clients of Arrow.
    grpc::Status toGrpcStatus(grpc::ServerContext * context) const;

private:
    template <typename... Args>
    static String concat(Args &&... args)
    {
        String result;
        ((result += fmt::to_string(std::forward<Args>(args))), ...);
        return result;
    }

    [[nodiscard]] String codeAsString() const;
    [[nodiscard]] std::optional<String> detail() const;

    Code code = Code::OK;
    String message_text;
    std::optional<FlightCode> flight_code;
};

/// A value or an error, as `arrow::Result`.
template <typename T>
class Result
{
public:
    /// As `arrow::Result`, a result is an error until it is given a value.
    Result() : value(Status::UnknownError("Uninitialized Result<T>")) {}
    Result(const Result &) = default;
    Result(Result &&) noexcept = default;
    Result & operator=(const Result &) = default;
    Result & operator=(Result &&) noexcept = default;

    // NOLINTNEXTLINE(google-explicit-constructor)
    Result(T value_) : value(std::move(value_)) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    Result(Status status_) : value(std::move(status_))
    {
        if (std::get<Status>(value).ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "An Arrow Flight result was made from an OK status without a value");
    }

    [[nodiscard]] bool ok() const { return std::holds_alternative<T>(value); }

    [[nodiscard]] Status status() const { return ok() ? Status::OK() : std::get<Status>(value); }

    T & ValueOrDie() &
    {
        check();
        return std::get<T>(value);
    }
    [[nodiscard]] const T & ValueOrDie() const &
    {
        check();
        return std::get<T>(value);
    }
    T ValueOrDie() &&
    {
        check();
        return std::move(std::get<T>(value));
    }
    T & ValueUnsafe() & { return ValueOrDie(); }
    [[nodiscard]] const T & ValueUnsafe() const & { return ValueOrDie(); }
    T ValueUnsafe() && { return std::move(*this).ValueOrDie(); }

    T & operator*() & { return ValueOrDie(); }
    T * operator->() { return &ValueOrDie(); }

private:
    void check() const
    {
        if (!ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The value of a failed Arrow Flight result was requested: {}", status().ToString());
    }

    std::variant<T, Status> value;
};

namespace detail
{
inline Status toStatus(Status status) { return status; }
template <typename T>
Status toStatus(const Result<T> & result) { return result.status(); }
}

}

#define FLIGHT_CONCAT_IMPL(x, y) x##y
#define FLIGHT_CONCAT(x, y) FLIGHT_CONCAT_IMPL(x, y)

/// As `ARROW_RETURN_NOT_OK`: returns the status of a failed `Status` or `Result` from the enclosing function.
#define FLIGHT_RETURN_NOT_OK(expr) \
    do \
    { \
        auto _flight_status = ::DB::ArrowFlight::detail::toStatus((expr)); \
        if (!_flight_status.ok()) \
            return _flight_status; \
    } while (false)

/// `lhs` can be a declaration, e.g. `auto value`, so it cannot be enclosed in parentheses.
/// NOLINTBEGIN(bugprone-macro-parentheses)
#define FLIGHT_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr) \
    auto && result_name = (rexpr); \
    if (!result_name.ok()) \
        return result_name.status(); \
    lhs = std::move(result_name).ValueUnsafe();
/// NOLINTEND(bugprone-macro-parentheses)

/// As `ARROW_ASSIGN_OR_RAISE`: assigns the value of a `Result`, or returns its status from the enclosing function.
#define FLIGHT_ASSIGN_OR_RAISE(lhs, rexpr) FLIGHT_ASSIGN_OR_RAISE_IMPL(FLIGHT_CONCAT(_flight_result_, __COUNTER__), lhs, rexpr)

#endif

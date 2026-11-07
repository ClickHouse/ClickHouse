#pragma once

#include <Common/Exception.h>
#include <base/types.h>

namespace Kafka
{

enum class ErrorKind : UInt8
{
    /// Invalid user input (bad config, bad arguments).
    Config,
    /// Malformed or unexpected bytes on the wire.
    Protocol,
    /// Socket-level failure (connect/read/write/reset).
    Connection,
    /// Deadline exceeded.
    Timeout,
    /// A blocking call was interrupted by Client::wakeup().
    Wakeup,
    /// The broker returned a non-zero Kafka error code (see getBrokerCode()).
    Broker,
    /// The feature is not implemented in this version (e.g. compressed batches).
    NotSupported,
};

/// Symbolic name for a Kafka broker error code ("NONE", "NOT_COORDINATOR", ...),
/// "UNKNOWN" for codes the client has no entry for.
const char * brokerErrorName(Int16 code) noexcept;

/// True for codes Kafka classifies as transient: a retry after refreshing
/// metadata / re-resolving the coordinator is expected to clear the condition.
bool isBrokerErrorRetriable(Int16 code) noexcept;

const char * errorKindName(ErrorKind kind) noexcept;

/// The DB::ErrorCodes code an ErrorKind maps to.
int clickHouseErrorCode(ErrorKind kind) noexcept;

/// Tags a Kafka protocol error code for Exception construction. A dedicated
/// leading type (instead of a plain Int16 parameter) keeps the fmt-string
/// constructors unambiguous — the same first-argument-type discrimination
/// DB::Exception uses, because FormatStringHelper accepts any argument
/// syntactically and would otherwise collide in overload resolution.
struct BrokerCode
{
    Int16 value = 0;
};

/// The single exception type thrown by the Kafka client. Carries the error
/// kind and, for ErrorKind::Broker, the Kafka protocol error code on top of
/// the usual DB::Exception machinery.
class Exception : public DB::Exception
{
public:
    /// Format the message with fmt, like the logging functions.
    template <typename... Args>
    Exception(ErrorKind kind_, FormatStringHelper<Args...> fmt, Args &&... args)
        : DB::Exception(PreformattedMessage{fmt.format(std::forward<Args>(args)...)}, clickHouseErrorCode(kind_))
        , kind(kind_)
    {
    }

    /// A broker-reported error (implies ErrorKind::Broker); the code's
    /// symbolic name is appended to the message.
    template <typename... Args>
    Exception(BrokerCode broker_code_, FormatStringHelper<Args...> fmt, Args &&... args)
        : DB::Exception(PreformattedMessage{fmt.format(std::forward<Args>(args)...)}, clickHouseErrorCode(ErrorKind::Broker))
        , kind(ErrorKind::Broker)
        , broker_code(broker_code_.value)
    {
        appendBrokerError();
    }

    /// For messages composed at runtime; prefer the fmt constructors.
    Exception(ErrorKind kind_, PreformattedMessage && message, Int16 broker_code_ = 0)
        : DB::Exception(std::move(message), clickHouseErrorCode(kind_))
        , kind(kind_)
        , broker_code(broker_code_)
    {
        appendBrokerError();
    }

    ErrorKind getKind() const noexcept { return kind; }

    /// Kafka broker error code when getKind() == Broker, 0 otherwise.
    Int16 getBrokerCode() const noexcept { return broker_code; }

    /// True when retrying the operation (possibly on a fresh connection /
    /// after a metadata refresh) can reasonably succeed.
    bool isRetriable() const noexcept;

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)

private:
    void appendBrokerError();

    ErrorKind kind;
    Int16 broker_code = 0;
};

}

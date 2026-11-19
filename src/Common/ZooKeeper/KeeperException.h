#pragma once

#include <utility>
#include <Common/Exception.h>
#include <Common/ZooKeeper/Types.h>

namespace DB::ErrorCodes
{
extern const int KEEPER_EXCEPTION;
}

namespace Coordination
{

class Exception : public DB::Exception
{
private:
    /// Delegate constructor, used to minimize repetition; last parameter used for overload resolution.
    Exception(const std::string & msg, Error code_, int); /// NOLINT
    Exception(PreformattedMessage && msg, Error code_);

    /// Message must be a compile-time constant
    template <typename T>
    requires std::is_convertible_v<T, String>
    Exception(T && message, Error code_) : DB::Exception(message, DB::ErrorCodes::KEEPER_EXCEPTION, /* format_string= */"", /* remote_= */ false), code(code_)
    {
        incrementErrorMetrics(code);
    }

    static void incrementErrorMetrics(Error code_);

public:
    explicit Exception(Error code_); /// NOLINT
    Exception(const Exception & exc);

    template <typename... Args>
    Exception(Error code_, FormatStringHelper<Args...> fmt, Args &&... args)
        : DB::Exception(DB::ErrorCodes::KEEPER_EXCEPTION, std::move(fmt), std::forward<Args>(args)...)
        , code(code_)
    {
        incrementErrorMetrics(code);
    }

    static Exception createDeprecated(const std::string & msg, Error code_)
    {
        return Exception(msg, code_, 0);
    }

    static Exception fromPath(Error code_, const std::string & path)
    {
        return Exception(code_, "Coordination error: {}, path {}", errorMessage(code_), path);
    }

    /// Message must be a compile-time constant
    template <typename T>
    requires std::is_convertible_v<T, String>
    static Exception fromMessage(Error code_, T && message)
    {
        return Exception(std::forward<T>(message), code_);
    }

    const char * name() const noexcept override { return "Coordination::Exception"; }
    const char * className() const noexcept override { return "Coordination::Exception"; }
    Exception * clone() const override { return new Exception(*this); }

    const Error code;
};

}

namespace zkutil
{


using KeeperException = Coordination::Exception;


class KeeperMultiException : public KeeperException
{
public:
    Coordination::Requests requests;
    Coordination::Responses responses;
    size_t failed_op_index = 0;

    std::string getPathForFirstFailedOp() const;

    /// If it is user error throws KeeperMultiException else throws ordinary KeeperException
    /// If it is ZOK does nothing
    static void check(Coordination::Error code, const Coordination::Requests & requests, const Coordination::Responses & responses);

    KeeperMultiException(Coordination::Error code, const Coordination::Requests & requests, const Coordination::Responses & responses);

private:
    KeeperMultiException(Coordination::Error code, size_t failed_op_index_, const Coordination::Requests & requests_, const Coordination::Responses & responses_);
};

size_t getFailedOpIndex(Coordination::Error code, const Coordination::Responses & responses);
}

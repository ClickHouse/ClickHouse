#pragma once

#include <Common/Exception.h>


namespace DB
{

class NetException : public Exception
{
public:
    template <typename T>
    requires std::is_convertible_v<T, String>
    NetException(int code, T && message) : Exception(message, code)
    {
        message_format_string = tryGetStaticFormatString(message);
    }

    template<> NetException(int code, const String & message) : Exception(message, code) {}
    template<> NetException(int code, String & message) : Exception(message, code) {}
    template<> NetException(int code, String && message) : Exception(std::move(message), code) {}

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    NetException(int code, FormatStringHelper<Args...> fmt, Args &&... args)
        : Exception(fmt::format(fmt.fmt_str, std::forward<Args>(args)...), code)
    {
        message_format_string = fmt.message_format_string;
    }

    NetException * clone() const override { return new NetException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)

private:
    const char * name() const noexcept override { return "DB::NetException"; }
    const char * className() const noexcept override { return "DB::NetException"; }
};

}

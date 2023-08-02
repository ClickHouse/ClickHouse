#pragma once

#include <Common/Exception.h>


namespace DB
{

class NetException : public Exception
{
public:
    NetException(const std::string & msg, int code) : Exception(msg, code) {}

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    NetException(int code, fmt::format_string<Args...> fmt, Args &&... args)
        : Exception(fmt::format(fmt, std::forward<Args>(args)...), code)
    {
    }

    NetException * clone() const override { return new NetException(*this); }
    void rethrow() const override { throw *this; }

private:
    const char * name() const noexcept override { return "DB::NetException"; }
    const char * className() const noexcept override { return "DB::NetException"; }
};

}

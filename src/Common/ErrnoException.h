#pragma once

#include <Common/Exception.h>
#include <base/errnoToString.h>

#include <cerrno>
#include <optional>


namespace DB
{

/// Contains an additional member `saved_errno`
class ErrnoException : public Exception
{
public:
    ErrnoException(std::string && msg, int code, int with_errno) : Exception(msg, code), saved_errno(with_errno)
    {
        capture_thread_frame_pointers = getThreadFramePointers();
        addMessage(", {}", errnoToString(saved_errno));
    }

    /// Message must be a compile-time constant
    template <typename T>
    requires std::is_convertible_v<T, String>
    ErrnoException(int code, T && message) : Exception(message, code), saved_errno(errno)
    {
        capture_thread_frame_pointers = getThreadFramePointers();
        addMessage(", {}", errnoToString(saved_errno));
    }

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    ErrnoException(int code, FormatStringHelper<Args...> fmt, Args &&... args)
        : Exception(fmt.format(std::forward<Args>(args)...), code), saved_errno(errno)
    {
        addMessage(", {}", errnoToString(saved_errno));
    }

    template <typename... Args>
    ErrnoException(int code, int with_errno, FormatStringHelper<Args...> fmt, Args &&... args)
        : Exception(fmt.format(std::forward<Args>(args)...), code), saved_errno(with_errno)
    {
        addMessage(", {}", errnoToString(saved_errno));
    }

    template <typename... Args>
    [[noreturn]] static void throwWithErrno(int code, int with_errno, FormatStringHelper<Args...> fmt, Args &&... args)
    {
        auto e = ErrnoException(code, with_errno, std::move(fmt), std::forward<Args>(args)...);
        throw e; /// NOLINT
    }

    template <typename... Args>
    [[noreturn]] static void throwFromPath(int code, const std::string & path, FormatStringHelper<Args...> fmt, Args &&... args)
    {
        auto e = ErrnoException(code, errno, std::move(fmt), std::forward<Args>(args)...);
        e.path = path;
        throw e; /// NOLINT
    }

    template <typename... Args>
    [[noreturn]] static void
    throwFromPathWithErrno(int code, const std::string & path, int with_errno, FormatStringHelper<Args...> fmt, Args &&... args)
    {
        auto e = ErrnoException(code, with_errno, std::move(fmt), std::forward<Args>(args)...);
        e.path = path;
        throw e; /// NOLINT
    }

    ErrnoException * clone() const override { return new ErrnoException(*this); }
    void rethrow() const override { throw *this; } // NOLINT

    int getErrno() const { return saved_errno; }
    std::optional<std::string> getPath() const { return path; }

private:
    int saved_errno;
    std::optional<std::string> path;

    const char * name() const noexcept override { return "DB::ErrnoException"; }
    const char * className() const noexcept override { return "DB::ErrnoException"; }
};

}

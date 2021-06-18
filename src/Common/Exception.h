#pragma once

#include <cerrno>
#include <vector>
#include <memory>

#include <Poco/Exception.h>

#include <Common/StackTrace.h>

#include <fmt/format.h>

namespace Poco { class Logger; }


namespace DB
{

class Exception : public Poco::Exception
{
public:
    Exception() = default;
    Exception(const std::string & msg, int code);

    // Format message with fmt::format, like the logging functions.
    template <typename ...Fmt>
    Exception(int code, Fmt&&... fmt)
        : Exception(fmt::format(std::forward<Fmt>(fmt)...), code)
    {}

    struct CreateFromPocoTag {};
    struct CreateFromSTDTag {};

    Exception(CreateFromPocoTag, const Poco::Exception & exc);
    Exception(CreateFromSTDTag, const std::exception & exc);

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; }
    const char * name() const throw() override { return "DB::Exception"; }
    const char * what() const throw() override { return message().data(); }

    /// Add something to the existing message.
    void addMessage(const std::string & arg) { extendedMessage(arg); }

    std::string getStackTraceString() const;

private:
#ifndef STD_EXCEPTION_HAS_STACK_TRACE
    StackTrace trace;
#endif

    const char * className() const throw() override { return "DB::Exception"; }
};


std::string getExceptionStackTraceString(const std::exception & e);


/// Contains an additional member `saved_errno`. See the throwFromErrno function.
class ErrnoException : public Exception
{
public:
    ErrnoException(const std::string & msg, int code, int saved_errno_, const std::optional<std::string> & path_ = {})
        : Exception(msg, code), saved_errno(saved_errno_), path(path_) {}

    ErrnoException * clone() const override { return new ErrnoException(*this); }
    void rethrow() const override { throw *this; }

    int getErrno() const { return saved_errno; }
    const std::optional<std::string> getPath() const { return path; }

private:
    int saved_errno;
    std::optional<std::string> path;

    const char * name() const throw() override { return "DB::ErrnoException"; }
    const char * className() const throw() override { return "DB::ErrnoException"; }
};


using Exceptions = std::vector<std::exception_ptr>;


[[noreturn]] void throwFromErrno(const std::string & s, int code, int the_errno = errno);
/// Useful to produce some extra information about available space and inodes on device
[[noreturn]] void throwFromErrnoWithPath(const std::string & s, const std::string & path, int code,
                                         int the_errno = errno);


/** Try to write an exception to the log (and forget about it).
  * Can be used in destructors in the catch-all block.
  */
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "");


/** Prints current exception in canonical format.
  * with_stacktrace - prints stack trace for DB::Exception.
  * check_embedded_stacktrace - if DB::Exception has embedded stacktrace then
  *  only this stack trace will be printed.
  * with_extra_info - add information about the filesystem in case of "No space left on device" and similar.
  */
std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace = false,
                                       bool with_extra_info = true);

/// Returns error code from ErrorCodes
int getCurrentExceptionCode();


/// An execution status of any piece of code, contains return code and optional error
struct ExecutionStatus
{
    int code = 0;
    std::string message;

    ExecutionStatus() = default;

    explicit ExecutionStatus(int return_code, const std::string & exception_message = "")
    : code(return_code), message(exception_message) {}

    static ExecutionStatus fromCurrentException(const std::string & start_of_message = "");

    std::string serializeText() const;

    void deserializeText(const std::string & data);

    bool tryDeserializeText(const std::string & data);
};


void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message = "");
void tryLogException(std::exception_ptr e, Poco::Logger * logger, const std::string & start_of_message = "");

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace = false);
std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace);


void rethrowFirstException(const Exceptions & exceptions);


template <typename T>
std::enable_if_t<std::is_pointer_v<T>, T> exception_cast(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (std::remove_pointer_t<T> & concrete)
    {
        return &concrete;
    }
    catch (...)
    {
        return nullptr;
    }
}

}

#pragma once

#include <cerrno>
#include <vector>
#include <memory>

#include <Poco/Exception.h>

#include <Common/StackTrace.h>

namespace Poco { class Logger; }


namespace DB
{

class Exception : public Poco::Exception
{
public:
    Exception(int code = 0) : Poco::Exception(code) {}
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    Exception(const std::string & msg, const std::string & arg, int code = 0) : Poco::Exception(msg, arg, code) {}
    Exception(const std::string & msg, const Exception & exc, int code = 0) : Poco::Exception(msg, exc, code), trace(exc.trace) {}
    Exception(const Exception & exc) : Poco::Exception(exc), trace(exc.trace) {}
    explicit Exception(const Poco::Exception & exc) : Poco::Exception(exc.displayText()) {}
    ~Exception() throw() override {}
    Exception & operator = (const Exception & exc)
    {
        Poco::Exception::operator=(exc);
        trace = exc.trace;
        return *this;
    }
    const char * name() const throw() override { return "DB::Exception"; }
    const char * className() const throw() override { return "DB::Exception"; }
    DB::Exception * clone() const override { return new DB::Exception(*this); }
    void rethrow() const override { throw *this; }

    /// Add something to the existing message.
    void addMessage(const std::string & arg) { extendedMessage(arg); }

    const StackTrace & getStackTrace() const { return trace; }

private:
    StackTrace trace;
};


/// Contains an additional member `saved_errno`. See the throwFromErrno function.
class ErrnoException : public Exception
{
public:
    ErrnoException(int code = 0, int saved_errno_ = 0)
        : Exception(code), saved_errno(saved_errno_) {}
    ErrnoException(const std::string & msg, int code = 0, int saved_errno_ = 0)
        : Exception(msg, code), saved_errno(saved_errno_) {}
    ErrnoException(const std::string & msg, const std::string & arg, int code = 0, int saved_errno_ = 0)
        : Exception(msg, arg, code), saved_errno(saved_errno_) {}
    ErrnoException(const std::string & msg, const Exception & exc, int code = 0, int saved_errno_ = 0)
        : Exception(msg, exc, code), saved_errno(saved_errno_) {}
    ErrnoException(const ErrnoException & exc)
        : Exception(exc), saved_errno(exc.saved_errno) {}

    int getErrno() const { return saved_errno; }

private:
    int saved_errno;
};


using Exceptions = std::vector<std::exception_ptr>;


void throwFromErrno(const std::string & s, int code = 0, int the_errno = errno);


/** Try to write an exception to the log (and forget about it).
  * Can be used in destructors in the catch-all block.
  */
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "");

/** Prints current exception in canonical format.
  * with_stacktrace - prints stack trace for DB::Exception.
  * check_embedded_stacktrace - if DB::Exception has embedded stacktrace then
  *  only this stack trace will be printed.
  */
std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace = false);

/// Returns error code from ErrorCodes
int getCurrentExceptionCode();

void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message = "");
void tryLogException(std::exception_ptr e, Poco::Logger * logger, const std::string & start_of_message = "");

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace);


void rethrowFirstException(const Exceptions & exceptions);

std::unique_ptr<Poco::Exception> convertCurrentException();


template <typename T>
typename std::enable_if<std::is_pointer<T>::value, T>::type exception_cast(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(e);
    }
    catch (typename std::remove_pointer<T>::type & concrete)
    {
        return &concrete;
    }
    catch (...)
    {
        return nullptr;
    }
}

}

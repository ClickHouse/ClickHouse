#pragma once

#include <Core/LogsLevel.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/FramePointers.h>
#include <Common/LoggingFormatStringHelpers.h>

#include <atomic>
#include <exception>
#include <vector>

#include <fmt/format.h>
#include <Poco/Exception.h>

namespace Poco
{
class Channel;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;
using LoggerRawPtr = Poco::Logger *;
class LogFrequencyLimiterImpl;

namespace DB
{

class AtomicLogger;

/// This flag can be set for testing purposes - to check that no exceptions are thrown.
extern bool terminate_on_any_exception;
extern std::atomic_bool abort_on_logical_error;

class Exception : public Poco::Exception
{
public:
    using Trace = std::vector<void *>;

    Exception()
    {
        if (terminate_on_any_exception)
            std::terminate();
        capture_thread_frame_pointers = getThreadFramePointers();
    }

    Exception(const PreformattedMessage & msg, int code): Exception(msg.text, code, std::string(msg.format_string))
    {
        if (terminate_on_any_exception)
            std::terminate();
        capture_thread_frame_pointers = getThreadFramePointers();
        message_format_string = msg.format_string;
        message_format_string_args = msg.format_string_args;
    }

    Exception(PreformattedMessage && msg, int code): Exception(std::move(msg.text), code, std::string(msg.format_string))
    {
        if (terminate_on_any_exception)
            std::terminate();
        capture_thread_frame_pointers = getThreadFramePointers();
        message_format_string = msg.format_string;
        message_format_string_args = std::move(msg.format_string_args);
    }

    ~Exception() override;

    Exception(const Exception & o)
        : Poco::Exception(o)
    {
        remote = o.remote;
        logged.store(o.logged.load());
        error_index = o.error_index;
        message_format_string = o.message_format_string;
        message_format_string_args = o.message_format_string_args;
        capture_thread_frame_pointers = o.capture_thread_frame_pointers;
    }

    Exception(Exception && o) noexcept
    {
        remote = o.remote;
        logged.store(o.logged.load());
        error_index = o.error_index;
        message_format_string = o.message_format_string;
        message_format_string_args = std::move(o.message_format_string_args);
        capture_thread_frame_pointers = std::move(o.capture_thread_frame_pointers);
        Poco::Exception::operator=(std::move(o));
    }

    Exception & operator=(const Exception & o)
    {
        if (this != &o)
        {
            remote = o.remote;
            logged.store(o.logged.load());
            error_index = o.error_index;
            message_format_string = o.message_format_string;
            message_format_string_args = o.message_format_string_args;
            capture_thread_frame_pointers = o.capture_thread_frame_pointers;
            Poco::Exception::operator=(o);
        }
        return *this;
    }


    Exception & operator=(Exception && o) noexcept
    {
        if (this != &o)
        {
            remote = o.remote;
            logged.store(o.logged.load());
            error_index = o.error_index;
            message_format_string = o.message_format_string;
            message_format_string_args = std::move(o.message_format_string_args);
            capture_thread_frame_pointers = std::move(o.capture_thread_frame_pointers);
            Poco::Exception::operator=(std::move(o));
        }
        return *this;
    }

    /// Collect call stacks of all previous jobs' schedulings leading to this thread job's execution
    static thread_local bool enable_job_stack_trace;
    using ThreadFramePointersBase = std::vector<FramePointers>;

    /// If thread is going to use thread_frame_pointers then this initializer should be called at the beginning of a thread function.
    /// It is necessary to force thread_frame_pointers to be initialized - static thread_local members are lazy initializable.
    static void initializeThreadFramePointers()
    {
        [[maybe_unused]] auto &v = thread_frame_pointers;
    }

    static const ThreadFramePointersBase & getThreadFramePointers();
    static void setThreadFramePointers(ThreadFramePointersBase frame_pointers);
    static void clearThreadFramePointers();

    /// Callback for any exception
    static std::function<void(std::string_view format_string, int code, bool remote, const Exception::Trace & trace)> callback;

protected:
    static thread_local bool can_use_thread_frame_pointers;
    /// Because of unknown order of static destructor calls,
    /// thread_frame_pointers can already be uninitialized when a different destructor generates an exception.
    /// To prevent such scenarios, a wrapper class is created and a function that will return empty vector
    /// if its destructor is already called
    struct ThreadFramePointers
    {
        ThreadFramePointers();
        ~ThreadFramePointers();

        ThreadFramePointersBase frame_pointers;
    };
    static thread_local ThreadFramePointers thread_frame_pointers;
    static const ThreadFramePointersBase dummy_frame_pointers;

    // used to remove the sensitive information from exceptions if query_masking_rules is configured
    struct MessageMasked
    {
        std::string msg;
        std::string format_string;
        explicit MessageMasked(const std::string & msg_, std::string format_string_);
        explicit MessageMasked(std::string && msg_, std::string format_string_);
        explicit MessageMasked(const std::string & msg_) : msg(msg_) {}
        explicit MessageMasked(std::string && msg_) : msg(msg_) {}
    };

    Exception(const MessageMasked & msg_masked, int code, bool remote_);
    Exception(MessageMasked && msg_masked, int code, bool remote_);

    // delegating constructor to mask sensitive information from the message
    Exception(const std::string & msg, int code, std::string format_string = "", bool remote_ = false): Exception(MessageMasked(msg, std::move(format_string)), code, remote_) {}
    Exception(std::string && msg, int code, std::string format_string = "", bool remote_ = false): Exception(MessageMasked(std::move(msg), std::move(format_string)), code, remote_) {}

public:
    /// This creator is for exceptions that should format a message using fmt::format from the variadic ctor Exception(code, fmt, ...),
    /// but were not rewritten yet. It will be removed.
    static Exception createDeprecated(const std::string & msg, int code, bool remote_ = false)
    {
        return Exception(msg, code, "", remote_);
    }

    /// These creators are for messages that were received by network or generated by a third-party library in runtime.
    /// Please use a constructor for all other cases.
    static Exception createRuntime(int code, const String & message) { return Exception(message, code); }
    static Exception createRuntime(int code, String & message) { return Exception(message, code); }
    static Exception createRuntime(int code, String && message) { return Exception(std::move(message), code); }

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    Exception(int code, FormatStringHelper<Args...> fmt, Args &&... args) : Exception(fmt.format(std::forward<Args>(args)...), code) {}

    struct CreateFromPocoTag {};
    struct CreateFromSTDTag {};

    Exception(CreateFromPocoTag, const Poco::Exception & exc);
    Exception(CreateFromSTDTag, const std::exception & exc);

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; } // NOLINT
    const char * name() const noexcept override { return "DB::Exception"; }
    const char * what() const noexcept override { return message().data(); }

    /// Add something to the existing message.
    template <typename... Args>
    void addMessage(fmt::format_string<Args...> format, Args &&... args)
    {
        addMessage(fmt::format(format, std::forward<Args>(args)...));
    }

    void addMessage(const std::string& message)
    {
        addMessage(MessageMasked(message));
    }

    void addMessage(const MessageMasked & msg_masked);

    /// Used to distinguish local exceptions from the one that was received from remote node.
    void setRemoteException(bool remote_ = true) { remote = remote_; }
    bool isRemoteException() const { return remote; }

    std::string getStackTraceString() const;
    /// Used for system.errors
    Trace getStackFramePointers() const;

    std::string_view tryGetMessageFormatString() const { return message_format_string; }

    std::vector<std::string> getMessageFormatStringArgs() const { return message_format_string_args; }

    void markAsLogged() { logged.store(true, std::memory_order_relaxed); }

    /// Indicates if the error code triggers alerts in ClickHouse Cloud
    bool isErrorCodeImportant() const;

private:
    bool remote = false;
    std::atomic<bool> logged = false;

    /// Number of this error among other errors with the same code and the same `remote` flag since the program startup.
    size_t error_index = static_cast<size_t>(-1);

    const char * className() const noexcept override { return "DB::Exception"; }

protected:
    std::string_view message_format_string;
    std::vector<std::string> message_format_string_args;
    /// Local copy of static per-thread thread_frame_pointers, should be mutable to be unpoisoned on printout
    mutable std::vector<FramePointers> capture_thread_frame_pointers;
};

/// Most common exception constructor (just a string). Forward declare to avoid many unnecessary instantiations
extern template Exception::Exception(int, FormatStringHelperImpl<>);

[[noreturn]] void abortOnFailedAssertion(const String & description, std::string_view format_string, void * const * trace, size_t trace_offset, size_t trace_size);
[[noreturn]] void abortOnFailedAssertion(const String & description);

std::string getExceptionStackTraceString(const std::exception & e);
std::string getExceptionStackTraceString(std::exception_ptr e);


/// An exception to use in unit tests to test interfaces.
/// It is distinguished from others, so it does not have to be logged.
class TestException : public Exception
{
public:
    using Exception::Exception;
};


using Exceptions = std::vector<std::exception_ptr>;

/** Try to write an exception to the log (and forget about it).
  * Can be used in destructors in the catch-all block.
  */
/// TODO: Logger leak constexpr overload
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "", LogsLevel level = LogsLevel::error);
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "", LogsLevel level = LogsLevel::error);
void tryLogCurrentException(LoggerPtr logger, const std::string & start_of_message = "", LogsLevel level = LogsLevel::error);
void tryLogCurrentException(const AtomicLogger & logger, const std::string & start_of_message = "", LogsLevel level = LogsLevel::error);
void tryLogCurrentException(LogFrequencyLimiterImpl && logger, const std::string & start_of_message = "", LogsLevel level = LogsLevel::error);


/** Prints current exception in canonical format.
  * with_stacktrace - prints stack trace for DB::Exception.
  * check_embedded_stacktrace - if DB::Exception has embedded stacktrace then
  *  only this stack trace will be printed.
  * with_extra_info - add information about the filesystem in case of "No space left on device" and similar.
  */
std::string getCurrentExceptionMessage(
    bool with_stacktrace,
    bool check_embedded_stacktrace = false,
    bool with_extra_info = true,
    bool with_version = true);
PreformattedMessage getCurrentExceptionMessageAndPattern(
    bool with_stacktrace,
    bool check_embedded_stacktrace = false,
    bool with_extra_info = true,
    bool with_version = true);

/// Returns error code from ErrorCodes
int getCurrentExceptionCode();
int getExceptionErrorCode(std::exception_ptr e);

/// Returns string containing extra diagnostic info for specific exceptions (like "no space left on device" and "memory limit exceeded")
std::string getExtraExceptionInfo(const std::exception & e);

/// An execution status of any piece of code, contains return code and optional error
struct ExecutionStatus
{
    int code = 0;
    std::string message;

    ExecutionStatus() = default;

    explicit ExecutionStatus(int return_code, const std::string & exception_message = "")
    : code(return_code), message(exception_message) {}

    static ExecutionStatus fromCurrentException(const std::string & start_of_message = "", bool with_stacktrace = false, bool with_version = true);

    static ExecutionStatus fromText(const std::string & data);

    std::string serializeText() const;

    void deserializeText(const std::string & data);

    bool tryDeserializeText(const std::string & data);
};

/// TODO: Logger leak constexpr overload
void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message = "");
void tryLogException(std::exception_ptr e, LoggerPtr logger, const std::string & start_of_message = "", LogsLevel level = LogsLevel::error);
void tryLogException(std::exception_ptr e, const AtomicLogger & logger, const std::string & start_of_message = "");

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace = false);
std::string getExceptionMessageForLogging(Exception & e, bool with_stacktrace, bool check_embedded_stacktrace = false);
PreformattedMessage getExceptionMessageAndPattern(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace = false);
std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace, bool check_embedded_stacktrace = false);


template <typename T>
requires std::is_pointer_v<T>
T current_exception_cast()
{
    try
    {
        throw;
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

/// Return copy of a current exception if it is a Poco::Exception (DB::Exception), since this exception is mutable, and returning reference is unsafe.
/// And a reference otherwise.
std::exception_ptr copyMutableException(std::exception_ptr ptr);

}

#include <base/MemorySanitizer.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/demangle.h>
#include <Core/LogsLevel.h>
#include <Common/AtomicLogger.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/ExceptionExt.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/Logger.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/StackTrace.h>
#include <Common/config_version.h>
#include <Common/filesystemHelpers.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <cxxabi.h>

#include <Poco/String.h>

static_assert(STD_EXCEPTION_HAS_STACK_TRACE == 1);

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int AVRO_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MREMAP;
    extern const int POTENTIALLY_BROKEN_DATA_PART;
    extern const int CORRUPTED_DATA;
}

void abortOnFailedAssertion(const String & description, std::string_view format_string, void * const * trace, size_t trace_offset, size_t trace_size)
{
    auto & logger = Poco::Logger::root();
    LOG_FATAL(&logger, "Logical error: '{}'.", description);
    if (!format_string.empty())
        LOG_FATAL(&logger, "Format string: '{}'.", format_string);
    if (trace)
        LOG_FATAL(&logger, "Stack trace (when copying this message, always include the lines below):\n\n{}", StackTrace::toString(trace, trace_offset, trace_size));
    abort();
}

void abortOnFailedAssertion(const String & description)
{
    StackTrace st;
    abortOnFailedAssertion(description, "", st.getFramePointers().data(), st.getOffset(), st.getSize());
}

bool terminate_on_any_exception = false;
std::atomic_bool abort_on_logical_error = false;
static int terminate_status_code = 128 + SIGABRT;
std::function<void(std::string_view format_string, int code, bool remote, const Exception::Trace & trace)> Exception::callback = {};

constexpr bool debug_or_sanitizer_build =
#ifdef DEBUG_OR_SANITIZER_BUILD
true
#else
false
#endif
;


/// - Aborts the process if error code is LOGICAL_ERROR.
/// - Increments error codes statistics.
static size_t handle_error_code(
    const std::string & msg, std::string_view format_string, int code, bool remote, const Exception::Trace & trace)
{
    // In debug builds and builds with sanitizers, treat LOGICAL_ERROR as an assertion failure.
    // Log the message before we fail.
    if (code == ErrorCodes::LOGICAL_ERROR)
    {
        if (debug_or_sanitizer_build || abort_on_logical_error.load(std::memory_order_relaxed))
            abortOnFailedAssertion(msg, format_string, trace.data(), 0, trace.size());
    }

    if (Exception::callback)
    {
        /// Only anonymized exception message (format_string) is send to the monitoring callback,
        /// So it does not include customer queries.
        Exception::callback(format_string, code, remote, trace);
    }

    return ErrorCodes::increment(code, remote, msg, std::string(format_string), trace);
}

template Exception::Exception(int, FormatStringHelperImpl<>);


Exception::MessageMasked::MessageMasked(const std::string & msg_, std::string format_string_)
    : msg(msg_)
    , format_string(std::move(format_string_))
{
    if (auto masker = SensitiveDataMasker::getInstance())
        masker->wipeSensitiveData(msg);
}

Exception::MessageMasked::MessageMasked(std::string && msg_, std::string format_string_)
    : msg(std::move(msg_))
    , format_string(std::move(format_string_))
{
    if (auto masker = SensitiveDataMasker::getInstance())
        masker->wipeSensitiveData(msg);
}

const Exception::ThreadFramePointersBase Exception::dummy_frame_pointers = {};

Exception::Exception(const MessageMasked & msg_masked, int code, bool remote_)
    : Poco::Exception(msg_masked.msg, code)
    , remote(remote_)
{
    if (terminate_on_any_exception)
        std::_Exit(terminate_status_code);
    capture_thread_frame_pointers = getThreadFramePointers();
    message_format_string = msg_masked.format_string;
    error_index = handle_error_code(msg_masked.msg, message_format_string, code, remote, getStackFramePointers());
}

Exception::Exception(MessageMasked && msg_masked, int code, bool remote_)
    : Poco::Exception(std::move(msg_masked.msg), code)
    , remote(remote_)
{
    if (terminate_on_any_exception)
        std::_Exit(terminate_status_code);
    capture_thread_frame_pointers = getThreadFramePointers();
    message_format_string = msg_masked.format_string;
    error_index = handle_error_code(message(), message_format_string, code, remote, getStackFramePointers());
}

Exception::Exception(CreateFromPocoTag, const Poco::Exception & exc)
    : Poco::Exception(exc.displayText(), ErrorCodes::POCO_EXCEPTION)
{
    if (terminate_on_any_exception)
        std::_Exit(terminate_status_code);
    capture_thread_frame_pointers = getThreadFramePointers();
    auto * stack_trace_frames = exc.get_stack_trace_frames();
    auto stack_trace_size = exc.get_stack_trace_size();
    __msan_unpoison(stack_trace_frames, stack_trace_size * sizeof(stack_trace_frames[0]));
    set_stack_trace(stack_trace_frames, stack_trace_size);
}

static int getCodeForSTDException(const std::exception & exc)
{
    /// This looks strange, but avoids a direct dependency on the external library.
    std::string name = demangle(typeid(exc).name());
    if (name.starts_with("avro::"))
        return ErrorCodes::AVRO_EXCEPTION;
    return ErrorCodes::STD_EXCEPTION;
}

Exception::Exception(CreateFromSTDTag, const std::exception & exc)
    : Poco::Exception(demangle(typeid(exc).name()) + ": " + String(exc.what()), getCodeForSTDException(exc))
{
    if (terminate_on_any_exception)
        std::_Exit(terminate_status_code);
    capture_thread_frame_pointers = getThreadFramePointers();
    auto * stack_trace_frames = exc.get_stack_trace_frames();
    auto stack_trace_size = exc.get_stack_trace_size();
    __msan_unpoison(stack_trace_frames, stack_trace_size * sizeof(stack_trace_frames[0]));
    set_stack_trace(stack_trace_frames, stack_trace_size);
}

void Exception::addMessage(const MessageMasked & msg_masked)
{
    extendedMessage(msg_masked.msg);
    if (error_index != static_cast<size_t>(-1))
        ErrorCodes::extendedMessage(code(), remote, error_index, message());
}


std::string getExceptionStackTraceString(const std::exception & e)
{
    /// Explicitly block MEMORY_LIMIT_EXCEEDED
    LockMemoryExceptionInThread lock(VariableContext::Global);

    auto * stack_trace_frames = e.get_stack_trace_frames();
    auto stack_trace_size = e.get_stack_trace_size();
    __msan_unpoison(stack_trace_frames, stack_trace_size * sizeof(stack_trace_frames[0]));
    return StackTrace::toString(stack_trace_frames, 0, stack_trace_size);
}

std::string getExceptionStackTraceString(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(e);
    }
    catch (const std::exception & exception)
    {
        return getExceptionStackTraceString(exception);
    }
    catch (...)
    {
        return {};
    }
}


std::string Exception::getStackTraceString() const
{
    auto * stack_trace_frames = get_stack_trace_frames();
    auto stack_trace_size = get_stack_trace_size();
    __msan_unpoison(stack_trace_frames, stack_trace_size * sizeof(stack_trace_frames[0]));
    String thread_stack_trace;
    std::for_each(capture_thread_frame_pointers.rbegin(), capture_thread_frame_pointers.rend(),
        [&thread_stack_trace](FramePointers & frame_pointers)
        {
            thread_stack_trace +=
                "\nJob's origin stack trace:\n" +
                StackTrace::toString(frame_pointers.data(), 0, std::ranges::find(frame_pointers, nullptr) - frame_pointers.begin());
        }
    );

    return StackTrace::toString(stack_trace_frames, 0, stack_trace_size) + thread_stack_trace;
}

Exception::Trace Exception::getStackFramePointers() const
{
    Trace frame_pointers;
    frame_pointers.resize(get_stack_trace_size());
    for (size_t i = 0; i < frame_pointers.size(); ++i)
    {
        frame_pointers[i] = get_stack_trace_frames()[i];
    }
    __msan_unpoison(frame_pointers.data(), frame_pointers.size() * sizeof(frame_pointers[0]));
    return frame_pointers;
}

thread_local bool Exception::enable_job_stack_trace = false;
thread_local bool Exception::can_use_thread_frame_pointers = false;
thread_local Exception::ThreadFramePointers Exception::thread_frame_pointers;

Exception::ThreadFramePointers::ThreadFramePointers()
{
    can_use_thread_frame_pointers = true;
}

Exception::ThreadFramePointers::~ThreadFramePointers()
{
    can_use_thread_frame_pointers = false;
}

const Exception::ThreadFramePointersBase & Exception::getThreadFramePointers()
{
    if (can_use_thread_frame_pointers)
        return thread_frame_pointers.frame_pointers;

    return dummy_frame_pointers;
}

void Exception::setThreadFramePointers(ThreadFramePointersBase frame_pointers)
{
    if (can_use_thread_frame_pointers)
        thread_frame_pointers.frame_pointers = std::move(frame_pointers);
}

void Exception::clearThreadFramePointers()
{
    if (can_use_thread_frame_pointers)
        thread_frame_pointers.frame_pointers.clear();
}

bool Exception::isErrorCodeImportant() const
{
    const int error_code = code();
    return error_code == ErrorCodes::LOGICAL_ERROR
        || error_code == ErrorCodes::POTENTIALLY_BROKEN_DATA_PART
        || error_code == ErrorCodes::CORRUPTED_DATA;
}

Exception::~Exception()
try
{
    if (!logged.load(std::memory_order_relaxed) && isErrorCodeImportant())
    {
        LOG_ERROR(getLogger("ForcedCriticalErrorsLogger"), "{}", getExceptionMessage(*this, /*with_stacktrace=*/ true));
    }
}
catch (...) // NOLINT(bugprone-empty-catch)
{
}

void tryLogCurrentException(const char * log_name, const std::string & start_of_message, LogsLevel level)
{
    /// Explicitly block MEMORY_LIMIT_EXCEEDED
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    /// getLogger can allocate memory too
    auto logger = getLogger(String{log_name});
    tryLogCurrentExceptionImpl(logger.get(), start_of_message, level);
}

void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message, LogsLevel level)
{
    /// Explicitly block MEMORY_LIMIT_EXCEEDED
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    tryLogCurrentExceptionImpl(logger, start_of_message, level);
}

void tryLogCurrentException(LoggerPtr logger, const std::string & start_of_message, LogsLevel level)
{
    tryLogCurrentException(logger.get(), start_of_message, level);
}

void tryLogCurrentException(const AtomicLogger & logger, const std::string & start_of_message, LogsLevel level)
{
    tryLogCurrentException(logger.load(), start_of_message, level);
}

static void getNoSpaceLeftInfoMessage(std::filesystem::path path, String & msg)
{
    path = std::filesystem::absolute(path);
    /// It's possible to get ENOSPC for non existent file (e.g. if there are no free inodes and creat() fails)
    /// So try to get info for existent parent directory.
    while (!std::filesystem::exists(path) && path.has_relative_path())
        path = path.parent_path();

    auto fs = getStatVFS(path);
    auto mount_point = getMountPoint(path).string();

    fmt::format_to(std::back_inserter(msg),
        "\nTotal space: {}\nAvailable space: {}\nTotal inodes: {}\nAvailable inodes: {}\nMount point: {}",
        ReadableSize(fs.f_blocks * fs.f_frsize),
        ReadableSize(fs.f_bavail * fs.f_frsize),
        formatReadableQuantity(fs.f_files),
        formatReadableQuantity(fs.f_favail),
        mount_point);

#if defined(OS_LINUX)
    msg += "\nFilesystem: " + getFilesystemName(mount_point);
#endif
}


/** It is possible that the system has enough memory,
  *  but we have shortage of the number of available memory mappings.
  * Provide good diagnostic to user in that case.
  */
static void getNotEnoughMemoryMessage(std::string & msg)
{
#if defined(OS_LINUX)
    try
    {
        static constexpr size_t buf_size = 1024;
        char buf[buf_size];

        UInt64 max_map_count = 0;
        {
            ReadBufferFromFile file("/proc/sys/vm/max_map_count", buf_size, -1, buf);
            readText(max_map_count, file);
        }

        UInt64 num_maps = 0;
        {
            ReadBufferFromFile file("/proc/self/maps", buf_size, -1, buf);
            while (!file.eof())
            {
                char * next_pos = find_first_symbols<'\n'>(file.position(), file.buffer().end());
                file.position() = next_pos;

                if (!file.hasPendingData())
                    continue;

                if (*file.position() == '\n')
                {
                    ++num_maps;
                    ++file.position();
                }
            }
        }

        if (static_cast<double>(num_maps) > static_cast<double>(max_map_count) * 0.90)
        {
            msg += fmt::format(
                "\nIt looks like that the process is near the limit on number of virtual memory mappings."
                "\nCurrent number of mappings (/proc/self/maps): {}."
                "\nLimit on number of mappings (/proc/sys/vm/max_map_count): {}."
                "\nYou should increase the limit for vm.max_map_count in /etc/sysctl.conf"
                "\n",
                num_maps, max_map_count);
        }
    }
    catch (...)
    {
        msg += "\nCannot obtain additional info about memory usage.";
    }
#else
    (void)msg;
#endif
}

std::string getExtraExceptionInfo(const std::exception & e)
{
    String msg;
    try
    {
        if (const auto * file_exception = dynamic_cast<const fs::filesystem_error *>(&e))
        {
            if (file_exception->code() == std::errc::no_space_on_device)
                getNoSpaceLeftInfoMessage(file_exception->path1(), msg);
            else
                msg += "\nCannot print extra info for Poco::Exception";
        }
        else if (const auto * errno_exception = dynamic_cast<const DB::ErrnoException *>(&e))
        {
            if (errno_exception->getErrno() == ENOSPC && errno_exception->getPath())
                getNoSpaceLeftInfoMessage(errno_exception->getPath().value(), msg);
            else if (errno_exception->code() == ErrorCodes::CANNOT_ALLOCATE_MEMORY
                || errno_exception->code() == ErrorCodes::CANNOT_MREMAP)
                getNotEnoughMemoryMessage(msg);
        }
        else if (dynamic_cast<const std::bad_alloc *>(&e))
        {
            getNotEnoughMemoryMessage(msg);
        }
    }
    catch (...)
    {
        msg += "\nCannot print extra info: " + getCurrentExceptionMessage(false, false, false);
    }

    return msg;
}

std::string getCurrentExceptionMessage(
    bool with_stacktrace,
    bool check_embedded_stacktrace /*= false*/,
    bool with_extra_info /*= true*/,
    bool with_version /*= true*/)
{
    return getCurrentExceptionMessageAndPattern(with_stacktrace, check_embedded_stacktrace, with_extra_info, with_version).text;
}

PreformattedMessage getCurrentExceptionMessageAndPattern(
    bool with_stacktrace,
    bool check_embedded_stacktrace /*= false*/,
    bool with_extra_info /*= true*/,
    bool with_version /*= true*/)
{
    /// Explicitly block MEMORY_LIMIT_EXCEEDED
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    WriteBufferFromOwnString stream;
    std::string_view message_format_string;
    std::vector<std::string> message_format_string_args;

    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        stream << getExceptionMessage(e, with_stacktrace, check_embedded_stacktrace)
               << (with_extra_info ? getExtraExceptionInfo(e) : "");
        if (with_version)
            stream << " (version " << VERSION_STRING << VERSION_OFFICIAL << ")";
        message_format_string = e.tryGetMessageFormatString();
        message_format_string_args = e.getMessageFormatStringArgs();
    }
    catch (const Poco::Exception & e)
    {
        try
        {
            stream << "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
                << ", " << e.displayText()
                << (with_stacktrace ? ", Stack trace (when copying this message, always include the lines below):\n\n" + getExceptionStackTraceString(e) : "")
                << (with_extra_info ? getExtraExceptionInfo(e) : "");
            if (with_version)
                stream << " (version " << VERSION_STRING << VERSION_OFFICIAL << ")";
        }
        catch (...) {} // NOLINT(bugprone-empty-catch)
    }
    catch (const std::exception & e)
    {
        try
        {
            int status = 0;
            auto name = demangle(typeid(e).name(), status);

            if (status)
                name += " (demangling status: " + toString(status) + ")";

            stream << "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", type: " << name << ", e.what() = " << e.what()
                << (with_stacktrace ? ", Stack trace (when copying this message, always include the lines below):\n\n" + getExceptionStackTraceString(e) : "")
                << (with_extra_info ? getExtraExceptionInfo(e) : "");
            if (with_version)
                stream << " (version " << VERSION_STRING << VERSION_OFFICIAL << ")";
        }
        catch (...) {} // NOLINT(bugprone-empty-catch)

        if (debug_or_sanitizer_build || abort_on_logical_error.load(std::memory_order_relaxed))
        {
            try
            {
                throw;
            }
            catch (const std::logic_error &)
            {
                if (!with_stacktrace)
                    stream << ", Stack trace:\n\n" << getExceptionStackTraceString(e);

                abortOnFailedAssertion(stream.str());
            }
            catch (...) {} // NOLINT(bugprone-empty-catch)
        }
    }
    catch (...)
    {
        try
        {
            int status = 0;
            auto name = demangle(abi::__cxa_current_exception_type()->name(), status);

            if (status)
                name += " (demangling status: " + toString(status) + ")";

            stream << "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ", type: " << name;
            if (with_version)
                stream << " (version " << VERSION_STRING << VERSION_OFFICIAL << ")";
        }
        catch (...) {} // NOLINT(bugprone-empty-catch)
    }

    return PreformattedMessage{stream.str(), message_format_string, message_format_string_args};
}


int getCurrentExceptionCode()
{
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    catch (const Poco::Exception &)
    {
        return ErrorCodes::POCO_EXCEPTION;
    }
    catch (const std::exception &)
    {
        return ErrorCodes::STD_EXCEPTION;
    }
    catch (...)
    {
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }
}

int getExceptionErrorCode(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(e);
    }
    catch (const Exception & exception)
    {
        return exception.code();
    }
    catch (const Poco::Exception &)
    {
        return ErrorCodes::POCO_EXCEPTION;
    }
    catch (const std::exception &)
    {
        return ErrorCodes::STD_EXCEPTION;
    }
    catch (...)
    {
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }
}


void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message)
{
    try
    {
        std::rethrow_exception(std::move(e)); // NOLINT
    }
    catch (...)
    {
        tryLogCurrentException(log_name, start_of_message);
    }
}

void tryLogException(std::exception_ptr e, LoggerPtr logger, const std::string & start_of_message, LogsLevel level)
{
    try
    {
        std::rethrow_exception(std::move(e)); // NOLINT
    }
    catch (...)
    {
        tryLogCurrentException(logger, start_of_message, level);
    }
}

void tryLogException(std::exception_ptr e, const AtomicLogger & logger, const std::string & start_of_message)
{
    tryLogException(e, logger.load(), start_of_message);
}

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    return getExceptionMessageAndPattern(e, with_stacktrace, check_embedded_stacktrace).text;
}

std::string getExceptionMessageForLogging(Exception & e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    e.markAsLogged();
    return getExceptionMessageAndPattern(e, with_stacktrace, check_embedded_stacktrace).text;
}

PreformattedMessage getExceptionMessageAndPattern(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    WriteBufferFromOwnString stream;

    try
    {
        std::string text = e.displayText();

        bool has_embedded_stack_trace = false;
        if (check_embedded_stacktrace)
        {
            auto embedded_stack_trace_pos = text.find("Stack trace");
            has_embedded_stack_trace = embedded_stack_trace_pos != std::string::npos;
            if (!with_stacktrace && has_embedded_stack_trace)
            {
                text.resize(embedded_stack_trace_pos);
                Poco::trimRightInPlace(text);
            }
        }

        stream << "Code: " << e.code() << ". " << text;

        if (!text.empty() && text.back() != '.')
            stream << '.';

        stream << " (" << ErrorCodes::getName(e.code()) << ")";

        if (with_stacktrace && !has_embedded_stack_trace)
            stream << ", Stack trace (when copying this message, always include the lines below):\n\n" << e.getStackTraceString();
    }
    catch (...) {} // NOLINT(bugprone-empty-catch)

    return PreformattedMessage{stream.str(), e.tryGetMessageFormatString(), e.getMessageFormatStringArgs()};
}

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    try
    {
        std::rethrow_exception(std::move(e)); // NOLINT
    }
    catch (...)
    {
        return getCurrentExceptionMessage(with_stacktrace, check_embedded_stacktrace);
    }
}


std::string ExecutionStatus::serializeText() const
{
    WriteBufferFromOwnString wb;
    wb << code << "\n" << escape << message;
    return wb.str();
}

void ExecutionStatus::deserializeText(const std::string & data)
{
    ReadBufferFromString rb(data);
    rb >> code >> "\n" >> escape >> message;
}

bool ExecutionStatus::tryDeserializeText(const std::string & data)
{
    try
    {
        deserializeText(data);
    }
    catch (...)
    {
        return false;
    }

    return true;
}

ExecutionStatus ExecutionStatus::fromCurrentException(const std::string & start_of_message, bool with_stacktrace, bool with_version)
{
    String msg = (start_of_message.empty() ? "" : (start_of_message + ": ")) + getCurrentExceptionMessage(with_stacktrace, /*check_embedded_stacktrace=*/true, /*with_extra_info=*/true, with_version);
    return ExecutionStatus(getCurrentExceptionCode(), msg);
}

ExecutionStatus ExecutionStatus::fromText(const std::string & data)
{
    ExecutionStatus status;
    status.deserializeText(data);
    return status;
}

std::exception_ptr copyMutableException(std::exception_ptr ptr)
{
    try
    {
        std::rethrow_exception(ptr);
    }
    catch (Poco::Exception & e)
    {
        try
        {
            e.rethrow();
        }
        catch (...)
        {
            return std::current_exception();
        }
    }
    catch (...)
    {
        return std::current_exception();
    }
}

}

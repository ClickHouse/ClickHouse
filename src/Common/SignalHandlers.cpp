#include <Common/SignalHandlers.h>
#include <Common/config_version.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/CurrentThread.h>
#include <Daemon/BaseDaemon.h>
#include <Daemon/SentryWriter.h>
#include <base/sleep.h>
#include <base/getThreadId.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Poco/Environment.h>

#pragma clang diagnostic ignored "-Wreserved-identifier"

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_SET_SIGNAL_HANDLER;
extern const int CANNOT_SEND_SIGNAL;
}

}

extern const char * GIT_HASH;

using namespace DB;


void call_default_signal_handler(int sig)
{
    if (SIG_ERR == signal(sig, SIG_DFL))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");

    if (0 != raise(sig))
        throw ErrnoException(ErrorCodes::CANNOT_SEND_SIGNAL, "Cannot send signal");
}


void writeSignalIDtoSignalPipe(int sig)
{
    auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    auto & signal_pipe = HandledSignals::instance().signal_pipe;
    WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);
    writeBinary(sig, out);
    out.next();

    errno = saved_errno;
}

void closeLogsSignalHandler(int sig, siginfo_t *, void *)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    writeSignalIDtoSignalPipe(sig);
}

void terminateRequestedSignalHandler(int sig, siginfo_t *, void *)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    writeSignalIDtoSignalPipe(sig);
}


void signalHandler(int sig, siginfo_t * info, void * context)
{
    if (asynchronous_stack_unwinding && sig == SIGSEGV)
        siglongjmp(asynchronous_stack_unwinding_signal_jump_buffer, 1);

    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    auto & signal_pipe = HandledSignals::instance().signal_pipe;
    WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const ucontext_t * signal_context = reinterpret_cast<ucontext_t *>(context);
    const StackTrace stack_trace(*signal_context);

#if USE_GWP_ASAN
    if (const auto fault_address = reinterpret_cast<uintptr_t>(info->si_addr);
        GWPAsan::isGWPAsanError(fault_address))
        GWPAsan::printReport(fault_address);
#endif

    writeBinary(sig, out);
    writePODBinary(*info, out);
    writePODBinary(signal_context, out);
    writePODBinary(stack_trace, out);
    writeVectorBinary(Exception::enable_job_stack_trace ? Exception::getThreadFramePointers() : std::vector<StackTrace::FramePointers>{}, out);
    writeBinary(static_cast<UInt32>(getThreadId()), out);
    writePODBinary(current_thread, out);

    out.next();

    if (sig != SIGTSTP) /// This signal is used for debugging.
    {
        /// The time that is usually enough for separate thread to print info into log.
        /// Under MSan full stack unwinding with DWARF info about inline functions takes 101 seconds in one case.
        for (size_t i = 0; i < 300; ++i)
        {
            /// We will synchronize with the thread printing the messages with an atomic variable to finish earlier.
            if (HandledSignals::instance().fatal_error_printed.test())
                break;

            /// This coarse method of synchronization is perfectly ok for fatal signals.
            sleepForSeconds(1);
        }

        /// Wait for all logs flush operations
        sleepForSeconds(3);
        call_default_signal_handler(sig);
    }

    errno = saved_errno;
}


[[noreturn]] void terminate_handler()
{
    static thread_local bool terminating = false;
    if (terminating)
        abort();

    terminating = true;

    std::string log_message;

    if (std::current_exception())
        log_message = "Terminate called for uncaught exception:\n" + getCurrentExceptionMessage(true);
    else
        log_message = "Terminate called without an active exception";

    /// POSIX.1 says that write(2)s of less than PIPE_BUF bytes must be atomic - man 7 pipe
    /// And the buffer should not be too small because our exception messages can be large.
    static constexpr size_t buf_size = PIPE_BUF;

    if (log_message.size() > buf_size - 16)
        log_message.resize(buf_size - 16);

    char buf[buf_size];
    auto & signal_pipe = HandledSignals::instance().signal_pipe;
    WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], buf_size, buf);

    writeBinary(static_cast<int>(SignalListener::StdTerminate), out);
    writeBinary(static_cast<UInt32>(getThreadId()), out);
    writeBinary(log_message, out);
    out.next();

    abort();
}

#if defined(SANITIZER)
template <typename T>
struct ValueHolder
{
    ValueHolder(T value_) : value(value_)
    {}

    T value;
};

extern "C" void __sanitizer_set_death_callback(void (*)());

/// Sanitizers may not expect some function calls from death callback.
/// Let's try to disable instrumentation to avoid possible issues.
/// However, this callback may call other functions that are still instrumented.
/// We can try [[clang::always_inline]] attribute for statements in future (available in clang-15)
/// See https://github.com/google/sanitizers/issues/1543 and https://github.com/google/sanitizers/issues/1549.
static DISABLE_SANITIZER_INSTRUMENTATION void sanitizerDeathCallback()
{
    DENY_ALLOCATIONS_IN_SCOPE;
    /// Also need to send data via pipe. Otherwise it may lead to deadlocks or failures in printing diagnostic info.

    char buf[signal_pipe_buf_size];
    auto & signal_pipe = HandledSignals::instance().signal_pipe;
    WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const StackTrace stack_trace;

    writeBinary(SignalListener::SanitizerTrap, out);
    writePODBinary(stack_trace, out);
    /// We create a dummy struct with a constructor so DISABLE_SANITIZER_INSTRUMENTATION is not applied to it
    /// otherwise, Memory sanitizer can't know that values initiialized inside this function are actually initialized
    /// because instrumentations are disabled leading to false positives later on
    ValueHolder<UInt32> thread_id{static_cast<UInt32>(getThreadId())};
    writeBinary(thread_id.value, out);
    writePODBinary(current_thread, out);

    out.next();

    /// The time that is usually enough for separate thread to print info into log.
    sleepForSeconds(20);
}
#endif


void HandledSignals::addSignalHandler(const std::vector<int> & signals, signal_function handler, bool register_signal)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO;

#if defined(OS_DARWIN)
    sigemptyset(&sa.sa_mask);
    for (auto signal : signals)
        sigaddset(&sa.sa_mask, signal);
#else
    if (sigemptyset(&sa.sa_mask))
        throw Poco::Exception("Cannot set signal handler.");

    for (auto signal : signals)
        if (sigaddset(&sa.sa_mask, signal))
            throw Poco::Exception("Cannot set signal handler.");
#endif

    for (auto signal : signals)
        if (sigaction(signal, &sa, nullptr))
            throw Poco::Exception("Cannot set signal handler.");

    if (register_signal)
        std::copy(signals.begin(), signals.end(), std::back_inserter(handled_signals));
}

void blockSignals(const std::vector<int> & signals)
{
    sigset_t sig_set;

#if defined(OS_DARWIN)
    sigemptyset(&sig_set);
    for (auto signal : signals)
        sigaddset(&sig_set, signal);
#else
    if (sigemptyset(&sig_set))
        throw Poco::Exception("Cannot block signal.");

    for (auto signal : signals)
        if (sigaddset(&sig_set, signal))
            throw Poco::Exception("Cannot block signal.");
#endif

    if (pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        throw Poco::Exception("Cannot block signal.");
}


void SignalListener::run()
{
    static_assert(PIPE_BUF >= 512);
    static_assert(signal_pipe_buf_size <= PIPE_BUF, "Only write of PIPE_BUF to pipe is atomic and the minimal known PIPE_BUF across supported platforms is 512");
    char buf[signal_pipe_buf_size];
    auto & signal_pipe = HandledSignals::instance().signal_pipe;
    ReadBufferFromFileDescriptor in(signal_pipe.fds_rw[0], signal_pipe_buf_size, buf);

    while (!in.eof())
    {
        int sig = 0;
        readBinary(sig, in);
        // We may log some specific signals afterwards, with different log
        // levels and more info, but for completeness we log all signals
        // here at trace level.
        // Don't use strsignal here, because it's not thread-safe.
        LOG_TRACE(log, "Received signal {}", sig);

        if (sig == StopThread)
        {
            LOG_INFO(log, "Stop SignalListener thread");
            break;
        }
        if (sig == SIGHUP)
        {
            LOG_DEBUG(log, "Received signal to close logs.");
            BaseDaemon::instance().closeLogs(BaseDaemon::instance().logger());
            LOG_INFO(log, "Opened new log file after received signal.");
        }
        else if (sig == StdTerminate)
        {
            UInt32 thread_num;
            std::string message;

            readBinary(thread_num, in);
            readBinary(message, in);

            onTerminate(message, thread_num);
        }
        else if (sig == SIGINT || sig == SIGQUIT || sig == SIGTERM)
        {
            if (daemon)
                daemon->handleSignal(sig);
        }
        else
        {
            siginfo_t info{};
            ucontext_t * context{};
            StackTrace stack_trace(NoCapture{});
            std::vector<StackTrace::FramePointers> thread_frame_pointers;
            UInt32 thread_num{};
            ThreadStatus * thread_ptr{};

            if (sig != SanitizerTrap)
            {
                readPODBinary(info, in);
                readPODBinary(context, in);
            }

            readPODBinary(stack_trace, in);
            if (sig != SanitizerTrap)
                readVectorBinary(thread_frame_pointers, in);
            readBinary(thread_num, in);
            readPODBinary(thread_ptr, in);

            /// This allows to receive more signals if failure happens inside onFault function.
            /// Example: segfault while symbolizing stack trace.
            try
            {
                std::thread([=, this] { onFault(sig, info, context, stack_trace, thread_frame_pointers, thread_num, thread_ptr); })
                    .detach();
            }
            catch (...)
            {
                /// Likely cannot allocate thread
                onFault(sig, info, context, stack_trace, thread_frame_pointers, thread_num, thread_ptr);
            }
        }
    }
}

void SignalListener::onTerminate(std::string_view message, UInt32 thread_num) const
{
    size_t pos = message.find('\n');

    LOG_FATAL(log, "(version {}{}, build id: {}, git hash: {}) (from thread {}) {}",
              VERSION_STRING, VERSION_OFFICIAL, daemon ? daemon->build_id : "", GIT_HASH, thread_num, message.substr(0, pos));

    /// Print trace from std::terminate exception line-by-line to make it easy for grep.
    while (pos != std::string_view::npos)
    {
        ++pos;
        size_t next_pos = message.find('\n', pos);
        size_t size = next_pos;
        if (next_pos != std::string_view::npos)
            size = next_pos - pos;

        LOG_FATAL(log, fmt::runtime(message.substr(pos, size)));
        pos = next_pos;
    }
}

void SignalListener::onFault(
    int sig,
    const siginfo_t & info,
    ucontext_t * context,
    const StackTrace & stack_trace,
    const std::vector<StackTrace::FramePointers> & thread_frame_pointers,
    UInt32 thread_num,
    DB::ThreadStatus * thread_ptr) const
try
{
    ThreadStatus thread_status;

    /// First log those fields that are safe to access and that should not cause new fault.
    /// That way we will have some duplicated info in the log but we don't loose important info
    /// in case of double fault.

    LOG_FATAL(log, "########## Short fault info ############");
    LOG_FATAL(log, "(version {}{}, build id: {}, git hash: {}, architecture: {}) (from thread {}) Received signal {}",
              VERSION_STRING, VERSION_OFFICIAL, daemon ? daemon->build_id : "", GIT_HASH, Poco::Environment::osArchitecture(),
              thread_num, sig);

    std::string signal_description = "Unknown signal";

    /// Some of these are not really signals, but our own indications on failure reason.
    if (sig == StdTerminate)
        signal_description = "std::terminate";
    else if (sig == SanitizerTrap)
        signal_description = "sanitizer trap";
    else if (sig >= 0)
        signal_description = strsignal(sig); // NOLINT(concurrency-mt-unsafe) // it is not thread-safe but ok in this context

    LOG_FATAL(log, "Signal description: {}", signal_description);

    String error_message;

    if (sig != SanitizerTrap)
        error_message = signalToErrorMessage(sig, info, *context);
    else
        error_message = "Sanitizer trap.";

    LOG_FATAL(log, fmt::runtime(error_message));

    String bare_stacktrace_str;
    if (stack_trace.getSize())
    {
        /// Write bare stack trace (addresses) just in case if we will fail to print symbolized stack trace.
        /// NOTE: This still require memory allocations and mutex lock inside logger.
        ///       BTW we can also print it to stderr using write syscalls.

        WriteBufferFromOwnString bare_stacktrace;
        writeString("Stack trace:", bare_stacktrace);
        for (size_t i = stack_trace.getOffset(); i < stack_trace.getSize(); ++i)
        {
            writeChar(' ', bare_stacktrace);
            writePointerHex(stack_trace.getFramePointers()[i], bare_stacktrace);
        }

        LOG_FATAL(log, fmt::runtime(bare_stacktrace.str()));
        bare_stacktrace_str = bare_stacktrace.str();
    }

    /// Now try to access potentially unsafe data in thread_ptr.

    String query_id;
    String query;

    /// Send logs from this thread to client if possible.
    /// It will allow client to see failure messages directly.
    if (thread_ptr)
    {
        query_id = thread_ptr->getQueryId();
        query = thread_ptr->getQueryForLog();

        if (auto logs_queue = thread_ptr->getInternalTextLogsQueue())
        {
            CurrentThread::attachInternalTextLogsQueue(logs_queue, LogsLevel::trace);
        }
    }

    LOG_FATAL(log, "########################################");

    if (query_id.empty())
    {
        LOG_FATAL(log, "(version {}{}, build id: {}, git hash: {}) (from thread {}) (no query) Received signal {} ({})",
                  VERSION_STRING, VERSION_OFFICIAL, daemon ? daemon->build_id : "", GIT_HASH,
                  thread_num, signal_description, sig);
    }
    else
    {
        LOG_FATAL(log, "(version {}{}, build id: {}, git hash: {}) (from thread {}) (query_id: {}) (query: {}) Received signal {} ({})",
                  VERSION_STRING, VERSION_OFFICIAL, daemon ? daemon->build_id : "", GIT_HASH,
                  thread_num, query_id, query, signal_description, sig);
    }

    LOG_FATAL(log, fmt::runtime(error_message));

    if (!bare_stacktrace_str.empty())
    {
        LOG_FATAL(log, fmt::runtime(bare_stacktrace_str));
    }

    /// Write symbolized stack trace line by line for better grep-ability.
    stack_trace.toStringEveryLine([&](std::string_view s) { LOG_FATAL(log, fmt::runtime(s)); });

    /// In case it's a scheduled job write all previous jobs origins call stacks
    std::for_each(thread_frame_pointers.rbegin(), thread_frame_pointers.rend(),
        [this](const StackTrace::FramePointers & frame_pointers)
        {
            if (size_t size = std::ranges::find(frame_pointers, nullptr) - frame_pointers.begin())
            {
                LOG_FATAL(log, "========================================");
                WriteBufferFromOwnString bare_stacktrace;
                writeString("Job's origin stack trace:", bare_stacktrace);
                std::for_each_n(frame_pointers.begin(), size,
                                [&bare_stacktrace](const void * ptr)
                                {
                                    writeChar(' ', bare_stacktrace);
                                    writePointerHex(ptr, bare_stacktrace);
                                }
                );

                LOG_FATAL(log, fmt::runtime(bare_stacktrace.str()));

                StackTrace::toStringEveryLine(const_cast<void **>(frame_pointers.data()), 0, size, [this](std::string_view s) { LOG_FATAL(log, fmt::runtime(s)); });
            }
        }
    );


#if defined(OS_LINUX)
    /// Write information about binary checksum. It can be difficult to calculate, so do it only after printing stack trace.
    /// Please keep the below log messages in-sync with the ones in programs/server/Server.cpp

    if (daemon && daemon->stored_binary_hash.empty())
    {
        LOG_FATAL(log, "Integrity check of the executable skipped because the reference checksum could not be read.");
    }
    else if (daemon)
    {
        String calculated_binary_hash = getHashOfLoadedBinaryHex();
        if (calculated_binary_hash == daemon->stored_binary_hash)
        {
            LOG_FATAL(log, "Integrity check of the executable successfully passed (checksum: {})", calculated_binary_hash);
        }
        else
        {
            LOG_FATAL(
                log,
                "Calculated checksum of the executable ({0}) does not correspond"
                " to the reference checksum stored in the executable ({1})."
                " This may indicate one of the following:"
                " - the executable was changed just after startup;"
                " - the executable was corrupted on disk due to faulty hardware;"
                " - the loaded executable was corrupted in memory due to faulty hardware;"
                " - the file was intentionally modified;"
                " - a logical error in the code.",
                calculated_binary_hash,
                daemon->stored_binary_hash);
        }
    }
#endif

    /// Write crash to system.crash_log table if available.
    if (collectCrashLog)
        collectCrashLog(sig, thread_num, query_id, stack_trace);

    Context::getGlobalContextInstance()->handleCrash();

    /// Send crash report to developers (if configured)
    if (sig != SanitizerTrap)
    {
        if (daemon)
        {
            if (auto * sentry = SentryWriter::getInstance())
                sentry->onSignal(sig, error_message, stack_trace.getFramePointers(), stack_trace.getOffset(), stack_trace.getSize());
        }

        /// Advice the user to send it manually.
        if (std::string_view(VERSION_OFFICIAL).contains("official build"))
        {
            const auto & date_lut = DateLUT::instance();

            /// Approximate support period, upper bound.
            if (time(nullptr) - date_lut.makeDate(2000 + VERSION_MAJOR, VERSION_MINOR, 1) < (365 + 30) * 86400)
            {
                LOG_FATAL(log, "Report this error to https://github.com/ClickHouse/ClickHouse/issues");
            }
            else
            {
                LOG_FATAL(log, "ClickHouse version {} is old and should be upgraded to the latest version.", VERSION_STRING);
            }
        }
        else
        {
            LOG_FATAL(log, "This ClickHouse version is not official and should be upgraded to the official build.");
        }
    }

    /// List changed settings.
    if (!query_id.empty())
    {
        ContextPtr query_context = thread_ptr->getQueryContext();
        if (query_context)
        {
            String changed_settings = query_context->getSettingsRef().toString();

            if (changed_settings.empty())
                LOG_FATAL(log, "No settings were changed");
            else
                LOG_FATAL(log, "Changed settings: {}", changed_settings);
        }
    }

    /// When everything is done, we will try to send these error messages to the client.
    if (thread_ptr)
        thread_ptr->onFatalError();

    HandledSignals::instance().fatal_error_printed.test_and_set();
}
catch (...)
{
    /// onFault is called from the std::thread, and it should catch all exceptions; otherwise, you can get unrelated fatal errors.
    PreformattedMessage message = getCurrentExceptionMessageAndPattern(true);
    LOG_FATAL(log, message);
}

HandledSignals::HandledSignals()
{
    signal_pipe.setNonBlockingWrite();
    signal_pipe.tryIncreaseSize(1 << 20);
}

void HandledSignals::reset()
{
    /// Reset signals to SIG_DFL to avoid trying to write to the signal_pipe that will be closed after.
    for (int sig : handled_signals)
    {
        if (SIG_ERR == signal(sig, SIG_DFL))
        {
            try
            {
                throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");
            }
            catch (ErrnoException &)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    signal_pipe.close();
}

HandledSignals::~HandledSignals()
{
    try
    {
        reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
};

HandledSignals & HandledSignals::instance()
{
    static HandledSignals res;
    return res;
}

void HandledSignals::setupTerminateHandler()
{
    std::set_terminate(terminate_handler);
}

void HandledSignals::setupCommonDeadlySignalHandlers()
{
    /// SIGTSTP is added for debugging purposes. To output a stack trace of any running thread at anytime.
    /// NOTE: that it is also used by clickhouse-test wrapper
    addSignalHandler({SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE, SIGTSTP, SIGTRAP}, signalHandler, true);

#if defined(SANITIZER)
    __sanitizer_set_death_callback(sanitizerDeathCallback);
#endif
}

void HandledSignals::setupCommonTerminateRequestSignalHandlers()
{
    addSignalHandler({SIGINT, SIGQUIT, SIGTERM}, terminateRequestedSignalHandler, true);
}

#pragma once
#include <csignal>

#include <base/defines.h>
#include <Common/Logger_fwd.h>
#include <Common/PipeFDs.h>
#include <Common/StackTrace.h>
#include <Common/ThreadStatus.h>
#include <Core/Types.h>
#include <Poco/Runnable.h>


class BaseDaemon;

/** Reset signal handler to the default and send signal to itself.
  * It's called from user signal handler to write core dump.
  */
void call_default_signal_handler(int sig);

const size_t signal_pipe_buf_size =
    sizeof(int)
    + sizeof(siginfo_t)
    + sizeof(ucontext_t*)
    + sizeof(StackTrace)
    + sizeof(UInt64)
    + sizeof(UInt32)
    + sizeof(void*)
#if defined(OS_LINUX)
    + sizeof(UInt8)
    + sizeof(FramePointers)
#endif
    ;

using signal_function = void(int, siginfo_t*, void*);

void writeSignalIDtoSignalPipe(int sig);

/** Signal handler for HUP */
void closeLogsSignalHandler(int sig, siginfo_t *, void *);

void terminateRequestedSignalHandler(int sig, siginfo_t *, void *);

void childSignalHandler(int sig, siginfo_t * info, void *);


/** To use with std::set_terminate.
  * Collects slightly more info than __gnu_cxx::__verbose_terminate_handler,
  *  and send it to pipe. Other thread will read this info from pipe and asynchronously write it to log.
  * Look at libstdc++-v3/libsupc++/vterminate.cc for example.
  */
[[noreturn]] void terminate_handler();

/// Avoid link time dependency on DB/Interpreters - will use this function only when linked.
__attribute__((__weak__)) void collectCrashLog(
    Int32 signal,
    Int32 signal_code,
    UInt64 thread_id,
    const String & query_id,
    const String & query,
    const StackTrace & stack_trace,
    std::optional<UInt64> fault_address,
    const String & fault_access_type,
    const String & signal_description,
    const FramePointers & current_exception_trace,
    size_t current_exception_trace_size);


/// Check if we are currently handing the fatal signal and going to terminate
/// it does not make sense to accept new connections and queries in this case.
bool isCrashed();


void blockSignals(const std::vector<int> & signals);


/** The thread that read info about signal or std::terminate from pipe.
  * On HUP, close log files (for new files to be opened later).
  * On information about std::terminate, write it to log.
  * On other signals, write info to log.
  */
class SignalListener : public Poco::Runnable
{
public:
    static constexpr int StdTerminate = -1;
    static constexpr int StopThread = -2;

    explicit SignalListener(BaseDaemon * daemon_, LoggerPtr log_);
    void run() override;

private:
    BaseDaemon * daemon;
    LoggerPtr log;
    std::function<String()> build_id;

    void onTerminate(std::string_view message, UInt32 thread_num) const;

    void onFault(
        int sig,
        const siginfo_t & info,
        ucontext_t * context,
        const StackTrace & stack_trace,
        const std::vector<FramePointers> & thread_frame_pointers,
        UInt32 thread_num,
        DB::ThreadStatus * thread_ptr,
        const FramePointers & exception_trace,
        size_t exception_trace_size) const;
};

struct HandledSignals
{
    std::vector<int> handled_signals;
    DB::PipeFDs signal_pipe;
    std::atomic_flag fatal_error_printed;

    HandledSignals();
    ~HandledSignals();

    void setupTerminateHandler();
    void setupCommonDeadlySignalHandlers();
    void setupCommonTerminateRequestSignalHandlers();

    void addSignalHandler(const std::vector<int> & signals, signal_function handler, bool register_signal);

    void reset(bool close_pipe = true);

    static HandledSignals & instance();
};

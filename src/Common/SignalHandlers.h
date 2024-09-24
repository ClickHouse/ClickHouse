#pragma once
#include <csignal>

#include <base/defines.h>
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
    + sizeof(void*);

using signal_function = void(int, siginfo_t*, void*);

void writeSignalIDtoSignalPipe(int sig);

/** Signal handler for HUP */
void closeLogsSignalHandler(int sig, siginfo_t *, void *);

void terminateRequestedSignalHandler(int sig, siginfo_t *, void *);


/** Handler for "fault" or diagnostic signals. Send data about fault to separate thread to write into log.
  */
void signalHandler(int sig, siginfo_t * info, void * context);


/** To use with std::set_terminate.
  * Collects slightly more info than __gnu_cxx::__verbose_terminate_handler,
  *  and send it to pipe. Other thread will read this info from pipe and asynchronously write it to log.
  * Look at libstdc++-v3/libsupc++/vterminate.cc for example.
  */
[[noreturn]] void terminate_handler();

/// Avoid link time dependency on DB/Interpreters - will use this function only when linked.
__attribute__((__weak__)) void collectCrashLog(
    Int32 signal, UInt64 thread_id, const String & query_id, const StackTrace & stack_trace);


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
    static constexpr int SanitizerTrap = -3;

    explicit SignalListener(BaseDaemon * daemon_, LoggerPtr log_)
        : daemon(daemon_), log(log_)
    {
    }

    void run() override;

private:
    BaseDaemon * daemon;
    LoggerPtr log;

    void onTerminate(std::string_view message, UInt32 thread_num) const;

    void onFault(
        int sig,
        const siginfo_t & info,
        ucontext_t * context,
        const StackTrace & stack_trace,
        const std::vector<StackTrace::FramePointers> & thread_frame_pointers,
        UInt32 thread_num,
        DB::ThreadStatus * thread_ptr) const;
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

    void reset();

    static HandledSignals & instance();
};

// Copyright 2010 Google LLC
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


#include <cstdio>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <client/linux/handler/minidump_descriptor.h>
#ifdef HAVE_CONFIG_H
#    include <config.h> // Must come first
#endif

#include "client/linux/handler/exception_handler.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <linux/limits.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/wait.h>

#include <ucontext.h>
#include <sys/ucontext.h>
#include <sys/user.h>

#include <algorithm>
#include <utility>
#include <vector>

#include "common/basictypes.h"
#include "common/linux/breakpad_getcontext.h"
#include "common/linux/eintr_wrapper.h"
#include "common/linux/linux_libc_support.h"
#include "common/memory_allocator.h"
#include "client/linux/log/log.h"
#include "client/linux/microdump_writer/microdump_writer.h"
#include "client/linux/minidump_writer/linux_dumper.h"
#include "client/linux/minidump_writer/minidump_writer.h"
#include "third_party/lss/linux_syscall_support.h"

#ifndef PR_SET_PTRACER
#    define PR_SET_PTRACER 0x59616d61
#endif

namespace ClickhouseBreakpad
{
using namespace google_breakpad;
using MinidumpDescriptor = google_breakpad::MinidumpDescriptor;
using CrashContext = google_breakpad::ExceptionHandler::CrashContext;

/// To generage coredump from minidump, see implementation in Minidump2Core.cpp
int minidump2Core(const char * minidump_path, const char * coredump_path);


// Implement signal handling logic with breakpad. This class shouldn't be used directly. Instead, call
// createExceptionHandler() to create one (and only one).
static class ExceptionHandler
{
    friend void createExceptionHandler(const std::string &, bool, bool);

public:
    ExceptionHandler() = delete;
    ExceptionHandler(const ExceptionHandler &) = default;
    ExceptionHandler(ExceptionHandler &&) = delete;
    ExceptionHandler & operator=(const ExceptionHandler &) = default;
    ExceptionHandler & operator=(ExceptionHandler &&) = delete;
    ~ExceptionHandler() = default;

    // Report a crash signal from an SA_SIGINFO signal handler.
    bool handleSignal(int sig, siginfo_t * info, void * uc);

private:
    ExceptionHandler(const std::string & dump_path, bool minidump_generate_core)
        : minidump_descriptor(dump_path), generate_core(minidump_generate_core)
    {
        memset(&g_crash_context, 0, sizeof(g_crash_context));
        updatePath();
    }
    void updatePath() { minidump_descriptor.UpdatePath(); }
    bool generateDump(CrashContext * context);
    void sendContinueSignalToChild();
    void waitForContinueSignal();

    static int threadEntry(void * arg);

    bool doDump(pid_t crashing_process, const void * context, size_t context_size);

    MinidumpDescriptor minidump_descriptor;
    bool generate_core;

    CrashContext g_crash_context;
    // We need to explicitly enable ptrace of parent processes on some
    // kernels, but we need to know the PID of the cloned process before we
    // can do this. We create a pipe which we can use to block the
    // cloned process after creating it, until we have explicitly enabled
    // ptrace. This is used to store the file descriptors for the pipe
    int fdes[2] = {-1, -1};
};

namespace
{
    /// Singleton
    std::mutex create_mtx;
    std::unique_ptr<ExceptionHandler> eh;
}

void createExceptionHandler(const std::string & dump_path, bool minidump, bool minidump_generate_core)
{
    if (!minidump || dump_path.empty())
        return;
    std::lock_guard lk(create_mtx);
    if (eh)
        return;
    eh = std::unique_ptr<ExceptionHandler>(new ExceptionHandler(dump_path, minidump_generate_core));
}

void handler(int sig, siginfo_t * info, void * context)
{
    if (!eh)
        return;
    eh->handleSignal(sig, info, context);
}

struct ThreadArgument
{
    pid_t pid; // the crashing process
    const MinidumpDescriptor * minidump_descriptor;
    ExceptionHandler * handler;
    const void * context; // a CrashContext structure
    size_t context_size;
};


// This function runs in a compromised context: see the top of the file.
// Runs on the crashing thread.
bool ExceptionHandler::handleSignal(int /*sig*/, siginfo_t * info, void * uc)
{
    // Allow ourselves to be dumped if the signal is trusted.
    bool signal_trusted = info->si_code > 0;
    bool signal_pid_trusted = info->si_code == SI_USER || info->si_code == SI_TKILL;
    if (signal_trusted || (signal_pid_trusted && info->si_pid == getpid()))
    {
        sys_prctl(PR_SET_DUMPABLE, 1, 0, 0, 0);
    }

    // Fill in all the holes in the struct to make Valgrind happy.
    memset(&g_crash_context, 0, sizeof(g_crash_context));
    memcpy(&g_crash_context.siginfo, info, sizeof(siginfo_t));
    memcpy(&g_crash_context.context, uc, sizeof(ucontext_t));
#if defined(__aarch64__)
    ucontext_t * uc_ptr = (ucontext_t *)uc;
    struct fpsimd_context * fp_ptr = (struct fpsimd_context *)&uc_ptr->uc_mcontext.__reserved;
    if (fp_ptr->head.magic == FPSIMD_MAGIC)
    {
        memcpy(&g_crash_context.float_state, fp_ptr, sizeof(g_crash_context.float_state));
    }
#elif GOOGLE_BREAKPAD_CRASH_CONTEXT_HAS_FLOAT_STATE
    ucontext_t * uc_ptr = (ucontext_t *)uc;
    if (uc_ptr->uc_mcontext.fpregs)
    {
        memcpy(&g_crash_context.float_state, uc_ptr->uc_mcontext.fpregs, sizeof(g_crash_context.float_state));
    }
#endif
    g_crash_context.tid = syscall(__NR_gettid);
    return generateDump(&g_crash_context);
}

// This function may run in a compromised context: see the top of the file.
bool ExceptionHandler::generateDump(CrashContext * context)
{
    // Allocating too much stack isn't a problem, and better to err on the side
    // of caution than smash it into random locations.
    static const unsigned kChildStackSize = 16000;
    PageAllocator allocator;
    uint8_t * stack = reinterpret_cast<uint8_t *>(allocator.Alloc(kChildStackSize));
    if (!stack)
        return false;
    // clone() needs the top-most address. (scrub just to be safe)
    stack += kChildStackSize;
    my_memset(stack - 16, 0, 16);

    ThreadArgument thread_arg;
    thread_arg.handler = this;
    thread_arg.minidump_descriptor = &minidump_descriptor;
    thread_arg.pid = getpid();
    thread_arg.context = context;
    thread_arg.context_size = sizeof(*context);

    // We need to explicitly enable ptrace of parent processes on some
    // kernels, but we need to know the PID of the cloned process before we
    // can do this. Create a pipe here which we can use to block the
    // cloned process after creating it, until we have explicitly enabled ptrace
    if (sys_pipe(fdes) == -1)
    {
        // Creating the pipe failed. We'll log an error but carry on anyway,
        // as we'll probably still get a useful crash report. All that will happen
        // is the write() and read() calls will fail with EBADF
        static const char no_pipe_msg[] = "ExceptionHandler::GenerateDump "
                                          "sys_pipe failed:";
        logger::write(no_pipe_msg, sizeof(no_pipe_msg) - 1);
        logger::write(strerror(errno), strlen(strerror(errno)));
        logger::write("\n", 1);

        // Ensure fdes[0] and fdes[1] are invalid file descriptors.
        fdes[0] = fdes[1] = -1;
    }

    const pid_t child = sys_clone(threadEntry, stack, CLONE_FS | CLONE_UNTRACED, &thread_arg, nullptr, nullptr, nullptr);
    if (child == -1)
    {
        sys_close(fdes[0]);
        sys_close(fdes[1]);
        return false;
    }

    // Close the read end of the pipe.
    sys_close(fdes[0]);
    // Allow the child to ptrace us
    sys_prctl(PR_SET_PTRACER, child, 0, 0, 0);
    sendContinueSignalToChild();

    int status = 0;
    const int r = HANDLE_EINTR(sys_waitpid(child, &status, __WALL));
    sys_close(fdes[1]);

    if (r == -1)
    {
        static const char msg[] = "ExceptionHandler::GenerateDump waitpid failed:";
        logger::write(msg, sizeof(msg) - 1);
        logger::write(strerror(errno), strlen(strerror(errno)));
        logger::write("\n", 1);
    }

    return r != -1 && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

// This is the entry function for the cloned process. We are in a compromised
// context here: see the top of the file
int ExceptionHandler::threadEntry(void * arg)
{
    const ThreadArgument * thread_arg = reinterpret_cast<ThreadArgument *>(arg);

    // Close the write end of the pipe. This allows us to fail if the parent dies
    // while waiting for the continue signal.
    sys_close(thread_arg->handler->fdes[1]);

    // Block here until the crashing process unblocks us when
    // we're allowed to use ptrace
    thread_arg->handler->waitForContinueSignal();
    sys_close(thread_arg->handler->fdes[0]);
    return thread_arg->handler->doDump(thread_arg->pid, thread_arg->context, thread_arg->context_size) == false;
}

// This function runs in a compromised context: see the top of the file.
void ExceptionHandler::sendContinueSignalToChild()
{
    static const char okToContinueMessage = 'a';
    int r;
    r = HANDLE_EINTR(sys_write(fdes[1], &okToContinueMessage, sizeof(char)));
    if (r == -1)
    {
        static const char msg[] = "ExceptionHandler::SendContinueSignalToChild "
                                  "sys_write failed:";
        logger::write(msg, sizeof(msg) - 1);
        logger::write(strerror(errno), strlen(strerror(errno)));
        logger::write("\n", 1);
    }
}

// This function runs in a compromised context: see the top of the file.
// Runs on the cloned process.
void ExceptionHandler::waitForContinueSignal()
{
    int r;
    char received_message;
    r = HANDLE_EINTR(sys_read(fdes[0], &received_message, sizeof(char)));
    if (r == -1)
    {
        static const char msg[] = "ExceptionHandler::WaitForContinueSignal "
                                  "sys_read failed:";
        logger::write(msg, sizeof(msg) - 1);
        logger::write(strerror(errno), strlen(strerror(errno)));
        logger::write("\n", 1);
    }
}

// This function runs in a compromised context: see the top of the file.
// Runs on the cloned process.
bool ExceptionHandler::doDump(pid_t crashing_process, const void * context, size_t context_size)
{
    const bool may_skip_dump = minidump_descriptor.skip_dump_if_principal_mapping_not_referenced();
    const uintptr_t principal_mapping_address = minidump_descriptor.address_within_principal_mapping();
    const bool sanitize_stacks = minidump_descriptor.sanitize_stacks();
    bool success = google_breakpad::WriteMinidump(
        minidump_descriptor.path(),
        minidump_descriptor.size_limit(),
        crashing_process,
        context,
        context_size,
        {},
        {},
        may_skip_dump,
        principal_mapping_address,
        sanitize_stacks);
    if (success)
    {
        (void)std::fprintf(stderr, "Minidump dump path: {}: %s\n", minidump_descriptor.path());
        if (generate_core)
        {
            auto coredump_path = std::filesystem::path(minidump_descriptor.path()).replace_extension("core");
            success = minidump2Core(minidump_descriptor.path(), coredump_path.c_str()) == 0;
            if (success)
                (void)std::fprintf(stderr, "Core dump path: {}: %s\n", coredump_path.c_str());
        }
    }
    return success;
}

}

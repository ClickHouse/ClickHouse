#pragma once

#include <string>
#include <vector>
#include <signal.h>

#if USE_UNWIND
#define UNW_LOCAL_ONLY
    #include <libunwind.h>
#endif

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif
#include <ucontext.h>


std::string signalToErrorMessage(int sig, siginfo_t & info, ucontext_t & context);

void * getCallerAddress(ucontext_t & context);

std::vector<void *> getBacktraceFrames(ucontext_t & context);

std::string backtraceFramesToString(const std::vector<void *> & frames, const std::string delimiter = "");

#include <csetjmp>
#include <csignal>
#include <cstring>
#include <iostream>
#include <Examples/clickhouse_examples.h>


/// This example demonstrates how is it possible to check if a pointer to memory is readable using a signal handler.

thread_local bool checking_pointer = false;
thread_local jmp_buf signal_jump_buffer;


static void signalHandler(int sig, siginfo_t *, void *)
{
    if (checking_pointer && sig == SIGSEGV)
        siglongjmp(signal_jump_buffer, 1);
}

static bool isPointerValid(const void * ptr)
{
    checking_pointer = true;
    if (0 == sigsetjmp(signal_jump_buffer, 1))
    {
        char res;
        memcpy(&res, ptr, 1);
        __asm__ __volatile__("" :: "r"(res) : "memory");
        checking_pointer = false;
        return true;
    }

    checking_pointer = false;
    return false;
}

int mainEntryExampleCheckPointerValid(int, char **)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
    sa.sa_sigaction = signalHandler;
#pragma clang diagnostic pop
    sa.sa_flags = SA_SIGINFO;

    if (sigemptyset(&sa.sa_mask)
        || sigaddset(&sa.sa_mask, SIGSEGV)
        || sigaction(SIGSEGV, &sa, nullptr))
        return 1;

    std::cerr << isPointerValid(reinterpret_cast<const void *>(0x123456789)) << "\n";
    std::cerr << isPointerValid(&sa) << "\n";

    return 0;
}

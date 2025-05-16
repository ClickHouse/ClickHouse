#include "memory_access_tracing.h"

#include <cstdint>
#include <fcntl.h>
#include <pthread.h>
#include <cstdio>
#include <string>
#include <unistd.h>

#pragma clang diagnostic ignored "-Wreserved-identifier"

#if defined(MEMORY_ACCESS_TRACING)


int ENABLE_TRACE = 0; // by default 0. If want 1, need to disbale tracing in DiskLocal::removeFileIfExists and reopen file.
int FAULT = 0;


int trace_fd = -1;

namespace
{
    uint64_t memory_access_counter = 0;
}



#define NO_SANITIZE __attribute__((no_sanitize("all")))


// For case with ENABLE_TRACE = 1 by default

// NO_SANITIZE
// __attribute__((constructor))
// static void init_trace() {
//     pid_t pid = getpid();
//     trace_fd = open(("/tmp/memory_trace_" + std::to_string(pid) + ".log").c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644); // O_TRUNC - опасно
// }

NO_SANITIZE
void enableMemoryAccessesCoverage() {
    pid_t pid = getpid();
    trace_fd = open(("/tmp/memory_trace_" + std::to_string(pid) + ".log").c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    ENABLE_TRACE = 1;
}

NO_SANITIZE
void disableMemoryAccessesCoverage() {
    ENABLE_TRACE = 0;
}

NO_SANITIZE
uint64_t getMemoryAccessCount()
{
    return memory_access_counter;
}

NO_SANITIZE
void resetMemoryAccessCount()
{
    memory_access_counter = 0;
}

extern "C"
{

    NO_SANITIZE
    void __sanitizer_cov_trace_pc()
    {
        // Just need this
    }

    NO_SANITIZE
    void __sanitizer_cov_load1(uint8_t* addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "l1\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_load2(uint16_t * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "l2\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_load4(uint32_t * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "l4\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_load8(uint64_t * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "l8\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_load16(__int128 * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "l16\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_store1(uint8_t * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "s1\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_store2(uint16_t * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "s2\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_store4(uint32_t * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "s4\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_store8(uint64_t * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "s8\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_store16(__int128 * addr)
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "s16\t%lu\t%lu\t%lu\n", 
                            reinterpret_cast<uintptr_t>(addr), pthread_self(), reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
            write(trace_fd, buffer, len);
        }
    }
}

#endif

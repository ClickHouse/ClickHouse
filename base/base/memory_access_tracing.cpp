#include "memory_access_tracing.h"

#include <cstdint>
#include <fcntl.h>
#include <pthread.h>
#include <cstdio>
#include <iostream>
#include <unistd.h>

#pragma clang diagnostic ignored "-Wreserved-identifier"

#if defined(MEMORY_ACCESS_TRACING)

int ENABLE_TRACE = 1;

namespace
{
    uint64_t memory_access_counter = 0;
}

static int trace_fd = -1;

__attribute__((constructor))
static void init_trace() {
    trace_fd = open("/tmp/memory_trace.log", O_WRONLY | O_CREAT | O_APPEND, 0644);
}

uint64_t getMemoryAccessCount()
{
    return memory_access_counter;
}

void resetMemoryAccessCount()
{
    memory_access_counter = 0;
}

extern "C"
{
    #define NO_SANITIZE __attribute__((no_sanitize("all")))

    NO_SANITIZE
    void __sanitizer_cov_trace_pc()
    {
        // Just need this
    }

    NO_SANITIZE
    void __sanitizer_cov_load1(uint8_t* addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "l1 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_load2(uint16_t * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "l2 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_load4(uint32_t * addr) // проблема
    {
        if (ENABLE_TRACE == 1) {
            ++memory_access_counter;
            char buffer[64];
            int len = snprintf(buffer, sizeof(buffer), "l4\t%p\t%lu\t%p\t%lu\n", 
                            reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
            write(trace_fd, buffer, len);
        }
    }

    NO_SANITIZE
    void __sanitizer_cov_load8(uint64_t * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "l8 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_load16(__int128 * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "l16 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_store1(uint8_t * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "s1 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_store2(uint16_t * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "s2 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_store4(uint32_t * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "s4 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_store8(uint64_t * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "s8 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }

    NO_SANITIZE
    void __sanitizer_cov_store16(__int128 * addr) // нет проблем
    {
        ++memory_access_counter;
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer), "s16 %p %lu %p %lu\n", 
                        reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter);
        write(trace_fd, buffer, len);
    }
}

#endif

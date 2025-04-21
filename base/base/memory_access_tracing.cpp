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

#define NO_SANITIZE __attribute__((no_sanitize("all")))


NO_SANITIZE
__attribute__((constructor))
static void init_trace() {
    pid_t pid = getpid();
    trace_fd = open(("/tmp/memory_trace_" + std::to_string(pid) + ".log").c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644); // O_TRUNC - опасно
}


NO_SANITIZE
uint64_t getMemoryAccessCount()
{
    char buffer[128];
    int len = snprintf(buffer, sizeof(buffer), "getMemoryAccessCount %lu %d %lu\n", pthread_self(), trace_fd, memory_access_counter);
    std::cout << buffer << std::endl;
    pid_t pid = getpid();
    trace_fd = open(("/tmp/memory_trace_" + std::to_string(pid) + ".log").c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    write(trace_fd, buffer, len);
    return memory_access_counter;
}

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
            int len = snprintf(buffer, sizeof(buffer), "l4\t%p\t%lu\t%p\t%lu\t%d\n", 
                            reinterpret_cast<void*>(addr), pthread_self(), __builtin_return_address(0), memory_access_counter, trace_fd);
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

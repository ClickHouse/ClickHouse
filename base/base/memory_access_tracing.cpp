#include "memory_access_tracing.h"

#pragma clang diagnostic ignored "-Wreserved-identifier"

#if defined(MEMORY_ACCESS_TRACING)

namespace
{
    uint64_t memory_access_counter = 0;
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
        // Left empty as in the example
    }

    NO_SANITIZE
    void __sanitizer_cov_load1(uint8_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_load2(uint16_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_load4(uint32_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_load8(uint64_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_load16(__int128 * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_store1(uint32_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_store2(uint32_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_store4(uint32_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_store8(uint32_t * /* addr */)
    {
        ++memory_access_counter;
    }

    NO_SANITIZE
    void __sanitizer_cov_store16(uint32_t * /* addr */)
    {
        ++memory_access_counter;
    }
}

#endif

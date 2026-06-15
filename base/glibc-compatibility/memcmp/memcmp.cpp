#include "memcmp.h"

__attribute__((no_sanitize("coverage")))
extern "C" int memcmp(const void * a, const void * b, size_t size)
{
    return inline_memcmp(a, b, size);
}

#include <common/getThreadNumber.h>
#include <common/likely.h>
#include <atomic>

static thread_local unsigned thread_number = 0;
static std::atomic_uint threads{0};

unsigned getThreadNumber()
{
    if (unlikely(thread_number == 0))
        thread_number = ++threads;

    return thread_number;
}

#include "gtest_global_context.h"

const ContextHolder & getContext()
{
    static ContextHolder * holder;
    static std::once_flag once;
    std::call_once(once, [&]() { holder = new ContextHolder(); });
    return *holder;
}

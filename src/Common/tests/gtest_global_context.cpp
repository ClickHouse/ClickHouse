#include "gtest_global_context.h"

const ContextHolder & getContext()
{
    static ContextHolder holder;
    return holder;
}

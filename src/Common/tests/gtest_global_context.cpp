#include "gtest_global_context.h"

const ContextHolder & getContext()
{
    return getMutableContext();
}

ContextHolder & getMutableContext()
{
    static ContextHolder holder;
    return holder;
}

void destroyContext()
{
    auto & holder = getMutableContext();
    return holder.destroy();
}

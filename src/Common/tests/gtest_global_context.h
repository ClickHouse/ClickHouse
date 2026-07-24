#pragma once

#include <Interpreters/Context.h>

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr context;

    ContextHolder();

    ContextHolder(ContextHolder &&) = default;

    void destroy()
    {
        context->shutdown();
        context.reset();
        shared_context.reset();
    }
};

const ContextHolder & getContext();

ContextHolder & getMutableContext();

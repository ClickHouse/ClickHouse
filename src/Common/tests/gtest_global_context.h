#pragma once

#include <Interpreters/Context.h>

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr context;

    ContextHolder()
        : shared_context(DB::Context::createShared())
        , context(DB::Context::createGlobal(shared_context.get()))
    {
        context->makeGlobalContext();
        context->setPath("./");
    }

    ContextHolder(ContextHolder &&) = default;
};

const ContextHolder & getContext()
{
    static ContextHolder * holder;
    static std::once_flag once;
    std::call_once(once, [&]() { holder = new ContextHolder(); });
    return *holder;
}


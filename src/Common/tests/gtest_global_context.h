#pragma once

#include <Interpreters/Context.h>

struct ContextHolder
{
    DB::SharedContextHolder shared_context;

    ContextHolder() : shared_context(DB::Context::createShared())
    {
        DB::Context::createGlobal(shared_context.get());
        DB::Context::getGlobal()->setPath("./");
    }

    ContextHolder(ContextHolder &&) = default;
};

inline const ContextHolder & getContext()
{
    static ContextHolder holder;
    return holder;
}

#pragma once

#include <Interpreters/Context.h>

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::Context context;

    ContextHolder()
        : shared_context(DB::Context::createShared())
        , context(DB::Context::createGlobal(shared_context.get()))
    {
    }

    ContextHolder(ContextHolder &&) = default;
};

inline ContextHolder getContext()
{
    ContextHolder holder;
    holder.context.makeGlobalContext();
    holder.context.setPath("./");
    return holder;
}

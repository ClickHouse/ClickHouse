#pragma once

#include <Interpreters/Context.h>

inline DB::Context createContext()
{
    static DB::SharedContextHolder shared_context = DB::Context::createShared();
    auto context = DB::Context::createGlobal(shared_context.get());
    context.makeGlobalContext();
    context.setPath("./");
    return context;
}

inline const DB::Context & getContext()
{
    static DB::Context global_context = createContext();
    return global_context;
}

#pragma once

#include <Interpreters/Context.h>

inline DB::Context createContext()
{
    auto context = DB::Context::createGlobal();
    context.makeGlobalContext();
    context.setPath("./");
    return context;
}

inline const DB::Context & getContext()
{
    static DB::Context global_context = createContext();
    return global_context;
}

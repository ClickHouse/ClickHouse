#pragma once

#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB::Setting
{
    extern const SettingsString local_filesystem_read_method;
}

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
        const_cast<DB::Settings &>(context->getSettingsRef())[DB::Setting::local_filesystem_read_method] = "pread";
    }

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

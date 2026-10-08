#include <Common/tests/gtest_global_context.h>

#include <Core/Settings.h>

namespace DB::Setting
{
extern const SettingsString local_filesystem_read_method;
}

ContextHolder::ContextHolder()
    : shared_context(DB::Context::createShared())
    , context(DB::Context::createGlobal(shared_context.get()))
{
    context->makeGlobalContext();
    context->setPath("./");
    const_cast<DB::Settings &>(context->getSettingsRef())[DB::Setting::local_filesystem_read_method] = "pread";
}

const ContextHolder & getContext()
{
    return getMutableContext();
}

ContextHolder & getMutableContext()
{
    static ContextHolder holder;
    return holder;
}

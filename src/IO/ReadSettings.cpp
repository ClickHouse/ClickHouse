#include <IO/ReadSettings.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_READ_METHOD;
    extern const int INVALID_SETTING_VALUE;
}

ReadSettings getReadSettings()
{
    auto query_context = CurrentThread::getQueryContext();
    if (query_context)
        return query_context->getReadSettings();

    auto global_context = Context::getGlobalContextInstance();
    if (global_context)
        return global_context->getReadSettings();

    return {};
}

}

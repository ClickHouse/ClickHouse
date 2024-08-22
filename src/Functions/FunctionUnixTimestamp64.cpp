#include <Core/Settings.h>
#include <Functions/FunctionUnixTimestamp64.h>

namespace DB
{

FunctionFromUnixTimestamp64::FunctionFromUnixTimestamp64(size_t target_scale_, const char * name_, ContextPtr context)
    : target_scale(target_scale_)
    , name(name_)
    , allow_nonconst_timezone_arguments(context->getSettingsRef().allow_nonconst_timezone_arguments)
{}

}

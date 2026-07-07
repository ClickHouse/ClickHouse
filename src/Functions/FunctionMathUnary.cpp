#include <Functions/FunctionMathUnary.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool fast_float_math;
}

bool fastFloatMathEnabled(const ContextPtr & context)
{
    return context && context->getSettingsRef()[Setting::fast_float_math];
}

}

#include <Functions/h3Common.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#if USE_H3

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
namespace Setting
{
    extern const SettingsBool functions_h3_default_if_invalid;
}

H3Validator::H3Validator(const ContextPtr & context)
    : throw_on_error(!context->getSettingsRef()[Setting::functions_h3_default_if_invalid])
{}

bool H3Validator::validateCell(UInt64 h) const
{
    if (!isValidCell(h))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid H3 cell index: {}", h);
        else
            return false;
    }
    return true;
}

}

#endif

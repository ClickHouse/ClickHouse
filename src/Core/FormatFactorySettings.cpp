#include <Core/BaseSettings.h>
#include <Core/FormatFactorySettingsDeclaration.h>
#include <Core/SettingsEnums.h>

namespace DB
{
/*
 * User-specified file format settings for File and URL engines.
 */
DECLARE_SETTINGS_TRAITS(FormatFactorySettingsTraits, LIST_OF_ALL_FORMAT_SETTINGS)

struct FormatFactorySettingsImpl : public BaseSettings<FormatFactorySettingsTraits>
{
};

IMPLEMENT_SETTINGS_TRAITS(FormatFactorySettingsTraits, LIST_OF_ALL_FORMAT_SETTINGS)

}

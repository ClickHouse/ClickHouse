#include <Core/SettingsSecrets.h>
#include <Core/Field.h>

namespace DB::CoreSettings
{

bool maskSettingValue(const String & setting_name, const Field & field, String & value)
{
    CustomType custom;
    if (field.tryGet<CustomType>(custom) && custom.isSecret())
    {
        value = custom.toString(/* show_secrets */ false);
        return true;
    }
    return maskSettingValue(setting_name, value);
}

}

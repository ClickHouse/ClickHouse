#include <Backups/SettingsFieldOptionalString.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
}

SettingFieldOptionalString::SettingFieldOptionalString(const Field & field)
{
    if (field.getType() == Field::Types::Null)
    {
        value = std::nullopt;
        return;
    }

    if (field.getType() == Field::Types::String)
    {
        value = field.safeGet<const String &>();
        return;
    }

    throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot get string from {}", field);
}

}

#include <Backups/SettingsFieldOptionalString.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
}

SettingFieldOptionalString::operator Field() const { return Field(value.value_or("")); }

SettingFieldOptionalString::SettingFieldOptionalString(const Field & field)
{
    if (field.getType() == Field::Types::Null)
    {
        value = std::nullopt;
        return;
    }

    if (field.getType() == Field::Types::String)
    {
        value = field.safeGet<String>();
        return;
    }

    throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot get string from {}", field);
}

}

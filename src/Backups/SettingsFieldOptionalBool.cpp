#include <Backups/SettingsFieldOptionalBool.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
}

SettingFieldOptionalBool::operator Field() const
{
    if (!value.has_value())
        return Field(Null{});
    return Field(*value);
}

SettingFieldOptionalBool::SettingFieldOptionalBool(const Field & field)
{
    if (field.getType() == Field::Types::Null)
    {
        value = std::nullopt;
        return;
    }

    if (field.getType() == Field::Types::Bool)
    {
        value = field.safeGet<bool>();
        return;
    }

    if (field.getType() == Field::Types::UInt64)
    {
        value = field.safeGet<UInt64>() != 0;
        return;
    }

    throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot get bool from {}", field);
}

}

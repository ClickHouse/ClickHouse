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

    if (field.getType() == Field::Types::Int64)
    {
        value = field.safeGet<Int64>() != 0;
        return;
    }

    /// Accept string forms ('true'/'false'/'1'/'0', case-insensitive) for consistency with
    /// regular boolean settings (`SettingFieldBool`). An empty string is treated as "unset".
    if (field.getType() == Field::Types::String)
    {
        const auto & str = field.safeGet<String>();
        if (str.empty())
        {
            value = std::nullopt;
            return;
        }
        value = stringToBool(str);
        return;
    }

    throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot get bool from {}", field);
}

}

#include <Backups/SettingsFieldOptionalUInt64.h>
#include <Common/ErrorCodes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
}

SettingFieldOptionalUInt64::SettingFieldOptionalUInt64(const Field & field)
{
    if (field.getType() == Field::Types::Null)
    {
        value = std::nullopt;
        return;
    }

    if (field.getType() == Field::Types::UInt64)
    {
        value = field.safeGet<UInt64>();
        return;
    }

    // 1eN is interpreted as Float64
    if (field.getType() == Field::Types::Float64)
    {
        auto converted = convertFieldToType(field.safeGet<Float64>(), DataTypeUInt64());

        value = converted.safeGet<UInt64>();
        return;
    }

    throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot get UInt64 from {}", field);
}

}

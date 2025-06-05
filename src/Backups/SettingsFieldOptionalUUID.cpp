#include <Backups/SettingsFieldOptionalUUID.h>
#include <Common/ErrorCodes.h>
#include <Core/SettingsFields.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
}


    SettingFieldOptionalUUID::SettingFieldOptionalUUID(const Field & field)
    {
        if (field.getType() == Field::Types::Null)
        {
            value = std::nullopt;
            return;
        }

        if (field.getType() == Field::Types::String)
        {
            const String & str = field.safeGet<const String &>();
            if (str.empty())
            {
                value = std::nullopt;
                return;
            }

            UUID id;
            if (tryParse(id, str))
            {
                value = id;
                return;
            }
        }

        throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot parse uuid from {}", field);
    }

}

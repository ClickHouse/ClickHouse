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

    /// An empty string is treated as "unset", matching how `toString` serializes an unset value.
    if (field.getType() == Field::Types::String && field.safeGet<String>().empty())
    {
        value = std::nullopt;
        return;
    }

    /// Delegate parsing to `SettingFieldBool` so that non-null values behave exactly like regular
    /// boolean settings: only `0`/`1`/`true`/`false` (as numbers or strings) are accepted, and
    /// out-of-range numerics such as `2` or `-1` are rejected (fail-closed) instead of silently
    /// being coerced to `true`. This matters because these flags control whether table data or
    /// ACL/UDF definitions are restored.
    try
    {
        value = static_cast<bool>(SettingFieldBool{field});
    }
    catch (const Exception & e)
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot get bool from {}: {}", field, e.message());
    }
}

String SettingFieldOptionalBool::toString() const
{
    /// An unset value serializes to an empty string, matching how the parsing constructor treats an
    /// empty string as "unset". A set value uses the canonical `1`/`0` form of `SettingFieldBool`, so
    /// the representation stays consistent with regular boolean settings in `system.backup_log`.
    if (!value.has_value())
        return {};
    return *value ? "1" : "0";
}

}

#pragma once

#include <Core/Types.h>


namespace DB
{

/// Represents the type of an access entity (see the IAccessEntity class).
enum class AccessEntityType
{
    USER,
    ROLE,
    SETTINGS_PROFILE,
    ROW_POLICY,
    QUOTA,

    MAX,
};

String toString(AccessEntityType type);

struct AccessEntityTypeInfo
{
    const char * const raw_name;
    const char * const plural_raw_name;
    const String name;  /// Uppercased with spaces instead of underscores, e.g. "SETTINGS PROFILE".
    const String alias; /// Alias of the keyword or empty string, e.g. "PROFILE".
    const String plural_name;  /// Uppercased with spaces plural name, e.g. "SETTINGS PROFILES".
    const String plural_alias; /// Uppercased with spaces plural name alias, e.g. "PROFILES".
    const String name_for_output_with_entity_name; /// Lowercased with spaces instead of underscores, e.g. "settings profile".
    const char unique_char;     /// Unique character for this type. E.g. 'P' for SETTINGS_PROFILE.
    const int not_found_error_code;

    String formatEntityNameWithType(const String & entity_name) const;

    static const AccessEntityTypeInfo & get(AccessEntityType type_);
    static AccessEntityType parseType(const String & name_);
};

}

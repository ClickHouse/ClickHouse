#pragma once

#include <string>
#include <Storages/ColumnsDescription.h>
#include <Core/Settings.h>


namespace DB
{

class Context;

struct DataTypeValidationSettings
{
    DataTypeValidationSettings() = default;

    explicit DataTypeValidationSettings(const Settings & settings)
        : allow_suspicious_low_cardinality_types(settings.allow_suspicious_low_cardinality_types)
        , allow_experimental_object_type(settings.allow_experimental_object_type)
        , allow_suspicious_fixed_string_types(settings.allow_suspicious_fixed_string_types)
        , allow_experimental_variant_type(settings.allow_experimental_variant_type)
        , allow_suspicious_variant_types(settings.allow_suspicious_variant_types)
        , validate_nested_types(settings.validate_experimental_and_suspicious_types_inside_nested_types)
        , allow_experimental_dynamic_type(settings.allow_experimental_dynamic_type)
    {
    }

    bool allow_suspicious_low_cardinality_types = true;
    bool allow_experimental_object_type = true;
    bool allow_suspicious_fixed_string_types = true;
    bool allow_experimental_variant_type = true;
    bool allow_suspicious_variant_types = true;
    bool validate_nested_types = true;
    bool allow_experimental_dynamic_type = true;
};

void validateDataType(const DataTypePtr & type, const DataTypeValidationSettings & settings);

/// Parses a common argument for table functions such as table structure given in string
[[nodiscard]] ColumnsDescription parseColumnsListFromString(const std::string & structure, const ContextPtr & context);

bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, const ContextPtr & context, String & error);

}

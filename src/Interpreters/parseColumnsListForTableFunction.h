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
        , allow_experimental_geo_types(settings.allow_experimental_geo_types)
        , allow_experimental_object_type(settings.allow_experimental_object_type)
        , allow_suspicious_fixed_string_types(settings.allow_suspicious_fixed_string_types)
    {
    }

    bool allow_suspicious_low_cardinality_types = true;
    bool allow_experimental_geo_types = true;
    bool allow_experimental_object_type = true;
    bool allow_suspicious_fixed_string_types = true;
};

void validateDataType(const DataTypePtr & type, const DataTypeValidationSettings & settings);

/// Parses a common argument for table functions such as table structure given in string
[[nodiscard]] ColumnsDescription parseColumnsListFromString(const std::string & structure, const ContextPtr & context);

bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, const ContextPtr & context, String & error);

}

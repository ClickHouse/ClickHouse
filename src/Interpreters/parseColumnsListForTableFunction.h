#pragma once

#include <string>
#include <Storages/ColumnsDescription.h>


namespace DB
{

class Context;
struct Settings;

struct DataTypeValidationSettings
{
    DataTypeValidationSettings() = default;

    explicit DataTypeValidationSettings(const Settings & settings);

    bool allow_suspicious_low_cardinality_types = true;
    bool allow_experimental_object_type = true;
    bool allow_suspicious_fixed_string_types = true;
    bool allow_experimental_variant_type = true;
    bool allow_suspicious_variant_types = true;
    bool validate_nested_types = true;
    bool allow_experimental_dynamic_type = true;
    bool allow_experimental_json_type = true;
};

void validateDataType(const DataTypePtr & type, const DataTypeValidationSettings & settings);

/// Parses a common argument for table functions such as table structure given in string
[[nodiscard]] ColumnsDescription parseColumnsListFromString(const std::string & structure, const ContextPtr & context);

bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, const ContextPtr & context, String & error);

}

#pragma once

#include <string>
#include <Core/Defines.h>
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
    bool allow_suspicious_fixed_string_types = true;
    bool allow_suspicious_variant_types = true;
    bool validate_nested_types = true;
    bool enable_time_time64_type = true;
    bool allow_experimental_nullable_tuple_type = true;

    /// Used to re-parse a rendered type name at the same depth the query itself was parsed with.
    UInt64 max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH;
    UInt64 max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS;
};

void validateDataType(const DataTypePtr & type, const DataTypeValidationSettings & settings);

/// Parses a common argument for table functions such as table structure given in string
[[nodiscard]] ColumnsDescription parseColumnsListFromString(const std::string & structure, const ContextPtr & context);

bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, const ContextPtr & context, String & error);

}

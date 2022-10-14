#pragma once

#include <Formats/FormatSettings.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>

namespace DB
{

FormatSettings::EscapingRule stringToEscapingRule(const String & escaping_rule);

String escapingRuleToString(FormatSettings::EscapingRule escaping_rule);

void skipFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings);

bool deserializeFieldByEscapingRule(
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    IColumn & column,
    ReadBuffer & buf,
    FormatSettings::EscapingRule escaping_rule,
    const FormatSettings & format_settings);

void serializeFieldByEscapingRule(
    const IColumn & column,
    const ISerialization & serialization,
    WriteBuffer & out,
    size_t row_num,
    FormatSettings::EscapingRule escaping_rule,
    const FormatSettings & format_settings);

void writeStringByEscapingRule(const String & value, WriteBuffer & out, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings);

String readStringByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings);
String readFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings);

/// Try to determine the type of the field written by a specific escaping rule.
/// If cannot, return nullptr.
/// - For Quoted escaping rule we can interpret a single field as a constant
///   expression and get it's type by evaluation this expression.
/// - For JSON escaping rule we can use JSON parser to parse a single field
///   and then convert JSON type of this field to ClickHouse type.
/// - For CSV escaping rule we can do the next:
///    - If the field is an unquoted string, then we try to parse it as a number,
///      and if we cannot, treat it as a String.
///    - If the field is a string in quotes, then we try to use some
///      tweaks and heuristics to determine the type inside quotes, and if we can't or
///      the result is a number or tuple (we don't parse numbers in quotes and don't
///      support tuples in CSV) we treat it as a String.
///    - If input_format_csv_use_best_effort_in_schema_inference is disabled, we
///      treat everything as a string.
/// - For TSV and TSVRaw we try to use some tweaks and heuristics to determine the type
///   of value if setting input_format_tsv_use_best_effort_in_schema_inference is enabled,
///   otherwise we treat everything as a string.
DataTypePtr determineDataTypeByEscapingRule(const String & field, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule);
DataTypes determineDataTypesByEscapingRule(const std::vector<String> & fields, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule);

DataTypePtr getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule escaping_rule);
DataTypes getDefaultDataTypeForEscapingRules(const std::vector<FormatSettings::EscapingRule> & escaping_rules);

}

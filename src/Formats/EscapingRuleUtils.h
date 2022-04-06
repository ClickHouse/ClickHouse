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
///    - If the field is an unquoted string, then we could try to evaluate it
///      as a constant expression, and if it fails, treat it as a String.
///    - If the field is a string in quotes, then we can try to evaluate
///      expression inside quotes as a constant expression, and if it fails or
///      the result is a number (we don't parse numbers in quotes) we treat it as a String.
/// - For TSV and TSVRaw we treat each field as a String (TODO: try to use some tweaks and heuristics here)
DataTypePtr determineDataTypeByEscapingRule(const String & field, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, ContextPtr context = nullptr);
DataTypes determineDataTypesByEscapingRule(const std::vector<String> & fields, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, ContextPtr context = nullptr);

DataTypePtr getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule escaping_rule);

}

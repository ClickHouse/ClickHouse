#pragma once

#include <Formats/FormatSettings.h>
#include <Formats/SchemaInferenceUtils.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>

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

/// Read String serialized in specified escaping rule.
String readStringByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings);
/// Read any field serialized in specified escaping rule. It can be any fild like number/array/etc.
/// This function should return value exactly as it was in the data without changes
/// (for example without parsing escaped sequences)
String readFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings);
/// In case if we don't know if we have String value or not, but need to read String values as String (with correct escaped sequences parsing).
String readStringOrFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings);

/// Try to determine the type of the field written by a specific escaping rule.
/// If cannot, return nullptr.
/// See tryInferDataTypeForSingle(JSON)Field in SchemaInferenceUtils.h
DataTypePtr tryInferDataTypeByEscapingRule(const String & field, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, JSONInferenceInfo * json_info = nullptr);
DataTypes tryInferDataTypesByEscapingRule(const std::vector<String> & fields, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, JSONInferenceInfo * json_info = nullptr);

/// Check if we need to transform types inferred from data and transform it if necessary.
/// See transformInferred(JSON)TypesIfNeeded in SchemaInferenceUtils.h
void transformInferredTypesByEscapingRuleIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings, FormatSettings::EscapingRule escaping_rule, JSONInferenceInfo * json_info = nullptr);

DataTypePtr getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule escaping_rule);
DataTypes getDefaultDataTypeForEscapingRules(const std::vector<FormatSettings::EscapingRule> & escaping_rules);

String getAdditionalFormatInfoForAllRowBasedFormats(const FormatSettings & settings);
String getAdditionalFormatInfoByEscapingRule(const FormatSettings & settings, FormatSettings::EscapingRule escaping_rule);

void checkSupportedDelimiterAfterField(FormatSettings::EscapingRule escaping_rule, const String & delimiter, const DataTypePtr & type);

}

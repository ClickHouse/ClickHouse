#pragma once
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Formats/FormatSettings.h>


namespace DB
{

struct JSONExtractInsertSettings
{
    /// If false, JSON boolean values won't be inserted into columns with integer types
    /// It's used in JSONExtractInt64/JSONExtractUInt64/... functions.
    bool convert_bool_to_integer = true;
    /// If true, when complex type like Array/Map has both valid and invalid elements,
    /// the default value will be inserted on invalid elements.
    /// For example, if we have [1, "hello", 2] and type Array(UInt32),
    /// we will insert [1, 0, 2] in the column. Used in all JSONExtract functions.
    bool insert_default_on_invalid_elements_in_complex_types = false;
    /// If false, JSON value will be inserted into column only if type of the value is
    /// the same as column type (no conversions like Integer -> String, Integer -> Float, etc).
    bool allow_type_conversion = true;
};

template <typename JSONParser>
class JSONExtractTreeNode
{
public:
    JSONExtractTreeNode() = default;
    virtual ~JSONExtractTreeNode() = default;
    virtual bool insertResultToColumn(IColumn &, const typename JSONParser::Element &, const JSONExtractInsertSettings & insert_setting, const FormatSettings & format_settings, String & error) const = 0;
};

/// Build a tree for insertion JSON element into a column with provided data type.
template <typename JSONParser>
std::unique_ptr<JSONExtractTreeNode<JSONParser>> buildJSONExtractTree(const DataTypePtr & type, const char * source_for_exception_message);

template <typename JSONParser>
void jsonElementToString(const typename JSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings);

template <typename JSONParser, typename NumberType>
bool tryGetNumericValueFromJSONElement(NumberType & value, const typename JSONParser::Element & element, bool convert_bool_to_integer, bool allow_type_conversion, String & error);

}

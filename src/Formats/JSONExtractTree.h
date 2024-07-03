#pragma once
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Formats/FormatSettings.h>


namespace DB
{

struct JSONExtractInsertSettings
{
    bool convert_bool_to_integer = true;
    bool insert_default_on_invalid_elements_in_complex_types = false;
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
bool tryGetNumericValueFromJSONElement(NumberType & value, const typename JSONParser::Element & element, bool convert_bool_to_integer, String & error);

}

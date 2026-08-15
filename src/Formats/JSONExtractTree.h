#pragma once
#include <DataTypes/IDataType.h>
#include <Columns/IColumn_fwd.h>

namespace DB
{

struct FormatSettings;

struct JSONExtractInsertSettings
{
    /// If true, when complex type like Array/Map has both valid and invalid elements,
    /// the default value will be inserted on invalid elements.
    /// For example, if we have [1, "hello", 2] and type Array(UInt32),
    /// we will insert [1, 0, 2] in the column. Used in all JSONExtract functions.
    bool insert_default_on_invalid_elements_in_complex_types = false;
    /// If false, JSON value will be inserted into column only if type of the value is
    /// the same as column type (no conversions like Integer -> String, Integer -> Float, etc).
    bool allow_type_conversion = true;
    /// If true, during insert into Dynamic column we first try to insert value into existing variants
    /// and only if failed we try to infer the new variant type.
    bool try_existing_variants_in_dynamic_first = true;
    /// If true, during constructing the JSON path dots in keys will be escaped.
    bool escape_dots_in_json_keys = false;
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
bool tryGetNumericValueFromJSONElement(NumberType & value, const typename JSONParser::Element & element, bool convert_bool_to_number, bool allow_type_conversion, String & error);

}

#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace  DB 
{

class ReadBuffer;

class FormInputFormat final : public IRowInputFormat
{
public:
    FormInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "FormInputFormat"; }
    void resetParser() override;

private:
    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readFormData(MutableColumns & columns);
    void readNestedFormData(const String & name, MutableColumns & columns);
    void readField(size_t index, MutableColumns & columns);
    void skipUnknownFormField(StringRef name_ref);
    const String & columnName(size_t i) const;
    size_t columnIndex(StringRef name);

    /// recursively split names separated by '.' into own columns
    void checkAndSplitIfNested(StringRef column_name);
    
    String name_buf;

    /// holds common prefix of nested column names
    String current_column_name;

    /// holds length of common prefix of nested column names 
    /// eg: given 'n.a', 'n.b' -> 'n.a' and 'n.b' are nested
    /// column names and 'n.' is the common prefix.  
    size_t nested_prefix_length = 0;

    /// Hash table matches field name to position in the block
    Block::NameMap name_map;

protected:
    const FormatSettings format_settings;
    std::vector<UInt8> read_columns;
    std::vector<UInt8> seen_columns;
};

class FormSchemaReader : public IRowWithNamesSchemaReader
{
public:
    FormSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
private:
    NamesAndTypesList readRowAndGetNamesAndDataTypes(bool & eof) override;
    NamesAndTypesList readRowAndGetNamesAndDataTypesForForm(ReadBuffer & in, const FormatSettings & settings);
};

String readFieldName(ReadBuffer & buf);

}


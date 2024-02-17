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

private:
    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension & extra) override;
    void readField(size_t index, MutableColumns & columns);
    const String & columnName(size_t i) const;
    
    const FormatSettings format_settings;
    String name_buf;
    std::vector<UInt8> read_columns;
    std::vector<UInt8> seen_columns;

    /// Hash table matches field name to position in the block
    Block::NameMap name_map;
};

class FormSchemaReader : public IRowWithNamesSchemaReader
{
public:
    FormSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
private:
    NamesAndTypesList readRowAndGetNamesAndDataTypes(bool & eof) override;
};

}


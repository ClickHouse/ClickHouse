#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class ReadBuffer;

class FormRowInputFormat final : public IRowInputFormat
{
public:
    FormRowInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "FormInputFormat"; }
    void resetParser() override;

private:
    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readFormData(MutableColumns & columns);
    void readField(size_t index, MutableColumns & columns);
    const String & columnName(size_t i) const;

    /// Hash table matches field name to position in the block
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;

protected:
    const FormatSettings format_settings;
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

}


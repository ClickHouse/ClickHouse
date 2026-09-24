#pragma once

#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>


namespace DB
{

class Block;
class ReadBuffer;

template <bool>
class BinaryFormatReader;

/** A stream for inputting data in a binary line-by-line format.
  */
template <bool with_defaults = false>
class BinaryFormatReader final : public FormatWithNamesAndTypesReader
{
public:
    BinaryFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;
    bool readFieldImpl(IColumn & column, const SerializationPtr & serialization);

    void skipField(size_t file_column) override;

    void skipNames() override;
    void skipTypes() override;
    void skipHeaderRow();

    std::vector<String> readNames() override;
    std::vector<String> readTypes() override;
    std::vector<String> readHeaderRow();

private:
    /// Data types read from input data.
    DataTypes read_data_types;
    UInt64 read_columns;
};

template <bool with_defaults = false>
class BinaryRowInputFormat final : public RowInputFormatWithNamesAndTypes<BinaryFormatReader<with_defaults>>
{
public:
    BinaryRowInputFormat(ReadBuffer & in_, SharedHeader header, IRowInputFormat::Params params_, bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "BinaryRowInputFormat"; }

    /// RowInputFormatWithNamesAndTypes implements logic with DiagnosticInfo, but
    /// in this format we cannot provide any DiagnosticInfo, because here we have
    /// just binary data.
    std::string getDiagnosticInfo() override { return {}; }

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
};

class BinaryWithNamesAndTypesSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    BinaryWithNamesAndTypesSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

private:
    BinaryFormatReader<false> reader;
};

}

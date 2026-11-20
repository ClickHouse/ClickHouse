#pragma once

#include <optional>

#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <IO/PeekableReadBuffer.h>


namespace DB
{

class Block;
class CSVFormatReader;

/** A stream for inputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it skips spaces and tabs between values.
  */
class CSVRowInputFormat : public RowInputFormatWithNamesAndTypes<CSVFormatReader>
{
public:
    /** with_names - in the first line the header with column names
      * with_types - on the next line header with type names
      */
    CSVRowInputFormat(SharedHeader header_, ReadBuffer & in_, const Params & params_,
                      bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "CSVRowInputFormat"; }

    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

protected:
    CSVRowInputFormat(SharedHeader header_, std::shared_ptr<PeekableReadBuffer> in_, const Params & params_,
                               bool with_names_, bool with_types_, const FormatSettings & format_settings_, std::unique_ptr<CSVFormatReader> format_reader_);

    CSVRowInputFormat(SharedHeader header_, std::shared_ptr<PeekableReadBuffer> in_buf_, const Params & params_,
                      bool with_names_, bool with_types_, const FormatSettings & format_settings_);

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    bool supportsCountRows() const override { return true; }

protected:
    std::shared_ptr<PeekableReadBuffer> buf;
};

class CSVFormatReader : public FormatWithNamesAndTypesReader
{
public:
    CSVFormatReader(PeekableReadBuffer & buf_, const FormatSettings & format_settings_);

    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;

    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override
    {
        return *pos != '\n' && *pos != '\r' && *pos != format_settings.csv.delimiter && *pos != ' ' && *pos != '\t';
    }

    void setDataTypes(const DataTypes & types) override;

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name, size_t column_index) override;

    void skipRow() override;

    void skipField(size_t /*file_column*/) override { skipField(); }
    void skipField();

    void skipHeaderRow();
    void skipNames() override { skipHeaderRow(); }
    void skipTypes() override { skipHeaderRow(); }
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;
    void skipPrefixBeforeHeader() override;

    bool checkForEndOfRow() override;

    std::vector<String> readNames() override { return readHeaderRow(); }
    std::vector<String> readTypes() override { return readHeaderRow(); }
    std::vector<String> readHeaderRow() { return readRowImpl<true>(); }
    std::vector<String> readRow() { return readRowImpl<false>(); }
    std::vector<String> readRowForHeaderDetection() override { return readHeaderRow(); }

    bool checkForSuffix() override;

    template <bool is_header>
    std::vector<String> readRowImpl();

    template <bool read_string>
    String readCSVFieldIntoString();

    void setReadBuffer(ReadBuffer & in_) override;

    FormatSettings::EscapingRule getEscapingRule() const override { return FormatSettings::EscapingRule::CSV; }
    bool readFieldImpl(ReadBuffer & istr, DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization);
    bool readFieldOrDefault(DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization);

protected:
    PeekableReadBuffer * buf;

    /// Whether the whitespace in front of a field has to be skipped, for every column of the header.
    /// With `input_format_csv_trim_whitespaces` disabled that whitespace is a part of the value, but
    /// only for String and FixedString, so the answer depends on nothing but the column type and is
    /// worked out once in `setDataTypes` rather than for every field of every row.
    std::vector<UInt8> skip_whitespaces_before_field;
};

class CSVSchemaReader final : public FormatWithNamesAndTypesSchemaReader
{
public:
    CSVSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, const FormatSettings & format_settings_);

private:
    bool allowVariableNumberOfColumns() const override { return format_settings.csv.allow_variable_number_of_columns; }

    std::optional<DataTypes> readRowAndGetDataTypesImpl() override;
    std::optional<std::pair<std::vector<String>, DataTypes>> readRowAndGetFieldsAndDataTypes() override;

    PeekableReadBuffer buf;
    CSVFormatReader reader;
    DataTypes buffered_types;
};

std::pair<bool, size_t> fileSegmentationEngineCSVImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows, const FormatSettings & settings);

}

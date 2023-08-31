#pragma once

#include <optional>
#include <unordered_map>

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** A stream for inputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it skips spaces and tabs between values.
  */
class CSVRowInputFormat : public RowInputFormatWithNamesAndTypes
{
public:
    /** with_names - in the first line the header with column names
      * with_types - on the next line header with type names
      */
    CSVRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                      bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "CSVRowInputFormat"; }

    void setReadBuffer(ReadBuffer & in_) override;
    void resetParser() override;

protected:
    CSVRowInputFormat(const Block & header_, std::shared_ptr<PeekableReadBuffer> in_, const Params & params_,
                               bool with_names_, bool with_types_, const FormatSettings & format_settings_, std::unique_ptr<FormatWithNamesAndTypesReader> format_reader_);

    CSVRowInputFormat(const Block & header_, std::shared_ptr<PeekableReadBuffer> in_buf_, const Params & params_,
                      bool with_names_, bool with_types_, const FormatSettings & format_settings_);

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

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

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

    void skipField(size_t /*file_column*/) override { skipField(); }
    void skipField();

    void skipHeaderRow();
    void skipNames() override { skipHeaderRow(); }
    void skipTypes() override { skipHeaderRow(); }
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;
    void skipPrefixBeforeHeader() override;

    std::vector<String> readNames() override { return readHeaderRow(); }
    std::vector<String> readTypes() override { return readHeaderRow(); }
    std::vector<String> readHeaderRow() { return readRowImpl<true>(); }
    std::vector<String> readRow() { return readRowImpl<false>(); }
    std::vector<String> readRowForHeaderDetection() override { return readHeaderRow(); }


    template <bool is_header>
    std::vector<String> readRowImpl();

    template <bool read_string>
    String readCSVFieldIntoString();

    void setReadBuffer(ReadBuffer & in_) override;

    FormatSettings::EscapingRule getEscapingRule() const override { return FormatSettings::EscapingRule::CSV; }

protected:
    PeekableReadBuffer * buf;
};

class CSVSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    CSVSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, const FormatSettings & format_settings_);

private:
    DataTypes readRowAndGetDataTypesImpl() override;
    std::pair<std::vector<String>, DataTypes> readRowAndGetFieldsAndDataTypes() override;

    PeekableReadBuffer buf;
    CSVFormatReader reader;
    DataTypes buffered_types;
};

std::pair<bool, size_t> fileSegmentationEngineCSVImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows);

}

#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class ReadBuffer;

/** A stream for inputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it skips spaces and tabs between values.
  */
class CSVRowInputFormat : public IRowInputFormat
{
public:
    /** with_names - in the first line the header with column names
      * with_types - on the next line header with type names
      */
    CSVRowInputFormat(ReadBuffer & in_, Block header, Params params, bool with_names, const FormatSettings & format_settings);

    String getName() const override { return "CSVRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    std::string getDiagnosticInfo() override;

private:
    bool with_names;
    DataTypes data_types;

    const FormatSettings format_settings;

    /// For convenient diagnostics in case of an error.

    size_t row_num = 0;

    /// How many bytes were read, not counting those that are still in the buffer.
    size_t bytes_read_at_start_of_buffer_on_current_row = 0;
    size_t bytes_read_at_start_of_buffer_on_prev_row = 0;

    char * pos_of_current_row = nullptr;
    char * pos_of_prev_row = nullptr;

    void updateDiagnosticInfo();

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns,
        WriteBuffer & out, size_t max_length_of_column_name, size_t max_length_of_data_type_name);
};

}

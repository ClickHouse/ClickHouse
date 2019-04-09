#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowInputFormat.h>


namespace DB
{

class ReadBuffer;


/** A stream to input data in tsv format.
  */
class TabSeparatedRowInputFormat : public IRowInputFormat
{
public:
    /** with_names - the first line is the header with the names of the columns
      * with_types - on the next line header with type names
      */
    TabSeparatedRowInputFormat(
        ReadBuffer & in_, Block header, bool with_names, bool with_types, Params params, const FormatSettings & format_settings);

    String getName() const override { return "TabSeparatedRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    std::string getDiagnosticInfo() override;

private:
    bool with_names;
    bool with_types;
    const FormatSettings format_settings;
    DataTypes data_types;

    /// For convenient diagnostics in case of an error.

    size_t row_num = 0;

    /// How many bytes were read, not counting those still in the buffer.
    size_t bytes_read_at_start_of_buffer_on_current_row = 0;
    size_t bytes_read_at_start_of_buffer_on_prev_row = 0;

    char * pos_of_current_row = nullptr;
    char * pos_of_prev_row = nullptr;

    void updateDiagnosticInfo();

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns,
        WriteBuffer & out, size_t max_length_of_column_name, size_t max_length_of_data_type_name);
};

}

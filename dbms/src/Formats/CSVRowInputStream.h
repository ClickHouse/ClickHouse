#pragma once

#include <optional>
#include <unordered_map>

#include <Core/Block.h>
#include <Formats/IRowInputStream.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class ReadBuffer;

/** A stream for inputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it skips spaces and tabs between values.
  */
class CSVRowInputStream : public IRowInputStream
{
public:
    /** with_names - in the first line the header with column names
      */
    CSVRowInputStream(ReadBuffer & istr_, const Block & header_, bool with_names_, const FormatSettings & format_settings_);

    bool read(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    std::string getDiagnosticInfo() override;

private:
    ReadBuffer & istr;
    Block header;
    bool with_names;
    DataTypes data_types;

    const FormatSettings format_settings;

    using IndexesMap = std::unordered_map<String, size_t>;
    IndexesMap column_indexes_by_names;

    /// Maps indexes of columns in the input file to indexes of table columns
    using OptionalIndexes = std::vector<std::optional<size_t>>;
    OptionalIndexes column_indexes_for_input_fields;

    /// Tracks which colums we have read in a single read() call.
    /// For columns that are never read, it is initialized to false when we
    /// read the file header, and never changed afterwards.
    /// For other columns, it is updated on each read() call.
    std::vector<UInt8> read_columns;

    /// Whether we have any columns that are not read from file at all,
    /// and must be always initialized with defaults.
    bool have_always_default_columns = false;

    void addInputColumn(const String & column_name);

    /// For convenient diagnostics in case of an error.
    size_t row_num = 0;

    /// How many bytes were read, not counting those that are still in the buffer.
    size_t bytes_read_at_start_of_buffer_on_current_row = 0;
    size_t bytes_read_at_start_of_buffer_on_prev_row = 0;

    char * pos_of_current_row = nullptr;
    char * pos_of_prev_row = nullptr;

    /// For setting input_format_null_as_default
    DataTypes nullable_types;
    MutableColumns nullable_columns;
    OptionalIndexes column_idx_to_nullable_column_idx;

    void updateDiagnosticInfo();

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns,
        WriteBuffer & out, size_t max_length_of_column_name, size_t max_length_of_data_type_name);

    bool readField(IColumn & column, const DataTypePtr & type, bool is_last_file_column, size_t column_idx);
};

}

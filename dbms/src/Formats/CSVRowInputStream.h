#pragma once

#include <optional>
#include <unordered_map>

#include <Core/Block.h>
#include <Formats/RowInputStreamWithDiagnosticInfo.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class ReadBuffer;

/** A stream for inputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it skips spaces and tabs between values.
  */
class CSVRowInputStream : public RowInputStreamWithDiagnosticInfo
{
public:
    /** with_names - in the first line the header with column names
      * with_types - on the next line header with type names
      */
    CSVRowInputStream(ReadBuffer & istr_, const Block & header_, bool with_names_, const FormatSettings & format_settings);

    bool read(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

private:
    bool with_names;
    const FormatSettings format_settings;
    DataTypes data_types;

    using IndexesMap = std::unordered_map<String, size_t>;
    IndexesMap column_indexes_by_names;

    using OptionalIndexes = std::vector<std::optional<size_t>>;
    OptionalIndexes column_indexes_for_input_fields;

    std::vector<UInt8> read_columns;
    std::vector<size_t> columns_to_fill_with_default_values;

    void addInputColumn(const String & column_name);
    void setupAllColumnsByTableSchema();
    void fillUnreadColumnsWithDefaults(MutableColumns & columns, RowReadExtension& ext);


    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) override;
    void tryDeserializeFiled(const DataTypePtr & type, IColumn & column, size_t input_position, ReadBuffer::Position & prev_pos,
                             ReadBuffer::Position & curr_pos) override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override
    {
        return *pos != '\n' && *pos != '\r' && *pos != format_settings.csv.delimiter;
    }
};

}

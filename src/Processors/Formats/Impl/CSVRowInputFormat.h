#pragma once

#include <optional>
#include <unordered_map>

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** A stream for inputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it skips spaces and tabs between values.
  */
class CSVRowInputFormat : public RowInputFormatWithDiagnosticInfo
{
public:
    /** with_names - in the first line the header with column names
      */
    CSVRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                      bool with_names_, const FormatSettings & format_settings_);

    String getName() const override { return "CSVRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;
    void resetParser() override;

private:
    /// There fields are computed in constructor.
    bool with_names;
    const FormatSettings format_settings;
    DataTypes data_types;
    using IndexesMap = std::unordered_map<String, size_t>;
    IndexesMap column_indexes_by_names;

    void addInputColumn(const String & column_name);

    void setupAllColumnsByTableSchema();
    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) override;
    void tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column) override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override
    {
        return *pos != '\n' && *pos != '\r' && *pos != format_settings.csv.delimiter && *pos != ' ' && *pos != '\t';
    }

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column);
};

}

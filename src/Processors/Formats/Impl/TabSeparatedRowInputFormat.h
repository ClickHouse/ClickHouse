#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>


namespace DB
{

/** A stream to input data in tsv format.
  */
class TabSeparatedRowInputFormat : public RowInputFormatWithDiagnosticInfo
{
public:
    /** with_names - the first line is the header with the names of the columns
      * with_types - on the next line header with type names
      */
    TabSeparatedRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                               bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "TabSeparatedRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    void resetParser() override;

protected:
    bool with_names;
    bool with_types;
    const FormatSettings format_settings;

    virtual bool readField(IColumn & column, const DataTypePtr & type,
        const SerializationPtr & serialization, bool is_last_file_column);

private:
    DataTypes data_types;

    using IndexesMap = std::unordered_map<String, size_t>;
    IndexesMap column_indexes_by_names;

    std::vector<size_t> columns_to_fill_with_default_values;

    void addInputColumn(const String & column_name);
    void setupAllColumnsByTableSchema();
    void fillUnreadColumnsWithDefaults(MutableColumns & columns, RowReadExtension & row_read_extension);

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) override;
    void tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column) override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override { return *pos != '\n' && *pos != '\t'; }
};

}

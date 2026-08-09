#pragma once

#include <Core/BlockNameMap.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class MySQLDumpRowInputFormat final : public IRowInputFormat
{
public:
    MySQLDumpRowInputFormat(ReadBuffer & in_, SharedHeader header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "MySQLDumpRowInputFormat"; }
    void readPrefix() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    bool readField(IColumn & column, size_t column_idx);
    void skipField();

    bool supportsCountRows() const override { return true; }
    size_t countRows(size_t max_block_size) override;

    String table_name;
    DataTypes types;
    BlockNameMap column_indexes_by_names;
    const FormatSettings format_settings;
};

class MySQLDumpSchemaReader final : public IRowSchemaReader
{
public:
    MySQLDumpSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings);

private:
    NamesAndTypesList readSchema() override;
    std::optional<DataTypes> readRowAndGetDataTypes() override;
    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;

    /// The parser maps the dump's columns onto the destination by name (through an exact-case name
    /// lookup, so `honorsColumnNameMatchingMode` stays false) when it saw column names — in a `CREATE`
    /// query for the table or in the column list of the `INSERT` query — and
    /// `input_format_mysql_dump_map_column_names` is enabled; otherwise it maps them positionally. Whether
    /// names were seen is known only after reading the schema, so the value is latched in `readSchema`.
    bool mapsColumnsByName() const override { return column_names_read_from_data && format_settings.mysql_dump.map_column_names; }

    String table_name;
    bool column_names_read_from_data = false;
};

}

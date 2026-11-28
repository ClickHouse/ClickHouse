#pragma once

#include <Core/BlockNameMap.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace DB
{

struct MySQLDumpColumnState
{
    Names create_names;
    Names first_names;
    Names current_names;
    bool has_explicit_columns = false;
};

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
    bool prepareRow();
    void updateColumnMapping(Names next_names);

    bool supportsCountRows() const override { return true; }
    size_t countRows(size_t max_block_size) override;

    String table_name;
    MySQLDumpColumnState column_state;
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

    bool prepareRow();
    void updateColumnPermutation(Names next_names);

    String table_name;
    MySQLDumpColumnState column_state;
    VectorWithMemoryTracking<size_t> input_to_first;
};

}

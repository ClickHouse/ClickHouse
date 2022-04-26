#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class MySQLDumpRowInputFormat final : public IRowInputFormat
{
public:
    MySQLDumpRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "MySQLDumpRowInputFormat"; }
    void readPrefix() override;

    bool needContext() const override { return true; }
    void setContext(ContextPtr & context_) override { context = Context::createCopy(context_); }

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    bool readField(IColumn & column, size_t column_idx);
    void skipField();

    String table_name;
    DataTypes types;
    std::unordered_map<String, size_t> column_indexes_by_names;
    const FormatSettings format_settings;
    ContextPtr context;
};

class MySQLDumpSchemaReader : public IRowSchemaReader
{
public:
    MySQLDumpSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings);

private:
    NamesAndTypesList readSchema() override;
    DataTypes readRowAndGetDataTypes() override;

    bool needContext() const override { return true; }
    void setContext(ContextPtr & context_) override { context = Context::createCopy(context_); }

    const FormatSettings format_settings;
    String table_name;
    ContextPtr context;
};

}

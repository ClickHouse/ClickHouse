#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Databases/SQLite/SQLiteUtils.h>

namespace arrow::io { class RandomAccessFile; }

namespace DB
{

class ReadBuffer;

class SQLiteInputFormat final : public IRowInputFormat
{
public:

    SQLiteInputFormat(ReadBuffer & in_, const Block & header_, const RowInputFormatParams & params_,
                           const FormatSettings & format_settings_);

    String getName() const override { return "SQLiteInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readPrefix() override;
    void prepareReader();
    std::vector<String> getTablesNames();

    SQLitePtr db;
    std::shared_ptr<arrow::io::RandomAccessFile> file_reader;
    std::shared_ptr<sqlite3_stmt> stmt;
    String table_name;
    const FormatSettings format_settings;
    bool continue_read = true;
};
};

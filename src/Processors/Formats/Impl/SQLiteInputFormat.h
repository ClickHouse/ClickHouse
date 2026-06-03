#pragma once

#include "config.h"

#if USE_SQLITE

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Databases/SQLite/SQLiteUtils.h>

namespace arrow::io { class RandomAccessFile; }

namespace DB
{

class ReadBuffer;

class SQLiteInputFormat final : public IRowInputFormat
{
public:

    SQLiteInputFormat(ReadBuffer & in_, SharedHeader header_, Params params_,
                           const FormatSettings & format_settings_);

    String getName() const override { return "SQLiteInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readPrefix() override;
    void prepareReader();
    std::vector<String> getTablesNames();

    /// Destruction order matters and is the reverse of the declaration order: the prepared
    /// statement must be finalized first, then the connection closed (closing it may still read
    /// through the VFS), and only then the underlying file released. So declare them as
    /// file_reader, db, stmt.
    std::shared_ptr<arrow::io::RandomAccessFile> file_reader;
    SQLitePtr db;
    std::shared_ptr<sqlite3_stmt> stmt;
    String table_name;
    const FormatSettings format_settings;
    bool continue_read = true;
};

}

#endif

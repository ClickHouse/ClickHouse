#pragma once

#include "config.h"

#if USE_SQLITE

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/SQLiteInputVFS.h>
#include <Databases/SQLite/SQLiteUtils.h>

#include <memory>

namespace DB
{

class ReadBuffer;
class SeekableReadBuffer;
class ReadBufferFromOwnString;

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
    /// through the VFS), and only then the underlying buffer released. So declare them as
    /// owned_buffer, db, stmt.
    ///
    /// When the input is not seekable (e.g. a pipe), the whole database is read into
    /// owned_buffer; otherwise the format reads directly from the input buffer and owned_buffer
    /// stays empty. read_source.buf points at whichever buffer is in use.
    std::unique_ptr<ReadBufferFromOwnString> owned_buffer;
    SQLiteReadSource read_source{};
    SQLitePtr db;
    std::shared_ptr<sqlite3_stmt> stmt;
    String table_name;
    const FormatSettings format_settings;
    bool continue_read = true;

    /// Cached from the header. For a Nullable column, nested_serializations holds the
    /// serialization of the nested (non-nullable) type; it is empty for non-nullable columns.
    DataTypes data_types;
    Serializations nested_serializations;
};

}

#endif

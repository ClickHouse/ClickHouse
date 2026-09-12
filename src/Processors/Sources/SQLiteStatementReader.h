#pragma once

#include "config.h"

#if USE_SQLITE

#include <Core/Block.h>
#include <Core/ExternalResultDescription.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>

#include <sqlite3.h>

#include <functional>
#include <optional>
#include <vector>

namespace DB
{

class IColumn;

class SQLiteStatementReader
{
public:
    enum class ValueReadMode
    {
        Native,
        Text,
    };

    SQLiteStatementReader(const Block & sample_block_, const FormatSettings & format_settings_, ValueReadMode value_read_mode_);

    /// Reads up to max_block_size rows. `is_cancelled` bounds how long the read waits for a locked
    /// database (SQLITE_BUSY): the wait is aborted and the read reports `finished` when it returns true.
    /// It also legitimizes an interrupted step: SQLITE_INTERRUPT reports `finished` only when `is_cancelled`
    /// returns true (our own cancellation issued the interrupt on a dedicated connection); otherwise the
    /// interrupt would silently truncate the result set, so the read fails instead.
    Chunk readChunk(sqlite3 * db, sqlite3_stmt * statement, UInt64 max_block_size, bool & finished, const std::function<bool()> & is_cancelled);

private:
    using ValueType = ExternalResultDescription::ValueType;

    struct ColumnReadInfo
    {
        String name;
        DataTypePtr data_type;
        SerializationPtr serialization;
        std::optional<ValueType> native_value_type;
        bool is_nullable = false;
    };

    ColumnReadInfo createColumnReadInfoForNative(
        const ColumnWithTypeAndName & column,
        ValueType native_value_type,
        bool is_nullable) const;
    ColumnReadInfo createColumnReadInfoForText(const ColumnWithTypeAndName & column) const;

    void insertValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const;
    void insertTextValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const;

    Block sample_block;
    FormatSettings format_settings;
    std::vector<ColumnReadInfo> columns_info;
};

}

#endif

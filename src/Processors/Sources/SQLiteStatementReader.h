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
        /// Whether every cell read through a native numeric accessor must hold the storage class that the
        /// accessor reads exactly. Set for a result column without a declared SQLite type (see
        /// `resolveUndeclaredColumns`).
        bool requires_exact_storage_class = false;
    };

    ColumnReadInfo createColumnReadInfoForNative(
        const ColumnWithTypeAndName & column,
        ValueType native_value_type,
        bool is_nullable) const;
    ColumnReadInfo createColumnReadInfoForText(const ColumnWithTypeAndName & column) const;

    /// A result column with a declared SQLite type (a direct column of a table) carries a contract for its
    /// cells: the declared affinity, or the STRICT table, fixes what the ClickHouse type mapped from it
    /// reads. A result column without one - an expression, a literal, an aggregate - has no such contract:
    /// its ClickHouse type was inferred from a single row (see `doQueryResultStructure`), or declared by the
    /// user, and SQLite is free to return a different storage class in every row (`CASE`, `UNION ALL`, an
    /// aggregate over mixed data). Reading such a cell through a coercing accessor (`sqlite3_column_int64`
    /// over a REAL cell truncates `1.5` to `1`, over a TEXT cell yields `0`) would silently produce wrong
    /// values, so such a column is marked to be read fail-closed: a cell whose storage class does not match
    /// the native type exactly makes the read fail (`checkStorageClass`).
    void resolveUndeclaredColumns(sqlite3_stmt * statement);
    void checkStorageClass(const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const;

    void insertValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const;
    void insertTextValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const;

    Block sample_block;
    FormatSettings format_settings;
    std::vector<ColumnReadInfo> columns_info;
    bool undeclared_columns_resolved = false;
};

}

#endif

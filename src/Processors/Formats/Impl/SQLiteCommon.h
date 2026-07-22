#pragma once

#include "config.h"

#if USE_SQLITE

#    include <Columns/IColumn.h>
#    include <Core/Field.h>
#    include <DataTypes/DataTypeLowCardinality.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/IDataType.h>
#    include <DataTypes/Serializations/ISerialization.h>
#    include <Formats/FormatSettings.h>
#    include <IO/ReadBuffer.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/copyData.h>
#    include <Common/Exception.h>
#    include <Common/NaNUtils.h>

#    include <sqlite3.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
}

namespace SQLiteFormatImpl
{

using SQLitePtr = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;
using SQLiteStatementPtr = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

class SQLiteDatabase
{
public:
    sqlite3 * get() const { return db.get(); }

    String serialized_database;
    SQLitePtr db{nullptr, sqlite3_close};
};

inline void checkSQLiteStatus(sqlite3 * db, int status, std::string_view message)
{
    if (status != SQLITE_OK && status != SQLITE_DONE && status != SQLITE_ROW)
    {
        throw Exception(
            ErrorCodes::SQLITE_ENGINE_ERROR,
            "{}. Status: {}. Message: {}",
            message,
            status,
            db ? sqlite3_errmsg(db) : sqlite3_errstr(status));
    }
}

inline SQLitePtr openSQLiteDatabaseWithFlags(const String & path, int flags, std::string_view message)
{
    sqlite3 * db = nullptr;
    int status = sqlite3_open_v2(path.c_str(), &db, flags, nullptr);
    if (status != SQLITE_OK)
    {
        String sqlite_message = db ? sqlite3_errmsg(db) : sqlite3_errstr(status);
        if (db)
            sqlite3_close(db);
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "{} {}. Status: {}. Message: {}", message, path, status, sqlite_message);
    }

    SQLitePtr result(db, sqlite3_close);
    checkSQLiteStatus(
        result.get(),
        sqlite3_db_config(result.get(), SQLITE_DBCONFIG_DQS_DDL, 0, nullptr),
        "Cannot disable SQLite DQS in DDL statements");
    checkSQLiteStatus(
        result.get(),
        sqlite3_db_config(result.get(), SQLITE_DBCONFIG_DQS_DML, 0, nullptr),
        "Cannot disable SQLite DQS in DML statements");

    return result;
}

inline SQLitePtr openSQLiteDatabase(const String & path)
{
    return openSQLiteDatabaseWithFlags(path, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "Cannot open SQLite database");
}

inline SQLitePtr openSQLiteDatabaseReadOnly(const String & path)
{
    return openSQLiteDatabaseWithFlags(path, SQLITE_OPEN_READONLY, "Cannot open read-only SQLite database");
}

inline SQLiteDatabase openSQLiteDatabaseFromMemory(ReadBuffer & in)
{
    SQLiteDatabase result;
    {
        WriteBufferFromString memory_out(result.serialized_database);
        copyData(in, memory_out);
    }

    result.db = openSQLiteDatabase(":memory:");
    int status = sqlite3_deserialize(
        result.db.get(),
        "main",
        reinterpret_cast<unsigned char *>(result.serialized_database.data()),
        static_cast<sqlite3_int64>(result.serialized_database.size()),
        static_cast<sqlite3_int64>(result.serialized_database.size()),
        SQLITE_DESERIALIZE_READONLY);
    checkSQLiteStatus(result.db.get(), status, "Cannot deserialize SQLite database from memory");

    return result;
}

inline SQLiteDatabase openSQLiteDatabaseForRead(ReadBuffer & in, const FormatSettings & settings)
{
    if (settings.seekable_read)
    {
        if (auto * file_in = dynamic_cast<ReadBufferFromFileBase *>(&in))
        {
            size_t view_offset = 0;
            if (file_in->isRegularLocalFile(&view_offset) && view_offset == 0)
            {
                SQLiteDatabase result;
                result.db = openSQLiteDatabaseReadOnly(file_in->getFileName());
                return result;
            }
        }
    }

    return openSQLiteDatabaseFromMemory(in);
}

inline void executeSQLite(sqlite3 * db, const String & query)
{
    char * err_message = nullptr;
    int status = sqlite3_exec(db, query.c_str(), nullptr, nullptr, &err_message);

    if (status != SQLITE_OK)
    {
        String message(err_message ? err_message : sqlite3_errmsg(db));
        sqlite3_free(err_message);
        throw Exception(
            ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot execute SQLite query: {}. Status: {}. Message: {}", query, status, message);
    }
}

inline SQLiteStatementPtr prepareSQLiteStatement(sqlite3 * db, const String & query)
{
    sqlite3_stmt * statement = nullptr;
    int status = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size() + 1), &statement, nullptr);
    checkSQLiteStatus(db, status, fmt::format("Cannot prepare SQLite query: {}", query));
    return SQLiteStatementPtr(statement, sqlite3_finalize);
}

inline void bindSQLiteTextValue(
    sqlite3 * db,
    sqlite3_stmt * statement,
    int sqlite_index,
    const IColumn & column,
    size_t row,
    const ISerialization & serialization,
    const FormatSettings & settings)
{
    WriteBufferFromOwnString value;
    serialization.serializeText(column, row, value, settings);
    const auto value_string = value.str();
    checkSQLiteStatus(
        db,
        sqlite3_bind_text(statement, sqlite_index, value_string.data(), static_cast<int>(value_string.size()), SQLITE_TRANSIENT),
        "Cannot bind text value");
}

/// Bind one non-NULL cell of a ClickHouse column as a parameter of a prepared SQLite statement (the caller
/// binds NULL cells itself). Binding (rather than formatting an `INSERT ... VALUES` SQL string) is the only
/// way to pass values to SQLite faithfully: SQLite string literals have no escape sequences at all, so control
/// characters would be corrupted when written as text and a NUL byte would truncate the whole statement.
/// Signed native integers and Bool are bound as SQLite INTEGER, finite floats as REAL; everything else -
/// including `UInt64` (which can exceed the INTEGER range) and NaN (which `sqlite3_bind_double` would turn
/// into SQLite NULL) - is bound as text using its ClickHouse text serialization, with an explicit byte length
/// so that control characters and embedded NUL bytes survive. SQLite then applies the target column's
/// affinity to the bound value. This single dispatch is shared by the `SQLite` output format and the
/// `SQLite` storage engine / `sqlite` table function sink, so both write paths round-trip values identically.
inline void bindSQLiteValue(
    sqlite3 * db,
    sqlite3_stmt * statement,
    int sqlite_index,
    const IColumn & column,
    size_t row,
    const DataTypePtr & type,
    const ISerialization & serialization,
    const FormatSettings & settings)
{
    auto nested_type = removeLowCardinalityAndNullable(type);
    WhichDataType which(nested_type);

    if (isBool(nested_type))
    {
        checkSQLiteStatus(
            db,
            sqlite3_bind_int64(statement, sqlite_index, column[row].safeGet<UInt64>() != 0),
            "Cannot bind boolean value");
        return;
    }

    if (which.isNativeInt())
    {
        checkSQLiteStatus(
            db,
            sqlite3_bind_int64(statement, sqlite_index, column[row].safeGet<Int64>()),
            "Cannot bind integer value");
        return;
    }

    if (which.isUInt8() || which.isUInt16() || which.isUInt32())
    {
        checkSQLiteStatus(
            db,
            sqlite3_bind_int64(statement, sqlite_index, static_cast<sqlite3_int64>(column[row].safeGet<UInt64>())),
            "Cannot bind unsigned integer value");
        return;
    }

    if (which.isFloat())
    {
        const auto float_value = column[row].safeGet<Float64>();
        if (isNaN(float_value))
        {
            bindSQLiteTextValue(db, statement, sqlite_index, column, row, serialization, settings);
            return;
        }

        checkSQLiteStatus(
            db,
            sqlite3_bind_double(statement, sqlite_index, float_value),
            "Cannot bind floating-point value");
        return;
    }

    bindSQLiteTextValue(db, statement, sqlite_index, column, row, serialization, settings);
}

/// Whether a `WHERE` predicate on a column of this ClickHouse type may be pushed down to SQLite without the
/// risk of false negatives. This mirrors the dispatch of `bindSQLiteValue` above: types bound as SQLite
/// INTEGER or REAL compare numerically on both sides, and `String` is stored as its exact bytes and compares
/// byte-wise in both systems. Every other type - `UInt64`, `Int128`/`UInt128`/`Int256`/`UInt256`, `Decimal`,
/// dates and times, `FixedString` (whose stored text keeps the padding bytes), `Enum`, `UUID`, ... - is
/// stored as its ClickHouse text serialization, so a pushed-down comparison can go wrong on the SQLite side:
/// SQLite orders every TEXT value after every numeric value and otherwise compares TEXT byte-wise, so a
/// predicate such as `x > 2` on a text-stored number treats `'10'` as smaller than `'2'` and `x = 5` never
/// matches the cell `'5'`. Rows wrongly dropped by SQLite cannot be recovered by the local re-filtering, so
/// such columns must be filtered by ClickHouse only.
inline bool isPushdownSafeType(const DataTypePtr & type)
{
    auto nested_type = removeLowCardinalityAndNullable(type);
    WhichDataType which(nested_type);

    if (which.isNativeInt() || which.isUInt8() || which.isUInt16() || which.isUInt32() || which.isFloat())
        return true;

    return which.isString();
}

}

}

#endif

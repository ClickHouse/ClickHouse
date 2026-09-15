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
#    include <Poco/String.h>
#    include <Common/CurrentThread.h>
#    include <Common/Exception.h>
#    include <Common/NaNUtils.h>
#    include <Common/Stopwatch.h>
#    include <Common/ThreadStatus.h>

#    include <base/sleep.h>

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

    /// Keep SQLite's default DQS behavior because stored schema SQL (for example a view) may rely on it.
    /// ClickHouse-generated identifiers use strict backquotes and cannot fall back to string literals.
    return SQLitePtr(db, sqlite3_close);
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

/// How long to sleep between retries when another connection holds a lock on the database (SQLITE_BUSY),
/// matching the retry back-off of the scan paths (`SQLiteSource`, `SQLiteStatementReader`).
constexpr UInt64 sqlite_busy_retry_sleep_ms = 10;

/// How long to keep waiting for a locked database when the wait cannot be cancelled (see
/// `keepWaitingForSQLiteLock`), after which the underlying SQLITE_BUSY error is surfaced.
constexpr UInt64 sqlite_busy_retry_timeout_ms = 10000;

/// Whether to keep waiting for a locked database. Most metadata lookups run on a query thread (schema
/// inference, `DESCRIBE`, the existence checks of the `SQLite` database engine), so a `KILL QUERY` or
/// `max_execution_time` stops the wait. Some of them run without a query - `ATTACH TABLE ... ENGINE =
/// SQLite(...)` reaches `fetchSQLiteTableStructure` while the server is starting up and loading table
/// metadata - and there is nothing to cancel such a wait, so it is bounded by a deadline instead of
/// blocking startup for as long as an external writer holds an exclusive lock. A cancelled query throws its
/// original cancellation exception; a wait without a query surfaces the underlying SQLITE_BUSY error once
/// its deadline expires.
inline bool keepWaitingForSQLiteLock(const Stopwatch & watch)
{
    if (CurrentThread::isInitialized() && !CurrentThread::getQueryId().empty())
    {
        CurrentThread::checkIfNotCancelled();
    }
    else if (watch.elapsedMilliseconds() >= sqlite_busy_retry_timeout_ms)
        return false;

    sleepForMilliseconds(sqlite_busy_retry_sleep_ms);
    return true;
}

/// Prepare that idles instead of failing while another connection holds an exclusive lock on the database
/// (loading the schema during `sqlite3_prepare_v2` needs a shared lock and returns SQLITE_BUSY otherwise).
/// Used by the metadata paths that run before a scan starts, mirroring the retry loop of the scan paths.
inline SQLiteStatementPtr prepareSQLiteStatementRetryOnBusy(sqlite3 * db, const String & query)
{
    Stopwatch watch;
    while (true)
    {
        sqlite3_stmt * statement = nullptr;
        int status = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size() + 1), &statement, nullptr);
        if (status == SQLITE_BUSY && keepWaitingForSQLiteLock(watch))
            continue;
        checkSQLiteStatus(db, status, fmt::format("Cannot prepare SQLite query: {}", query));
        return SQLiteStatementPtr(statement, sqlite3_finalize);
    }
}

/// Step that idles instead of failing while the database is locked. Returns the first status other than
/// SQLITE_BUSY (normally SQLITE_ROW or SQLITE_DONE); the caller interprets and reports other statuses.
inline int stepSQLiteStatementRetryOnBusy(sqlite3_stmt * statement)
{
    Stopwatch watch;
    while (true)
    {
        int status = sqlite3_step(statement);
        if (status == SQLITE_BUSY && keepWaitingForSQLiteLock(watch))
            continue;
        return status;
    }
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

/// The ClickHouse-side half of the pushdown-safety check (see `isPushdownSafeColumn` below): whether a
/// `WHERE` predicate on a column of this ClickHouse type may be pushed down to SQLite without the risk of
/// false negatives. Eligibility requires the read accessor to be *exact over the whole remote domain*: the
/// local re-filtering only sees the rows SQLite returns, so whenever the read path coerces a remote value
/// (`SQLiteStatementReader::insertValue`), a predicate that matches the coerced local value can be false
/// against the remote one and SQLite drops the row for good. Only three ClickHouse types have an exact
/// accessor over some SQLite storage class:
///   - `Int64` reads through `sqlite3_column_int64`, which is lossless over the whole signed 64-bit range
///     of a SQLite INTEGER cell;
///   - `Float64` reads through `sqlite3_column_double`, which is lossless over a REAL cell (both are IEEE
///     754 doubles);
///   - `String` reads the exact bytes of a TEXT cell.
/// Every other type coerces or re-encodes on at least one side. The narrower integers truncate (`UInt8`
/// over an INTEGER cell holding `300` reads locally as `44`, so `x = 44` matches locally while the
/// pushed-down `x = 44` is false remotely), `Float32` rounds a REAL cell to single precision, and the
/// text-stored types - `UInt64`, `Int128`/`UInt128`/`Int256`/`UInt256`, `Decimal`, dates and times,
/// `FixedString` (whose stored text keeps the padding bytes), `Enum`, `UUID`, ... - compare by value in
/// ClickHouse while SQLite orders every TEXT value after every numeric value and otherwise compares TEXT
/// byte-wise (`x > 2` on a text-stored number treats `'10'` as smaller than `'2'`). Such columns must be
/// filtered by ClickHouse only.
///
/// A `LowCardinality(...)` wrapper also disqualifies a column, whatever it wraps: the storage read path
/// (`SQLiteStatementReader` in its native read mode) routes every `LowCardinality` column through the text
/// path instead of the native accessors, so the locally read value is SQLite's *text rendering* of the
/// cell, not the cell itself. Nothing pins that rendering to be exact: it depends on the SQLite version
/// (only 3.43+ renders a REAL cell with round-trip precision; older versions shorten
/// `1.2345678901234567` to `1.23456789012346`), so a predicate matching the locally read value could be
/// false against the remote cell. Eligibility must follow the read path actually taken, hence fail closed
/// and filter `LowCardinality` columns locally.
inline bool isPushdownSafeType(const DataTypePtr & type)
{
    if (type->lowCardinality())
        return false;

    WhichDataType which(removeNullable(type));

    return which.isInt64() || which.isFloat64() || which.isString();
}

/// Whether the SQLite table `table_name` in the main schema is declared STRICT
/// (https://www.sqlite.org/stricttables.html). This matters for the pushdown gate below: in an ordinary
/// SQLite table the declared column type only sets an *affinity* - a coercion preference applied to newly
/// inserted values - while every cell keeps its own runtime storage class, so an INTEGER-declared column can
/// still hold the TEXT cell `'abc'` and a BLOB-declared column any value at all. Only a STRICT table
/// guarantees that every stored cell actually has the storage class of its declared column type. Errors and
/// A missing table fails closed to `false`. A locked database (SQLITE_BUSY) is not a proof of anything, so
/// it does not fail closed: the probe waits like the other metadata paths (`prepareSQLiteStatementRetryOnBusy`,
/// `stepSQLiteStatementRetryOnBusy`) and surfaces the error once the wait is cancelled or times out.
inline bool isStrictTable(sqlite3 * db, const String & table_name)
{
    auto statement = prepareSQLiteStatementRetryOnBusy(db, "SELECT strict FROM pragma_table_list WHERE schema = 'main' AND name = ?");

    checkSQLiteStatus(
        db,
        sqlite3_bind_text64(statement.get(), 1, table_name.data(), table_name.size(), SQLITE_STATIC, SQLITE_UTF8),
        "Cannot bind the table name to a SQLite statement");

    int status = stepSQLiteStatementRetryOnBusy(statement.get());
    checkSQLiteStatus(db, status, "Cannot query the SQLite table list");
    if (status != SQLITE_ROW)
        return false;

    return sqlite3_column_int(statement.get(), 0) != 0;
}

/// Whether a `WHERE` predicate on this column of the SQLite table `table_name` may be pushed down without
/// the risk of false negatives (rows wrongly dropped by SQLite cannot be recovered by the local
/// re-filtering; false positives are harmless because every row that comes back is re-filtered locally).
///
/// Two independent conditions must hold. First, the ClickHouse type must be one that compares by value on
/// both sides (`isPushdownSafeType` above). Second, the *remote* column must actually compare the way
/// ClickHouse does for every cell it can hold. Declared affinity and collation alone cannot prove that:
/// SQLite is dynamically typed per cell, so an arbitrary pre-existing INTEGER-declared column can still hold
/// the TEXT cell `'abc'`, which ClickHouse reads through `sqlite3_column_int64` as `0` while the pushed-down
/// `x = 0` compares against the TEXT storage class on the SQLite side and drops the row. The gate therefore
/// requires the table to be STRICT (`isStrictTable` above), which pins the storage class of every cell to
/// the declared column type, and then requires the exact representation-preserving pairing of the declared
/// type with the ClickHouse one (`isPushdownSafeType` above explains why only exact pairs qualify - a
/// coercing read accessor makes SQLite and ClickHouse evaluate the same predicate against different
/// values):
///   - `Int64` requires a declared INT or INTEGER column: every cell is a signed 64-bit integer, which the
///     read path returns losslessly, and both sides compare it numerically. A REAL column is not eligible -
///     `sqlite3_column_int64` truncates the cell `1.9` to `1`, so `x = 1` matches locally but the
///     pushed-down `x = 1` is false remotely;
///   - `Float64` requires a declared REAL column: every cell is an IEEE 754 double, returned losslessly. An
///     INTEGER column is not eligible - `sqlite3_column_double` rounds cells above 2^53 to the nearest
///     double, so the pushed-down exact integer comparison disagrees with the local one;
///   - narrower ClickHouse integers and `Float32` are never eligible (already rejected by
///     `isPushdownSafeType`): their read accessors truncate remote INTEGER/REAL cells outside the local
///     type's domain;
///   - `String` requires a declared TEXT column (cells guaranteed TEXT, compared byte-wise against a text
///     literal) with the byte-wise BINARY collation - a non-BINARY collation such as NOCASE or RTRIM orders
///     differently from ClickHouse (`'a' > 'B'` is true byte-wise but false under NOCASE). A BLOB-declared
///     column is not eligible even though ClickHouse reads its cells as `String`: SQLite never equates a
///     BLOB cell with a TEXT literal, so every pushed-down comparison would be a false negative;
///   - a STRICT ANY column places no constraint on its cells and is not eligible.
/// The remote column must also be guaranteed non-`NULL` when the ClickHouse type cannot contain `NULL`.
/// Otherwise, a pushed-down predicate could discard a `NULL` that the local read path would reject.
///
/// Everything else fails closed to local filtering: a non-STRICT table, a view (`pragma_table_list` reports
/// no STRICT views), or remote metadata that cannot be fetched because the column vanished remotely. A
/// locked database (SQLITE_BUSY) is the one condition that does not fail closed: it proves nothing about
/// the column, so the metadata probes wait for the lock like every other metadata path and surface the
/// error once the wait is cancelled or times out.
inline bool isPushdownSafeColumn(sqlite3 * db, const String & table_name, const String & column_name, const DataTypePtr & type)
{
    if (!isPushdownSafeType(type))
        return false;

    /// `sqlite3_table_column_metadata` takes NUL-terminated names; a name with an embedded NUL byte would be
    /// silently truncated and could report another column's metadata.
    if (table_name.contains('\0') || column_name.contains('\0'))
        return false;

    if (!isStrictTable(db, table_name))
        return false;

    const char * declared_type = nullptr;
    const char * collation = nullptr;
    int not_null = 0;
    int primary_key = 0;
    int autoincrement = 0;
    Stopwatch watch;
    int status = SQLITE_OK;
    while (true)
    {
        status = sqlite3_table_column_metadata(
            db, "main", table_name.c_str(), column_name.c_str(), &declared_type, &collation, &not_null, &primary_key, &autoincrement);
        if (status == SQLITE_BUSY && keepWaitingForSQLiteLock(watch))
            continue;
        break;
    }
    /// A locked database means the metadata is unknown, not unsafe: wait like the other metadata paths and
    /// surface the error after a cancelled or timed-out wait instead of failing closed, which would wrongly
    /// reject a pushdown-safe filter with `INCORRECT_QUERY` under `external_table_strict_query = 1`. Only
    /// SQLITE_ERROR - the column vanished remotely - fails closed to local filtering.
    if (status != SQLITE_OK && status != SQLITE_ERROR)
        checkSQLiteStatus(db, status, "Cannot fetch SQLite column metadata");
    if (status != SQLITE_OK)
        return false;

    if (!canContainNull(*type) && !not_null && !primary_key)
        return false;

    /// A STRICT table only admits the declared types INT, INTEGER, REAL, TEXT, BLOB and ANY (stored as
    /// written, in any letter case), so exact token comparison suffices.
    const String declared = Poco::toUpper(String(declared_type ? declared_type : ""));

    /// `LowCardinality` wrappers never get this far: `isPushdownSafeType` above rejects them.
    WhichDataType which(removeNullable(type));
    if (which.isString())
        return declared == "TEXT" && collation && Poco::toUpper(String(collation)) == "BINARY";

    if (which.isInt64())
        return declared == "INT" || declared == "INTEGER";

    if (which.isFloat64())
        return declared == "REAL";

    return false;
}

}

}

#endif

#pragma once

#include "config.h"

#if USE_SQLITE

#    include <Formats/FormatSettings.h>
#    include <IO/ReadBuffer.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/copyData.h>
#    include <Common/Exception.h>

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

}

}

#endif

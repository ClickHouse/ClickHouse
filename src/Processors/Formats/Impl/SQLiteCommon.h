#pragma once

#include "config.h"

#if USE_SQLITE

#    include <IO/ReadBuffer.h>
#    include <IO/WriteBufferFromFile.h>
#    include <IO/copyData.h>
#    include <Common/Exception.h>

#    include <Poco/TemporaryFile.h>
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

class SQLiteTemporaryFile
{
public:
    SQLiteTemporaryFile()
        : file(std::make_unique<Poco::TemporaryFile>())
        , path(file->path())
    {
    }

    const String & getPath() const { return path; }

private:
    std::unique_ptr<Poco::TemporaryFile> file;
    String path;
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

inline SQLitePtr openSQLiteDatabase(const String & path)
{
    sqlite3 * db = nullptr;
    int status = sqlite3_open(path.c_str(), &db);
    if (status != SQLITE_OK)
    {
        String message = db ? sqlite3_errmsg(db) : sqlite3_errstr(status);
        if (db)
            sqlite3_close(db);
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot open SQLite database {}. Status: {}. Message: {}", path, status, message);
    }
    return SQLitePtr(db, sqlite3_close);
}

inline SQLitePtr copyToTemporaryFileAndOpenSQLiteDatabase(ReadBuffer & in, const SQLiteTemporaryFile & temporary_file)
{
    WriteBufferFromFile file_out(temporary_file.getPath());
    copyData(in, file_out);
    file_out.finalize();
    return openSQLiteDatabase(temporary_file.getPath());
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

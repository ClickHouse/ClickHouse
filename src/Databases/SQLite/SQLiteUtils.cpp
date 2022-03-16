#include "SQLiteUtils.h"

#if USE_SQLITE
#include <base/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}


void processSQLiteError(const String & message, bool throw_on_error)
{
    if (throw_on_error)
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, message);
    else
        LOG_ERROR(&Poco::Logger::get("SQLiteEngine"), fmt::runtime(message));
}


String validateSQLiteDatabasePath(const String & path, const String & user_files_path, bool throw_on_error)
{
    String canonical_user_files_path = fs::canonical(user_files_path);

    String canonical_path;
    std::error_code err;

    if (fs::path(path).is_relative())
        canonical_path = fs::canonical(fs::path(user_files_path) / path, err);
    else
        canonical_path = fs::canonical(path, err);

    if (err)
        processSQLiteError(fmt::format("SQLite database path '{}' is invalid. Error: {}", path, err.message()), throw_on_error);

    if (!canonical_path.starts_with(canonical_user_files_path))
        processSQLiteError(fmt::format("SQLite database file path '{}' must be inside 'user_files' directory", path), throw_on_error);

    return canonical_path;
}


SQLitePtr openSQLiteDB(const String & database_path, ContextPtr context, bool throw_on_error)
{
    auto validated_path = validateSQLiteDatabasePath(database_path, context->getUserFilesPath(), throw_on_error);

    sqlite3 * tmp_sqlite_db = nullptr;
    int status = sqlite3_open(validated_path.c_str(), &tmp_sqlite_db);

    if (status != SQLITE_OK)
    {
        processSQLiteError(fmt::format("Cannot access sqlite database. Error status: {}. Message: {}",
                                       status, sqlite3_errstr(status)), throw_on_error);
        return nullptr;
    }

    return std::shared_ptr<sqlite3>(tmp_sqlite_db, sqlite3_close);
}

}

#endif

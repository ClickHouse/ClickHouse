#include "SQLiteUtils.h"

#if USE_SQLITE
#include <Common/logger_useful.h>
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
    if (fs::path(path).is_relative())
        return fs::absolute(fs::path(user_files_path) / path).lexically_normal();

    String absolute_path = fs::absolute(path).lexically_normal();
    String absolute_user_files_path = fs::absolute(user_files_path).lexically_normal();

    if (!absolute_path.starts_with(absolute_user_files_path))
    {
        processSQLiteError(fmt::format("SQLite database file path '{}' must be inside 'user_files' directory", path), throw_on_error);
        return "";
    }
    return absolute_path;
}

SQLitePtr openSQLiteDB(const String & path, ContextPtr context, bool throw_on_error)
{
    auto user_files_path = context->getUserFilesPath();
    auto database_path = validateSQLiteDatabasePath(path, user_files_path, throw_on_error);

    /// For attach database there is no throw mode.
    if (database_path.empty())
        return nullptr;

    if (!fs::exists(database_path))
        LOG_DEBUG(&Poco::Logger::get("SQLite"), "SQLite database path {} does not exist, will create an empty SQLite database", database_path);

    sqlite3 * tmp_sqlite_db = nullptr;
    int status = sqlite3_open(database_path.c_str(), &tmp_sqlite_db);

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

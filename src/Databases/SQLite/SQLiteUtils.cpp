#include <Databases/SQLite/SQLiteUtils.h>

#if USE_SQLITE
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}

static std::mutex init_sqlite_db_mutex;

void processSQLiteError(const String & message, bool throw_on_error)
{
    if (throw_on_error)
        throw Exception::createDeprecated(message, ErrorCodes::PATH_ACCESS_DENIED);
    LOG_ERROR(getLogger("SQLiteEngine"), fmt::runtime(message));
}

String validateSQLiteDatabasePath(const String & path, const String & user_files_path, bool need_check, bool throw_on_error)
{
    String absolute_path = fs::absolute(path).lexically_normal();

    if (fs::path(path).is_relative())
        absolute_path = fs::absolute(fs::path(user_files_path) / path).lexically_normal();

    String absolute_user_files_path = fs::absolute(user_files_path).lexically_normal();

    if (need_check && !absolute_path.starts_with(absolute_user_files_path))
    {
        processSQLiteError(fmt::format("SQLite database file path '{}' must be inside 'user_files' directory", path), throw_on_error);
        return "";
    }
    return absolute_path;
}

SQLitePtr openSQLiteDB(const String & path, ContextPtr context, bool throw_on_error)
{
    // If run in Local mode, no need for path checking.
    bool need_check = context->getApplicationType() != Context::ApplicationType::LOCAL;

    auto user_files_path = context->getUserFilesPath();
    auto database_path = validateSQLiteDatabasePath(path, user_files_path, need_check, throw_on_error);

    /// For attach database there is no throw mode.
    if (database_path.empty())
        return nullptr;

    if (!fs::exists(database_path))
        LOG_DEBUG(getLogger("SQLite"), "SQLite database path {} does not exist, will create an empty SQLite database", database_path);

    sqlite3 * tmp_sqlite_db = nullptr;
    int status;
    {
        std::lock_guard lock(init_sqlite_db_mutex);
        status = sqlite3_open(database_path.c_str(), &tmp_sqlite_db);
    }

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

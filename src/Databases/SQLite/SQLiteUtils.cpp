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

String validateSQLiteDatabasePath(const String & path, const Strings & user_files_paths, bool need_check, bool throw_on_error)
{
    String absolute_path;

    if (fs::path(path).is_relative())
    {
        /// For relative paths, try each user_files_path and use the first one where the file exists.
        bool found = false;
        for (const auto & ufp : user_files_paths)
        {
            String candidate = fs::absolute(fs::path(ufp) / path).lexically_normal();
            if (fs::exists(candidate))
            {
                absolute_path = candidate;
                found = true;
                break;
            }
        }
        if (!found)
            absolute_path = fs::absolute(fs::path(user_files_paths.front()) / path).lexically_normal();
    }
    else
    {
        absolute_path = fs::absolute(path).lexically_normal();
    }

    if (need_check)
    {
        bool inside = false;
        for (const auto & ufp : user_files_paths)
        {
            String absolute_ufp = fs::absolute(ufp).lexically_normal();
            if (absolute_path.starts_with(absolute_ufp))
            {
                inside = true;
                break;
            }
        }
        if (!inside)
        {
            processSQLiteError(fmt::format("SQLite database file path '{}' must be inside 'user_files' directory", path), throw_on_error);
            return "";
        }
    }
    return absolute_path;
}

String validateSQLiteDatabasePath(const String & path, const String & user_files_path, bool need_check, bool throw_on_error)
{
    return validateSQLiteDatabasePath(path, Strings{user_files_path}, need_check, throw_on_error);
}

SQLitePtr openSQLiteDB(const String & path, ContextPtr context, bool throw_on_error)
{
    // If run in Local mode, no need for path checking.
    bool need_check = context->getApplicationType() != Context::ApplicationType::LOCAL;

    const auto user_files_paths = context->getUserFilesPaths();
    auto database_path = validateSQLiteDatabasePath(path, user_files_paths, need_check, throw_on_error);

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

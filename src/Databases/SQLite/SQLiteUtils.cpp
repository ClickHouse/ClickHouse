#include <Databases/SQLite/SQLiteUtils.h>

#if USE_SQLITE
#include <filesystem>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}

static std::mutex init_sqlite_db_mutex;

String quoteSQLiteIdentifier(std::string_view identifier)
{
    return backQuoteSQLite(identifier);
}

static void processSQLiteError(const String & message, bool throw_on_error)
{
    if (throw_on_error)
        throw Exception::createDeprecated(message, ErrorCodes::PATH_ACCESS_DENIED);
    LOG_ERROR(getLogger("SQLiteEngine"), fmt::runtime(message));
}

static String validateSQLiteDatabasePath(const String & path, const String & user_files_path, bool need_check, bool throw_on_error)
{
    String absolute_path = fs::absolute(path).lexically_normal();

    if (fs::path(path).is_relative())
        absolute_path = fs::absolute(fs::path(user_files_path) / path).lexically_normal();

    String absolute_user_files_path = fs::absolute(user_files_path).lexically_normal();

    if (need_check && !fileOrSymlinkPathStartsWith(absolute_path, absolute_user_files_path))
    {
        processSQLiteError(fmt::format("SQLite database file path '{}' must be inside 'user_files' directory", path), throw_on_error);
        return "";
    }
    return absolute_path;
}

SQLitePtr openSQLiteDB(const String & path, ContextPtr context, bool throw_on_error, bool allow_create)
{
    // If run in Local mode, no need for path checking.
    bool need_check = context->getApplicationType() != Context::ApplicationType::LOCAL;

    auto user_files_path = context->getUserFilesPath();
    auto database_path = validateSQLiteDatabasePath(path, user_files_path, need_check, throw_on_error);

    /// For attach database there is no throw mode.
    if (database_path.empty())
        return nullptr;

    if (allow_create && !fs::exists(database_path))
        LOG_DEBUG(getLogger("SQLite"), "SQLite database path {} does not exist, will create an empty SQLite database", database_path);

    /// Do not implicitly create a new empty database when `allow_create` is off. This is used when reopening a
    /// persisted table whose file was unavailable at load time: fabricating an empty database would hide the
    /// still-missing file and, worse, mark the deferred generated-column reclassification as complete against a
    /// database that does not contain the table yet.
    const int open_flags = SQLITE_OPEN_READWRITE | (allow_create ? SQLITE_OPEN_CREATE : 0);

    sqlite3 * tmp_sqlite_db = nullptr;
    int status = 0;
    {
        std::lock_guard lock(init_sqlite_db_mutex);
        status = sqlite3_open_v2(database_path.c_str(), &tmp_sqlite_db, open_flags, nullptr);
    }

    if (status != SQLITE_OK)
    {
        /// `sqlite3_open_v2` allocates the connection handle even when it fails to open the database file
        /// (the only exception being an out-of-memory condition, in which case the handle is left null).
        /// The handle must be closed to avoid a memory leak, see https://www.sqlite.org/c3ref/open.html.
        /// `sqlite3_close` is a harmless no-op when passed a null pointer.
        sqlite3_close(tmp_sqlite_db);
        processSQLiteError(fmt::format("Cannot access sqlite database. Error status: {}. Message: {}",
                                       status, sqlite3_errstr(status)), throw_on_error);
        return nullptr;
    }

    /// Keep SQLite's default DQS behavior for stored schema SQL and user-provided `query` text. ClickHouse-generated
    /// identifiers use strict backquotes, so an unresolved generated projection still fails closed.
    return std::shared_ptr<sqlite3>(tmp_sqlite_db, sqlite3_close);
}

}

#endif

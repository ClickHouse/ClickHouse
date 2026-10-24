#include <Databases/SQLite/SQLiteUtils.h>

#if USE_SQLITE
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
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

static String validateSQLiteDatabasePath(const String & path, ContextPtr context, bool need_check, bool throw_on_error)
{
    const String user_files_path = context->getUserFilesPath();

    String absolute_path;

    if (fs::path(path).is_relative())
        absolute_path = fs::absolute(fs::path(user_files_path) / path).lexically_normal();
    else
        absolute_path = fs::absolute(path).lexically_normal();

    /// `Context::isUserFilesPath` applies the boundary check that corresponds to the
    /// configuration: the resolved-path check when `user_files_policy` is configured, and the
    /// legacy lexical check on a plain `user_files_path`, where an admin-managed symlink inside
    /// the directory is an established way to expose an external location. Both forms reject a
    /// sibling root that merely shares a textual prefix (e.g. allowed
    /// `/var/lib/clickhouse/user_files` vs input `/var/lib/clickhouse/user_files_evil/db.sqlite`).
    if (need_check && !context->isUserFilesPath(absolute_path))
    {
        processSQLiteError(fmt::format("SQLite database file path '{}' must be inside 'user_files' directory", path), throw_on_error);
        return "";
    }

    return absolute_path;
}

SQLitePtr openSQLiteDB(const String & path, ContextPtr context, bool throw_on_error, SQLiteOpenMode mode)
{
    // If run in Local mode, no need for path checking.
    bool need_check = context->getApplicationType() != Context::ApplicationType::LOCAL;

    /// `sqlite3_open` works only on the local filesystem. With `user_files_policy`
    /// configured on a non-local disk (for example `s3_plain`), the configured
    /// user-files root resolves to a local metadata directory, not the disk's
    /// actual backing store. Reject up front instead of silently creating or
    /// reading an unrelated local file, mirroring the explicit guards added in
    /// `InputFormatErrorsLogger`, `EmbeddedRocksDB`, and the `file` dictionary
    /// source.
    if (auto user_files_volume = context->getUserFilesVolume())
    {
        for (const auto & disk : user_files_volume->getDisks())
        {
            if (!isPlainLocalDisk(*disk))
            {
                processSQLiteError(fmt::format("SQLite is not supported "
                                               "with non-plain-local `user_files_policy` disks (disk `{}` is not a plain local filesystem disk)",
                                               disk->getName()),
                                   throw_on_error);
                return nullptr;
            }
        }
    }

    auto database_path = validateSQLiteDatabasePath(path, context, need_check, throw_on_error);

    /// For attach database there is no throw mode.
    if (database_path.empty())
        return nullptr;

    const bool allow_create = mode == SQLiteOpenMode::ReadWriteCreate;
    if (allow_create && !fs::exists(database_path))
        LOG_DEBUG(getLogger("SQLite"), "SQLite database path {} does not exist, will create an empty SQLite database", database_path);

    /// Do not implicitly create a new empty database unless the mode is `ReadWriteCreate`. This is used when reopening a
    /// persisted table whose file was unavailable at load time: fabricating an empty database would hide the
    /// still-missing file and, worse, mark the deferred generated-column reclassification as complete against a
    /// database that does not contain the table yet.
    int open_flags = 0;
    switch (mode)
    {
        case SQLiteOpenMode::ReadOnly:
            open_flags = SQLITE_OPEN_READONLY;
            break;
        case SQLiteOpenMode::ReadWrite:
            open_flags = SQLITE_OPEN_READWRITE;
            break;
        case SQLiteOpenMode::ReadWriteCreate:
            open_flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
            break;
    }

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

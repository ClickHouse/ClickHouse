#include <Databases/SQLite/SQLiteUtils.h>

#if USE_SQLITE
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
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
    String absolute_path;

    if (fs::path(path).is_relative())
        absolute_path = fs::absolute(fs::path(user_files_path) / path).lexically_normal();
    else
        absolute_path = fs::absolute(path).lexically_normal();

    if (need_check)
    {
        /// Use a path-aware boundary check that catches both sibling roots sharing a
        /// textual prefix (e.g. allowed `/var/lib/clickhouse/user_files` vs input
        /// `/var/lib/clickhouse/user_files_evil/db.sqlite`) and in-root symlinks
        /// escaping the allowed prefix (e.g. `<user_files>/escape -> /etc` accessed
        /// as `escape/db.sqlite`).
        ///
        /// `fs::weakly_canonical` resolves symlinks for the longest existing prefix
        /// of the path and lexically appends the rest. We invoke it explicitly so
        /// the symlink-resolution step is visible at the call site - the security
        /// property of this check should not depend on subtle library behavior of
        /// `fs::relative` (which `pathStartsWith` would otherwise call internally).
        std::error_code ec;
        const fs::path resolved = fs::weakly_canonical(fs::path(absolute_path), ec);
        if (ec)
        {
            processSQLiteError(fmt::format("Cannot resolve SQLite database path '{}': {}", path, ec.message()), throw_on_error);
            return "";
        }

        const fs::path resolved_root = fs::weakly_canonical(fs::path(user_files_path), ec);
        if (ec || !pathStartsWith(resolved, resolved_root))
        {
            processSQLiteError(fmt::format("SQLite database file path '{}' must be inside 'user_files' directory", path), throw_on_error);
            return "";
        }
    }
    return absolute_path;
}

SQLitePtr openSQLiteDB(const String & path, ContextPtr context, bool throw_on_error)
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
            if (disk->isRemote())
            {
                processSQLiteError(fmt::format("SQLite is not supported "
                                               "with non-local `user_files_policy` disks (disk `{}` is remote)",
                                               disk->getName()),
                                   throw_on_error);
                return nullptr;
            }
        }
    }

    auto database_path = validateSQLiteDatabasePath(path, context->getUserFilesPath(), need_check, throw_on_error);

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

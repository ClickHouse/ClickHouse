#include "SQLiteUtils.h"

#if USE_SQLITE
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}


String validateSQLiteDatabasePath(const String & path, const String & user_files_path)
{
    String canonical_user_files_path = fs::canonical(user_files_path);

    String canonical_path;
    std::error_code err;

    if (fs::path(path).is_relative())
        canonical_path = fs::canonical(fs::path(user_files_path) / path, err);
    else
        canonical_path = fs::canonical(path, err);

    if (err)
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "SQLite database path '{}' is invalid. Error: {}", path, err.message());

    if (!canonical_path.starts_with(canonical_user_files_path))
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                        "SQLite database file path '{}' must be inside 'user_files' directory", path);

    return canonical_path;
}


SQLitePtr openSQLiteDB(const String & database_path, ContextPtr context)
{
    auto validated_path = validateSQLiteDatabasePath(database_path, context->getUserFilesPath());

    sqlite3 * tmp_sqlite_db = nullptr;
    int status = sqlite3_open(validated_path.c_str(), &tmp_sqlite_db);

    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                        "Cannot access sqlite database. Error status: {}. Message: {}",
                        status, sqlite3_errstr(status));

    return std::shared_ptr<sqlite3>(tmp_sqlite_db, sqlite3_close);
}

}

#endif

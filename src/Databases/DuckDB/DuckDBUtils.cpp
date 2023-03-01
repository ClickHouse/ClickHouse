#include "DuckDBUtils.h"

#if USE_DUCKDB
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}


void processDuckDBError(const String & message, bool throw_on_error)
{
    if (throw_on_error)
        throw Exception::createDeprecated(message, ErrorCodes::PATH_ACCESS_DENIED);
    else
        LOG_ERROR(&Poco::Logger::get("DuckDBEngine"), fmt::runtime(message));
}

String validateDuckDBDatabasePath(const String & path, const String & user_files_path, bool need_check, bool throw_on_error)
{
    if (fs::path(path).is_relative())
        return fs::absolute(fs::path(user_files_path) / path).lexically_normal();

    String absolute_path = fs::absolute(path).lexically_normal();
    String absolute_user_files_path = fs::absolute(user_files_path).lexically_normal();

    if (need_check && !absolute_path.starts_with(absolute_user_files_path))
    {
        processDuckDBError(fmt::format("DuckDB database file path '{}' must be inside 'user_files' directory", path), throw_on_error);
        return "";
    }

    return absolute_path;
}

DuckDBPtr openDuckDB(const String & path, ContextPtr context, bool throw_on_error)
{
    // If run in Local mode, no need for path checking.
    bool need_check = context->getApplicationType() != Context::ApplicationType::LOCAL;

    auto user_files_path = context->getUserFilesPath();
    auto database_path = validateDuckDBDatabasePath(path, user_files_path, need_check, throw_on_error);

    /// For attach database there is no throw mode.
    if (database_path.empty())
        return nullptr;

    if (!fs::exists(database_path))
        LOG_DEBUG(&Poco::Logger::get("DuckDB"), "DuckDB database path {} does not exist, will create an empty DuckDB database", database_path);

    return std::make_shared<duckdb::DuckDB>(database_path.c_str());
}

String quoteIdentifierDuckDB(const String & identifier)
{
    WriteBufferFromOwnString wb;
    writeCSVString<'"'>(identifier, wb);
    return wb.str();
}

String quoteStringDuckDB(const String & identifier)
{
    WriteBufferFromOwnString wb;
    writeCSVString<'\''>(identifier, wb);
    return wb.str();
}

}

#endif

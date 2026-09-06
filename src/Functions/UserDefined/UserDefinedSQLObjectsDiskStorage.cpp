#include <Functions/UserDefined/UserDefinedSQLObjectsDiskStorage.h>

#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>

#include <Common/StringUtils.h>
#include <Common/atomicRename.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <Core/Settings.h>

#include <Disks/LocalDirectorySyncGuard.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>

#include <Poco/DirectoryIterator.h>

#include <filesystem>
#include <optional>

namespace fs = std::filesystem;


namespace DB
{
namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
}


namespace
{
    /// Converts a path to an absolute path and append it with a separator.
    String makeDirectoryPathCanonical(const String & directory_path)
    {
        auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
        if (canonical_directory_path.has_filename())
            canonical_directory_path += std::filesystem::path::preferred_separator;
        return canonical_directory_path;
    }
}

UserDefinedSQLObjectsDiskStorage::UserDefinedSQLObjectsDiskStorage(const ContextPtr & global_context_, const String & dir_path_)
    : UserDefinedSQLObjectsStorageBase(global_context_)
    , dir_path{makeDirectoryPathCanonical(dir_path_)}
    , log{getLogger("UserDefinedSQLObjectsLoaderFromDisk")}
{
}


ASTPtr UserDefinedSQLObjectsDiskStorage::tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    return tryLoadObject(object_type, object_name, getFilePath(object_type, object_name), /* check_file_exists= */ true);
}


ASTPtr UserDefinedSQLObjectsDiskStorage::tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name, const String & path, bool check_file_exists)
{
    LOG_DEBUG(log, "Loading user defined object {} from file {}", backQuote(object_name), path);

    try
    {
        if (check_file_exists && !fs::exists(path))
            return nullptr;

        /// There is .sql file with user defined object creation statement.
        ReadBufferFromFile in(path);

        String object_create_query;
        readStringUntilEOF(object_create_query, in);

        switch (object_type)
        {
            case UserDefinedSQLObjectType::Function:
            {
                auto context = getContext();
                ParserCreateFunctionQuery parser;
                ASTPtr ast = parseQuery(
                    parser,
                    object_create_query.data(),
                    object_create_query.data() + object_create_query.size(),
                    "",
                    0,
                    context->getSettingsRef()[Setting::max_parser_depth],
                    context->getSettingsRef()[Setting::max_parser_backtracks]);
                return ast;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while loading user defined SQL object {} from path {}", backQuote(object_name), path));
        return nullptr; /// Failed to load this sql object, will ignore it
    }
}


void UserDefinedSQLObjectsDiskStorage::loadObjects()
{
    if (!objects_loaded)
        loadObjectsImpl();
}


void UserDefinedSQLObjectsDiskStorage::reloadObjects()
{
    loadObjectsImpl();
}


void UserDefinedSQLObjectsDiskStorage::loadObjectsImpl()
{
    LOG_INFO(log, "Loading user defined objects from {}", dir_path);

    if (!std::filesystem::exists(dir_path))
    {
        LOG_DEBUG(log, "The directory for user defined objects ({}) does not exist: nothing to load", dir_path);
        return;
    }

    VectorWithMemoryTracking<std::pair<String, ASTPtr>> function_names_and_queries;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isDirectory())
            continue;

        const String & file_name = it.name();
        if (!startsWith(file_name, "function_") || !endsWith(file_name, ".sql"))
            continue;

        size_t prefix_length = strlen("function_");
        size_t suffix_length = strlen(".sql");
        String function_name = unescapeForFileName(file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length));

        if (function_name.empty())
            continue;

        ASTPtr ast = tryLoadObject(UserDefinedSQLObjectType::Function, function_name, dir_path + it.name(), /* check_file_exists= */ false);
        if (ast)
            function_names_and_queries.emplace_back(function_name, ast);
    }

    setAllObjects(function_names_and_queries);
    objects_loaded = true;

    LOG_DEBUG(log, "User defined objects loaded");
}


void UserDefinedSQLObjectsDiskStorage::reloadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    auto ast = tryLoadObject(object_type, object_name);
    if (ast)
        setObject(object_name, *ast);
    else
        removeObject(object_name);
}


void UserDefinedSQLObjectsDiskStorage::createDirectory(bool fsync)
{
    std::error_code create_dir_error_code;
    createDirectoriesAndSync(dir_path, fsync, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Couldn't create directory {} reason: '{}'",
                        dir_path, create_dir_error_code.message());
}


bool UserDefinedSQLObjectsDiskStorage::storeObjectImpl(
    const ContextPtr & /*current_context*/,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    ASTPtr create_object_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    const bool fsync = settings[Setting::fsync_metadata];
    createDirectory(fsync);
    String file_path = getFilePath(object_type, object_name);
    LOG_DEBUG(log, "Storing user-defined object {} to file {}", backQuote(object_name), file_path);

    if (fs::exists(file_path))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "File {} for user-defined function '{}' already exists", file_path, object_name);

        if (!replace_if_exists)
            return false;
    }

    WriteBufferFromOwnString create_statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false);
    create_object_query->format(create_statement_buf, format_settings);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    String temp_file_path = file_path + ".tmp";

    try
    {
        WriteBufferFromFile out(temp_file_path, create_statement.size());
        writeString(create_statement, out);
        out.next();
        if (fsync)
            out.sync();
        out.close();

        /// Opened before the rename, so a directory that cannot be opened does not leave the
        /// rename committed, and synced after it, so the object is not reported as created
        /// until its new directory entry is durable.
        std::optional<CheckedDirectorySync> dir_sync;
        if (fsync)
            dir_sync.emplace(fs::path(file_path).parent_path().string());

        if (replace_if_exists)
            fs::rename(temp_file_path, file_path);
        else
            renameNoReplace(temp_file_path, file_path);

        if (dir_sync)
            dir_sync->sync();
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }

    LOG_TRACE(log, "Object {} stored", backQuote(object_name));
    return true;
}


bool UserDefinedSQLObjectsDiskStorage::removeObjectImpl(
    const ContextPtr & current_context,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    bool throw_if_not_exists)
{
    String file_path = getFilePath(object_type, object_name);
    LOG_DEBUG(log, "Removing user defined object {} stored in file {}", backQuote(object_name), file_path);

    /// Opened before the unlink, so a directory that cannot be opened does not leave the object
    /// removed, and synced after it, so a DROP is not reported as done while the removal could
    /// still revert and resurrect the object.
    std::optional<CheckedDirectorySync> dir_sync;
    if (current_context->getSettingsRef()[Setting::fsync_metadata])
        dir_sync.emplace(fs::path(file_path).parent_path().string());

    bool existed = fs::remove(file_path);

    if (!existed)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", object_name);
        return false;
    }

    if (dir_sync)
        dir_sync->sync();

    LOG_TRACE(log, "Object {} removed", backQuote(object_name));
    return true;
}


String UserDefinedSQLObjectsDiskStorage::getFilePath(UserDefinedSQLObjectType object_type, const String & object_name) const
{
    String file_path;
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
        {
            file_path = dir_path + "function_" + escapeForFileName(object_name) + ".sql";
            break;
        }
    }
    return file_path;
}

}

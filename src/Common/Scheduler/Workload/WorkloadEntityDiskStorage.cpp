#include <Common/Scheduler/Workload/WorkloadEntityDiskStorage.h>

#include <Common/StringUtils.h>
#include <Common/atomicRename.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <Core/Settings.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>

#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int WORKLOAD_ENTITY_ALREADY_EXISTS;
    extern const int UNKNOWN_WORKLOAD_ENTITY;
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

WorkloadEntityDiskStorage::WorkloadEntityDiskStorage(const ContextPtr & global_context_, const String & dir_path_)
    : WorkloadEntityStorageBase(global_context_)
    , dir_path{makeDirectoryPathCanonical(dir_path_)}
    , log{getLogger("WorkloadEntityDiskStorage")}
{
}


ASTPtr WorkloadEntityDiskStorage::tryLoadEntity(WorkloadEntityType entity_type, const String & entity_name)
{
    return tryLoadEntity(entity_type, entity_name, getFilePath(entity_type, entity_name), /* check_file_exists= */ true);
}


ASTPtr WorkloadEntityDiskStorage::tryLoadEntity(WorkloadEntityType entity_type, const String & entity_name, const String & path, bool check_file_exists)
{
    LOG_DEBUG(log, "Loading workload entity {} from file {}", backQuote(entity_name), path);

    try
    {
        if (check_file_exists && !fs::exists(path))
            return nullptr;

        /// There is .sql file with workload entity creation statement.
        ReadBufferFromFile in(path);

        String entity_create_query;
        readStringUntilEOF(entity_create_query, in);

        switch (entity_type)
        {
            case WorkloadEntityType::Workload:
            {
                ParserCreateWorkloadQuery parser;
                ASTPtr ast = parseQuery(
                    parser,
                    entity_create_query.data(),
                    entity_create_query.data() + entity_create_query.size(),
                    "",
                    0,
                    global_context->getSettingsRef().max_parser_depth,
                    global_context->getSettingsRef().max_parser_backtracks);
                return ast;
            }
            case WorkloadEntityType::Resource:
            {
                ParserCreateResourceQuery parser;
                ASTPtr ast = parseQuery(
                    parser,
                    entity_create_query.data(),
                    entity_create_query.data() + entity_create_query.size(),
                    "",
                    0,
                    global_context->getSettingsRef().max_parser_depth,
                    global_context->getSettingsRef().max_parser_backtracks);
                return ast;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while loading workload entity {} from path {}", backQuote(entity_name), path));
        return nullptr; /// Failed to load this entity, will ignore it
    }
}


void WorkloadEntityDiskStorage::loadEntities()
{
    if (!entities_loaded)
        loadEntitiesImpl();
}


void WorkloadEntityDiskStorage::reloadEntities()
{
    loadEntitiesImpl();
}


void WorkloadEntityDiskStorage::loadEntitiesImpl()
{
    LOG_INFO(log, "Loading workload entities from {}", dir_path);

    if (!std::filesystem::exists(dir_path))
    {
        LOG_DEBUG(log, "The directory for workload entities ({}) does not exist: nothing to load", dir_path);
        return;
    }

    std::vector<std::pair<String, ASTPtr>> entities_name_and_queries;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isDirectory())
            continue;

        const String & file_name = it.name();

        if (startsWith(file_name, "workload_") && endsWith(file_name, ".sql"))
        {
            size_t prefix_length = strlen("workload_");
            size_t suffix_length = strlen(".sql");
            String name = unescapeForFileName(file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length));

            if (name.empty())
                continue;

            ASTPtr ast = tryLoadEntity(WorkloadEntityType::Workload, name, dir_path + it.name(), /* check_file_exists= */ false);
            if (ast)
                entities_name_and_queries.emplace_back(name, ast);
        }

        if (startsWith(file_name, "resource_") && endsWith(file_name, ".sql"))
        {
            size_t prefix_length = strlen("resource_");
            size_t suffix_length = strlen(".sql");
            String name = unescapeForFileName(file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length));

            if (name.empty())
                continue;

            ASTPtr ast = tryLoadEntity(WorkloadEntityType::Resource, name, dir_path + it.name(), /* check_file_exists= */ false);
            if (ast)
                entities_name_and_queries.emplace_back(name, ast);
        }
    }

    setAllEntities(entities_name_and_queries);
    entities_loaded = true;

    LOG_DEBUG(log, "Workload entities loaded");
}


void WorkloadEntityDiskStorage::createDirectory()
{
    std::error_code create_dir_error_code;
    fs::create_directories(dir_path, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Couldn't create directory {} reason: '{}'",
                        dir_path, create_dir_error_code.message());
}


bool WorkloadEntityDiskStorage::storeEntityImpl(
    const ContextPtr & /*current_context*/,
    WorkloadEntityType entity_type,
    const String & entity_name,
    ASTPtr create_entity_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    createDirectory();
    String file_path = getFilePath(entity_type, entity_name);
    LOG_DEBUG(log, "Storing workload entity {} to file {}", backQuote(entity_name), file_path);

    if (fs::exists(file_path))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::WORKLOAD_ENTITY_ALREADY_EXISTS, "Workload entity '{}' already exists", entity_name);
        else if (!replace_if_exists)
            return false;
    }

    WriteBufferFromOwnString create_statement_buf;
    formatAST(*create_entity_query, create_statement_buf, false);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    String temp_file_path = file_path + ".tmp";

    try
    {
        WriteBufferFromFile out(temp_file_path, create_statement.size());
        writeString(create_statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();

        if (replace_if_exists)
            fs::rename(temp_file_path, file_path);
        else
            renameNoReplace(temp_file_path, file_path);
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }

    LOG_TRACE(log, "Entity {} stored", backQuote(entity_name));
    return true;
}


bool WorkloadEntityDiskStorage::removeEntityImpl(
    const ContextPtr & /*current_context*/,
    WorkloadEntityType entity_type,
    const String & entity_name,
    bool throw_if_not_exists)
{
    String file_path = getFilePath(entity_type, entity_name);
    LOG_DEBUG(log, "Removing workload entity {} stored in file {}", backQuote(entity_name), file_path);

    bool existed = fs::remove(file_path);

    if (!existed)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_WORKLOAD_ENTITY, "Workload entity '{}' doesn't exist", entity_name);
        else
            return false;
    }

    LOG_TRACE(log, "Entity {} removed", backQuote(entity_name));
    return true;
}


String WorkloadEntityDiskStorage::getFilePath(WorkloadEntityType entity_type, const String & entity_name) const
{
    String file_path;
    switch (entity_type)
    {
        case WorkloadEntityType::Workload:
        {
            file_path = dir_path + "workload_" + escapeForFileName(entity_name) + ".sql";
            break;
        }
        case WorkloadEntityType::Resource:
        {
            file_path = dir_path + "resource_" + escapeForFileName(entity_name) + ".sql";
            break;
        }
    }
    return file_path;
}

}

#include <Formats/FormatSchemaLoader.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>
#include <Interpreters/Context.h>
#include <IO/ConnectionTimeouts.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}


FormatSchemaLoader::FormatSchemaLoader(const Context & context_) : context(context_) {}
FormatSchemaLoader::~FormatSchemaLoader() = default;


void FormatSchemaLoader::setDirectory(const String & directory_, bool allow_paths_outside_, bool allow_absolute_paths_)
{
    Poco::Path new_directory;
    if (!directory_.empty())
    {
        new_directory = Poco::Path(directory_).makeAbsolute().makeDirectory();
        Poco::File(new_directory).createDirectories();
    }
    directory = new_directory;
    allow_paths_outside = allow_paths_outside_;
    allow_absolute_paths = allow_absolute_paths_;
}


void FormatSchemaLoader::setConnectionParameters(const String & host_, UInt16 port_, const String & user_, const String & password_)
{
    host = host_;
    port = port_;
    user = user_;
    password = password_;
}


String FormatSchemaLoader::getSchema(const String & path)
{
    String schema;
    if (!tryGetSchema(path, schema))
        throw Exception("Format schema '" + path + "' not found", ErrorCodes::BAD_ARGUMENTS);
    return schema;
}


bool FormatSchemaLoader::tryGetSchema(const String & path, String & schema)
{
    {
        std::lock_guard lock{cache_mutex};
        auto it = schemas_by_paths_cache.find(path);
        if (it != schemas_by_paths_cache.end())
        {
            schema = it->second;
            return true;
        }
    }

    if (!tryGetSchemaFromDirectory(path, schema) && !tryGetSchemaFromConnection(path, schema))
        return false;

    {
        std::lock_guard lock{cache_mutex};
        auto it = schemas_by_paths_cache.try_emplace(path, schema).first;
        schema = it->second;
        return true;
    }
}


Strings FormatSchemaLoader::getAllPaths()
{
    Strings all_paths;
    getAllPathsFromDirectory(all_paths);
    getAllPathsFromConnection(all_paths);
    return all_paths;
}


bool FormatSchemaLoader::tryGetSchemaFromDirectory(const String & path, String & schema)
{
    if (!directory)
        return false;
    Poco::Path use_path(path);
    if (use_path.isAbsolute())
    {
        if (!allow_absolute_paths)
            throw Exception("Path '" + path + "' to format_schema should be relative", ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        if (use_path.depth() >= 1 && use_path.directory(0) == ".." && !allow_paths_outside)
            throw Exception("Path '" + path + "' to format_schema should be inside the format_schema_path directory specified in the setting", ErrorCodes::BAD_ARGUMENTS);
        use_path = Poco::Path(*directory).resolve(use_path);
    }

    std::cout << "FormatSchemaLoader: Reading file " << use_path.toString() << std::endl;

    try
    {
        ReadBufferFromFile buf(use_path.toString());
        readStringUntilEOF(schema, buf);
        return true;
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            return false;
        e.rethrow();
        __builtin_unreachable();
    }
}


bool FormatSchemaLoader::tryGetSchemaFromConnection(const String & path, String & schema)
{
    if (host.empty())
        return false;

    WriteBufferFromOwnString escaped_path;
    writeQuotedString(path, escaped_path);
    String query = "SELECT schema FROM system.format_schemas WHERE path=" + escaped_path.str();
    Strings result;
    sendQueryToConnection(query, result);
    if (result.empty())
        return false;

    schema = result.front();
    return true;
}


namespace
{
    void findAllPathsInDirectoryTree(const Poco::Path & directory, const Poco::Path & subdirectory, std::vector<String> & all_paths)
    {
        Poco::DirectoryIterator it(Poco::Path(directory).append(subdirectory));
        Poco::DirectoryIterator end;
        for (; it != end; ++it)
        {
            if (it->isFile())
                all_paths.emplace_back(Poco::Path{subdirectory}.setFileName(it.name()).toString());
            else if (it->isDirectory())
                findAllPathsInDirectoryTree(directory, Poco::Path{subdirectory}.pushDirectory(it.name()), all_paths);
        }
    }
}


void FormatSchemaLoader::getAllPathsFromDirectory(Strings & all_paths)
{
    if (!directory)
        return;

    std::cout << "FormatSchemaLoader: Searching files in directory " << directory->toString() << std::endl;
    findAllPathsInDirectoryTree(*directory, {}, all_paths);
}


void FormatSchemaLoader::getAllPathsFromConnection(Strings & all_paths)
{
    if (host.empty())
        return;

    String query = "SELECT path FROM system.format_schemas";
    sendQueryToConnection(query, all_paths);
}


size_t FormatSchemaLoader::sendQueryToConnection(const String & query, Strings & result)
{
    Block block;
    {
        std::lock_guard lock{connection_mutex};
        if (!connection)
            connection = std::make_unique<Connection>(
                host, port, "default", user, password, ConnectionTimeouts::getTCPTimeoutsWithoutFailover(context.getSettingsRef()));

        Block header;
        RemoteBlockInputStream in(*connection, query, header, context);
        block = in.read();
    }

    if (!block)
        return 0;

    if (block.columns() != 1)
        throw Exception("Wrong number of columns received for query to read format schemas", ErrorCodes::LOGICAL_ERROR);

    const ColumnString & column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);
    size_t num_rows = block.rows();
    result.reserve(result.size() + num_rows);
    for (size_t i = 0; i != num_rows; ++i)
        result.emplace_back(column.getDataAt(i).toString());
    return num_rows;
}

}

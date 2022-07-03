#include "UserDefinedSQLObjectsLoader.h"

#include <filesystem>

#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/StringUtils/StringUtils.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateFunctionQuery.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int OBJECT_ALREADY_STORED_ON_DISK;
    extern const int OBJECT_WAS_NOT_STORED_ON_DISK;
}

UserDefinedSQLObjectsLoader & UserDefinedSQLObjectsLoader::instance()
{
    static UserDefinedSQLObjectsLoader ret;
    return ret;
}

UserDefinedSQLObjectsLoader::UserDefinedSQLObjectsLoader()
    : log(&Poco::Logger::get("UserDefinedSQLObjectsLoader"))
{}

void UserDefinedSQLObjectsLoader::loadUserDefinedObject(ContextPtr context, UserDefinedSQLObjectType object_type, const std::string_view & name, const String & path)
{
    auto name_ref = StringRef(name.data(), name.size());
    LOG_DEBUG(log, "Loading user defined object {} from file {}", backQuote(name_ref), path);

    /// There is .sql file with user defined object creation statement.
    ReadBufferFromFile in(path);

    String object_create_query;
    readStringUntilEOF(object_create_query, in);

    try
    {
        switch (object_type)
        {
            case UserDefinedSQLObjectType::Function:
            {
                ParserCreateFunctionQuery parser;
                ASTPtr ast = parseQuery(
                    parser,
                    object_create_query.data(),
                    object_create_query.data() + object_create_query.size(),
                    "in file " + path,
                    0,
                    context->getSettingsRef().max_parser_depth);

                InterpreterCreateFunctionQuery interpreter(ast, context, false /*persist_function*/);
                interpreter.execute();
            }
        }
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while loading user defined objects {} from path {}", backQuote(name_ref), path));
        throw;
    }
}

void UserDefinedSQLObjectsLoader::loadObjects(ContextPtr context)
{
    if (unlikely(!enable_persistence))
        return;

    LOG_DEBUG(log, "loading user defined objects");

    String dir_path = context->getUserDefinedPath();
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isLink())
            continue;

        const auto & file_name = it.name();

        /// For '.svn', '.gitignore' directory and similar.
        if (file_name.at(0) == '.')
            continue;

        if (!it->isDirectory() && endsWith(file_name, ".sql"))
        {
            std::string_view object_name = file_name;
            object_name.remove_suffix(strlen(".sql"));
            object_name.remove_prefix(strlen("function_"));
            loadUserDefinedObject(context, UserDefinedSQLObjectType::Function, object_name, dir_path + it.name());
        }
    }
}

void UserDefinedSQLObjectsLoader::storeObject(ContextPtr context, UserDefinedSQLObjectType object_type, const String & object_name, const IAST & ast, bool replace)
{
    if (unlikely(!enable_persistence))
        return;

    String dir_path = context->getUserDefinedPath();
    String file_path;

    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
        {
            file_path = dir_path + "function_" + escapeForFileName(object_name) + ".sql";
        }
    }

    if (!replace && std::filesystem::exists(file_path))
        throw Exception(ErrorCodes::OBJECT_ALREADY_STORED_ON_DISK, "User defined object {} already stored on disk", backQuote(file_path));

    LOG_DEBUG(log, "Storing object {} to file {}", backQuote(object_name), file_path);

    WriteBufferFromOwnString create_statement_buf;
    formatAST(ast, create_statement_buf, false);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    WriteBufferFromFile out(file_path, create_statement.size());
    writeString(create_statement, out);
    out.next();
    if (context->getSettingsRef().fsync_metadata)
        out.sync();
    out.close();

    LOG_DEBUG(log, "Stored object {}", backQuote(object_name));
}

void UserDefinedSQLObjectsLoader::removeObject(ContextPtr context, UserDefinedSQLObjectType object_type, const String & object_name)
{
    if (unlikely(!enable_persistence))
        return;

    String dir_path = context->getUserDefinedPath();
    LOG_DEBUG(log, "Removing file for user defined object {} from {}", backQuote(object_name), dir_path);

    std::filesystem::path file_path;

    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
        {
            file_path = dir_path + "function_" + escapeForFileName(object_name) + ".sql";
        }
    }

    if (!std::filesystem::exists(file_path))
        throw Exception(ErrorCodes::OBJECT_WAS_NOT_STORED_ON_DISK, "User defined object {} was not stored on disk", backQuote(file_path.string()));

    std::filesystem::remove(file_path);
}

void UserDefinedSQLObjectsLoader::enable(bool enable_persistence_)
{
    enable_persistence = enable_persistence_;
}

}

#include "UserDefinedObjectsLoader.h"

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
#include <Interpreters/InterpreterCreateDataTypeQuery.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/ParserCreateDataTypeQuery.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int OBJECT_ALREADY_STORED_ON_DISK;
    extern const int OBJECT_WAS_NOT_STORED_ON_DISK;
}

String userDefinedObjecTypeToString(UserDefinedObjectType object_type)
{
    switch (object_type)
    {
        case UserDefinedObjectType::Function:
        {
            return "function";
        }
        case UserDefinedObjectType::DataType:
        {
            return "data_type";
        }
    }
    return "";
}

UserDefinedObjectsLoader & UserDefinedObjectsLoader::instance()
{
    static UserDefinedObjectsLoader ret;
    return ret;
}

UserDefinedObjectsLoader::UserDefinedObjectsLoader()
    : log(&Poco::Logger::get("UserDefinedObjectsLoader"))
{}

String UserDefinedObjectsLoader::makeFilePath(ContextPtr context, UserDefinedObjectType object_type, const String & name)
{
    String dir_path = context->getPath() + "user_defined/";
    String object_type_str = userDefinedObjecTypeToString(object_type);
    return dir_path + object_type_str + "_" + escapeForFileName(name) + ".sql";
}

void UserDefinedObjectsLoader::loadUserDefinedObject(ContextPtr context, UserDefinedObjectType object_type, const std::string_view & name, const String & path)
{
    auto name_ref = StringRef(name.data(), name.size());
    LOG_DEBUG(log, "Loading user defined object {} from file {}", backQuote(name_ref), path);

    /// There is .sql file with user defined object creation statement.
    ReadBufferFromFile in(path);

    String object_create_query;
    readStringUntilEOF(object_create_query, in);

    auto parse = [&context, &object_create_query, &path](DB::IParser & parser)
    {
        return parseQuery(
            parser,
            object_create_query.data(),
            object_create_query.data() + object_create_query.size(),
            "in file " + path,
            0,
            context->getSettingsRef().max_parser_depth);
    };

    try
    {
        switch (object_type)
        {
            case UserDefinedObjectType::Function:
            {
                ParserCreateFunctionQuery function_parser;
                ASTPtr ast = parse(function_parser);
                InterpreterCreateFunctionQuery interpreter(ast, context, true /*is internal*/);
                interpreter.execute();
                break;
            }
            case UserDefinedObjectType::DataType:
            {
                ParserCreateDataTypeQuery data_type_parser;
                ASTPtr ast = parse(data_type_parser);
                InterpreterCreateDataTypeQuery interpreter(ast, context, true /*is internal*/);
                interpreter.execute();
                break;
            }
        }
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while loading user defined objects {} from path {}", backQuote(name_ref), path));
        throw;
    }
}

void UserDefinedObjectsLoader::loadObjects(ContextPtr context)
{
    LOG_DEBUG(log, "loading user defined objects");

    String dir_path = context->getPath() + "user_defined/";
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
            if (object_name.starts_with("function_"))
            {
                object_name.remove_prefix(strlen("function_"));
                loadUserDefinedObject(context, UserDefinedObjectType::Function, object_name, dir_path + it.name());
            }
            else if (object_name.starts_with("data_type_"))
            {
                object_name.remove_prefix(strlen("data_type_"));
                loadUserDefinedObject(context, UserDefinedObjectType::DataType, object_name, dir_path + it.name());
            }
        }
    }
}

void UserDefinedObjectsLoader::storeObject(ContextPtr context, UserDefinedObjectType object_type, const String & object_name, const IAST & ast)
{
    String file_path = makeFilePath(context, object_type, object_name);

    if (std::filesystem::exists(file_path))
        throw Exception(ErrorCodes::OBJECT_ALREADY_STORED_ON_DISK, "User defined object {} already stored on disk", backQuote(file_path));

    LOG_DEBUG(log, "Storing object {} to file {}", backQuote(object_name), file_path);

    WriteBufferFromOwnString create_statement_buf;
    formatAST(ast, create_statement_buf, false);
    writeChar('\n', create_statement_buf);

    String create_statement = create_statement_buf.str();
    WriteBufferFromFile out(file_path, create_statement.size(), O_WRONLY | O_CREAT | O_EXCL);
    writeString(create_statement, out);
    out.next();
    if (context->getSettingsRef().fsync_metadata)
        out.sync();
    out.close();

    LOG_DEBUG(log, "Stored object {}", backQuote(object_name));
}

void UserDefinedObjectsLoader::removeObject(ContextPtr context, UserDefinedObjectType object_type, const String & object_name)
{
    String file_path = makeFilePath(context, object_type, object_name);

    if (!std::filesystem::exists(file_path))
        throw Exception(ErrorCodes::OBJECT_WAS_NOT_STORED_ON_DISK, "User defined object {} was not stored on disk", backQuote(file_path));

    LOG_DEBUG(log, "Removing user defined object {} in file {}", backQuote(object_name), file_path);

    std::filesystem::remove(file_path);

    LOG_DEBUG(log, "Removed object {}", backQuote(object_name));
}

}

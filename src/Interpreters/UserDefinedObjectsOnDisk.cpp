#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/StringUtils/StringUtils.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>
#include <Interpreters/UserDefinedObjectsOnDisk.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateFunctionQuery.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>

#include <re2/re2.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_STORED_ON_DISK;
    extern const int FUNCTION_WAS_NOT_STORED_ON_DISK;
}

UserDefinedObjectsOnDisk & UserDefinedObjectsOnDisk::instance()
{
    static UserDefinedObjectsOnDisk ret;
    return ret;
}


void UserDefinedObjectsOnDisk::executeCreateFunctionQuery(
    const String & query,
    ContextMutablePtr context,
    const String & file_name)
{
    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(
        parser, query.data(), query.data() + query.size(), "in file " + file_name, 0, context->getSettingsRef().max_parser_depth);

    InterpreterCreateFunctionQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.execute();
}

void UserDefinedObjectsOnDisk::loadUserDefinedObject(ContextMutablePtr context, const String & name, const String & path)
{
    Poco::Logger * log = &Poco::Logger::get("LoadUserDefinedObject");
    String object_create_query;

    LOG_DEBUG(log, "Loading function {} from file {}", backQuote(name), path);
    if (Poco::File(path).exists())
    {
        /// There is .sql file with user defined object creation statement.
        ReadBufferFromFile in(path, 1024);
        readStringUntilEOF(object_create_query, in);
    }
    try
    {
        executeCreateFunctionQuery(object_create_query, context, path);
        LOG_DEBUG(log, "Loaded function {}", backQuote(name));
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while loading user defined function {} from path {}", backQuote(name), path));
        throw;
    }
}

void UserDefinedObjectsOnDisk::loadUserDefinedObjects(ContextMutablePtr context)
{
    String dir_path = context->getPath() + "user_defined/";
    std::vector<std::pair<int, String>> user_defined_objects_with_priority;
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isLink())
            continue;

        if (!it->isDirectory() && endsWith(it.name(), ".sql"))
        {
            int priority = std::stoi(it.name().substr(0, it.name().find('_')));
            user_defined_objects_with_priority.emplace_back(priority, it.name());

            continue;
        }

        /// For '.svn', '.gitignore' directory and similar.
        if (it.name().at(0) == '.')
            continue;
    }
    std::sort(user_defined_objects_with_priority.begin(), user_defined_objects_with_priority.end());

    for (const auto & [priority, file_name] : user_defined_objects_with_priority)
    {
        int name_start_index = file_name.find('_') + 1;
        String name = file_name.substr(name_start_index, file_name.size() - 4 - name_start_index);
        loadUserDefinedObject(context, name, dir_path + file_name);
    }
    if (user_defined_objects_with_priority.empty())
        user_defined_objects_count.store(0);
    else
        user_defined_objects_count.store(user_defined_objects_with_priority.back().first);
}

void UserDefinedObjectsOnDisk::storeUserDefinedFunction(ContextPtr context, const ASTCreateFunctionQuery & ast)
{
    Poco::Logger * log = &Poco::Logger::get("StoreUserDefinedFunction");

    String dir_path = context->getPath() + "user_defined/";
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        re2::StringPiece input(it.name());
        re2::RE2 re("[0-9]+_" + escapeForFileName(ast.function_name) + "\\.sql");

        if (re2::RE2::FullMatch(input, re))
        {
            throw Exception("User defined function " + backQuote(it.name()) + " already stored on disk", ErrorCodes::FUNCTION_ALREADY_STORED_ON_DISK);
        }
    }

    int object_priority = ++user_defined_objects_count;
    String new_file_path = dir_path + toString(object_priority) + "_" + escapeForFileName(ast.function_name) + ".sql";
    LOG_DEBUG(log, "Storing function {} to file {}", backQuote(ast.function_name), new_file_path);

    WriteBufferFromOwnString create_statement_buf;
    formatAST(ast, create_statement_buf, false);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    WriteBufferFromFile out(new_file_path, create_statement.size(), O_WRONLY | O_CREAT | O_EXCL);
    writeString(create_statement, out);
    out.next();
    if (context->getSettingsRef().fsync_metadata)
        out.sync();
    out.close();
    LOG_DEBUG(log, "Stored function {}", backQuote(ast.function_name));
}

void UserDefinedObjectsOnDisk::removeUserDefinedFunction(ContextPtr context, const String & name)
{
    Poco::Logger * log = &Poco::Logger::get("RemoveUserDefinedFunction");

    String dir_path = context->getPath() + "user_defined/";

    LOG_DEBUG(log, "Removing file for function {} from {}", backQuote(name), dir_path);

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        String file_name = it.name();
        re2::StringPiece input(file_name);
        re2::RE2 re("[0-9]+_" + escapeForFileName(name) + "\\.sql");

        if (re2::RE2::FullMatch(input, re))
        {
            it->remove();
            LOG_DEBUG(log, "Removed file {}", dir_path + file_name);
            return;
        }
    }

    throw Exception("Stored file for user defined function " + backQuote(name) + " was not found", ErrorCodes::FUNCTION_WAS_NOT_STORED_ON_DISK);
}

}

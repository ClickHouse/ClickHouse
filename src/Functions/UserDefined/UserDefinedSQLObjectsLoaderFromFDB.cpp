// #include "Functions/UserDefined/UserDefinedSQLObjectsLoaderFromFDB.h"

// #include "Functions/UserDefined/UserDefinedSQLFunctionFactory.h"
// #include "Functions/UserDefined/UserDefinedSQLObjectType.h"

// #include <Common/StringUtils/StringUtils.h>
// #include <Common/atomicRename.h>
// #include <Common/escapeForFileName.h>
// #include <Common/logger_useful.h>
// #include <Common/quoteString.h>
// #include <Common/FoundationDB/FoundationDBCommon.h>
// #include <Common/FoundationDB/MetadataStoreFoundationDB.h>

// #include <IO/ReadBufferFromFile.h>
// #include <IO/ReadHelpers.h>
// #include <IO/WriteBufferFromFile.h>
// #include <IO/WriteHelpers.h>

// #include <Interpreters/Context.h>

// #include <Parsers/parseQuery.h>
// #include <Parsers/formatAST.h>
// #include <Parsers/ParserCreateFunctionQuery.h>

// #include <Poco/DirectoryIterator.h>
// #include <Poco/Logger.h>

// #include <filesystem>

// namespace fs = std::filesystem;


// namespace DB
// {

// namespace ErrorCodes
// {
//     extern const int DIRECTORY_DOESNT_EXIST;
//     extern const int FUNCTION_ALREADY_EXISTS;
//     extern const int UNKNOWN_FUNCTION;
// }


// namespace
// {
//     /// Converts a path to an absolute path and append it with a separator.
//     String makeDirectoryPathCanonical(const String & directory_path)
//     {
//         auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
//         if (canonical_directory_path.has_filename())
//             canonical_directory_path += std::filesystem::path::preferred_separator;
//         return canonical_directory_path;
//     }
// }

// UserDefinedSQLObjectsLoaderFromFDB::UserDefinedSQLObjectsLoaderFromFDB(const ContextPtr & global_context_, const String & dir_path_)
//     : global_context(global_context_)
//     , dir_path{makeDirectoryPathCanonical(dir_path_)}
//     , log{&Poco::Logger::get("UserDefinedSQLObjectsLoaderFromFDB")}
// {
//     static UserDefinedSQLObjectsLoaderFromFDB ret;
//     ret.meta_store = global_context->getMetadataStoreFoundationDB();
//     createDirectory();
// }


// ASTPtr UserDefinedSQLObjectsLoaderFromFDB::tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name)
// {
//     return tryLoadObject(object_type, object_name, getFilePath(object_type, object_name), /* check_file_exists= */ true);
// }


// ASTPtr UserDefinedSQLObjectsLoaderFromFDB::tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name, const String & path, bool check_file_exists)
// {
//     LOG_DEBUG(log, "Loading user defined object {} from file {}", backQuote(object_name), path);

//     try
//     {
//         if (check_file_exists && !fs::exists(path))
//             return nullptr;

//         /// There is .sql file with user defined object creation statement.
//         ReadBufferFromFile in(path);

//         String object_create_query;
//         readStringUntilEOF(object_create_query, in);

//         switch (object_type)
//         {
//             case UserDefinedSQLObjectType::Function:
//             {
//                 ParserCreateFunctionQuery parser;
//                 ASTPtr ast = parseQuery(
//                     parser,
//                     object_create_query.data(),
//                     object_create_query.data() + object_create_query.size(),
//                     "",
//                     0,
//                     global_context->getSettingsRef().max_parser_depth);
//                 UserDefinedSQLFunctionFactory::checkCanBeRegistered(global_context, object_name, *ast);
//                 return ast;
//             }
//         }
//     }
//     catch (...)
//     {
//         tryLogCurrentException(log, fmt::format("while loading user defined SQL object {} from path {}", backQuote(object_name), path));
//         return nullptr; /// Failed to load this sql object, will ignore it
//     }
// }


// void UserDefinedSQLObjectsLoaderFromFDB::loadObjects()
// {
//     if (!objects_loaded)
//         loadObjectsImpl();
// }


// void UserDefinedSQLObjectsLoaderFromFDB::reloadObjects()
// {
//     loadObjectsImpl();
// }


// void UserDefinedSQLObjectsLoaderFromFDB::loadObjectsImpl()
// {
//     /// If first boot with FDB
//     if (meta_store->isFirstBoot())
//     {
//         LOG_DEBUG(log, "Removing user defined objects on fdb");

//         meta_store->clearAllSqlFunctions();

//         LOG_DEBUG(log, "Loading user defined objects from {} to fdb", dir_path);
//         createDirectory();

//         std::vector<std::pair<String, ASTPtr>> function_names_and_queries;

//         Poco::DirectoryIterator dir_end;
//         for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
//         {
//             if (it->isDirectory())
//                 continue;

//             const String & file_name = it.name();
//             if (!startsWith(file_name, "function_") || !endsWith(file_name, ".sql"))
//                 continue;

//             size_t prefix_length = strlen("function_");
//             size_t suffix_length = strlen(".sql");
//             String function_name = unescapeForFileName(file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length));

//             if (function_name.empty())
//                 continue;

//             ASTPtr ast = tryLoadObject(UserDefinedSQLObjectType::Function, function_name, dir_path + it.name(), /* check_file_exists= */ false);
//             if (ast)
//                 function_names_and_queries.emplace_back(function_name, ast);
//         }

//         UserDefinedSQLFunctionFactory::instance().setAllFunctions(function_names_and_queries);
//         objects_loaded = true;

//         LOG_DEBUG(log, "User defined objects loaded");
//     }
//     else
//     {  
//         LOG_DEBUG(log, "Loading user defined objects from fdb");

//         auto funcs = meta_store->getAllSqlFunctions();
//         for (auto & func : funcs)
//         {
//             loadUserDefinedObjectFromFDB(context, UserDefinedSQLObjectType::Function, func->sql());
//         }
//     }
// }

// void UserDefinedSQLObjectsLoaderFromFDB::loadUserDefinedObjectFromFDB(ContextPtr context, UserDefinedSQLObjectType object_type, const String & sql)
// {
//     LOG_DEBUG(log, "Loading user defined object from FDB");

//     try
//     {
//         switch (object_type)
//         {
//             case UserDefinedSQLObjectType::Function:
//             {
//                 ParserCreateFunctionQuery parser;
//                 ASTPtr ast = parseQuery(
//                     parser,
//                     sql.data(),
//                     sql.data() + sql.size(),
//                     "load from FDB",
//                     0,
//                     context->getSettingsRef().max_parser_depth);

//                 InterpreterCreateFunctionQuery interpreter(ast, context, false /*persist_function*/);
//                 interpreter.execute();
//             }
//         }
//     }
//     catch (Exception & e)
//     {
//         e.addMessage(fmt::format("while loading user defined objects from FDB"));
//         throw;
//     }
// }

// void UserDefinedSQLObjectsLoaderFromFDB::reloadObject(UserDefinedSQLObjectType object_type, const String & object_name)
// {
//     createDirectory();
//     auto ast = tryLoadObject(object_type, object_name);
//     auto & factory = UserDefinedSQLFunctionFactory::instance();
//     if (ast)
//         factory.setFunction(object_name, *ast);
//     else
//         factory.removeFunction(object_name);
// }


// void UserDefinedSQLObjectsLoaderFromFDB::createDirectory()
// {
//     std::error_code create_dir_error_code;
//     fs::create_directories(dir_path, create_dir_error_code);
//     if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
//         throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Couldn't create directory {} reason: '{}'",
//                         dir_path, create_dir_error_code.message());
// }


// bool UserDefinedSQLObjectsLoaderFromFDB::storeObject(
//     UserDefinedSQLObjectType object_type,
//     const String & object_name,
//     const IAST & create_object_query,
//     bool throw_if_exists,
//     bool replace_if_exists,
//     const Settings & settings)
// {
//     LOG_DEBUG(log, "Storing object {} to FDB", backQuote(object_name));

//     bool udf_exists;

//     try
//     {
//         udf_exists = meta_store->existSQLFunction(object_name);
//     }
//     catch (Exception & e)
//     {
//         e.addMessage(fmt::format("while finding sqlFunction on FoundationDB happens error!"));
//         throw;
//     }

//     if(udf_exists)
//     {
//         if (throw_if_exists)
//             throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined object {} already stored on FDB", object_name);
//         else if (!replace_if_exists)
//             return false;
//     }

//     if (replace_if_exists && udf_exists)
//     {
//         try
//         {
//             meta_store->deleteSQLFunction(object_name);
//         }
//         catch (FoundationDBException & e)
//         {
//             e.addMessage(fmt::format("while deleting SQLFunction from FoundationDB happens error!"));
//             throw;
//         }
//     }

//     WriteBufferFromOwnString create_statement_buf;
//     formatAST(create_object_query, create_statement_buf, false);
//     writeChar('\n', create_statement_buf);
//     String create_statement = create_statement_buf.str();

//     FoundationDB::Proto::SqlFuncInfo func_info;
//     func_info.set_sql(create_statement);

//     try
    
//     {
//         meta_store->addSQLFunction(func_info, object_name);
//     }
//     catch (Exception & e)
//     {
//         e.addMessage(fmt::format("while writing sqlFunction into FoundationDB happens error!"));
//         throw;
//     }

//     LOG_DEBUG(log, "Stored object {}", backQuote(object_name));
//     return true;
// }


// bool UserDefinedSQLObjectsLoaderFromFDB::removeObject(
//     UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists)
// {
//     LOG_DEBUG(log, "Removing user defined object {} from FDB", backQuote(object_name));

//     bool udf_exists;

//     try
//     {
//         udf_exists = meta_store->existSQLFunction(object_name);
//     }
//     catch (FoundationDBException & e)
//     {
//         e.addMessage(fmt::format("while finding sqlFunction on FoundationDB happens error!"));
//         throw;
//     }

//     if (!udf_exists)
//     {
//         if (throw_if_not_exists)
//             throw Exception(ErrorCodes::FDB_META_EXCEPTION, "User defined object {} was not stored on FDB", backQuote(object_name));
//         else
//             return false;
//     }

//     try
//     {
//         meta_store->deleteSQLFunction(object_name);
//     }
//     catch (FoundationDBException & e)
//     {
//         e.addMessage(fmt::format("while deleting SQLFunction from FoundationDB happens error!"));
//         throw;
//     }

//     LOG_TRACE(log, "Object {} removed", backQuote(object_name));
//     return true;
// }


// String UserDefinedSQLObjectsLoaderFromFDB::getFilePath(UserDefinedSQLObjectType object_type, const String & object_name) const
// {
//     String file_path;
//     switch (object_type)
//     {
//         case UserDefinedSQLObjectType::Function:
//         {
//             file_path = dir_path + "function_" + escapeForFileName(object_name) + ".sql";
//             break;
//         }
//     }
//     return file_path;
// }

// }
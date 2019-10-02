#include <Common/escapeForFileName.h>
#include <Databases/DatabaseOnDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Event.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
}


namespace detail
{
    String getTableMetadataPath(const String & base_path, const String & table_name)
    {
        return base_path + (endsWith(base_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
    }

    String getDatabaseMetadataPath(const String & base_path)
    {
        return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
    }
}

}
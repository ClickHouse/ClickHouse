#include <Common/Exception.h>
#include <Interpreters/Context.h>

#include <TableFunctions/TableFunctionMerge.h>
#include <TableFunctions/TableFunctionRemote.h>
#include <TableFunctions/TableFunctionShardByHash.h>

#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int UNKNOWN_FUNCTION;
}


TableFunctionPtr TableFunctionFactory::get(
    const std::string & name,
    const Context & context) const
{
    if (context.getSettings().limits.readonly == 1)        /** For example, for readonly = 2 - allowed. */
        throw Exception("Table functions are forbidden in readonly mode", ErrorCodes::READONLY);

         if (name == "merge")        return std::make_shared<TableFunctionMerge>();
    else if (name == "remote")        return std::make_shared<TableFunctionRemote>();
    else if (name == "shardByHash")    return std::make_shared<TableFunctionShardByHash>();
    else
        throw Exception("Unknown table function " + name, ErrorCodes::UNKNOWN_FUNCTION);
}

}

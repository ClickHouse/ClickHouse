#pragma once

#include <unordered_map>
#include <common/singleton.h>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** Lets you get a table function by its name.
  */
class TableFunctionFactory : public Singleton<TableFunctionFactory>
{
private:
    /// No std::function, for smaller object size and less indirection.
    using Creator = TableFunctionPtr(*)();
    using TableFunctions = std::unordered_map<String, Creator>;

    TableFunctions functions;

public:
    TableFunctionPtr get(
        const String & name,
        const Context & context) const;

    /// Register a table function by its name.
    /// No locking, you must register all functions before usage of get.
    template <typename Function>
    void registerFunction()
    {
        if (!functions.emplace(std::string(Function::name), []{ return TableFunctionPtr(std::make_unique<Function>()); }).second)
            throw Exception("TableFunctionFactory: the table function name '" + String(Function::name) + "' is not unique",
                ErrorCodes::LOGICAL_ERROR);
    }
};

}

#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Common/IFactoryWithAliases.h>
#include <Common/NamePrompter.h>


#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <boost/noncopyable.hpp>


namespace DB
{

class Context;

using TableFunctionCreator = std::function<TableFunctionPtr()>;

/** Lets you get a table function by its name.
  */
class TableFunctionFactory final: private boost::noncopyable, public IFactoryWithAliases<TableFunctionCreator>
{
public:
    static TableFunctionFactory & instance();

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(const std::string & name, Value creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    template <typename Function>
    void registerFunction(CaseSensitiveness case_sensitiveness = CaseSensitive)
    {
        auto creator = [] () -> TableFunctionPtr
        {
            return std::make_shared<Function>();
        };
        registerFunction(Function::name, std::move(creator), case_sensitiveness);
    }

    /// Throws an exception if not found.
    TableFunctionPtr get(const std::string & name, const Context & context) const;

    /// Returns nullptr if not found.
    TableFunctionPtr tryGet(const std::string & name, const Context & context) const;

    bool isTableFunctionName(const std::string & name) const;

private:
    using TableFunctions = std::unordered_map<std::string, Value>;

    const TableFunctions & getMap() const override { return table_functions; }

    const TableFunctions & getCaseInsensitiveMap() const override { return case_insensitive_table_functions; }

    String getFactoryName() const override { return "TableFunctionFactory"; }

    TableFunctions table_functions;
    TableFunctions case_insensitive_table_functions;
};

}

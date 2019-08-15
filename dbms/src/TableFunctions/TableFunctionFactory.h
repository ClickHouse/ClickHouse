#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Common/NamePrompter.h>

#include <ext/singleton.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>


namespace DB
{

class Context;


/** Lets you get a table function by its name.
  */
class TableFunctionFactory final: public ext::singleton<TableFunctionFactory>, public IHints<1, TableFunctionFactory>
{
public:
    using Creator = std::function<TableFunctionPtr()>;

    using TableFunctions = std::unordered_map<std::string, Creator>;
    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(const std::string & name, Creator creator);

    template <typename Function>
    void registerFunction()
    {
        auto creator = [] () -> TableFunctionPtr
        {
            return std::make_shared<Function>();
        };
        registerFunction(Function::name, std::move(creator));
    }

    /// Throws an exception if not found.
    TableFunctionPtr get(
        const std::string & name,
        const Context & context) const;

    bool isTableFunctionName(const std::string & name) const;

    const TableFunctions & getAllTableFunctions() const
    {
        return functions;
    }

    std::vector<String> getAllRegisteredNames() const override
    {
        std::vector<String> result;
        auto getter = [](const auto & pair) { return pair.first; };
        std::transform(functions.begin(), functions.end(), std::back_inserter(result), getter);
        return result;
    }

private:
    TableFunctions functions;
};

}

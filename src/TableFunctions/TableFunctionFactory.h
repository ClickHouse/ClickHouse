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

struct TableFunctionFactoryData
{
    TableFunctionCreator creator;
    TableFunctionProperties properties;

    TableFunctionFactoryData() = default;
    TableFunctionFactoryData(const TableFunctionFactoryData &) = default;
    TableFunctionFactoryData & operator = (const TableFunctionFactoryData &) = default;

    template <typename Creator>
        requires (!std::is_same_v<Creator, TableFunctionFactoryData>)
    TableFunctionFactoryData(Creator creator_, TableFunctionProperties properties_ = {}) /// NOLINT
        : creator(std::forward<Creator>(creator_)), properties(std::move(properties_))
    {
    }
};


/** Lets you get a table function by its name.
  */
class TableFunctionFactory final: private boost::noncopyable, public IFactoryWithAliases<TableFunctionFactoryData>
{
public:
    static TableFunctionFactory & instance();

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const std::string & name,
        Value value,
        Case case_sensitiveness = Case::Sensitive);

    template <typename Function>
    void registerFunction(TableFunctionProperties properties = {}, Case case_sensitiveness = Case::Sensitive)
    {
        auto creator = []() -> TableFunctionPtr { return std::make_shared<Function>(); };
        registerFunction(Function::name,
                         TableFunctionFactoryData{std::move(creator), {std::move(properties)}} ,
                         case_sensitiveness);
    }

    /// Throws an exception if not found.
    TableFunctionPtr get(const ASTPtr & ast_function, ContextPtr context) const;

    /// Returns nullptr if not found.
    TableFunctionPtr tryGet(const std::string & name, ContextPtr context) const;

    std::optional<TableFunctionProperties> tryGetProperties(const String & name) const;

    bool isTableFunctionName(const std::string & name) const;

private:
    using TableFunctions = std::unordered_map<std::string, Value>;

    const TableFunctions & getMap() const override { return table_functions; }

    const TableFunctions & getCaseInsensitiveMap() const override { return case_insensitive_table_functions; }

    String getFactoryName() const override { return "TableFunctionFactory"; }

    std::optional<TableFunctionProperties> tryGetPropertiesImpl(const String & name) const;

    TableFunctions table_functions;
    TableFunctions case_insensitive_table_functions;
};

}

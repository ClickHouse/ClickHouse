#pragma once

#include <TableFunctions/ITableFunction.h>
#include "common/types.h"
#include <Common/IFactoryWithAliases.h>
#include <Common/NamePrompter.h>


#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <Documentation/IDocumentation.h>
#include <Documentation/SimpleDocumentation.h>


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
    void registerFunction(const std::string & name, Value creator, CaseSensitiveness case_sensitiveness = CaseSensitive, IDocumentationPtr documentation=nullptr);

    template <typename Function>
    void registerFunction(CaseSensitiveness case_sensitiveness = CaseSensitive, IDocumentationPtr documentation=nullptr)
    {
        auto creator = [] () -> TableFunctionPtr
        {
            return std::make_shared<Function>();
        };
        registerFunction(Function::name, std::move(creator), case_sensitiveness, std::move(documentation));
    }

    /// Throws an exception if not found.
    TableFunctionPtr get(const ASTPtr & ast_function, ContextPtr context) const;

    /// Returns nullptr if not found.
    TableFunctionPtr tryGet(const std::string & name, ContextPtr context) const;

    bool isTableFunctionName(const std::string & name) const;

    /// If there will be no documentation returns "Not found"
    std::string getDocumetation(const std::string & name) const;
private:
    using TableFunctions = std::unordered_map<std::string, Value>;
    using TableFunctionsDocs = std::unordered_map<std::string, IDocumentationPtr>;

    const TableFunctions & getMap() const override { return table_functions; }

    const TableFunctions & getCaseInsensitiveMap() const override { return case_insensitive_table_functions; }

    String getFactoryName() const override { return "TableFunctionFactory"; }

    TableFunctions table_functions;
    TableFunctions case_insensitive_table_functions;
    
    TableFunctionsDocs table_docs;
    TableFunctionsDocs case_insensitive_table_docs;
};

}

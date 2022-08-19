#include <TableFunctions/TableFunctionFactory.h>

#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
}


void TableFunctionFactory::registerFunction(const std::string & name, Value creator, CaseSensitiveness case_sensitiveness)
{
    if (!table_functions.emplace(name, creator).second)
        throw Exception("TableFunctionFactory: the table function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_table_functions.emplace(Poco::toLower(name), creator).second)
        throw Exception("TableFunctionFactory: the case insensitive table function name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

TableFunctionPtr TableFunctionFactory::get(
    const ASTPtr & ast_function,
    ContextPtr context) const
{
    const auto * table_function = ast_function->as<ASTFunction>();
    auto res = tryGet(table_function->name, context);
    if (!res)
    {
        auto hints = getHints(table_function->name);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown table function {}. Maybe you meant: {}", table_function->name , toString(hints));
        else
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown table function {}", table_function->name);
    }

    res->parseArguments(ast_function, context);
    return res;
}

TableFunctionPtr TableFunctionFactory::tryGet(
        const std::string & name_param,
        ContextPtr) const
{
    String name = getAliasToOrName(name_param);
    TableFunctionPtr res;

    auto it = table_functions.find(name);
    if (table_functions.end() != it)
        res = it->second();
    else
    {
        it = case_insensitive_table_functions.find(Poco::toLower(name));
        if (case_insensitive_table_functions.end() != it)
            res = it->second();
    }

    if (!res)
        return nullptr;

    if (CurrentThread::isInitialized())
    {
        auto query_context = CurrentThread::get().getQueryContext();
        if (query_context && query_context->getSettingsRef().log_queries)
            query_context->addQueryFactoriesInfo(Context::QueryLogFactories::TableFunction, name);
    }

    return res;
}

bool TableFunctionFactory::isTableFunctionName(const std::string & name) const
{
    return table_functions.count(name);
}

TableFunctionFactory & TableFunctionFactory::instance()
{
    static TableFunctionFactory ret;
    return ret;
}

}

#include <Documentation/TableDocumentationFactory.h>

namespace DB
{

TableDocumentationFactory & TableDocumentationFactory::instance()
{
    static TableDocumentationFactory ret;
    return ret;
}

void TableDocumentationFactory::registerDocForFunction(const std::string& name, Value documentation, CaseSensitiveness case_sensitiveness)
{
    if (!documentations.emplace(name, documentation).second)
        throw Exception("TableFunctionFactory: the table function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_documentations.emplace(Poco::toLower(name), documentation).second)
        throw Exception("TableFunctionFactory: the case insensitive table function name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

std::string TableDocumentationFactory::tryGet(const std::string& name_param)
{
    String name = getAliasToOrName(name_param);
    IDocumentationPtr res;

    auto it = documentations.find(name);
    if (documentations.end() != it)
        res = it->second;
    else
    {
        it = case_insensitive_documentations.find(Poco::toLower(name));
        if (case_insensitive_documentations.end() != it)
            res = it->second;
    }

    if (!res)
        return "Not Found";

    // if (CurrentThread::isInitialized())
    // {
    //     auto query_context = CurrentThread::get().getQueryContext();
    //     if (query_context && query_context->getSettingsRef().log_queries)
    //         query_context->addQueryFactoriesInfo(Context::QueryLogFactories::TableFunction, name);
    // }

    return res->getDocumentation();
}

}

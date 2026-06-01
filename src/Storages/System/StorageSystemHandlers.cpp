#include <Storages/System/StorageSystemHandlers.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersFactory.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>


namespace DB
{

ColumnsDescription StorageSystemHandlers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the handler."},
        {"protocol", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Composable protocol the handler is restricted to, or NULL for all http/https protocols."},
        {"url_match_type", std::make_shared<DataTypeString>(), "How the URL is matched: exact, prefix or regexp."},
        {"url", std::make_shared<DataTypeString>(), "The URL pattern that the handler matches."},
        {"methods", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Allowed HTTP methods."},
        {"type", std::make_shared<DataTypeString>(), "The handler type (only `query` is supported)."},
        {"query", std::make_shared<DataTypeString>(), "The SQL query executed by the handler."},
        {"create_query", std::make_shared<DataTypeString>(), "The CREATE HANDLER statement."},
    };
}

StorageSystemHandlers::StorageSystemHandlers(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemHandlers::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto handlers = SQLDefinedHandlersFactory::instance().getAll();
    if (!handlers)
        return;

    for (const auto & [name, handler] : *handlers)
    {
        size_t col = 0;
        res_columns[col++]->insert(name);

        if (handler->protocol)
            res_columns[col++]->insert(*handler->protocol);
        else
            res_columns[col++]->insertDefault();

        String match_type;
        switch (handler->url_match_type)
        {
            case SQLDefinedHandler::URLMatchType::Exact: match_type = "exact"; break;
            case SQLDefinedHandler::URLMatchType::Prefix: match_type = "prefix"; break;
            case SQLDefinedHandler::URLMatchType::Regexp: match_type = "regexp"; break;
        }
        res_columns[col++]->insert(match_type);
        res_columns[col++]->insert(handler->url);

        Array methods_array;
        methods_array.reserve(handler->methods.size());
        for (const auto & method : handler->methods)
            methods_array.push_back(method);
        res_columns[col++]->insert(methods_array);

        res_columns[col++]->insert(handler->type);
        res_columns[col++]->insert(handler->query);
        res_columns[col++]->insert(handler->create_statement);
    }
}

}

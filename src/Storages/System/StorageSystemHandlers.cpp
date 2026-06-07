#include <Storages/System/StorageSystemHandlers.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnArray.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ParserCreateHandlerQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersFactory.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

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
        {"query", std::make_shared<DataTypeString>(), "The SQL query executed by the handler. Secrets are hidden unless the user is allowed to see them."},
        {"create_query", std::make_shared<DataTypeString>(), "The CREATE HANDLER statement. Secrets are hidden unless the user is allowed to see them."},
    };
}

StorageSystemHandlers::StorageSystemHandlers(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemHandlers::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// A handler's query and create_query can embed credentials (e.g. for table functions), so reading
    /// system.handlers is gated by the SHOW HANDLERS privilege, mirroring system.named_collections.
    if (!context->getAccess()->isGranted(AccessType::SHOW_HANDLERS))
        return;

    auto handlers = SQLDefinedHandlersFactory::instance().getAll();
    if (!handlers)
        return;

    const auto & settings = context->getSettingsRef();

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

        /// The stored statement keeps secrets in clear text (it must be re-executable). Re-parse it and
        /// re-format it with secret hiding driven by the user's privileges and settings, so a user without
        /// the secret-display grant sees masked credentials - exactly as in SHOW CREATE TABLE.
        ParserCreateHandlerQuery parser(handler->create_statement.data() + handler->create_statement.size());
        auto ast = parseQuery(
            parser,
            handler->create_statement,
            "in handler " + name,
            0,
            settings[Setting::max_parser_depth],
            settings[Setting::max_parser_backtracks]);
        const auto & create_query = ast->as<const ASTCreateHandlerQuery &>();

        res_columns[col++]->insert(format({context, *create_query.query}));
        res_columns[col++]->insert(format({context, *ast}));
    }
}

}

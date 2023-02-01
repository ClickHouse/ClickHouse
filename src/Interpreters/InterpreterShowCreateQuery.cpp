#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int THERE_IS_NO_QUERY;
    extern const int BAD_ARGUMENTS;
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{
        ColumnString::create(),
        std::make_shared<DataTypeString>(),
        "statement"}};
}


QueryPipeline InterpreterShowCreateQuery::executeImpl()
{
    ASTPtr create_query;
    ASTQueryWithTableAndOutput * show_query;
    if ((show_query = query_ptr->as<ASTShowCreateTableQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateViewQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateDictionaryQuery>()))
    {
        auto resolve_table_type = show_query->temporary ? Context::ResolveExternal : Context::ResolveOrdinary;
        auto table_id = getContext()->resolveStorageID(*show_query, resolve_table_type);

        bool is_dictionary = static_cast<bool>(query_ptr->as<ASTShowCreateDictionaryQuery>());

        if (is_dictionary)
            getContext()->checkAccess(AccessType::SHOW_DICTIONARIES, table_id);
        else
            getContext()->checkAccess(AccessType::SHOW_COLUMNS, table_id);

        create_query = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getCreateTableQuery(table_id.table_name, getContext());

        auto & ast_create_query = create_query->as<ASTCreateQuery &>();
        if (query_ptr->as<ASTShowCreateViewQuery>())
        {
            if (!ast_create_query.isView())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a VIEW",
                    backQuote(ast_create_query.getDatabase()), backQuote(ast_create_query.getTable()));
        }
        else if (is_dictionary)
        {
            if (!ast_create_query.is_dictionary)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a DICTIONARY",
                    backQuote(ast_create_query.getDatabase()), backQuote(ast_create_query.getTable()));
        }
    }
    else if ((show_query = query_ptr->as<ASTShowCreateDatabaseQuery>()))
    {
        if (show_query->temporary)
            throw Exception("Temporary databases are not possible.", ErrorCodes::SYNTAX_ERROR);
        show_query->setDatabase(getContext()->resolveDatabase(show_query->getDatabase()));
        getContext()->checkAccess(AccessType::SHOW_DATABASES, show_query->getDatabase());
        create_query = DatabaseCatalog::instance().getDatabase(show_query->getDatabase())->getCreateDatabaseQuery();
    }

    if (!create_query)
        throw Exception("Unable to show the create query of " + show_query->getTable() + ". Maybe it was created by the system.", ErrorCodes::THERE_IS_NO_QUERY);

    if (!getContext()->getSettingsRef().show_table_uuid_in_table_create_query_if_not_nil)
    {
        auto & create = create_query->as<ASTCreateQuery &>();
        create.uuid = UUIDHelpers::Nil;
        create.to_inner_uuid = UUIDHelpers::Nil;
    }

    WriteBufferFromOwnString buf;
    formatAST(*create_query, buf, false, false);
    String res = buf.str();

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}}));
}

}

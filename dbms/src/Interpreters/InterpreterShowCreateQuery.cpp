#include <sstream>

#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Access/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int THERE_IS_NO_QUERY;
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{
        ColumnString::create(),
        std::make_shared<DataTypeString>(),
        "statement"}};
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
    ASTPtr create_query;
    ASTQueryWithTableAndOutput * show_query;
    StorageID table_id(query_ptr);
    if ((show_query = query_ptr->as<ASTShowCreateTableQuery>()))
    {
        auto resolve_table_type = show_query->temporary ? Context::ResolveExternal : Context::ResolveOrdinary;
        table_id = context.resolveStorageID(table_id, resolve_table_type);
        context.checkAccess(AccessType::SHOW, table_id);
        create_query = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getCreateTableQuery(context, table_id.table_name);
    }
    else if ((show_query = query_ptr->as<ASTShowCreateDatabaseQuery>()))
    {
        if (show_query->temporary)
            throw Exception("Temporary databases are not possible.", ErrorCodes::SYNTAX_ERROR);
        show_query->database = context.resolveDatabase(show_query->database);
        context.checkAccess(AccessType::SHOW, show_query->database);
        create_query = DatabaseCatalog::instance().getDatabase(show_query->database)->getCreateDatabaseQuery(context);
    }
    else if ((show_query = query_ptr->as<ASTShowCreateDictionaryQuery>()))
    {
        if (show_query->temporary)
            throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);
        show_query->database = context.resolveDatabase(show_query->database);
        context.checkAccess(AccessType::SHOW, show_query->database, show_query->table);
        create_query = DatabaseCatalog::instance().getDatabase(show_query->database)->getCreateDictionaryQuery(context, show_query->table);
    }

    if (!create_query && show_query && show_query->temporary)
        throw Exception("Unable to show the create query of " + show_query->table + ". Maybe it was created by the system.", ErrorCodes::THERE_IS_NO_QUERY);

    std::stringstream stream;
    formatAST(*create_query, stream, false, true);
    String res = stream.str();

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return std::make_shared<OneBlockInputStream>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}});
}

}

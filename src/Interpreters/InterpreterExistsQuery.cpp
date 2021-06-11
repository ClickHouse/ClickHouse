#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Access/AccessFlags.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

BlockIO InterpreterExistsQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterExistsQuery::getSampleBlock()
{
    return Block{{
        ColumnUInt8::create(),
        std::make_shared<DataTypeUInt8>(),
        "result" }};
}


BlockInputStreamPtr InterpreterExistsQuery::executeImpl()
{
    ASTQueryWithTableAndOutput * exists_query;
    bool result = false;
    if ((exists_query = query_ptr->as<ASTExistsTableQuery>()))
    {
        if (exists_query->temporary)
        {
            result = context.tryResolveStorageID({"", exists_query->table}, Context::ResolveExternal);
        }
        else
        {
            String database = context.resolveDatabase(exists_query->database);
            context.checkAccess(AccessType::SHOW_TABLES, database, exists_query->table);
            result = DatabaseCatalog::instance().isTableExist({database, exists_query->table}, context);
        }
    }
    else if ((exists_query = query_ptr->as<ASTExistsDictionaryQuery>()))
    {
        if (exists_query->temporary)
            throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);
        String database = context.resolveDatabase(exists_query->database);
        context.checkAccess(AccessType::SHOW_DICTIONARIES, database, exists_query->table);
        result = DatabaseCatalog::instance().isDictionaryExist({database, exists_query->table});
    }

    return std::make_shared<OneBlockInputStream>(Block{{
        ColumnUInt8::create(1, result),
        std::make_shared<DataTypeUInt8>(),
        "result" }});
}

}

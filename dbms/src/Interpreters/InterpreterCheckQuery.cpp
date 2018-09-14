#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTCheckQuery.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>


namespace DB
{

InterpreterCheckQuery::InterpreterCheckQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterCheckQuery::execute()
{
    ASTCheckQuery & alter = typeid_cast<ASTCheckQuery &>(*query_ptr);
    String & table_name = alter.table;
    String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;

    StoragePtr table = context.getTable(database_name, table_name);

    auto column = ColumnUInt8::create();
    column->insert(UInt64(table->checkData()));
    result = Block{{ std::move(column), std::make_shared<DataTypeUInt8>(), "result" }};

    BlockIO res;
    res.in = std::make_shared<OneBlockInputStream>(result);

    return res;
}

}

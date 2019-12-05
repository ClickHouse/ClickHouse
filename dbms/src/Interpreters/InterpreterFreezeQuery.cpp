#include <Interpreters/InterpreterFreezeQuery.h>

#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Parsers/ASTFreezeQuery.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterFreezeQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterFreezeQuery::getSampleBlock()
{
    Block block;

    ColumnWithTypeAndName col;
    col.name = "increment";
    col.type = std::make_shared<DataTypeUInt64>();
    col.column = col.type->createColumn();
    block.insert(col);

    return block;
}


BlockInputStreamPtr InterpreterFreezeQuery::executeImpl()
{
    const auto & ast = query_ptr->as<ASTFreezeQuery &>();

    StoragePtr table = context.getTable(ast.database, ast.table);
    UInt64 increment = table->freeze(ast.partition, ast.with_name, context);

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();
    res_columns[0]->insert(increment);

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}

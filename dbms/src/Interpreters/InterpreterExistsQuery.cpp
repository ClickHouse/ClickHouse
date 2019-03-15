#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

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
    const auto & ast = query_ptr->as<ASTExistsQuery &>();
    bool res = ast.temporary ? context.isExternalTableExist(ast.table) : context.isTableExist(ast.database, ast.table);

    return std::make_shared<OneBlockInputStream>(Block{{
        ColumnUInt8::create(1, res),
        std::make_shared<DataTypeUInt8>(),
        "result" }});
}

}

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
    res.in_sample = getSampleBlock();

    return res;
}


Block InterpreterExistsQuery::getSampleBlock()
{
    return {{ nullptr, std::make_shared<DataTypeUInt8>(), "result" }};
}


BlockInputStreamPtr InterpreterExistsQuery::executeImpl()
{
    const ASTExistsQuery & ast = typeid_cast<const ASTExistsQuery &>(*query_ptr);
    bool res = context.isTableExist(ast.database, ast.table);

    return std::make_shared<OneBlockInputStream>(Block{{
        std::make_shared<ColumnUInt8>(1, res),
        std::make_shared<DataTypeUInt8>(),
        "result" }});
}

}

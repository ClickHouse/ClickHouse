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
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>


namespace DB
{

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    res.in_sample = getSampleBlock();

    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return {{ nullptr, std::make_shared<DataTypeString>(), "statement" }};
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
    const ASTShowCreateQuery & ast = typeid_cast<const ASTShowCreateQuery &>(*query_ptr);

    std::stringstream stream;
    formatAST(*context.getCreateQuery(ast.database, ast.table), stream, 0, false, true);
    String res = stream.str();

    ColumnPtr column = std::make_shared<ColumnString>();
    column->insert(res);

    return std::make_shared<OneBlockInputStream>(Block{{
        column,
        std::make_shared<DataTypeString>(),
        "statement"}});
}

}

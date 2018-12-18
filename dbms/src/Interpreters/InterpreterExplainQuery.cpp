#include <sstream>

#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/DumpASTNode.h>


namespace DB
{

BlockIO InterpreterExplainQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterExplainQuery::getSampleBlock()
{
    Block block;

    ColumnWithTypeAndName col;
    col.name = "ast";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    return block;
}


BlockInputStreamPtr InterpreterExplainQuery::executeImpl()
{
    const ASTExplainQuery & ast = typeid_cast<const ASTExplainQuery &>(*query);

    std::stringstream ss;
    dumpAST(ast, ss);

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();
    res_columns[0]->insert(ss.str());

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}

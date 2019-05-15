#include <Interpreters/InterpreterExplainQuery.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>

#include <sstream>


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
    col.name = "explain";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    return block;
}


BlockInputStreamPtr InterpreterExplainQuery::executeImpl()
{
    const auto & ast = query->as<ASTExplainQuery &>();
    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    std::stringstream ss;

    if (ast.getKind() == ASTExplainQuery::ParsedAST)
    {
        dumpAST(ast, ss);
    }
    else if (ast.getKind() == ASTExplainQuery::AnalyzedSyntax)
    {
        InterpreterSelectWithUnionQuery interpreter(ast.children.at(0), context,
                                                    SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());
        interpreter.getQuery()->format(IAST::FormatSettings(ss, false));
    }

    res_columns[0]->insert(ss.str());

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}

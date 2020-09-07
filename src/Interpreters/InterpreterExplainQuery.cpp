#include <Interpreters/InterpreterExplainQuery.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>

#include <Storages/StorageView.h>
#include <sstream>


namespace DB
{

namespace
{
    struct ExplainAnalyzedSyntaxMatcher
    {
        struct Data
        {
            const Context & context;
        };

        static bool needChildVisit(ASTPtr & node, ASTPtr &)
        {
            return !node->as<ASTSelectQuery>();
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select = ast->as<ASTSelectQuery>())
                visit(*select, ast, data);
        }

        static void visit(ASTSelectQuery & select, ASTPtr & node, Data & data)
        {
            InterpreterSelectQuery interpreter(
                node, data.context, SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());

            const SelectQueryInfo & query_info = interpreter.getQueryInfo();
            if (query_info.view_query)
            {
                ASTPtr tmp;
                StorageView::replaceWithSubquery(select, query_info.view_query->clone(), tmp);
            }
        }
    };

    using ExplainAnalyzedSyntaxVisitor = InDepthNodeVisitor<ExplainAnalyzedSyntaxMatcher, true>;

}

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
        ExplainAnalyzedSyntaxVisitor::Data data{.context = context};
        ExplainAnalyzedSyntaxVisitor(data).visit(query);

        ast.children.at(0)->format(IAST::FormatSettings(ss, false));
    }

    res_columns[0]->insert(ss.str());

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}

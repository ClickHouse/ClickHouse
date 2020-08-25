#include <Interpreters/InterpreterExplainQuery.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Core/Field.h>
#include <Common/typeid_cast.h>
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
            bool analyzed = false;
            const Context & context;
        };

        static bool needChildVisit(ASTPtr &, ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select_query = ast->as<ASTSelectQuery>())
                visit(*select_query, ast, data);
            if (auto * union_select_query = ast->as<ASTSelectWithUnionQuery>())
                visit(*union_select_query, ast, data);
        }

        static void visit(ASTSelectQuery & select_query, ASTPtr &, Data & data)
        {
            if (!select_query.tables())
                return;

            for (const auto & child : select_query.tables()->children)
            {
                auto * tables_element = child->as<ASTTablesInSelectQueryElement>();

                if (tables_element && tables_element->table_expression)
                    visit(*tables_element->table_expression->as<ASTTableExpression>(), select_query, data);
            }
        }

        static void visit(ASTSelectWithUnionQuery &, ASTPtr & node, Data & data)
        {
            if (!data.analyzed)
            {
                data.analyzed = true;
                InterpreterSelectWithUnionQuery interpreter(
                    node, data.context, SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());
            }
        }

        static void visit(ASTTableExpression & expression, ASTSelectQuery & select_query, Data & data)
        {
            if (data.context.getSettingsRef().enable_optimize_predicate_expression && expression.database_and_table_name)
            {
                if (const auto * identifier = expression.database_and_table_name->as<ASTIdentifier>())
                {
                    auto table_id = data.context.resolveStorageID(*identifier);
                    const auto & storage = DatabaseCatalog::instance().getTable(table_id);

                    if (auto * storage_view = dynamic_cast<StorageView *>(storage.get()))
                        storage_view->getRuntimeViewQuery(&select_query, data.context, true);
                }
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

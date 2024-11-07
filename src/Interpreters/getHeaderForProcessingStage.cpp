#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDummy.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Planner/Utils.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool hasJoin(const ASTSelectQuery & select)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    return joined_table.table_join != nullptr;
}

/// Rewrite original query removing joined tables from it
bool removeJoin(ASTSelectQuery & select, TreeRewriterResult & rewriter_result, ContextPtr context)
{
    if (!hasJoin(select))
        return false;

    select.tables()->children.resize(1);

    /// Also remove GROUP BY cause ExpressionAnalyzer would check if it has all aggregate columns but joined columns would be missed.
    select.setExpression(ASTSelectQuery::Expression::GROUP_BY, {});
    rewriter_result.aggregates.clear();

    /// Replace select list to remove joined columns
    auto select_list = std::make_shared<ASTExpressionList>();
    for (const auto & column : rewriter_result.required_source_columns)
        select_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

    select.setExpression(ASTSelectQuery::Expression::SELECT, select_list);

    const DB::IdentifierMembershipCollector membership_collector{select, context};

    /// Remove unknown identifiers from where, leave only ones from left table
    auto replace_where = [&membership_collector](ASTSelectQuery & query, ASTSelectQuery::Expression expr)
    {
        auto where = query.getExpression(expr, false);
        if (!where)
            return;

        const size_t left_table_pos = 0;
        /// Test each argument of `and` function and select ones related to only left table
        std::shared_ptr<ASTFunction> new_conj = makeASTFunction("and");
        for (auto && node : splitConjunctionsAst(where))
        {
            if (membership_collector.getIdentsMembership(node) == left_table_pos)
                new_conj->arguments->children.push_back(std::move(node));
        }

        if (new_conj->arguments->children.empty())
            /// No identifiers from left table
            query.setExpression(expr, {});
        else if (new_conj->arguments->children.size() == 1)
            /// Only one expression, lift from `and`
            query.setExpression(expr, std::move(new_conj->arguments->children[0]));
        else
            /// Set new expression
            query.setExpression(expr, std::move(new_conj));
    };
    replace_where(select, ASTSelectQuery::Expression::WHERE);
    replace_where(select, ASTSelectQuery::Expression::PREWHERE);
    select.setExpression(ASTSelectQuery::Expression::HAVING, {});
    select.setExpression(ASTSelectQuery::Expression::ORDER_BY, {});

    return true;
}

Block getHeaderForProcessingStage(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage)
{
    switch (processed_stage)
    {
        case QueryProcessingStage::FetchColumns:
        {
            Block header = storage_snapshot->getSampleBlockForColumns(column_names);
            header = SourceStepWithFilter::applyPrewhereActions(header, query_info.prewhere_info);
            return header;
        }
        case QueryProcessingStage::WithMergeableState:
        case QueryProcessingStage::Complete:
        case QueryProcessingStage::WithMergeableStateAfterAggregation:
        case QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit:
        case QueryProcessingStage::MAX:
        {
            ASTPtr query = query_info.query;
            if (const auto * select = query_info.query->as<ASTSelectQuery>(); select && hasJoin(*select))
            {
                if (!query_info.syntax_analyzer_result)
                {
                    if (!query_info.planner_context)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query is not analyzed: no planner context");

                    const auto & query_node = query_info.query_tree->as<QueryNode &>();
                    const auto & join_tree = query_node.getJoinTree();
                    auto left_table_expression = extractLeftTableExpression(join_tree);

                    auto & table_expression_data = query_info.planner_context->getTableExpressionDataOrThrow(left_table_expression);
                    const auto & query_context = query_info.planner_context->getQueryContext();

                    NamesAndTypes columns;
                    const auto & column_name_to_column = table_expression_data.getColumnNameToColumn();
                    for (const auto & column_name : table_expression_data.getSelectedColumnsNames())
                        columns.push_back(column_name_to_column.at(column_name));

                    auto new_query_node = buildSubqueryToReadColumnsFromTableExpression(columns, left_table_expression, query_context);
                    query = new_query_node->toAST();
                }
                else
                {
                    query = query_info.query->clone();
                    TreeRewriterResult new_rewriter_result = *query_info.syntax_analyzer_result;
                    removeJoin(*query->as<ASTSelectQuery>(), new_rewriter_result, context);
                }
            }

            Block result;

            if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                auto storage = std::make_shared<StorageDummy>(storage_snapshot->storage.getStorageID(),
                                                                                        storage_snapshot->getAllColumnsDescription(),
                                                                                        storage_snapshot);
                InterpreterSelectQueryAnalyzer interpreter(query, context, storage, SelectQueryOptions(processed_stage).analyze());
                result = interpreter.getSampleBlock();
            }
            else
            {
                auto pipe = Pipe(std::make_shared<SourceFromSingleChunk>(
                        storage_snapshot->getSampleBlockForColumns(column_names)));
                result = InterpreterSelectQuery(query, context, std::move(pipe), SelectQueryOptions(processed_stage).analyze()).getSampleBlock();
            }

            return result;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown processed stage.");
}

}

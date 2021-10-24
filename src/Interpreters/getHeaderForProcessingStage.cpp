#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Storages/IStorage.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

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
        for (const auto & node : collectConjunctions(where))
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
        const IStorage & storage,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage)
{
    switch (processed_stage)
    {
        case QueryProcessingStage::FetchColumns:
        {
            Block header = metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
            if (query_info.prewhere_info)
            {
                auto & prewhere_info = *query_info.prewhere_info;

                if (prewhere_info.row_level_filter)
                {
                    header = prewhere_info.row_level_filter->updateHeader(std::move(header));
                    header.erase(prewhere_info.row_level_column_name);
                }

                if (prewhere_info.prewhere_actions)
                    header = prewhere_info.prewhere_actions->updateHeader(std::move(header));

                if (prewhere_info.remove_prewhere_column)
                    header.erase(prewhere_info.prewhere_column_name);
            }
            return header;
        }
        case QueryProcessingStage::WithMergeableState:
        case QueryProcessingStage::Complete:
        case QueryProcessingStage::WithMergeableStateAfterAggregation:
        case QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit:
        case QueryProcessingStage::MAX:
        {
            auto query = query_info.query->clone();
            TreeRewriterResult new_rewriter_result = *query_info.syntax_analyzer_result;
            removeJoin(*query->as<ASTSelectQuery>(), new_rewriter_result, context);

            auto stream = std::make_shared<OneBlockInputStream>(
                    metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID()));
            return InterpreterSelectQuery(query, context, stream, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();
        }
    }
    throw Exception("Logical Error: unknown processed stage.", ErrorCodes::LOGICAL_ERROR);
}

}


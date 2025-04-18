// #include <Processors/TTL/getTTLAlgorithms.h>
// #include "Interpreters/ExpressionAnalyzer.h"
// #include "Interpreters/addTypeConversionToAST.h"
// #include "Processors/TTL/TTLAggregationAlgorithm.h"
// #include "Processors/TTL/TTLDeleteAlgorithm.h"
// #include <Storages/StorageInMemoryMetadata.h>

// namespace DB
// {

// static TTLExpressions getExpressions(const TTLDescription & ttl_descr, PreparedSets::Subqueries & subqueries_for_sets, const ContextPtr & context)
// {
//     auto expr = ttl_descr.buildExpression(context);
//     auto expr_queries = expr.sets->getSubqueries();
//     subqueries_for_sets.insert(subqueries_for_sets.end(), expr_queries.begin(), expr_queries.end());

//     auto where_expr = ttl_descr.buildWhereExpression(context);

//     if (where_expr.sets)
//     {
//         auto where_expr_queries = where_expr.sets->getSubqueries();
//         subqueries_for_sets.insert(subqueries_for_sets.end(), where_expr_queries.begin(), where_expr_queries.end());
//     }

//     return {expr.expression, where_expr.expression};
// }

// std::vector<TTLAlgorithmPtr> getTTLAlgorithms(
//     const StorageMetadataPtr & metadata_snapshot,
//     const MergeTreeDataPartTTLInfos & old_ttl_infos,
//     const std::optional<NameSet> & ttl_names_to_update,
//     const ContextPtr & context,
//     const Block & input_header,
//     PreparedSets::Subqueries & subqueries_for_sets,
//     time_t current_time,
//     bool force,
//     bool is_compact_part)
// {
//     std::vector<TTLAlgorithmPtr> algorithms;

//     if (metadata_snapshot->hasRowsTTL())
//     {
//         const auto & rows_ttl = metadata_snapshot->getRowsTTL();
//         auto algorithm = std::make_unique<TTLDeleteAlgorithm>(
//             getExpressions(rows_ttl, subqueries_for_sets, context), rows_ttl,
//             old_ttl_infos.table_ttl, current_time, force);

//         // /// Skip all data if table ttl is expired for part
//         // if (algorithm->isMaxTTLExpired() && !rows_ttl.where_expression_ast)
//         //     all_data_dropped = true;

//         // delete_algorithm = algorithm.get();
//         // algorithms.emplace_back(std::move(algorithm));
//     }

//     for (const auto & where_ttl : metadata_snapshot->getRowsWhereTTLs())
//         algorithms.emplace_back(std::make_unique<TTLDeleteAlgorithm>(
//             getExpressions(where_ttl, subqueries_for_sets, context), where_ttl,
//             old_ttl_infos.rows_where_ttl[where_ttl.result_column], current_time, force));

//     for (const auto & group_by_ttl : metadata_snapshot->getGroupByTTLs())
//         algorithms.emplace_back(std::make_unique<TTLAggregationAlgorithm>(
//             getExpressions(group_by_ttl, subqueries_for_sets, context), group_by_ttl,
//             old_ttl_infos.group_by_ttl[group_by_ttl.result_column], current_time, force,
//             getInputPort().getHeader(), context));

//     if (metadata_snapshot->hasAnyColumnTTL())
//     {
//         const auto & storage_columns = metadata_snapshot->getColumns();
//         const auto & column_defaults = storage_columns.getDefaults();

//         for (const auto & [name, description] : metadata_snapshot->getColumnTTLs())
//         {
//             ExpressionActionsPtr default_expression;
//             String default_column_name;
//             auto it = column_defaults.find(name);
//             if (it != column_defaults.end())
//             {
//                 const auto & column = storage_columns.get(name);
//                 auto default_ast = it->second.expression->clone();
//                 default_ast = addTypeConversionToAST(std::move(default_ast), column.type->getName());

//                 auto syntax_result = TreeRewriter(context).analyze(default_ast, metadata_snapshot->getColumns().getAllPhysical());
//                 default_expression = ExpressionAnalyzer{default_ast, syntax_result, context}.getActions(true);
//                 default_column_name = default_ast->getColumnName();
//             }

//             algorithms.emplace_back(std::make_unique<TTLColumnAlgorithm>(
//                 getExpressions(description, subqueries_for_sets, context), description,
//                 old_ttl_infos.columns_ttl[name], current_time,
//                 force, name, default_expression, default_column_name, isCompactPart(data_part)));
//         }
//     }

//     for (const auto & move_ttl : metadata_snapshot->getMoveTTLs())
//         algorithms.emplace_back(std::make_unique<TTLUpdateInfoAlgorithm>(
//             getExpressions(move_ttl, subqueries_for_sets, context), move_ttl,
//             TTLUpdateField::MOVES_TTL, move_ttl.result_column, old_ttl_infos.moves_ttl[move_ttl.result_column], current_time, force_));

//     for (const auto & recompression_ttl : metadata_snapshot->getRecompressionTTLs())
//         algorithms.emplace_back(std::make_unique<TTLUpdateInfoAlgorithm>(
//             getExpressions(recompression_ttl, subqueries_for_sets, context), recompression_ttl,
//             TTLUpdateField::RECOMPRESSION_TTL, recompression_ttl.result_column, old_ttl_infos.recompression_ttl[recompression_ttl.result_column], current_time, force_));

// }

// }

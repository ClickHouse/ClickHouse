#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/LimitByBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/MergingAggregatedBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/DistinctBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/TotalsHavingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/RollupBlockInputStream.h>
#include <DataStreams/ConvertColumnWithDictionaryToFullBlockInputStream.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>

#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Core/Field.h>
#include <Columns/Collator.h>
#include <Common/typeid_cast.h>
#include <Parsers/queryToString.h>
#include <ext/map.h>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int THERE_IS_NO_COLUMN;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int ILLEGAL_FINAL;
    extern const int ILLEGAL_PREWHERE;
    extern const int TOO_MANY_COLUMNS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const Names & required_result_column_names,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_,
    bool only_analyze_)
    : InterpreterSelectQuery(query_ptr_, context_, nullptr, nullptr, required_result_column_names, to_stage_, subquery_depth_, only_analyze_)
{
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const BlockInputStreamPtr & input_,
    QueryProcessingStage::Enum to_stage_,
    bool only_analyze_)
    : InterpreterSelectQuery(query_ptr_, context_, input_, nullptr, Names{}, to_stage_, 0, only_analyze_)
{
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const StoragePtr & storage_,
    QueryProcessingStage::Enum to_stage_,
    bool only_analyze_)
    : InterpreterSelectQuery(query_ptr_, context_, nullptr, storage_, Names{}, to_stage_, 0, only_analyze_)
{
}

InterpreterSelectQuery::~InterpreterSelectQuery() = default;


/** There are no limits on the maximum size of the result for the subquery.
  *  Since the result of the query is not the result of the entire query.
  */
static Context getSubqueryContext(const Context & context)
{
    Context subquery_context = context;
    Settings subquery_settings = context.getSettings();
    subquery_settings.max_result_rows = 0;
    subquery_settings.max_result_bytes = 0;
    /// The calculation of extremes does not make sense and is not necessary (if you do it, then the extremes of the subquery can be taken for whole query).
    subquery_settings.extremes = 0;
    subquery_context.setSettings(subquery_settings);
    return subquery_context;
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const BlockInputStreamPtr & input_,
    const StoragePtr & storage_,
    const Names & required_result_column_names,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_,
    bool only_analyze_)
    : query_ptr(query_ptr_->clone())    /// Note: the query is cloned because it will be modified during analysis.
    , query(typeid_cast<ASTSelectQuery &>(*query_ptr))
    , context(context_)
    , to_stage(to_stage_)
    , subquery_depth(subquery_depth_)
    , only_analyze(only_analyze_)
    , storage(storage_)
    , input(input_)
    , log(&Logger::get("InterpreterSelectQuery"))
{
    if (!context.hasQueryContext())
        context.setQueryContext(context);

    initSettings();
    const Settings & settings = context.getSettingsRef();

    if (settings.max_subquery_depth && subquery_depth > settings.max_subquery_depth)
        throw Exception("Too deep subqueries. Maximum: " + settings.max_subquery_depth.toString(),
            ErrorCodes::TOO_DEEP_SUBQUERIES);

    max_streams = settings.max_threads;

    const auto & table_expression = query.table();

    if (input)
    {
        /// Read from prepared input.
        source_header = input->getHeader();
    }
    else if (table_expression && typeid_cast<const ASTSelectWithUnionQuery *>(table_expression.get()))
    {
        /// Read from subquery.
        interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            table_expression, getSubqueryContext(context), required_columns, QueryProcessingStage::Complete, subquery_depth + 1, only_analyze);

        source_header = interpreter_subquery->getSampleBlock();
    }
    else if (!storage)
    {
        if (table_expression && typeid_cast<const ASTFunction *>(table_expression.get()))
        {
            /// Read from table function.
            storage = context.getQueryContext().executeTableFunction(table_expression);
        }
        else
        {
            /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
            String database_name;
            String table_name;

            getDatabaseAndTableNames(database_name, table_name);

            storage = context.getTable(database_name, table_name);
        }
    }

    if (storage)
        table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);

    query_analyzer = std::make_unique<ExpressionAnalyzer>(
        query_ptr, context, storage, source_header.getNamesAndTypesList(), required_result_column_names, subquery_depth, !only_analyze);

    if (!only_analyze)
    {
        if (query.sample_size() && (input || !storage || !storage->supportsSampling()))
            throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

        if (query.final() && (input || !storage || !storage->supportsFinal()))
            throw Exception((!input && storage) ? "Storage " + storage->getName() + " doesn't support FINAL" : "Illegal FINAL", ErrorCodes::ILLEGAL_FINAL);

        if (query.prewhere_expression && (input || !storage || !storage->supportsPrewhere()))
            throw Exception((!input && storage) ? "Storage " + storage->getName() + " doesn't support PREWHERE" : "Illegal PREWHERE", ErrorCodes::ILLEGAL_PREWHERE);

        /// Save the new temporary tables in the query context
        for (const auto & it : query_analyzer->getExternalTables())
            if (!context.tryGetExternalTable(it.first))
                context.addExternalTable(it.first, it.second);

        if (query_analyzer->isRewriteSubqueriesPredicate())
            interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
                table_expression, getSubqueryContext(context), required_columns, QueryProcessingStage::Complete, subquery_depth + 1, only_analyze);
    }

    if (interpreter_subquery)
    {
        /// If there is an aggregation in the outer query, WITH TOTALS is ignored in the subquery.
        if (query_analyzer->hasAggregation())
            interpreter_subquery->ignoreWithTotals();
    }

    required_columns = query_analyzer->getRequiredSourceColumns();

    if (storage)
        source_header = storage->getSampleBlockForColumns(required_columns);

    /// Calculate structure of the result.
    {
        Pipeline pipeline;
        executeImpl(pipeline, nullptr, true);
        result_header = pipeline.firstStream()->getHeader();
    }
}


void InterpreterSelectQuery::getDatabaseAndTableNames(String & database_name, String & table_name)
{
    auto query_database = query.database();
    auto query_table = query.table();

    /** If the table is not specified - use the table `system.one`.
     *  If the database is not specified - use the current database.
     */
    if (query_database)
        database_name = typeid_cast<ASTIdentifier &>(*query_database).name;
    if (query_table)
        table_name = typeid_cast<ASTIdentifier &>(*query_table).name;

    if (!query_table)
    {
        database_name = "system";
        table_name = "one";
    }
    else if (!query_database)
    {
        if (context.tryGetTable("", table_name))
            database_name = "";
        else
            database_name = context.getCurrentDatabase();
    }
}


Block InterpreterSelectQuery::getSampleBlock()
{
    return result_header;
}


BlockIO InterpreterSelectQuery::execute()
{
    Pipeline pipeline;
    executeImpl(pipeline, input, only_analyze);
    executeUnion(pipeline);

    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}

BlockInputStreams InterpreterSelectQuery::executeWithMultipleStreams()
{
    Pipeline pipeline;
    executeImpl(pipeline, input, only_analyze);
    return pipeline.streams;
}

InterpreterSelectQuery::AnalysisResult InterpreterSelectQuery::analyzeExpressions(QueryProcessingStage::Enum from_stage, bool dry_run)
{
    AnalysisResult res;

    /// Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
    res.first_stage = from_stage < QueryProcessingStage::WithMergeableState
        && to_stage >= QueryProcessingStage::WithMergeableState;
    /// Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.
    res.second_stage = from_stage <= QueryProcessingStage::WithMergeableState
        && to_stage > QueryProcessingStage::WithMergeableState;

    /** First we compose a chain of actions and remember the necessary steps from it.
        *  Regardless of from_stage and to_stage, we will compose a complete sequence of actions to perform optimization and
        *  throw out unnecessary columns based on the entire query. In unnecessary parts of the query, we will not execute subqueries.
        */

    bool has_prewhere = false;
    bool has_where = false;
    size_t where_step_num;

    auto finalizeChain = [&](ExpressionActionsChain & chain)
    {
        chain.finalize();

        if (has_prewhere)
        {
            const ExpressionActionsChain::Step & step = chain.steps.at(0);
            res.prewhere_info->remove_prewhere_column = step.can_remove_required_output.at(0);

            Names columns_to_remove_after_sampling;
            for (size_t i = 1; i < step.required_output.size(); ++i)
            {
                if (step.can_remove_required_output[i])
                    columns_to_remove_after_sampling.push_back(step.required_output[i]);
            }

            if (!columns_to_remove_after_sampling.empty())
            {
                auto columns = res.prewhere_info->prewhere_actions->getSampleBlock().getNamesAndTypesList();
                ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(columns, context);
                for (const auto & column : columns_to_remove_after_sampling)
                    actions->add(ExpressionAction::removeColumn(column));

                res.prewhere_info->after_sampling_actions = std::move(actions);
            }
        }
        if (has_where)
            res.remove_where_filter = chain.steps.at(where_step_num).can_remove_required_output.at(0);

        has_prewhere = has_where = false;

        chain.clear();
    };

    {
        ExpressionActionsChain chain(context);

        ASTPtr sampling_expression = storage && query.sample_size() ? storage->getSamplingExpression() : nullptr;
        if (query_analyzer->appendPrewhere(chain, !res.first_stage, sampling_expression))
        {
            has_prewhere = true;

            res.prewhere_info = std::make_shared<PrewhereInfo>(
                    chain.steps.front().actions, query.prewhere_expression->getColumnName());

            chain.addStep();
        }

        res.need_aggregate = query_analyzer->hasAggregation();

        query_analyzer->appendArrayJoin(chain, dry_run || !res.first_stage);

        if (query_analyzer->appendJoin(chain, dry_run || !res.first_stage))
        {
            res.has_join = true;
            res.before_join = chain.getLastActions();
            chain.addStep();
        }

        if (query_analyzer->appendWhere(chain, dry_run || !res.first_stage))
        {
            where_step_num = chain.steps.size() - 1;
            has_where = res.has_where = true;
            res.before_where = chain.getLastActions();
            chain.addStep();
        }

        if (res.need_aggregate)
        {
            query_analyzer->appendGroupBy(chain, dry_run || !res.first_stage);
            query_analyzer->appendAggregateFunctionsArguments(chain, dry_run || !res.first_stage);
            res.before_aggregation = chain.getLastActions();

            finalizeChain(chain);

            if (query_analyzer->appendHaving(chain, dry_run || !res.second_stage))
            {
                res.has_having = true;
                res.before_having = chain.getLastActions();
                chain.addStep();
            }
        }

        /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
        query_analyzer->appendSelect(chain, dry_run || (res.need_aggregate ? !res.second_stage : !res.first_stage));
        res.selected_columns = chain.getLastStep().required_output;
        res.has_order_by = query_analyzer->appendOrderBy(chain, dry_run || (res.need_aggregate ? !res.second_stage : !res.first_stage));
        res.before_order_and_select = chain.getLastActions();
        chain.addStep();

        if (query_analyzer->appendLimitBy(chain, dry_run || !res.second_stage))
        {
            res.has_limit_by = true;
            res.before_limit_by = chain.getLastActions();
            chain.addStep();
        }

        query_analyzer->appendProjectResult(chain);
        res.final_projection = chain.getLastActions();

        finalizeChain(chain);
    }

    /// Before executing WHERE and HAVING, remove the extra columns from the block (mostly the aggregation keys).
    if (res.has_where)
        res.before_where->prependProjectInput();
    if (res.has_having)
        res.before_having->prependProjectInput();

    res.subqueries_for_sets = query_analyzer->getSubqueriesForSets();

    return res;
}


void InterpreterSelectQuery::executeImpl(Pipeline & pipeline, const BlockInputStreamPtr & prepared_input, bool dry_run)
{
    /** Streams of data. When the query is executed in parallel, we have several data streams.
     *  If there is no GROUP BY, then perform all operations before ORDER BY and LIMIT in parallel, then
     *  if there is an ORDER BY, then glue the streams using UnionBlockInputStream, and then MergeSortingBlockInputStream,
     *  if not, then glue it using UnionBlockInputStream,
     *  then apply LIMIT.
     *  If there is GROUP BY, then we will perform all operations up to GROUP BY, inclusive, in parallel;
     *  a parallel GROUP BY will glue streams into one,
     *  then perform the remaining operations with one resulting stream.
     */

    const Settings & settings = context.getSettingsRef();

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

    /// PREWHERE optimization
    if (storage)
    {
        if (!dry_run)
            from_stage = storage->getQueryProcessingStage(context);

        query_analyzer->makeSetsForIndex();

        auto optimize_prewhere = [&](auto & merge_tree)
        {
            SelectQueryInfo query_info;
            query_info.query = query_ptr;
            query_info.sets = query_analyzer->getPreparedSets();

            /// Try transferring some condition from WHERE to PREWHERE if enabled and viable
            if (settings.optimize_move_to_prewhere && query.where_expression && !query.prewhere_expression && !query.final())
                MergeTreeWhereOptimizer{query_info, context, merge_tree.getData(), query_analyzer->getRequiredSourceColumns(), log};
        };

        if (const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get()))
            optimize_prewhere(*merge_tree);
        else if (const StorageReplicatedMergeTree * replicated_merge_tree = dynamic_cast<const StorageReplicatedMergeTree *>(storage.get()))
            optimize_prewhere(*replicated_merge_tree);
    }

    AnalysisResult expressions;

    if (dry_run)
    {
        pipeline.streams.emplace_back(std::make_shared<NullBlockInputStream>(source_header));
        expressions = analyzeExpressions(QueryProcessingStage::FetchColumns, true);

        if (expressions.prewhere_info)
            pipeline.streams.back() = std::make_shared<FilterBlockInputStream>(
                    pipeline.streams.back(), expressions.prewhere_info->prewhere_actions,
                    expressions.prewhere_info->prewhere_column_name, expressions.prewhere_info->remove_prewhere_column
            );
    }
    else
    {
        if (prepared_input)
            pipeline.streams.push_back(prepared_input);

        expressions = analyzeExpressions(from_stage, false);

        if (from_stage == QueryProcessingStage::WithMergeableState &&
            to_stage == QueryProcessingStage::WithMergeableState)
            throw Exception("Distributed on Distributed is not supported", ErrorCodes::NOT_IMPLEMENTED);

        /** Read the data from Storage. from_stage - to what stage the request was completed in Storage. */
        executeFetchColumns(from_stage, pipeline, expressions.prewhere_info);

        LOG_TRACE(log, QueryProcessingStage::toString(from_stage) << " -> " << QueryProcessingStage::toString(to_stage));
    }

    if (to_stage > QueryProcessingStage::FetchColumns)
    {
        /// Now we will compose block streams that perform the necessary actions.

        /// Do I need to aggregate in a separate row rows that have not passed max_rows_to_group_by.
        bool aggregate_overflow_row =
            expressions.need_aggregate &&
            query.group_by_with_totals &&
            settings.max_rows_to_group_by &&
            settings.group_by_overflow_mode == OverflowMode::ANY &&
            settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;

        /// Do I need to immediately finalize the aggregate functions after the aggregation?
        bool aggregate_final =
            expressions.need_aggregate &&
            to_stage > QueryProcessingStage::WithMergeableState &&
            !query.group_by_with_totals && !query.group_by_with_rollup;

        if (expressions.first_stage)
        {
            if (expressions.has_join)
            {
                const ASTTableJoin & join = static_cast<const ASTTableJoin &>(*query.join()->table_join);
                if (join.kind == ASTTableJoin::Kind::Full || join.kind == ASTTableJoin::Kind::Right)
                    pipeline.stream_with_non_joined_data = expressions.before_join->createStreamWithNonJoinedDataIfFullOrRightJoin(
                        pipeline.firstStream()->getHeader(), settings.max_block_size);

                for (auto & stream : pipeline.streams)   /// Applies to all sources except stream_with_non_joined_data.
                    stream = std::make_shared<ExpressionBlockInputStream>(stream, expressions.before_join);
            }

            if (expressions.has_where)
                executeWhere(pipeline, expressions.before_where, expressions.remove_where_filter);

            if (expressions.need_aggregate)
                executeAggregation(pipeline, expressions.before_aggregation, aggregate_overflow_row, aggregate_final);
            else
            {
                executeExpression(pipeline, expressions.before_order_and_select);
                executeDistinct(pipeline, true, expressions.selected_columns);
            }

            /** For distributed query processing,
              *  if no GROUP, HAVING set,
              *  but there is an ORDER or LIMIT,
              *  then we will perform the preliminary sorting and LIMIT on the remote server.
              */
            if (!expressions.second_stage && !expressions.need_aggregate && !expressions.has_having)
            {
                if (expressions.has_order_by)
                    executeOrder(pipeline);

                if (expressions.has_order_by && query.limit_length)
                    executeDistinct(pipeline, false, expressions.selected_columns);

                if (query.limit_length)
                    executePreLimit(pipeline);
            }
        }

        if (expressions.second_stage)
        {
            bool need_second_distinct_pass = false;
            bool need_merge_streams = false;

            if (expressions.need_aggregate)
            {
                /// If you need to combine aggregated results from multiple servers
                if (!expressions.first_stage)
                    executeMergeAggregated(pipeline, aggregate_overflow_row, aggregate_final);

                if (!aggregate_final)
                {
                    if (query.group_by_with_totals)
                        executeTotalsAndHaving(pipeline, expressions.has_having, expressions.before_having, aggregate_overflow_row, !query.group_by_with_rollup);

                     if (query.group_by_with_rollup)
                        executeRollup(pipeline);
                }
                else if (expressions.has_having)
                    executeHaving(pipeline, expressions.before_having);

                executeExpression(pipeline, expressions.before_order_and_select);
                executeDistinct(pipeline, true, expressions.selected_columns);

                need_second_distinct_pass = query.distinct && pipeline.hasMoreThanOneStream();
            }
            else
            {
                need_second_distinct_pass = query.distinct && pipeline.hasMoreThanOneStream();

                if (query.group_by_with_totals && !aggregate_final)
                    executeTotalsAndHaving(pipeline, false, nullptr, aggregate_overflow_row, !query.group_by_with_rollup);

                if (query.group_by_with_rollup && !aggregate_final)
                    executeRollup(pipeline);
            }

            if (expressions.has_order_by)
            {
                /** If there is an ORDER BY for distributed query processing,
                  *  but there is no aggregation, then on the remote servers ORDER BY was made
                  *  - therefore, we merge the sorted streams from remote servers.
                  */
                if (!expressions.first_stage && !expressions.need_aggregate && !(query.group_by_with_totals && !aggregate_final))
                    executeMergeSorted(pipeline);
                else    /// Otherwise, just sort.
                    executeOrder(pipeline);
            }

            /** Optimization - if there are several sources and there is LIMIT, then first apply the preliminary LIMIT,
              * limiting the number of rows in each up to `offset + limit`.
              */
            if (query.limit_length && pipeline.hasMoreThanOneStream() && !query.distinct && !expressions.has_limit_by && !settings.extremes)
            {
                executePreLimit(pipeline);
            }

            if (need_second_distinct_pass
                || query.limit_length
                || query.limit_by_expression_list
                || pipeline.stream_with_non_joined_data)
            {
                need_merge_streams = true;
            }

            if (need_merge_streams)
                executeUnion(pipeline);

            /** If there was more than one stream,
              * then DISTINCT needs to be performed once again after merging all streams.
              */
            if (need_second_distinct_pass)
                executeDistinct(pipeline, false, expressions.selected_columns);

            if (expressions.has_limit_by)
            {
                executeExpression(pipeline, expressions.before_limit_by);
                executeLimitBy(pipeline);
            }

            /** We must do projection after DISTINCT because projection may remove some columns.
              */
            executeProjection(pipeline, expressions.final_projection);

            /** Extremes are calculated before LIMIT, but after LIMIT BY. This is Ok.
              */
            executeExtremes(pipeline);

            executeLimit(pipeline);
        }
    }

    if (!expressions.subqueries_for_sets.empty())
        executeSubqueriesInSetsAndJoins(pipeline, expressions.subqueries_for_sets);
}


static void getLimitLengthAndOffset(ASTSelectQuery & query, size_t & length, size_t & offset)
{
    length = 0;
    offset = 0;
    if (query.limit_length)
    {
        length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);
        if (query.limit_offset)
            offset = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_offset).value);
    }
}

void InterpreterSelectQuery::executeFetchColumns(
    QueryProcessingStage::Enum processing_stage, Pipeline & pipeline, const PrewhereInfoPtr & prewhere_info)
{

    const Settings & settings = context.getSettingsRef();

    /// Actions to calculate ALIAS if required.
    ExpressionActionsPtr alias_actions;
    /// Are ALIAS columns required for query execution?
    auto alias_columns_required = false;

    if (storage && !storage->getColumns().aliases.empty())
    {
        const auto & column_defaults = storage->getColumns().defaults;
        for (const auto & column : required_columns)
        {
            const auto default_it = column_defaults.find(column);
            if (default_it != std::end(column_defaults) && default_it->second.kind == ColumnDefaultKind::Alias)
            {
                alias_columns_required = true;
                break;
            }
        }

        if (alias_columns_required)
        {
            /// Columns required for prewhere actions.
            NameSet required_prewhere_columns;
            /// Columns required for prewhere actions which are aliases in storage.
            NameSet required_prewhere_aliases;
            Block prewhere_actions_result;
            if (prewhere_info)
            {
                auto required_columns = prewhere_info->prewhere_actions->getRequiredColumns();
                required_prewhere_columns.insert(required_columns.begin(), required_columns.end());
                prewhere_actions_result = prewhere_info->prewhere_actions->getSampleBlock();
            }

            /// We will create an expression to return all the requested columns, with the calculation of the required ALIAS columns.
            auto required_columns_expr_list = std::make_shared<ASTExpressionList>();
            /// Separate expression for columns used in prewhere.
            auto required_prewhere_columns_expr_list = std::make_shared<ASTExpressionList>();

            for (const auto & column : required_columns)
            {
                ASTPtr column_expr;
                const auto default_it = column_defaults.find(column);
                bool is_alias = default_it != std::end(column_defaults) && default_it->second.kind == ColumnDefaultKind::Alias;
                if (is_alias)
                    column_expr = setAlias(default_it->second.expression->clone(), column);
                else
                    column_expr = std::make_shared<ASTIdentifier>(column);

                if (required_prewhere_columns.count(column))
                {
                    required_prewhere_columns_expr_list->children.emplace_back(std::move(column_expr));

                    if (is_alias)
                        required_prewhere_aliases.insert(column);
                }
                else
                    required_columns_expr_list->children.emplace_back(std::move(column_expr));
            }

            /// Columns which we will get after prewhere execution.
            NamesAndTypesList additional_source_columns;
            /// Add columns which will be added by prewhere (otherwise we will remove them in project action).
            for (const auto & column : prewhere_actions_result)
            {
                if (prewhere_info->remove_prewhere_column && column.name == prewhere_info->prewhere_column_name)
                    continue;

                required_columns_expr_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
                additional_source_columns.emplace_back(column.name, column.type);
            }
            auto additional_source_columns_set = ext::map<NameSet>(additional_source_columns, [] (const auto & it) { return it.name; });

            alias_actions = ExpressionAnalyzer(required_columns_expr_list, context, storage, additional_source_columns).getActions(true);

            /// The set of required columns could be added as a result of adding an action to calculate ALIAS.
            required_columns = alias_actions->getRequiredColumns();

            /// Do not remove prewhere filter if it is a column which is used as alias.
            if (prewhere_info && prewhere_info->remove_prewhere_column)
                if (required_columns.end()
                    != std::find(required_columns.begin(), required_columns.end(), prewhere_info->prewhere_column_name))
                    prewhere_info->remove_prewhere_column = false;

            /// Remove columns which will be added by prewhere.
            size_t next_req_column_pos = 0;
            for (size_t i = 0; i < required_columns.size(); ++i)
            {
                if (!additional_source_columns_set.count(required_columns[i]))
                {
                    if (next_req_column_pos < i)
                        std::swap(required_columns[i], required_columns[next_req_column_pos]);
                    ++next_req_column_pos;
                }
            }
            required_columns.resize(next_req_column_pos);

            if (prewhere_info)
            {
                /// Don't remove columns which are needed to be aliased.
                auto new_actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions->getRequiredColumnsWithTypes(), context);
                for (const auto & action : prewhere_info->prewhere_actions->getActions())
                {
                    if (action.type != ExpressionAction::REMOVE_COLUMN
                        || required_columns.end() == std::find(required_columns.begin(), required_columns.end(), action.source_name))
                        new_actions->add(action);
                }
                prewhere_info->prewhere_actions = std::move(new_actions);

                prewhere_info->alias_actions = ExpressionAnalyzer(required_prewhere_columns_expr_list, context, storage).getActions(true, false);

                /// Add columns required by alias actions.
                auto required_aliased_columns = prewhere_info->alias_actions->getRequiredColumns();
                for (auto & column : required_aliased_columns)
                    if (!prewhere_actions_result.has(column))
                        if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column))
                            required_columns.push_back(column);

                /// Add columns required by prewhere actions.
                for (const auto & column : required_prewhere_columns)
                    if (required_prewhere_aliases.count(column) == 0)
                        if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column))
                            required_columns.push_back(column);
            }
        }
    }


    /// Limitation on the number of columns to read.
    /// It's not applied in 'only_analyze' mode, because the query could be analyzed without removal of unnecessary columns.
    if (!only_analyze && settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read)
        throw Exception("Limit for number of columns to read exceeded. "
            "Requested: " + toString(required_columns.size())
            + ", maximum: " + settings.max_columns_to_read.toString(),
            ErrorCodes::TOO_MANY_COLUMNS);

    size_t limit_length = 0;
    size_t limit_offset = 0;
    getLimitLengthAndOffset(query, limit_length, limit_offset);

    /** With distributed query processing, almost no computations are done in the threads,
     *  but wait and receive data from remote servers.
     *  If we have 20 remote servers, and max_threads = 8, then it would not be very good
     *  connect and ask only 8 servers at a time.
     *  To simultaneously query more remote servers,
     *  instead of max_threads, max_distributed_connections is used.
     */
    bool is_remote = false;
    if (storage && storage->isRemote())
    {
        is_remote = true;
        max_streams = settings.max_distributed_connections;
    }

    size_t max_block_size = settings.max_block_size;

    if (!max_block_size)
        throw Exception("Setting 'max_block_size' cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

    /** Optimization - if not specified DISTINCT, WHERE, GROUP, HAVING, ORDER, LIMIT BY but LIMIT is specified, and limit + offset < max_block_size,
     *  then as the block size we will use limit + offset (not to read more from the table than requested),
     *  and also set the number of threads to 1.
     */
    if (!query.distinct
        && !query.prewhere_expression
        && !query.where_expression
        && !query.group_expression_list
        && !query.having_expression
        && !query.order_expression_list
        && !query.limit_by_expression_list
        && query.limit_length
        && !query_analyzer->hasAggregation()
        && limit_length + limit_offset < max_block_size)
    {
        max_block_size = limit_length + limit_offset;
        max_streams = 1;
    }

    /// Initialize the initial data streams to which the query transforms are superimposed. Table or subquery or prepared input?
    if (!pipeline.streams.empty())
    {
        /// Prepared input.
    }
    else if (interpreter_subquery)
    {
        /// Subquery.
        /// If we need less number of columns that subquery have - update the interpreter.
        if (required_columns.size() < source_header.columns())
        {
            interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
                query.table(), getSubqueryContext(context), required_columns, QueryProcessingStage::Complete, subquery_depth + 1, only_analyze);

            if (query_analyzer->hasAggregation())
                interpreter_subquery->ignoreWithTotals();
        }

        pipeline.streams = interpreter_subquery->executeWithMultipleStreams();
    }
    else if (storage)
    {
        /// Table.

        if (max_streams == 0)
            throw Exception("Logical error: zero number of streams requested", ErrorCodes::LOGICAL_ERROR);

        /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads.
        if (max_streams > 1 && !is_remote)
            max_streams *= settings.max_streams_to_max_threads_ratio;

        SelectQueryInfo query_info;
        query_info.query = query_ptr;
        query_info.sets = query_analyzer->getPreparedSets();
        query_info.prewhere_info = prewhere_info;

        pipeline.streams = storage->read(required_columns, query_info, context, processing_stage, max_block_size, max_streams);

        if (pipeline.streams.empty())
        {
            pipeline.streams.emplace_back(std::make_shared<NullBlockInputStream>(storage->getSampleBlockForColumns(required_columns)));

            if (query_info.prewhere_info)
                pipeline.streams.back() = std::make_shared<FilterBlockInputStream>(
                        pipeline.streams.back(), prewhere_info->prewhere_actions,
                        prewhere_info->prewhere_column_name, prewhere_info->remove_prewhere_column
                );
        }

        pipeline.transform([&](auto & stream)
        {
            stream->addTableLock(table_lock);
        });

        /// Set the limits and quota for reading data, the speed and time of the query.
        {
            IProfilingBlockInputStream::LocalLimits limits;
            limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
            limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
            limits.max_execution_time = settings.max_execution_time;
            limits.timeout_overflow_mode = settings.timeout_overflow_mode;

            /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
              *  because the initiating server has a summary of the execution of the request on all servers.
              *
              * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
              *  additionally on each remote server, because these limits are checked per block of data processed,
              *  and remote servers may process way more blocks of data than are received by initiator.
              */
            if (to_stage == QueryProcessingStage::Complete)
            {
                limits.min_execution_speed = settings.min_execution_speed;
                limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;
            }

            QuotaForIntervals & quota = context.getQuota();

            pipeline.transform([&](auto & stream)
            {
                if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
                {
                    p_stream->setLimits(limits);

                    if (to_stage == QueryProcessingStage::Complete)
                        p_stream->setQuota(quota);
                }
            });
        }
    }
    else
        throw Exception("Logical error in InterpreterSelectQuery: nowhere to read", ErrorCodes::LOGICAL_ERROR);

    /// Aliases in table declaration.
    if (processing_stage == QueryProcessingStage::FetchColumns && alias_actions)
    {
        pipeline.transform([&](auto & stream)
        {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, alias_actions);
        });
    }
}


void InterpreterSelectQuery::executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expression, bool remove_fiter)
{
    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<FilterBlockInputStream>(stream, expression, query.where_expression->getColumnName(), remove_fiter);
    });
}


void InterpreterSelectQuery::executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expression, bool overflow_row, bool final)
{
    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<ConvertColumnWithDictionaryToFullBlockInputStream>(
                std::make_shared<ExpressionBlockInputStream>(stream, expression));
    });

    Names key_names;
    AggregateDescriptions aggregates;
    query_analyzer->getAggregateInfo(key_names, aggregates);

    Block header = pipeline.firstStream()->getHeader();
    ColumnNumbers keys;
    for (const auto & name : key_names)
        keys.push_back(header.getPositionByName(name));
    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(header.getPositionByName(name));

    const Settings & settings = context.getSettingsRef();

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;

    Aggregator::Params params(header, keys, aggregates,
        overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
        settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
        context.getTemporaryPath());

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams, pipeline.stream_with_non_joined_data, params, final,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads
                ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                : static_cast<size_t>(settings.max_threads));

        pipeline.stream_with_non_joined_data = nullptr;
        pipeline.streams.resize(1);
    }
    else
    {
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());
        else
            pipeline.streams.resize(1);

        if (pipeline.stream_with_non_joined_data)
            inputs.push_back(pipeline.stream_with_non_joined_data);

        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(std::make_shared<ConcatBlockInputStream>(inputs), params, final);

        pipeline.stream_with_non_joined_data = nullptr;
    }
}


void InterpreterSelectQuery::executeMergeAggregated(Pipeline & pipeline, bool overflow_row, bool final)
{
    Names key_names;
    AggregateDescriptions aggregates;
    query_analyzer->getAggregateInfo(key_names, aggregates);

    Block header = pipeline.firstStream()->getHeader();

    ColumnNumbers keys;
    for (const auto & name : key_names)
        keys.push_back(header.getPositionByName(name));

    /** There are two modes of distributed aggregation.
      *
      * 1. In different threads read from the remote servers blocks.
      * Save all the blocks in the RAM. Merge blocks.
      * If the aggregation is two-level - parallelize to the number of buckets.
      *
      * 2. In one thread, read blocks from different servers in order.
      * RAM stores only one block from each server.
      * If the aggregation is a two-level aggregation, we consistently merge the blocks of each next level.
      *
      * The second option consumes less memory (up to 256 times less)
      *  in the case of two-level aggregation, which is used for large results after GROUP BY,
      *  but it can work more slowly.
      */

    Aggregator::Params params(header, keys, aggregates, overflow_row);

    const Settings & settings = context.getSettingsRef();

    if (!settings.distributed_aggregation_memory_efficient)
    {
        /// We union several sources into one, parallelizing the work.
        executeUnion(pipeline);

        /// Now merge the aggregated blocks
        pipeline.firstStream() = std::make_shared<MergingAggregatedBlockInputStream>(pipeline.firstStream(), params, final, settings.max_threads);
    }
    else
    {
        pipeline.firstStream() = std::make_shared<MergingAggregatedMemoryEfficientBlockInputStream>(pipeline.streams, params, final,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads
                ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                : static_cast<size_t>(settings.max_threads));

        pipeline.streams.resize(1);
    }
}


void InterpreterSelectQuery::executeHaving(Pipeline & pipeline, const ExpressionActionsPtr & expression)
{
    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<FilterBlockInputStream>(stream, expression, query.having_expression->getColumnName());
    });
}


void InterpreterSelectQuery::executeTotalsAndHaving(Pipeline & pipeline, bool has_having, const ExpressionActionsPtr & expression, bool overflow_row, bool final)
{
    executeUnion(pipeline);

    const Settings & settings = context.getSettingsRef();

    pipeline.firstStream() = std::make_shared<TotalsHavingBlockInputStream>(
        pipeline.firstStream(), overflow_row, expression,
        has_having ? query.having_expression->getColumnName() : "", settings.totals_mode, settings.totals_auto_threshold, final);
}

void InterpreterSelectQuery::executeRollup(Pipeline & pipeline)
{
    executeUnion(pipeline);

    Names key_names;
    AggregateDescriptions aggregates;
    query_analyzer->getAggregateInfo(key_names, aggregates);

    Block header = pipeline.firstStream()->getHeader();

    ColumnNumbers keys;

    for (const auto & name : key_names)
        keys.push_back(header.getPositionByName(name));

    const Settings & settings = context.getSettingsRef();

    Aggregator::Params params(header, keys, aggregates,
        false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
        settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
        SettingUInt64(0), SettingUInt64(0),
        settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
        context.getTemporaryPath());

    pipeline.firstStream() = std::make_shared<RollupBlockInputStream>(pipeline.firstStream(), params);
}


void InterpreterSelectQuery::executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expression)
{
    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
    });
}


static SortDescription getSortDescription(ASTSelectQuery & query)
{
    SortDescription order_descr;
    order_descr.reserve(query.order_expression_list->children.size());
    for (const auto & elem : query.order_expression_list->children)
    {
        String name = elem->children.front()->getColumnName();
        const ASTOrderByElement & order_by_elem = typeid_cast<const ASTOrderByElement &>(*elem);

        std::shared_ptr<Collator> collator;
        if (order_by_elem.collation)
            collator = std::make_shared<Collator>(typeid_cast<const ASTLiteral &>(*order_by_elem.collation).value.get<String>());

        order_descr.emplace_back(name, order_by_elem.direction, order_by_elem.nulls_direction, collator);
    }

    return order_descr;
}

static size_t getLimitForSorting(ASTSelectQuery & query)
{
    /// Partial sort can be done if there is LIMIT but no DISTINCT or LIMIT BY.
    size_t limit = 0;
    if (!query.distinct && !query.limit_by_expression_list)
    {
        size_t limit_length = 0;
        size_t limit_offset = 0;
        getLimitLengthAndOffset(query, limit_length, limit_offset);
        limit = limit_length + limit_offset;
    }

    return limit;
}


void InterpreterSelectQuery::executeOrder(Pipeline & pipeline)
{
    SortDescription order_descr = getSortDescription(query);
    size_t limit = getLimitForSorting(query);

    const Settings & settings = context.getSettingsRef();

    pipeline.transform([&](auto & stream)
    {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(), order_descr, settings.max_block_size, limit,
        settings.max_bytes_before_external_sort, context.getTemporaryPath());
}


void InterpreterSelectQuery::executeMergeSorted(Pipeline & pipeline)
{
    SortDescription order_descr = getSortDescription(query);
    size_t limit = getLimitForSorting(query);

    const Settings & settings = context.getSettingsRef();

    /// If there are several streams, then we merge them into one
    if (pipeline.hasMoreThanOneStream())
    {
        /** MergingSortedBlockInputStream reads the sources sequentially.
          * To make the data on the remote servers prepared in parallel, we wrap it in AsynchronousBlockInputStream.
          */
        pipeline.transform([&](auto & stream)
        {
            stream = std::make_shared<AsynchronousBlockInputStream>(stream);
        });

        /// Merge the sorted sources into one sorted source.
        pipeline.firstStream() = std::make_shared<MergingSortedBlockInputStream>(pipeline.streams, order_descr, settings.max_block_size, limit);
        pipeline.streams.resize(1);
    }
}


void InterpreterSelectQuery::executeProjection(Pipeline & pipeline, const ExpressionActionsPtr & expression)
{
    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
    });
}


void InterpreterSelectQuery::executeDistinct(Pipeline & pipeline, bool before_order, Names columns)
{
    if (query.distinct)
    {
        const Settings & settings = context.getSettingsRef();

        size_t limit_length = 0;
        size_t limit_offset = 0;
        getLimitLengthAndOffset(query, limit_length, limit_offset);

        size_t limit_for_distinct = 0;

        /// If after this stage of DISTINCT ORDER BY is not executed, then you can get no more than limit_length + limit_offset of different rows.
        if (!query.order_expression_list || !before_order)
            limit_for_distinct = limit_length + limit_offset;

        pipeline.transform([&](auto & stream)
        {
            SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);
            stream = std::make_shared<DistinctBlockInputStream>(stream, limits, limit_for_distinct, columns);
        });
    }
}


void InterpreterSelectQuery::executeUnion(Pipeline & pipeline)
{
    /// If there are still several streams, then we combine them into one
    if (pipeline.hasMoreThanOneStream())
    {
        pipeline.firstStream() = std::make_shared<UnionBlockInputStream<>>(pipeline.streams, pipeline.stream_with_non_joined_data, max_streams);
        pipeline.stream_with_non_joined_data = nullptr;
        pipeline.streams.resize(1);
    }
    else if (pipeline.stream_with_non_joined_data)
    {
        pipeline.streams.push_back(pipeline.stream_with_non_joined_data);
        pipeline.stream_with_non_joined_data = nullptr;
    }
}


/// Preliminary LIMIT - is used in every source, if there are several sources, before they are combined.
void InterpreterSelectQuery::executePreLimit(Pipeline & pipeline)
{
    size_t limit_length = 0;
    size_t limit_offset = 0;
    getLimitLengthAndOffset(query, limit_length, limit_offset);

    /// If there is LIMIT
    if (query.limit_length)
    {
        pipeline.transform([&](auto & stream)
        {
            stream = std::make_shared<LimitBlockInputStream>(stream, limit_length + limit_offset, 0, false);
        });
    }
}


void InterpreterSelectQuery::executeLimitBy(Pipeline & pipeline)
{
    if (!query.limit_by_value || !query.limit_by_expression_list)
        return;

    Names columns;
    for (const auto & elem : query.limit_by_expression_list->children)
        columns.emplace_back(elem->getColumnName());

    size_t value = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_by_value).value);

    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<LimitByBlockInputStream>(stream, value, columns);
    });
}


bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery & query)
{
    if (query.group_by_with_totals)
        return true;

    /** NOTE You can also check that the table in the subquery is distributed, and that it only looks at one shard.
      * In other cases, totals will be computed on the initiating server of the query, and it is not necessary to read the data to the end.
      */

    auto query_table = query.table();
    if (query_table)
    {
        auto ast_union = typeid_cast<const ASTSelectWithUnionQuery *>(query_table.get());
        if (ast_union)
        {
            for (const auto & elem : ast_union->list_of_selects->children)
                if (hasWithTotalsInAnySubqueryInFromClause(typeid_cast<const ASTSelectQuery &>(*elem)))
                    return true;
        }
    }

    return false;
}


void InterpreterSelectQuery::executeLimit(Pipeline & pipeline)
{
    size_t limit_length = 0;
    size_t limit_offset = 0;
    getLimitLengthAndOffset(query, limit_length, limit_offset);

    /// If there is LIMIT
    if (query.limit_length)
    {
        /** Rare case:
          *  if there is no WITH TOTALS and there is a subquery in FROM, and there is WITH TOTALS on one of the levels,
          *  then when using LIMIT, you should read the data to the end, rather than cancel the query earlier,
          *  because if you cancel the query, we will not get `totals` data from the remote server.
          *
          * Another case:
          *  if there is WITH TOTALS and there is no ORDER BY, then read the data to the end,
          *  otherwise TOTALS is counted according to incomplete data.
          */
        bool always_read_till_end = false;

        if (query.group_by_with_totals && !query.order_expression_list)
            always_read_till_end = true;

        if (!query.group_by_with_totals && hasWithTotalsInAnySubqueryInFromClause(query))
            always_read_till_end = true;

        pipeline.transform([&](auto & stream)
        {
            stream = std::make_shared<LimitBlockInputStream>(stream, limit_length, limit_offset, always_read_till_end);
        });
    }
}


void InterpreterSelectQuery::executeExtremes(Pipeline & pipeline)
{
    if (!context.getSettingsRef().extremes)
        return;

    pipeline.transform([&](auto & stream)
    {
        if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
            p_stream->enableExtremes();
    });
}


void InterpreterSelectQuery::executeSubqueriesInSetsAndJoins(Pipeline & pipeline, SubqueriesForSets & subqueries_for_sets)
{
    const Settings & settings = context.getSettingsRef();

    executeUnion(pipeline);
    pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(
        pipeline.firstStream(), subqueries_for_sets,
        SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode));
}


void InterpreterSelectQuery::ignoreWithTotals()
{
    query.group_by_with_totals = false;
}


void InterpreterSelectQuery::initSettings()
{
    if (query.settings)
        InterpreterSetQuery(query.settings, context).executeForCurrentContext();
}

}

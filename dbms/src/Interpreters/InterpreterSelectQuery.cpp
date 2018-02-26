#include <optional>

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
#include <DataStreams/DistinctSortedBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/TotalsHavingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/InterpreterSelectQuery.h>
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


namespace ProfileEvents
{
    extern const Event SelectQuery;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int ILLEGAL_FINAL;
    extern const int ILLEGAL_PREWHERE;
    extern const int TOO_MUCH_COLUMNS;
    extern const int LOGICAL_ERROR;
}


InterpreterSelectQuery::~InterpreterSelectQuery() = default;


void InterpreterSelectQuery::init(const Names & required_column_names)
{
    ProfileEvents::increment(ProfileEvents::SelectQuery);

    initSettings();
    const Settings & settings = context.getSettingsRef();

    if (settings.limits.max_subquery_depth && subquery_depth > settings.limits.max_subquery_depth)
        throw Exception("Too deep subqueries. Maximum: " + settings.limits.max_subquery_depth.toString(),
            ErrorCodes::TOO_DEEP_SUBQUERIES);

    max_streams = settings.max_threads;

    if (is_first_select_inside_union_all)
    {
        /// Create a SELECT query chain.
        InterpreterSelectQuery * interpreter = this;
        ASTPtr tail = query.next_union_all;

        while (tail)
        {
            ASTPtr head = tail;

            ASTSelectQuery & head_query = static_cast<ASTSelectQuery &>(*head);
            tail = head_query.next_union_all;

            interpreter->next_select_in_union_all = std::make_unique<InterpreterSelectQuery>(head, context, to_stage, subquery_depth);
            interpreter = interpreter->next_select_in_union_all.get();
        }
    }

    if (is_first_select_inside_union_all && hasAsterisk())
    {
        basicInit();

        // We execute this code here, because otherwise the following kind of query would not work
        // SELECT X FROM (SELECT * FROM (SELECT 1 AS X, 2 AS Y) UNION ALL SELECT 3, 4)
        // because the asterisk is replaced with columns only when query_analyzer objects are created in basicInit().
        renameColumns();

        if (!required_column_names.empty() && (source_header.columns() != required_column_names.size()))
        {
            rewriteExpressionList(required_column_names);
            /// Now there is obsolete information to execute the query. We update this information.
            initQueryAnalyzer();
        }
    }
    else
    {
        renameColumns();
        if (!required_column_names.empty())
        {
            rewriteExpressionList(required_column_names);

            if (is_first_select_inside_union_all)
            {
                for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
                    p->query_analyzer = std::make_unique<ExpressionAnalyzer>(
                        p->query_ptr, p->context, p->storage, p->source_header.getNamesAndTypesList(), p->subquery_depth,
                        false, p->query_analyzer->getSubqueriesForSets());
            }
        }

        basicInit();
    }
}


void InterpreterSelectQuery::basicInit()
{
    /// Read from prepared input.
    if (input)
    {
        source_header = input->getHeader();
    }
    else
    {
        auto table_expression = query.table();

        /// Read from subquery.
        if (table_expression && typeid_cast<const ASTSelectQuery *>(table_expression.get()))
        {
            source_header = InterpreterSelectQuery::getSampleBlock(table_expression, context);
        }
        else
        {
            /// Read from table function.
            if (table_expression && typeid_cast<const ASTFunction *>(table_expression.get()))
            {
                /// Get the table function
                TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(
                    typeid_cast<const ASTFunction *>(table_expression.get())->name, context);
                /// Run it and remember the result
                storage = table_function_ptr->execute(table_expression, context);
            }
            else
            {
                /// Read from table.
                String database_name;
                String table_name;

                getDatabaseAndTableNames(database_name, table_name);

                storage = context.getTable(database_name, table_name);
            }

            table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);

            /// TODO This looks weird.
            source_header = storage->getSampleBlockNonMaterialized();
        }
    }

    if (!source_header)
        throw Exception("There are no available columns", ErrorCodes::THERE_IS_NO_COLUMN);

    query_analyzer = std::make_unique<ExpressionAnalyzer>(query_ptr, context, storage, source_header.getNamesAndTypesList(), subquery_depth, !only_analyze);

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
}

void InterpreterSelectQuery::initQueryAnalyzer()
{
    query_analyzer = std::make_unique<ExpressionAnalyzer>(query_ptr, context, storage, source_header.getNamesAndTypesList(), subquery_depth, !only_analyze);

    for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
        p->query_analyzer = std::make_unique<ExpressionAnalyzer>(p->query_ptr, p->context, p->storage, p->source_header.getNamesAndTypesList(), p->subquery_depth, !only_analyze);
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_,
    const BlockInputStreamPtr & input)
    : InterpreterSelectQuery(query_ptr_, context_, {}, to_stage_, subquery_depth_, input)
{
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const Names & required_column_names_,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_,
    const BlockInputStreamPtr & input)
    : query_ptr(query_ptr_)
    , query(typeid_cast<ASTSelectQuery &>(*query_ptr))
    , context(context_)
    , to_stage(to_stage_)
    , subquery_depth(subquery_depth_)
    , is_first_select_inside_union_all(query.isUnionAllHead())
    , input(input)
    , log(&Logger::get("InterpreterSelectQuery"))
{
    init(required_column_names_);
}


InterpreterSelectQuery::InterpreterSelectQuery(OnlyAnalyzeTag, const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_)
    , query(typeid_cast<ASTSelectQuery &>(*query_ptr))
    , context(context_)
    , to_stage(QueryProcessingStage::Complete)
    , subquery_depth(0)
    , is_first_select_inside_union_all(false), only_analyze(true)
    , log(&Logger::get("InterpreterSelectQuery"))
{
    init({});
}


bool InterpreterSelectQuery::hasAsterisk() const
{
    if (query.hasAsterisk())
        return true;

    if (is_first_select_inside_union_all)
        for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
            if (p->query.hasAsterisk())
                return true;

    return false;
}

void InterpreterSelectQuery::renameColumns()
{
    if (is_first_select_inside_union_all)
        for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
            p->query.renameColumns(query);
}

void InterpreterSelectQuery::rewriteExpressionList(const Names & required_column_names)
{
    if (query.distinct)
        return;

    if (is_first_select_inside_union_all)
        for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
            if (p->query.distinct)
                return;

    query.rewriteSelectExpressionList(required_column_names);

    if (is_first_select_inside_union_all)
        for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
            p->query.rewriteSelectExpressionList(required_column_names);
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
    Pipeline pipeline;
    executeWithoutUnionImpl(pipeline, std::make_shared<OneBlockInputStream>(source_header));
    auto res = pipeline.firstStream()->getHeader();
    return res;
}


Block InterpreterSelectQuery::getSampleBlock(const ASTPtr & query_ptr_, const Context & context_)
{
    return InterpreterSelectQuery(OnlyAnalyzeTag(), query_ptr_, context_).getSampleBlock();
}


BlockIO InterpreterSelectQuery::execute()
{
    Pipeline pipeline;
    executeWithoutUnionImpl(pipeline, input);
    executeUnion(pipeline);

    /// Constraints on the result, the quota on the result, and also callback for progress.
    if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(pipeline.firstStream().get()))
    {
        /// Constraints apply only to the final result.
        if (to_stage == QueryProcessingStage::Complete && subquery_depth == 0)
        {
            const Settings & settings = context.getSettingsRef();

            IProfilingBlockInputStream::LocalLimits limits;
            limits.mode = IProfilingBlockInputStream::LIMITS_CURRENT;
            limits.max_rows_to_read = settings.limits.max_result_rows;
            limits.max_bytes_to_read = settings.limits.max_result_bytes;
            limits.read_overflow_mode = settings.limits.result_overflow_mode;

            stream->setLimits(limits);
            stream->setQuota(context.getQuota());
        }
    }

    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}

BlockInputStreams InterpreterSelectQuery::executeWithoutUnion()
{
    Pipeline pipeline;
    executeWithoutUnionImpl(pipeline, input);
    return pipeline.streams;
}

void InterpreterSelectQuery::executeWithoutUnionImpl(Pipeline & pipeline, const BlockInputStreamPtr & input)
{
    if (input)
        pipeline.streams.push_back(input);

    if (is_first_select_inside_union_all)
    {
        executeSingleQuery(pipeline);
        for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
        {
            Pipeline other_pipeline;
            p->executeSingleQuery(other_pipeline);
            pipeline.streams.insert(pipeline.streams.end(), other_pipeline.streams.begin(), other_pipeline.streams.end());
        }

        pipeline.transform([&](auto & stream)
        {
            stream = std::make_shared<MaterializingBlockInputStream>(stream);
        });
    }
    else
        executeSingleQuery(pipeline);
}


InterpreterSelectQuery::AnalysisResult InterpreterSelectQuery::analyzeExpressions(QueryProcessingStage::Enum from_stage)
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

    {
        ExpressionActionsChain chain;

        res.need_aggregate = query_analyzer->hasAggregation();

        query_analyzer->appendArrayJoin(chain, !res.first_stage);

        if (query_analyzer->appendJoin(chain, !res.first_stage))
        {
            res.has_join = true;
            res.before_join = chain.getLastActions();
            chain.addStep();
        }

        if (query_analyzer->appendWhere(chain, !res.first_stage))
        {
            res.has_where = true;
            res.before_where = chain.getLastActions();
            chain.addStep();
        }

        if (res.need_aggregate)
        {
            query_analyzer->appendGroupBy(chain, !res.first_stage);
            query_analyzer->appendAggregateFunctionsArguments(chain, !res.first_stage);
            res.before_aggregation = chain.getLastActions();

            chain.finalize();
            chain.clear();

            if (query_analyzer->appendHaving(chain, !res.second_stage))
            {
                res.has_having = true;
                res.before_having = chain.getLastActions();
                chain.addStep();
            }
        }

        /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
        query_analyzer->appendSelect(chain, res.need_aggregate ? !res.second_stage : !res.first_stage);
        res.selected_columns = chain.getLastStep().required_output;
        res.has_order_by = query_analyzer->appendOrderBy(chain, res.need_aggregate ? !res.second_stage : !res.first_stage);
        res.before_order_and_select = chain.getLastActions();
        chain.addStep();

        query_analyzer->appendProjectResult(chain);
        res.final_projection = chain.getLastActions();

        chain.finalize();
        chain.clear();
    }

    /// Before executing WHERE and HAVING, remove the extra columns from the block (mostly the aggregation keys).
    if (res.has_where)
        res.before_where->prependProjectInput();
    if (res.has_having)
        res.before_having->prependProjectInput();

    res.subqueries_for_sets = query_analyzer->getSubqueriesForSets();

    return res;
}


void InterpreterSelectQuery::executeSingleQuery(Pipeline & pipeline)
{
    /** Streams of data. When the query is executed in parallel, we have several data streams.
     *  If there is no GROUP BY, then perform all operations before ORDER BY and LIMIT in parallel, then
     *  if there is an ORDER BY, then glue the streams using UnionBlockInputStream, and then MergeSortingBlockInputStream,
     *  if not, then glue it using UnionBlockInputStream,
     *  then apply LIMIT.
     *  If there is GROUP BY, then we will perform all operations up to GROUP BY, inclusive, in parallel;
     *  a parallel GROUP BY will glue streams into one,
     *  then perform the remaining operations with one resulting stream.
     *  If the query is a member of the UNION ALL chain and does not contain GROUP BY, ORDER BY, DISTINCT, or LIMIT,
     *  then the data sources are merged not at this level, but at the upper level.
     */

    union_within_single_query = false;

    /** Read the data from Storage. from_stage - to what stage the request was completed in Storage. */
    QueryProcessingStage::Enum from_stage = executeFetchColumns(pipeline);

    LOG_TRACE(log, QueryProcessingStage::toString(from_stage) << " -> " << QueryProcessingStage::toString(to_stage));

    AnalysisResult expressions = analyzeExpressions(from_stage);

    const Settings & settings = context.getSettingsRef();

    if (to_stage > QueryProcessingStage::FetchColumns)
    {
        /// Now we will compose block streams that perform the necessary actions.

        /// Do I need to aggregate in a separate row rows that have not passed max_rows_to_group_by.
        bool aggregate_overflow_row =
            expressions.need_aggregate &&
            query.group_by_with_totals &&
            settings.limits.max_rows_to_group_by &&
            settings.limits.group_by_overflow_mode == OverflowMode::ANY &&
            settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;

        /// Do I need to immediately finalize the aggregate functions after the aggregation?
        bool aggregate_final =
            expressions.need_aggregate &&
            to_stage > QueryProcessingStage::WithMergeableState &&
            !query.group_by_with_totals;

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
                executeWhere(pipeline, expressions.before_where);

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
            bool need_second_distinct_pass;

            if (expressions.need_aggregate)
            {
                /// If you need to combine aggregated results from multiple servers
                if (!expressions.first_stage)
                    executeMergeAggregated(pipeline, aggregate_overflow_row, aggregate_final);

                if (!aggregate_final)
                    executeTotalsAndHaving(pipeline, expressions.has_having, expressions.before_having, aggregate_overflow_row);
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
                    executeTotalsAndHaving(pipeline, false, nullptr, aggregate_overflow_row);
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

            executeProjection(pipeline, expressions.final_projection);

            /// At this stage, we can calculate the minimums and maximums, if necessary.
            if (settings.extremes)
            {
                pipeline.transform([&](auto & stream)
                {
                    if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
                        p_stream->enableExtremes();
                });
            }

            /** Optimization - if there are several sources and there is LIMIT, then first apply the preliminary LIMIT,
              * limiting the number of rows in each up to `offset + limit`.
              */
            if (query.limit_length && pipeline.hasMoreThanOneStream() && !query.distinct && !query.limit_by_expression_list)
                executePreLimit(pipeline);

            if (pipeline.stream_with_non_joined_data || need_second_distinct_pass)
                union_within_single_query = true;

            /// To execute LIMIT BY we should merge all streams together.
            if (query.limit_by_expression_list && pipeline.hasMoreThanOneStream())
                union_within_single_query = true;

            if (union_within_single_query)
                executeUnion(pipeline);

            if (pipeline.streams.size() == 1)
            {
                /** If there was more than one stream,
                  * then DISTINCT needs to be performed once again after merging all streams.
                  */
                if (need_second_distinct_pass)
                    executeDistinct(pipeline, false, Names());

                executeLimitBy(pipeline);
                executeLimit(pipeline);
            }
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

QueryProcessingStage::Enum InterpreterSelectQuery::executeFetchColumns(Pipeline & pipeline)
{
    /// The subquery interpreter, if the subquery
    std::optional<InterpreterSelectQuery> interpreter_subquery;

    /// List of columns to read to execute the query.
    Names required_columns = query_analyzer->getRequiredColumns();
    /// Actions to calculate ALIAS if required.
    ExpressionActionsPtr alias_actions;
    /// Are ALIAS columns required for query execution?
    auto alias_columns_required = false;

    if (storage && !storage->alias_columns.empty())
    {
        for (const auto & column : required_columns)
        {
            const auto default_it = storage->column_defaults.find(column);
            if (default_it != std::end(storage->column_defaults) && default_it->second.type == ColumnDefaultType::Alias)
            {
                alias_columns_required = true;
                break;
            }
        }

        if (alias_columns_required)
        {
            /// We will create an expression to return all the requested columns, with the calculation of the required ALIAS columns.
            auto required_columns_expr_list = std::make_shared<ASTExpressionList>();

            for (const auto & column : required_columns)
            {
                const auto default_it = storage->column_defaults.find(column);
                if (default_it != std::end(storage->column_defaults) && default_it->second.type == ColumnDefaultType::Alias)
                    required_columns_expr_list->children.emplace_back(setAlias(default_it->second.expression->clone(), column));
                else
                    required_columns_expr_list->children.emplace_back(std::make_shared<ASTIdentifier>(column));
            }

            alias_actions = ExpressionAnalyzer{required_columns_expr_list, context, storage, source_header.getNamesAndTypesList()}.getActions(true);

            /// The set of required columns could be added as a result of adding an action to calculate ALIAS.
            required_columns = alias_actions->getRequiredColumns();
        }
    }

    auto query_table = query.table();
    if (query_table && typeid_cast<ASTSelectQuery *>(query_table.get()))
    {
        /** There are no limits on the maximum size of the result for the subquery.
         *  Since the result of the query is not the result of the entire query.
         */
        Context subquery_context = context;
        Settings subquery_settings = context.getSettings();
        subquery_settings.limits.max_result_rows = 0;
        subquery_settings.limits.max_result_bytes = 0;
        /// The calculation of extremes does not make sense and is not necessary (if you do it, then the extremes of the subquery can be taken for whole query).
        subquery_settings.extremes = 0;
        subquery_context.setSettings(subquery_settings);

        interpreter_subquery.emplace(
            query_table, subquery_context, required_columns, QueryProcessingStage::Complete, subquery_depth + 1);

        /// If there is an aggregation in the outer query, WITH TOTALS is ignored in the subquery.
        if (query_analyzer->hasAggregation())
            interpreter_subquery->ignoreWithTotals();
    }

    const Settings & settings = context.getSettingsRef();

    /// Limitation on the number of columns to read.
    if (settings.limits.max_columns_to_read && required_columns.size() > settings.limits.max_columns_to_read)
        throw Exception("Limit for number of columns to read exceeded. "
            "Requested: " + toString(required_columns.size())
            + ", maximum: " + settings.limits.max_columns_to_read.toString(),
            ErrorCodes::TOO_MUCH_COLUMNS);

    size_t limit_length = 0;
    size_t limit_offset = 0;
    getLimitLengthAndOffset(query, limit_length, limit_offset);

    size_t max_block_size = settings.max_block_size;

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
        && limit_length + limit_offset < settings.max_block_size)
    {
        max_block_size = limit_length + limit_offset;
        max_streams = 1;
    }

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

    query_analyzer->makeSetsForIndex();

    /// Initialize the initial data streams to which the query transforms are superimposed. Table or subquery or prepared input?
    if (!pipeline.streams.empty())
    {
        /// Prepared input.
    }
    else if (interpreter_subquery)
    {
        /// Subquery.
        interpreter_subquery->executeWithoutUnionImpl(pipeline, {});
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

        /// PREWHERE optimization
        {
            auto optimize_prewhere = [&](auto & merge_tree)
            {
                /// Try transferring some condition from WHERE to PREWHERE if enabled and viable
                if (settings.optimize_move_to_prewhere && query.where_expression && !query.prewhere_expression && !query.final())
                    MergeTreeWhereOptimizer{query_info, context, merge_tree.getData(), required_columns, log};
            };

            if (const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get()))
                optimize_prewhere(*merge_tree);
            else if (const StorageReplicatedMergeTree * merge_tree = dynamic_cast<const StorageReplicatedMergeTree *>(storage.get()))
                optimize_prewhere(*merge_tree);
        }

        /// If there was no already prepared input.
        if (pipeline.streams.empty())
            pipeline.streams = storage->read(required_columns, query_info, context, from_stage, max_block_size, max_streams);

        if (pipeline.streams.empty())
            pipeline.streams.emplace_back(std::make_shared<NullBlockInputStream>(storage->getSampleBlockForColumns(required_columns)));

        if (alias_actions)
        {
            /// Wrap each stream returned from the table to calculate and add ALIAS columns
            pipeline.transform([&] (auto & stream)
            {
                stream = std::make_shared<ExpressionBlockInputStream>(stream, alias_actions);
            });
        }

        pipeline.transform([&](auto & stream)
        {
            stream->addTableLock(table_lock);
        });

        /** Set the limits and quota for reading data, the speed and time of the query.
          *  Such restrictions are checked on the initiating server of the request, and not on remote servers.
          *  Because the initiating server has a summary of the execution of the request on all servers.
          */
        if (to_stage == QueryProcessingStage::Complete)
        {
            IProfilingBlockInputStream::LocalLimits limits;
            limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
            limits.max_rows_to_read = settings.limits.max_rows_to_read;
            limits.max_bytes_to_read = settings.limits.max_bytes_to_read;
            limits.read_overflow_mode = settings.limits.read_overflow_mode;
            limits.max_execution_time = settings.limits.max_execution_time;
            limits.timeout_overflow_mode = settings.limits.timeout_overflow_mode;
            limits.min_execution_speed = settings.limits.min_execution_speed;
            limits.timeout_before_checking_execution_speed = settings.limits.timeout_before_checking_execution_speed;

            QuotaForIntervals & quota = context.getQuota();

            pipeline.transform([&](auto & stream)
            {
                if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
                {
                    p_stream->setLimits(limits);
                    p_stream->setQuota(quota);
                }
            });
        }
    }
    else
        throw Exception("Logical error in InterpreterSelectQuery: nowhere to read", ErrorCodes::LOGICAL_ERROR);

    return from_stage;
}


void InterpreterSelectQuery::executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expression)
{
    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<FilterBlockInputStream>(stream, expression, query.where_expression->getColumnName());
    });
}


void InterpreterSelectQuery::executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expression, bool overflow_row, bool final)
{
    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
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
    bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.limits.max_bytes_before_external_group_by != 0;

    Aggregator::Params params(header, keys, aggregates,
        overflow_row, settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode,
        settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.limits.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
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


void InterpreterSelectQuery::executeTotalsAndHaving(Pipeline & pipeline, bool has_having, const ExpressionActionsPtr & expression, bool overflow_row)
{
    executeUnion(pipeline);

    const Settings & settings = context.getSettingsRef();

    pipeline.firstStream() = std::make_shared<TotalsHavingBlockInputStream>(
        pipeline.firstStream(), overflow_row, expression,
        has_having ? query.having_expression->getColumnName() : "", settings.totals_mode, settings.totals_auto_threshold);
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
        limits.max_rows_to_read = settings.limits.max_rows_to_sort;
        limits.max_bytes_to_read = settings.limits.max_bytes_to_sort;
        limits.read_overflow_mode = settings.limits.sort_overflow_mode;
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(), order_descr, settings.max_block_size, limit,
        settings.limits.max_bytes_before_external_sort, context.getTemporaryPath());
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
            if (stream->isGroupedOutput())
                stream = std::make_shared<DistinctSortedBlockInputStream>(stream, settings.limits, limit_for_distinct, columns);
            else
                stream = std::make_shared<DistinctBlockInputStream>(stream, settings.limits, limit_for_distinct, columns);
        });

        if (pipeline.hasMoreThanOneStream())
            union_within_single_query = true;
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
        union_within_single_query = false;
    }
    else if (pipeline.stream_with_non_joined_data)
    {
        pipeline.streams.push_back(pipeline.stream_with_non_joined_data);
        pipeline.stream_with_non_joined_data = nullptr;
        union_within_single_query = false;
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

        if (pipeline.hasMoreThanOneStream())
            union_within_single_query = true;
    }
}


void InterpreterSelectQuery::executeLimitBy(Pipeline & pipeline)
{
    if (!query.limit_by_value || !query.limit_by_expression_list)
        return;

    Names columns;
    size_t value = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_by_value).value);

    for (const auto & elem : query.limit_by_expression_list->children)
    {
        columns.emplace_back(elem->getAliasOrColumnName());
    }

    pipeline.transform([&](auto & stream)
    {
        stream = std::make_shared<LimitByBlockInputStream>(
            stream, value, columns
        );
    });
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
        {
            always_read_till_end = true;
        }

        auto query_table = query.table();
        if (!query.group_by_with_totals && query_table && typeid_cast<const ASTSelectQuery *>(query_table.get()))
        {
            const ASTSelectQuery * subquery = static_cast<const ASTSelectQuery *>(query_table.get());

            while (subquery->table())
            {
                if (subquery->group_by_with_totals)
                {
                    /** NOTE You can also check that the table in the subquery is distributed, and that it only looks at one shard.
                      * In other cases, totals will be computed on the initiating server of the query, and it is not necessary to read the data to the end.
                      */

                    always_read_till_end = true;
                    break;
                }

                auto subquery_table = subquery->table();
                if (typeid_cast<const ASTSelectQuery *>(subquery_table.get()))
                    subquery = static_cast<const ASTSelectQuery *>(subquery_table.get());
                else
                    break;
            }
        }

        pipeline.transform([&](auto & stream)
        {
            stream = std::make_shared<LimitBlockInputStream>(stream, limit_length, limit_offset, always_read_till_end);
        });
    }
}


void InterpreterSelectQuery::executeSubqueriesInSetsAndJoins(Pipeline & pipeline, SubqueriesForSets & subqueries_for_sets)
{
    const Settings & settings = context.getSettingsRef();

    executeUnion(pipeline);
    pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(pipeline.firstStream(), subqueries_for_sets, settings.limits);
}


/// TODO This is trash.
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

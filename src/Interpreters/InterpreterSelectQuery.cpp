#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Access/AccessFlags.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/QueryAliasesVisitor.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/IStorage.h>
#include <Storages/StorageView.h>

#include <TableFunctions/ITableFunction.h>

#include <Functions/IFunction.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <Columns/Collator.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <ext/map.h>
#include <ext/scope_guard.h>
#include <memory>

#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/InflatingExpressionTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/Transforms/LimitByTransform.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/FillingTransform.h>
#include <Processors/LimitTransform.h>
#include <Processors/OffsetTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataStreams/materializeBlock.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int ILLEGAL_FINAL;
    extern const int ILLEGAL_PREWHERE;
    extern const int TOO_MANY_COLUMNS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int INVALID_LIMIT_EXPRESSION;
    extern const int INVALID_WITH_FILL_EXPRESSION;
    extern const int INVALID_SETTING_VALUE;
}

/// Assumes `storage` is set and the table filter (row-level security) is not empty.
String InterpreterSelectQuery::generateFilterActions(
    ExpressionActionsPtr & actions, const ASTPtr & row_policy_filter, const Names & prerequisite_columns) const
{
    const auto & db_name = table_id.getDatabaseName();
    const auto & table_name = table_id.getTableName();

    /// TODO: implement some AST builders for this kind of stuff
    ASTPtr query_ast = std::make_shared<ASTSelectQuery>();
    auto * select_ast = query_ast->as<ASTSelectQuery>();

    select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    auto expr_list = select_ast->select();

    // The first column is our filter expression.
    expr_list->children.push_back(row_policy_filter);

    /// Keep columns that are required after the filter actions.
    for (const auto & column_str : prerequisite_columns)
    {
        ParserExpression expr_parser;
        expr_list->children.push_back(parseQuery(expr_parser, column_str, 0, context->getSettingsRef().max_parser_depth));
    }

    select_ast->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select_ast->tables();
    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    tables->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);
    table_expr->database_and_table_name = createTableIdentifier(db_name, table_name);
    table_expr->children.push_back(table_expr->database_and_table_name);

    /// Using separate expression analyzer to prevent any possible alias injection
    auto syntax_result = SyntaxAnalyzer(*context).analyzeSelect(query_ast, SyntaxAnalyzerResult({}, storage));
    SelectQueryExpressionAnalyzer analyzer(query_ast, syntax_result, *context);
    actions = analyzer.simpleSelectActions();

    return expr_list->children.at(0)->getColumnName();
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const SelectQueryOptions & options_,
    const Names & required_result_column_names_)
    : InterpreterSelectQuery(query_ptr_, context_, nullptr, std::nullopt, nullptr, options_, required_result_column_names_)
{
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const BlockInputStreamPtr & input_,
    const SelectQueryOptions & options_)
    : InterpreterSelectQuery(query_ptr_, context_, input_, std::nullopt, nullptr, options_.copy().noSubquery())
{}

InterpreterSelectQuery::InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        Pipe input_pipe_,
        const SelectQueryOptions & options_)
        : InterpreterSelectQuery(query_ptr_, context_, nullptr, std::move(input_pipe_), nullptr, options_.copy().noSubquery())
{}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const StoragePtr & storage_,
    const SelectQueryOptions & options_)
    : InterpreterSelectQuery(query_ptr_, context_, nullptr, std::nullopt, storage_, options_.copy().noSubquery())
{}

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
    subquery_settings.extremes = false;
    subquery_context.setSettings(subquery_settings);
    return subquery_context;
}

static void rewriteMultipleJoins(ASTPtr & query, const TablesWithColumns & tables, const String & database, const Settings & settings)
{
    ASTSelectQuery & select = query->as<ASTSelectQuery &>();

    Aliases aliases;
    if (ASTPtr with = select.with())
        QueryAliasesNoSubqueriesVisitor(aliases).visit(with);
    QueryAliasesNoSubqueriesVisitor(aliases).visit(select.select());

    CrossToInnerJoinVisitor::Data cross_to_inner{tables, aliases, database};
    CrossToInnerJoinVisitor(cross_to_inner).visit(query);

    size_t rewriter_version = settings.multiple_joins_rewriter_version;
    if (!rewriter_version || rewriter_version > 2)
        throw Exception("Bad multiple_joins_rewriter_version setting value: " + settings.multiple_joins_rewriter_version.toString(),
                        ErrorCodes::INVALID_SETTING_VALUE);
    JoinToSubqueryTransformVisitor::Data join_to_subs_data{tables, aliases, rewriter_version};
    JoinToSubqueryTransformVisitor(join_to_subs_data).visit(query);
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const BlockInputStreamPtr & input_,
    std::optional<Pipe> input_pipe_,
    const StoragePtr & storage_,
    const SelectQueryOptions & options_,
    const Names & required_result_column_names)
    : options(options_)
    /// NOTE: the query almost always should be cloned because it will be modified during analysis.
    , query_ptr(options.modify_inplace ? query_ptr_ : query_ptr_->clone())
    , context(std::make_shared<Context>(context_))
    , storage(storage_)
    , input(input_)
    , input_pipe(std::move(input_pipe_))
    , log(&Poco::Logger::get("InterpreterSelectQuery"))
{
    checkStackSize();

    initSettings();
    const Settings & settings = context->getSettingsRef();

    if (settings.max_subquery_depth && options.subquery_depth > settings.max_subquery_depth)
        throw Exception("Too deep subqueries. Maximum: " + settings.max_subquery_depth.toString(),
            ErrorCodes::TOO_DEEP_SUBQUERIES);

    bool has_input = input || input_pipe;
    if (input)
    {
        /// Read from prepared input.
        source_header = input->getHeader();
    }
    else if (input_pipe)
    {
        /// Read from prepared input.
        source_header = input_pipe->getHeader();
    }

    JoinedTables joined_tables(getSubqueryContext(*context), getSelectQuery());

    if (!has_input && !storage)
        storage = joined_tables.getLeftTableStorage();

    if (storage)
    {
        table_lock = storage->lockStructureForShare(
                false, context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);
        table_id = storage->getStorageID();
    }

    if (has_input || !joined_tables.resolveTables())
        joined_tables.makeFakeTable(storage, source_header);

    /// Rewrite JOINs
    if (!has_input && joined_tables.tablesCount() > 1)
    {
        rewriteMultipleJoins(query_ptr, joined_tables.tablesWithColumns(), context->getCurrentDatabase(), settings);

        joined_tables.reset(getSelectQuery());
        joined_tables.resolveTables();

        if (storage && joined_tables.isLeftTableSubquery())
        {
            /// Rewritten with subquery. Free storage locks here.
            storage = {};
            table_lock.release();
            table_id = StorageID::createEmpty();
        }
    }

    if (!has_input)
    {
        interpreter_subquery = joined_tables.makeLeftTableSubquery(options.subquery());
        if (interpreter_subquery)
            source_header = interpreter_subquery->getSampleBlock();
    }

    joined_tables.rewriteDistributedInAndJoins(query_ptr);

    max_streams = settings.max_threads;
    ASTSelectQuery & query = getSelectQuery();
    std::shared_ptr<TableJoin> table_join = joined_tables.makeTableJoin(query);

    ASTPtr row_policy_filter;
    if (storage)
        row_policy_filter = context->getRowPolicyCondition(table_id.getDatabaseName(), table_id.getTableName(), RowPolicy::SELECT_FILTER);

    StorageView * view = nullptr;
    if (storage)
        view = dynamic_cast<StorageView *>(storage.get());

    auto analyze = [&] (bool try_move_to_prewhere)
    {
        /// Allow push down and other optimizations for VIEW: replace with subquery and rewrite it.
        ASTPtr view_table;
        if (view)
            view->replaceWithSubquery(getSelectQuery(), view_table);

        syntax_analyzer_result = SyntaxAnalyzer(*context).analyzeSelect(
                query_ptr, SyntaxAnalyzerResult(source_header.getNamesAndTypesList(), storage),
                options, joined_tables.tablesWithColumns(), required_result_column_names, table_join);

        if (view)
        {
            /// Restore original view name. Save rewritten subquery for future usage in StorageView.
            query_info.view_query = view->restoreViewName(getSelectQuery(), view_table);
            view = nullptr;
        }

        if (try_move_to_prewhere && storage && !row_policy_filter && query.where() && !query.prewhere() && !query.final())
        {
            /// PREWHERE optimization: transfer some condition from WHERE to PREWHERE if enabled and viable
            if (const auto * merge_tree = dynamic_cast<const MergeTreeData *>(storage.get()))
            {
                SelectQueryInfo current_info;
                current_info.query = query_ptr;
                current_info.syntax_analyzer_result = syntax_analyzer_result;

                MergeTreeWhereOptimizer{current_info, *context, *merge_tree, syntax_analyzer_result->requiredSourceColumns(), log};
            }
        }

        /// Save scalar sub queries's results in the query context
        if (!options.only_analyze && context->hasQueryContext())
            for (const auto & it : syntax_analyzer_result->getScalars())
                context->getQueryContext().addScalar(it.first, it.second);

        query_analyzer = std::make_unique<SelectQueryExpressionAnalyzer>(
                query_ptr, syntax_analyzer_result, *context,
                NameSet(required_result_column_names.begin(), required_result_column_names.end()),
                !options.only_analyze, options);

        if (!options.only_analyze)
        {
            if (query.sampleSize() && (input || input_pipe || !storage || !storage->supportsSampling()))
                throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

            if (query.final() && (input || input_pipe || !storage || !storage->supportsFinal()))
                throw Exception((!input && !input_pipe && storage) ? "Storage " + storage->getName() + " doesn't support FINAL" : "Illegal FINAL", ErrorCodes::ILLEGAL_FINAL);

            if (query.prewhere() && (input || input_pipe || !storage || !storage->supportsPrewhere()))
                throw Exception((!input && !input_pipe && storage) ? "Storage " + storage->getName() + " doesn't support PREWHERE" : "Illegal PREWHERE", ErrorCodes::ILLEGAL_PREWHERE);

            /// Save the new temporary tables in the query context
            for (const auto & it : query_analyzer->getExternalTables())
                if (!context->tryResolveStorageID({"", it.first}, Context::ResolveExternal))
                    context->addExternalTable(it.first, std::move(*it.second));
        }

        if (!options.only_analyze || options.modify_inplace)
        {
            if (syntax_analyzer_result->rewrite_subqueries)
            {
                /// remake interpreter_subquery when PredicateOptimizer rewrites subqueries and main table is subquery
                interpreter_subquery = joined_tables.makeLeftTableSubquery(options.subquery());
            }
        }

        if (interpreter_subquery)
        {
            /// If there is an aggregation in the outer query, WITH TOTALS is ignored in the subquery.
            if (query_analyzer->hasAggregation())
                interpreter_subquery->ignoreWithTotals();
        }

        required_columns = syntax_analyzer_result->requiredSourceColumns();

        if (storage)
        {
            source_header = storage->getSampleBlockForColumns(required_columns);

            /// Fix source_header for filter actions.
            if (row_policy_filter)
            {
                filter_info = std::make_shared<FilterInfo>();
                filter_info->column_name = generateFilterActions(filter_info->actions, row_policy_filter, required_columns);
                source_header = storage->getSampleBlockForColumns(filter_info->actions->getRequiredColumns());
            }
        }

        if (!options.only_analyze && storage && filter_info && query.prewhere())
            throw Exception("PREWHERE is not supported if the table is filtered by row-level security expression", ErrorCodes::ILLEGAL_PREWHERE);

        /// Calculate structure of the result.
        result_header = getSampleBlockImpl();
    };

    analyze(settings.optimize_move_to_prewhere);

    bool need_analyze_again = false;
    if (analysis_result.prewhere_constant_filter_description.always_false || analysis_result.prewhere_constant_filter_description.always_true)
    {
        if (analysis_result.prewhere_constant_filter_description.always_true)
            query.setExpression(ASTSelectQuery::Expression::PREWHERE, {});
        else
            query.setExpression(ASTSelectQuery::Expression::PREWHERE, std::make_shared<ASTLiteral>(0u));
        need_analyze_again = true;
    }
    if (analysis_result.where_constant_filter_description.always_false || analysis_result.where_constant_filter_description.always_true)
    {
        if (analysis_result.where_constant_filter_description.always_true)
            query.setExpression(ASTSelectQuery::Expression::WHERE, {});
        else
            query.setExpression(ASTSelectQuery::Expression::WHERE, std::make_shared<ASTLiteral>(0u));
        need_analyze_again = true;
    }
    if (query.prewhere() && query.where())
    {
        /// Filter block in WHERE instead to get better performance
        query.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", query.prewhere()->clone(), query.where()->clone()));
        need_analyze_again = true;
    }

    if (need_analyze_again)
    {
        /// Do not try move conditions to PREWHERE for the second time.
        /// Otherwise, we won't be able to fallback from inefficient PREWHERE to WHERE later.
        analyze(/* try_move_to_prewhere = */ false);
    }

    /// If there is no WHERE, filter blocks as usual
    if (query.prewhere() && !query.where())
        analysis_result.prewhere_info->need_filter = true;

    const StorageID & left_table_id = joined_tables.leftTableID();

    if (left_table_id)
        context->checkAccess(AccessType::SELECT, left_table_id, required_columns);

    /// Remove limits for some tables in the `system` database.
    if (left_table_id.database_name == "system")
    {
        static const boost::container::flat_set<String> system_tables_ignoring_quota{"quotas", "quota_limits", "quota_usage", "quotas_usage", "one"};
        if (system_tables_ignoring_quota.count(left_table_id.table_name))
        {
            options.ignore_quota = true;
            options.ignore_limits = true;
        }
    }

    /// Blocks used in expression analysis contains size 1 const columns for constant folding and
    ///  null non-const columns to avoid useless memory allocations. However, a valid block sample
    ///  requires all columns to be of size 0, thus we need to sanitize the block here.
    sanitizeBlock(result_header);
}


Block InterpreterSelectQuery::getSampleBlock()
{
    return result_header;
}


BlockIO InterpreterSelectQuery::execute()
{
    BlockIO res;
    executeImpl(res.pipeline, input, std::move(input_pipe));
    res.pipeline.addInterpreterContext(context);
    res.pipeline.addStorageHolder(storage);

    /// We must guarantee that result structure is the same as in getSampleBlock()
    if (!blocksHaveEqualStructure(res.pipeline.getHeader(), result_header))
    {
        res.pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ConvertingTransform>(header, result_header, ConvertingTransform::MatchColumnsMode::Name);
        });
    }

    return res;
}

Block InterpreterSelectQuery::getSampleBlockImpl()
{
    if (storage && !options.only_analyze)
        from_stage = storage->getQueryProcessingStage(*context, options.to_stage, query_ptr);

    /// Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
    bool first_stage = from_stage < QueryProcessingStage::WithMergeableState
        && options.to_stage >= QueryProcessingStage::WithMergeableState;
    /// Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.
    bool second_stage = from_stage <= QueryProcessingStage::WithMergeableState
        && options.to_stage > QueryProcessingStage::WithMergeableState;

    analysis_result = ExpressionAnalysisResult(
            *query_analyzer,
            first_stage,
            second_stage,
            options.only_analyze,
            filter_info,
            source_header);

    if (options.to_stage == QueryProcessingStage::Enum::FetchColumns)
    {
        auto header = source_header;

        if (analysis_result.prewhere_info)
        {
            analysis_result.prewhere_info->prewhere_actions->execute(header);
            header = materializeBlock(header);
            if (analysis_result.prewhere_info->remove_prewhere_column)
                header.erase(analysis_result.prewhere_info->prewhere_column_name);
        }
        return header;
    }

    if (options.to_stage == QueryProcessingStage::Enum::WithMergeableState)
    {
        if (!analysis_result.need_aggregate)
            return analysis_result.before_order_and_select->getSampleBlock();

        auto header = analysis_result.before_aggregation->getSampleBlock();

        Block res;

        for (const auto & key : query_analyzer->aggregationKeys())
            res.insert({nullptr, header.getByName(key.name).type, key.name});

        for (const auto & aggregate : query_analyzer->aggregates())
        {
            size_t arguments_size = aggregate.argument_names.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = header.getByName(aggregate.argument_names[j]).type;

            DataTypePtr type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, argument_types, aggregate.parameters);

            res.insert({nullptr, type, aggregate.column_name});
        }

        return res;
    }

    return analysis_result.final_projection->getSampleBlock();
}

static Field getWithFillFieldValue(const ASTPtr & node, const Context & context)
{
    const auto & [field, type] = evaluateConstantExpression(node, context);

    if (!isColumnedAsNumber(type))
        throw Exception("Illegal type " + type->getName() + " of WITH FILL expression, must be numeric type", ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

    return field;
}

static FillColumnDescription getWithFillDescription(const ASTOrderByElement & order_by_elem, const Context & context)
{
    FillColumnDescription descr;
    if (order_by_elem.fill_from)
        descr.fill_from = getWithFillFieldValue(order_by_elem.fill_from, context);
    if (order_by_elem.fill_to)
        descr.fill_to = getWithFillFieldValue(order_by_elem.fill_to, context);
    if (order_by_elem.fill_step)
        descr.fill_step = getWithFillFieldValue(order_by_elem.fill_step, context);
    else
        descr.fill_step = order_by_elem.direction;

    if (applyVisitor(FieldVisitorAccurateEquals(), descr.fill_step, Field{0}))
        throw Exception("WITH FILL STEP value cannot be zero", ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

    if (order_by_elem.direction == 1)
    {
        if (applyVisitor(FieldVisitorAccurateLess(), descr.fill_step, Field{0}))
            throw Exception("WITH FILL STEP value cannot be negative for sorting in ascending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

        if (!descr.fill_from.isNull() && !descr.fill_to.isNull() &&
            applyVisitor(FieldVisitorAccurateLess(), descr.fill_to, descr.fill_from))
        {
            throw Exception("WITH FILL TO value cannot be less than FROM value for sorting in ascending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
        }
    }
    else
    {
        if (applyVisitor(FieldVisitorAccurateLess(), Field{0}, descr.fill_step))
            throw Exception("WITH FILL STEP value cannot be positive for sorting in descending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

        if (!descr.fill_from.isNull() && !descr.fill_to.isNull() &&
            applyVisitor(FieldVisitorAccurateLess(), descr.fill_from, descr.fill_to))
        {
            throw Exception("WITH FILL FROM value cannot be less than TO value for sorting in descending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
        }
    }

    return descr;
}

static SortDescription getSortDescription(const ASTSelectQuery & query, const Context & context)
{
    SortDescription order_descr;
    order_descr.reserve(query.orderBy()->children.size());
    SpecialSort special_sort = context.getSettings().special_sort.value;
    for (const auto & elem : query.orderBy()->children)
    {
        String name = elem->children.front()->getColumnName();
        const auto & order_by_elem = elem->as<ASTOrderByElement &>();

        std::shared_ptr<Collator> collator;
        if (order_by_elem.collation)
            collator = std::make_shared<Collator>(order_by_elem.collation->as<ASTLiteral &>().value.get<String>());

        if (order_by_elem.with_fill)
        {
            FillColumnDescription fill_desc = getWithFillDescription(order_by_elem, context);
            order_descr.emplace_back(name, order_by_elem.direction,
                order_by_elem.nulls_direction, collator, special_sort, true, fill_desc);
        }
        else
            order_descr.emplace_back(name, order_by_elem.direction, order_by_elem.nulls_direction, collator, special_sort);
    }

    return order_descr;
}

static SortDescription getSortDescriptionFromGroupBy(const ASTSelectQuery & query)
{
    SortDescription order_descr;
    order_descr.reserve(query.groupBy()->children.size());

    for (const auto & elem : query.groupBy()->children)
    {
        String name = elem->getColumnName();
        order_descr.emplace_back(name, 1, 1);
    }

    return order_descr;
}

static UInt64 getLimitUIntValue(const ASTPtr & node, const Context & context, const std::string & expr)
{
    const auto & [field, type] = evaluateConstantExpression(node, context);

    if (!isNativeNumber(type))
        throw Exception("Illegal type " + type->getName() + " of " + expr + " expression, must be numeric type", ErrorCodes::INVALID_LIMIT_EXPRESSION);

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception("The value " + applyVisitor(FieldVisitorToString(), field) + " of " + expr + " expression is not representable as UInt64", ErrorCodes::INVALID_LIMIT_EXPRESSION);

    return converted.safeGet<UInt64>();
}


static std::pair<UInt64, UInt64> getLimitLengthAndOffset(const ASTSelectQuery & query, const Context & context)
{
    UInt64 length = 0;
    UInt64 offset = 0;

    if (query.limitLength())
    {
        length = getLimitUIntValue(query.limitLength(), context, "LIMIT");
        if (query.limitOffset() && length)
            offset = getLimitUIntValue(query.limitOffset(), context, "OFFSET");
    }
    else if (query.limitOffset())
        offset = getLimitUIntValue(query.limitOffset(), context, "OFFSET");
    return {length, offset};
}


static UInt64 getLimitForSorting(const ASTSelectQuery & query, const Context & context)
{
    /// Partial sort can be done if there is LIMIT but no DISTINCT or LIMIT BY, neither ARRAY JOIN.
    if (!query.distinct && !query.limitBy() && !query.limit_with_ties && !query.arrayJoinExpressionList() && query.limitLength())
    {
        auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, context);
        return limit_length + limit_offset;
    }
    return 0;
}

void InterpreterSelectQuery::executeImpl(QueryPipeline & pipeline, const BlockInputStreamPtr & prepared_input, std::optional<Pipe> prepared_pipe)
{
    /** Streams of data. When the query is executed in parallel, we have several data streams.
     *  If there is no GROUP BY, then perform all operations before ORDER BY and LIMIT in parallel, then
     *  if there is an ORDER BY, then glue the streams using ResizeProcessor, and then MergeSorting transforms,
     *  if not, then glue it using ResizeProcessor,
     *  then apply LIMIT.
     *  If there is GROUP BY, then we will perform all operations up to GROUP BY, inclusive, in parallel;
     *  a parallel GROUP BY will glue streams into one,
     *  then perform the remaining operations with one resulting stream.
     */

    /// Now we will compose block streams that perform the necessary actions.
    auto & query = getSelectQuery();
    const Settings & settings = context->getSettingsRef();
    auto & expressions = analysis_result;
    const auto & subqueries_for_sets = query_analyzer->getSubqueriesForSets();
    bool intermediate_stage = false;

    if (options.only_analyze)
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(source_header)));

        if (expressions.prewhere_info)
        {
            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FilterTransform>(
                        header,
                        expressions.prewhere_info->prewhere_actions,
                        expressions.prewhere_info->prewhere_column_name,
                        expressions.prewhere_info->remove_prewhere_column);
            });

            // To remove additional columns in dry run
            // For example, sample column which can be removed in this stage
            if (expressions.prewhere_info->remove_columns_actions)
            {
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<ExpressionTransform>(header, expressions.prewhere_info->remove_columns_actions);
                });
            }
        }
    }
    else
    {
        if (prepared_input)
        {
            pipeline.init(Pipe(std::make_shared<SourceFromInputStream>(prepared_input)));
        }
        else if (prepared_pipe)
        {
            pipeline.init(std::move(*prepared_pipe));
        }

        if (from_stage == QueryProcessingStage::WithMergeableState &&
            options.to_stage == QueryProcessingStage::WithMergeableState)
            intermediate_stage = true;

        if (storage && expressions.filter_info && expressions.prewhere_info)
            throw Exception("PREWHERE is not supported if the table is filtered by row-level security expression", ErrorCodes::ILLEGAL_PREWHERE);

        /** Read the data from Storage. from_stage - to what stage the request was completed in Storage. */
        executeFetchColumns(from_stage, pipeline, expressions.prewhere_info, expressions.columns_to_remove_after_prewhere);

        LOG_TRACE(log, "{} -> {}", QueryProcessingStage::toString(from_stage), QueryProcessingStage::toString(options.to_stage));
    }

    if (options.to_stage > QueryProcessingStage::FetchColumns)
    {
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
            options.to_stage > QueryProcessingStage::WithMergeableState &&
            !query.group_by_with_totals && !query.group_by_with_rollup && !query.group_by_with_cube;

        auto preliminary_sort = [&]()
        {
            /** For distributed query processing,
              *  if no GROUP, HAVING set,
              *  but there is an ORDER or LIMIT,
              *  then we will perform the preliminary sorting and LIMIT on the remote server.
              */
            if (!expressions.second_stage && !expressions.need_aggregate && !expressions.hasHaving())
            {
                if (expressions.has_order_by)
                    executeOrder(pipeline, query_info.input_order_info);

                if (expressions.has_order_by && query.limitLength())
                    executeDistinct(pipeline, false, expressions.selected_columns);

                if (expressions.hasLimitBy())
                {
                    executeExpression(pipeline, expressions.before_limit_by);
                    executeLimitBy(pipeline);
                }

                if (query.limitLength())
                    executePreLimit(pipeline, true);
            }
        };

        if (intermediate_stage)
        {
            if (expressions.first_stage || expressions.second_stage)
                throw Exception("Query with intermediate stage cannot have any other stages", ErrorCodes::LOGICAL_ERROR);

            preliminary_sort();
            if (expressions.need_aggregate)
                executeMergeAggregated(pipeline, aggregate_overflow_row, aggregate_final);
        }

        if (expressions.first_stage)
        {
            if (expressions.hasFilter())
            {
                pipeline.addSimpleTransform([&](const Block & block, QueryPipeline::StreamType stream_type) -> ProcessorPtr
                {
                    bool on_totals = stream_type == QueryPipeline::StreamType::Totals;

                    return std::make_shared<FilterTransform>(
                        block,
                        expressions.filter_info->actions,
                        expressions.filter_info->column_name,
                        expressions.filter_info->do_remove_column,
                        on_totals);
                });
            }

            if (expressions.hasJoin())
            {
                Block join_result_sample;
                JoinPtr join = expressions.before_join->getTableJoinAlgo();

                join_result_sample = ExpressionBlockInputStream(
                    std::make_shared<OneBlockInputStream>(pipeline.getHeader()), expressions.before_join).getHeader();

                /// In case joined subquery has totals, and we don't, add default chunk to totals.
                bool default_totals = false;
                if (!pipeline.hasTotals())
                {
                    pipeline.addDefaultTotals();
                    default_totals = true;
                }

                bool inflating_join = false;
                if (join)
                {
                    inflating_join = true;
                    if (auto * hash_join = typeid_cast<HashJoin *>(join.get()))
                        inflating_join = isCross(hash_join->getKind());
                }

                pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType type)
                {
                    bool on_totals = type == QueryPipeline::StreamType::Totals;
                    std::shared_ptr<IProcessor> ret;
                    if (inflating_join)
                        ret = std::make_shared<InflatingExpressionTransform>(header, expressions.before_join, on_totals, default_totals);
                    else
                        ret = std::make_shared<ExpressionTransform>(header, expressions.before_join, on_totals, default_totals);

                    return ret;
                });

                if (join)
                {
                    if (auto stream = join->createStreamWithNonJoinedRows(join_result_sample, settings.max_block_size))
                    {
                        auto source = std::make_shared<SourceFromInputStream>(std::move(stream));
                        pipeline.addDelayedStream(source);
                    }
                }
            }

            if (expressions.hasWhere())
                executeWhere(pipeline, expressions.before_where, expressions.remove_where_filter);

            if (expressions.need_aggregate)
            {
                executeAggregation(pipeline, expressions.before_aggregation, aggregate_overflow_row, aggregate_final, query_info.input_order_info);
                /// We need to reset input order info, so that executeOrder can't use  it
                query_info.input_order_info.reset();
            }
            else
            {
                executeExpression(pipeline, expressions.before_order_and_select);
                executeDistinct(pipeline, true, expressions.selected_columns);
            }

            preliminary_sort();

            // If there is no global subqueries, we can run subqueries only when receive them on server.
            if (!query_analyzer->hasGlobalSubqueries() && !subqueries_for_sets.empty())
                executeSubqueriesInSetsAndJoins(pipeline, subqueries_for_sets);
        }

        if (expressions.second_stage)
        {
            bool need_second_distinct_pass = false;

            if (expressions.need_aggregate)
            {
                /// If you need to combine aggregated results from multiple servers
                if (!expressions.first_stage)
                    executeMergeAggregated(pipeline, aggregate_overflow_row, aggregate_final);

                if (!aggregate_final)
                {
                    if (query.group_by_with_totals)
                    {
                        bool final = !query.group_by_with_rollup && !query.group_by_with_cube;
                        executeTotalsAndHaving(pipeline, expressions.hasHaving(), expressions.before_having, aggregate_overflow_row, final);
                    }

                    if (query.group_by_with_rollup)
                        executeRollupOrCube(pipeline, Modificator::ROLLUP);
                    else if (query.group_by_with_cube)
                        executeRollupOrCube(pipeline, Modificator::CUBE);

                    if ((query.group_by_with_rollup || query.group_by_with_cube) && expressions.hasHaving())
                    {
                        if (query.group_by_with_totals)
                            throw Exception("WITH TOTALS and WITH ROLLUP or CUBE are not supported together in presence of HAVING", ErrorCodes::NOT_IMPLEMENTED);
                        executeHaving(pipeline, expressions.before_having);
                    }
                }
                else if (expressions.hasHaving())
                    executeHaving(pipeline, expressions.before_having);

                executeExpression(pipeline, expressions.before_order_and_select);
                executeDistinct(pipeline, true, expressions.selected_columns);

            }
            else if (query.group_by_with_totals || query.group_by_with_rollup || query.group_by_with_cube)
                throw Exception("WITH TOTALS, ROLLUP or CUBE are not supported without aggregation", ErrorCodes::NOT_IMPLEMENTED);

            need_second_distinct_pass = query.distinct && pipeline.hasMixedStreams();

            if (expressions.has_order_by)
            {
                /** If there is an ORDER BY for distributed query processing,
                  *  but there is no aggregation, then on the remote servers ORDER BY was made
                  *  - therefore, we merge the sorted streams from remote servers.
                  */

                if (!expressions.first_stage && !expressions.need_aggregate && !(query.group_by_with_totals && !aggregate_final))
                    executeMergeSorted(pipeline);
                else    /// Otherwise, just sort.
                    executeOrder(pipeline, query_info.input_order_info);
            }

            /** Optimization - if there are several sources and there is LIMIT, then first apply the preliminary LIMIT,
              * limiting the number of rows in each up to `offset + limit`.
              */
            bool has_prelimit = false;
            if (query.limitLength() && !query.limit_with_ties && pipeline.hasMoreThanOneStream() &&
                !query.distinct && !expressions.hasLimitBy() && !settings.extremes)
            {
                executePreLimit(pipeline, false);
                has_prelimit = true;
            }

            bool need_merge_streams = need_second_distinct_pass || query.limitBy();

            if (need_merge_streams)
                pipeline.resize(1);

            /** If there was more than one stream,
              * then DISTINCT needs to be performed once again after merging all streams.
              */
            if (need_second_distinct_pass)
                executeDistinct(pipeline, false, expressions.selected_columns);

            if (expressions.hasLimitBy())
            {
                executeExpression(pipeline, expressions.before_limit_by);
                executeLimitBy(pipeline);
            }

            executeWithFill(pipeline);

            /// If we have 'WITH TIES', we need execute limit before projection,
            /// because in that case columns from 'ORDER BY' are used.
            if (query.limit_with_ties)
            {
                executeLimit(pipeline);
                has_prelimit = true;
            }

            /** We must do projection after DISTINCT because projection may remove some columns.
              */
            executeProjection(pipeline, expressions.final_projection);

            /** Extremes are calculated before LIMIT, but after LIMIT BY. This is Ok.
              */
            executeExtremes(pipeline);

            if (!has_prelimit)  /// Limit is no longer needed if there is prelimit.
                executeLimit(pipeline);

            executeOffset(pipeline);
        }
    }

    if (query_analyzer->hasGlobalSubqueries() && !subqueries_for_sets.empty())
        executeSubqueriesInSetsAndJoins(pipeline, subqueries_for_sets);
}

void InterpreterSelectQuery::executeFetchColumns(
    QueryProcessingStage::Enum processing_stage, QueryPipeline & pipeline,
    const PrewhereInfoPtr & prewhere_info, const Names & columns_to_remove_after_prewhere)
{
    auto & query = getSelectQuery();
    const Settings & settings = context->getSettingsRef();

    /// Optimization for trivial query like SELECT count() FROM table.
    bool optimize_trivial_count =
        syntax_analyzer_result->optimize_trivial_count
        && storage
        && !filter_info
        && processing_stage == QueryProcessingStage::FetchColumns
        && query_analyzer->hasAggregation()
        && (query_analyzer->aggregates().size() == 1)
        && typeid_cast<AggregateFunctionCount *>(query_analyzer->aggregates()[0].function.get());

    if (optimize_trivial_count)
    {
        const auto & desc = query_analyzer->aggregates()[0];
        const auto & func = desc.function;
        std::optional<UInt64> num_rows = storage->totalRows();
        if (num_rows)
        {
            AggregateFunctionCount & agg_count = static_cast<AggregateFunctionCount &>(*func);

            /// We will process it up to "WithMergeableState".
            std::vector<char> state(agg_count.sizeOfData());
            AggregateDataPtr place = state.data();

            agg_count.create(place);
            SCOPE_EXIT(agg_count.destroy(place));

            agg_count.set(place, *num_rows);

            auto column = ColumnAggregateFunction::create(func);
            column->insertFrom(place);

            auto header = analysis_result.before_aggregation->getSampleBlock();
            size_t arguments_size = desc.argument_names.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = header.getByName(desc.argument_names[j]).type;

            Block block_with_count{
                {std::move(column), std::make_shared<DataTypeAggregateFunction>(func, argument_types, desc.parameters), desc.column_name}};

            auto istream = std::make_shared<OneBlockInputStream>(block_with_count);
            pipeline.init(Pipe(std::make_shared<SourceFromInputStream>(istream)));
            from_stage = QueryProcessingStage::WithMergeableState;
            analysis_result.first_stage = false;
            return;
        }
    }

    /// Actions to calculate ALIAS if required.
    ExpressionActionsPtr alias_actions;

    if (storage)
    {
        /// Append columns from the table filter to required
        auto row_policy_filter = context->getRowPolicyCondition(table_id.getDatabaseName(), table_id.getTableName(), RowPolicy::SELECT_FILTER);
        if (row_policy_filter)
        {
            auto initial_required_columns = required_columns;
            ExpressionActionsPtr actions;
            generateFilterActions(actions, row_policy_filter, initial_required_columns);
            auto required_columns_from_filter = actions->getRequiredColumns();

            for (const auto & column : required_columns_from_filter)
            {
                if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column))
                    required_columns.push_back(column);
            }
        }

        /// Detect, if ALIAS columns are required for query execution
        auto alias_columns_required = false;
        const ColumnsDescription & storage_columns = storage->getColumns();
        for (const auto & column_name : required_columns)
        {
            auto column_default = storage_columns.getDefault(column_name);
            if (column_default && column_default->kind == ColumnDefaultKind::Alias)
            {
                alias_columns_required = true;
                break;
            }
        }

        /// There are multiple sources of required columns:
        ///  - raw required columns,
        ///  - columns deduced from ALIAS columns,
        ///  - raw required columns from PREWHERE,
        ///  - columns deduced from ALIAS columns from PREWHERE.
        /// PREWHERE is a special case, since we need to resolve it and pass directly to `IStorage::read()`
        /// before any other executions.
        if (alias_columns_required)
        {
            NameSet required_columns_from_prewhere; /// Set of all (including ALIAS) required columns for PREWHERE
            NameSet required_aliases_from_prewhere; /// Set of ALIAS required columns for PREWHERE

            if (prewhere_info)
            {
                /// Get some columns directly from PREWHERE expression actions
                auto prewhere_required_columns = prewhere_info->prewhere_actions->getRequiredColumns();
                required_columns_from_prewhere.insert(prewhere_required_columns.begin(), prewhere_required_columns.end());
            }

            /// Expression, that contains all raw required columns
            ASTPtr required_columns_all_expr = std::make_shared<ASTExpressionList>();

            /// Expression, that contains raw required columns for PREWHERE
            ASTPtr required_columns_from_prewhere_expr = std::make_shared<ASTExpressionList>();

            /// Sort out already known required columns between expressions,
            /// also populate `required_aliases_from_prewhere`.
            for (const auto & column : required_columns)
            {
                ASTPtr column_expr;
                const auto column_default = storage_columns.getDefault(column);
                bool is_alias = column_default && column_default->kind == ColumnDefaultKind::Alias;
                if (is_alias)
                {
                    auto column_decl = storage_columns.get(column);
                    /// TODO: can make CAST only if the type is different (but requires SyntaxAnalyzer).
                    auto cast_column_default = addTypeConversionToAST(column_default->expression->clone(), column_decl.type->getName());
                    column_expr = setAlias(cast_column_default->clone(), column);
                }
                else
                    column_expr = std::make_shared<ASTIdentifier>(column);

                if (required_columns_from_prewhere.count(column))
                {
                    required_columns_from_prewhere_expr->children.emplace_back(std::move(column_expr));

                    if (is_alias)
                        required_aliases_from_prewhere.insert(column);
                }
                else
                    required_columns_all_expr->children.emplace_back(std::move(column_expr));
            }

            /// Columns, which we will get after prewhere and filter executions.
            NamesAndTypesList required_columns_after_prewhere;
            NameSet required_columns_after_prewhere_set;

            /// Collect required columns from prewhere expression actions.
            if (prewhere_info)
            {
                NameSet columns_to_remove(columns_to_remove_after_prewhere.begin(), columns_to_remove_after_prewhere.end());
                Block prewhere_actions_result = prewhere_info->prewhere_actions->getSampleBlock();

                /// Populate required columns with the columns, added by PREWHERE actions and not removed afterwards.
                /// XXX: looks hacky that we already know which columns after PREWHERE we won't need for sure.
                for (const auto & column : prewhere_actions_result)
                {
                    if (prewhere_info->remove_prewhere_column && column.name == prewhere_info->prewhere_column_name)
                        continue;

                    if (columns_to_remove.count(column.name))
                        continue;

                    required_columns_all_expr->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
                    required_columns_after_prewhere.emplace_back(column.name, column.type);
                }

                required_columns_after_prewhere_set
                    = ext::map<NameSet>(required_columns_after_prewhere, [](const auto & it) { return it.name; });
            }

            auto syntax_result = SyntaxAnalyzer(*context).analyze(required_columns_all_expr, required_columns_after_prewhere, storage);
            alias_actions = ExpressionAnalyzer(required_columns_all_expr, syntax_result, *context).getActions(true);

            /// The set of required columns could be added as a result of adding an action to calculate ALIAS.
            required_columns = alias_actions->getRequiredColumns();

            /// Do not remove prewhere filter if it is a column which is used as alias.
            if (prewhere_info && prewhere_info->remove_prewhere_column)
                if (required_columns.end()
                    != std::find(required_columns.begin(), required_columns.end(), prewhere_info->prewhere_column_name))
                    prewhere_info->remove_prewhere_column = false;

            /// Remove columns which will be added by prewhere.
            required_columns.erase(std::remove_if(required_columns.begin(), required_columns.end(), [&](const String & name)
            {
                return required_columns_after_prewhere_set.count(name) != 0;
            }), required_columns.end());

            if (prewhere_info)
            {
                /// Don't remove columns which are needed to be aliased.
                auto new_actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions->getRequiredColumnsWithTypes(), *context);
                for (const auto & action : prewhere_info->prewhere_actions->getActions())
                {
                    if (action.type != ExpressionAction::REMOVE_COLUMN
                        || required_columns.end() == std::find(required_columns.begin(), required_columns.end(), action.source_name))
                        new_actions->add(action);
                }
                prewhere_info->prewhere_actions = std::move(new_actions);

                auto analyzed_result
                    = SyntaxAnalyzer(*context).analyze(required_columns_from_prewhere_expr, storage->getColumns().getAllPhysical());
                prewhere_info->alias_actions
                    = ExpressionAnalyzer(required_columns_from_prewhere_expr, analyzed_result, *context).getActions(true, false);

                /// Add (physical?) columns required by alias actions.
                auto required_columns_from_alias = prewhere_info->alias_actions->getRequiredColumns();
                Block prewhere_actions_result = prewhere_info->prewhere_actions->getSampleBlock();
                for (auto & column : required_columns_from_alias)
                    if (!prewhere_actions_result.has(column))
                        if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column))
                            required_columns.push_back(column);

                /// Add physical columns required by prewhere actions.
                for (const auto & column : required_columns_from_prewhere)
                    if (required_aliases_from_prewhere.count(column) == 0)
                        if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column))
                            required_columns.push_back(column);
            }
        }
    }

    /// Limitation on the number of columns to read.
    /// It's not applied in 'only_analyze' mode, because the query could be analyzed without removal of unnecessary columns.
    if (!options.only_analyze && settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read)
        throw Exception("Limit for number of columns to read exceeded. "
            "Requested: " + toString(required_columns.size())
            + ", maximum: " + settings.max_columns_to_read.toString(),
            ErrorCodes::TOO_MANY_COLUMNS);

    /// General limit for then number of threads.
    pipeline.setMaxThreads(settings.max_threads);

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
        pipeline.setMaxThreads(max_streams);
    }

    UInt64 max_block_size = settings.max_block_size;

    auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, *context);

    /** Optimization - if not specified DISTINCT, WHERE, GROUP, HAVING, ORDER, LIMIT BY, WITH TIES but LIMIT is specified, and limit + offset < max_block_size,
     *  then as the block size we will use limit + offset (not to read more from the table than requested),
     *  and also set the number of threads to 1.
     */
    if (!query.distinct
        && !query.limit_with_ties
        && !query.prewhere()
        && !query.where()
        && !query.groupBy()
        && !query.having()
        && !query.orderBy()
        && !query.limitBy()
        && query.limitLength()
        && !query_analyzer->hasAggregation()
        && limit_length + limit_offset < max_block_size)
    {
        max_block_size = std::max(UInt64(1), limit_length + limit_offset);
        max_streams = 1;
        pipeline.setMaxThreads(max_streams);
    }

    if (!max_block_size)
        throw Exception("Setting 'max_block_size' cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

    /// Initialize the initial data streams to which the query transforms are superimposed. Table or subquery or prepared input?
    if (pipeline.initialized())
    {
        /// Prepared input.
    }
    else if (interpreter_subquery)
    {
        /// Subquery.
        /// If we need less number of columns that subquery have - update the interpreter.
        if (required_columns.size() < source_header.columns())
        {
            ASTPtr subquery = extractTableExpression(query, 0);
            if (!subquery)
                throw Exception("Subquery expected", ErrorCodes::LOGICAL_ERROR);

            interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
                subquery, getSubqueryContext(*context),
                options.copy().subquery().noModify(), required_columns);

            if (query_analyzer->hasAggregation())
                interpreter_subquery->ignoreWithTotals();
        }

        pipeline = interpreter_subquery->execute().pipeline;
    }
    else if (storage)
    {
        /// Table.

        if (max_streams == 0)
            throw Exception("Logical error: zero number of streams requested", ErrorCodes::LOGICAL_ERROR);

        /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads.
        if (max_streams > 1 && !is_remote)
            max_streams *= settings.max_streams_to_max_threads_ratio;

        query_info.query = query_ptr;
        query_info.syntax_analyzer_result = syntax_analyzer_result;
        query_info.sets = query_analyzer->getPreparedSets();
        query_info.prewhere_info = prewhere_info;

        /// Create optimizer with prepared actions.
        /// Maybe we will need to calc input_order_info later, e.g. while reading from StorageMerge.
        if (analysis_result.optimize_read_in_order || analysis_result.optimize_aggregation_in_order)
        {
            if (analysis_result.optimize_read_in_order)
                query_info.order_optimizer = std::make_shared<ReadInOrderOptimizer>(
                    analysis_result.order_by_elements_actions,
                    getSortDescription(query, *context),
                    query_info.syntax_analyzer_result);
            else
                query_info.order_optimizer = std::make_shared<ReadInOrderOptimizer>(
                    analysis_result.group_by_elements_actions,
                    getSortDescriptionFromGroupBy(query),
                    query_info.syntax_analyzer_result);

            query_info.input_order_info = query_info.order_optimizer->getInputOrder(storage);
        }

        Pipes pipes = storage->read(required_columns, query_info, *context, processing_stage, max_block_size, max_streams);

        if (pipes.empty())
        {
            Pipe pipe(std::make_shared<NullSource>(storage->getSampleBlockForColumns(required_columns)));

            if (query_info.prewhere_info)
            {
                if (query_info.prewhere_info->alias_actions)
                    pipe.addSimpleTransform(std::make_shared<ExpressionTransform>(
                        pipe.getHeader(), query_info.prewhere_info->alias_actions));

                pipe.addSimpleTransform(std::make_shared<FilterTransform>(
                        pipe.getHeader(),
                        prewhere_info->prewhere_actions,
                        prewhere_info->prewhere_column_name,
                        prewhere_info->remove_prewhere_column));

                // To remove additional columns
                // In some cases, we did not read any marks so that the pipeline.streams is empty
                // Thus, some columns in prewhere are not removed as expected
                // This leads to mismatched header in distributed table
                if (query_info.prewhere_info->remove_columns_actions)
                    pipe.addSimpleTransform(std::make_shared<ExpressionTransform>(pipe.getHeader(), query_info.prewhere_info->remove_columns_actions));
            }

            pipes.emplace_back(std::move(pipe));
        }

        /// Table lock is stored inside pipeline here.
        pipeline.addTableLock(table_lock);

        /// Set the limits and quota for reading data, the speed and time of the query.
        {
            IBlockInputStream::LocalLimits limits;
            limits.mode = IBlockInputStream::LIMITS_TOTAL;
            limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
            limits.speed_limits.max_execution_time = settings.max_execution_time;
            limits.timeout_overflow_mode = settings.timeout_overflow_mode;

            /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
              *  because the initiating server has a summary of the execution of the request on all servers.
              *
              * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
              *  additionally on each remote server, because these limits are checked per block of data processed,
              *  and remote servers may process way more blocks of data than are received by initiator.
              *
              * The limits to throttle maximum execution speed is also checked on all servers.
              */
            if (options.to_stage == QueryProcessingStage::Complete)
            {
                limits.speed_limits.min_execution_rps = settings.min_execution_speed;
                limits.speed_limits.min_execution_bps = settings.min_execution_speed_bytes;
            }

            limits.speed_limits.max_execution_rps = settings.max_execution_speed;
            limits.speed_limits.max_execution_bps = settings.max_execution_speed_bytes;
            limits.speed_limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;

            auto quota = context->getQuota();

            for (auto & pipe : pipes)
            {
                if (!options.ignore_limits)
                    pipe.setLimits(limits);

                if (!options.ignore_quota && (options.to_stage == QueryProcessingStage::Complete))
                    pipe.setQuota(quota);
            }
        }

        if (pipes.size() == 1)
            pipeline.setMaxThreads(1);

        for (auto & pipe : pipes)
            pipe.enableQuota();

        pipeline.init(std::move(pipes));
    }
    else
        throw Exception("Logical error in InterpreterSelectQuery: nowhere to read", ErrorCodes::LOGICAL_ERROR);

    /// Aliases in table declaration.
    if (processing_stage == QueryProcessingStage::FetchColumns && alias_actions)
    {
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, alias_actions);
        });
    }
}


void InterpreterSelectQuery::executeWhere(QueryPipeline & pipeline, const ExpressionActionsPtr & expression, bool remove_filter)
{
    pipeline.addSimpleTransform([&](const Block & block, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<FilterTransform>(block, expression, getSelectQuery().where()->getColumnName(), remove_filter, on_totals);
    });
}


void InterpreterSelectQuery::executeAggregation(QueryPipeline & pipeline, const ExpressionActionsPtr & expression, bool overflow_row, bool final, InputOrderInfoPtr group_by_info)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression);
    });

    Block header_before_aggregation = pipeline.getHeader();
    ColumnNumbers keys;
    for (const auto & key : query_analyzer->aggregationKeys())
        keys.push_back(header_before_aggregation.getPositionByName(key.name));

    AggregateDescriptions aggregates = query_analyzer->aggregates();
    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(header_before_aggregation.getPositionByName(name));

    const Settings & settings = context->getSettingsRef();

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || settings.max_bytes_before_external_group_by != 0;

    Aggregator::Params params(header_before_aggregation, keys, aggregates,
                              overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                              allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
                              allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
                              settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
                              context->getTemporaryVolume(), settings.max_threads, settings.min_free_disk_space_for_temporary_data);

    auto transform_params = std::make_shared<AggregatingTransformParams>(params, final);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    if (group_by_info && settings.optimize_aggregation_in_order)
    {
        auto & query = getSelectQuery();
        SortDescription group_by_descr = getSortDescriptionFromGroupBy(query);
        bool need_finish_sorting = (group_by_info->order_key_prefix_descr.size() < group_by_descr.size());

        if (need_finish_sorting)
        {
            /// TOO SLOW
        }
        else
        {
            if (pipeline.getNumStreams() > 1)
            {
                auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
                size_t counter = 0;
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<AggregatingInOrderTransform>(header, transform_params, group_by_descr, settings.max_block_size, many_data, counter++);
                });

                for (auto & column_description : group_by_descr)
                {
                    if (!column_description.column_name.empty())
                    {
                        column_description.column_number = pipeline.getHeader().getPositionByName(column_description.column_name);
                        column_description.column_name.clear();
                    }
                }

                auto transform = std::make_shared<AggregatingSortedTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    group_by_descr,
                    settings.max_block_size);

                pipeline.addPipe({ std::move(transform) });
            }
            else
            {
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<AggregatingInOrderTransform>(header, transform_params, group_by_descr, settings.max_block_size);
                });
            }

            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FinalizingSimpleTransform>(header, transform_params);
            });

            pipeline.enableQuotaForCurrentStreams();
            return;
        }
    }

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!(storage && storage->hasEvenlyDistributedRead()))
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
        auto merge_threads = settings.aggregation_memory_efficient_merge_threads
                ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                : static_cast<size_t>(settings.max_threads);

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params, many_data, counter++, max_streams, merge_threads);
        });

        pipeline.resize(1);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params);
        });
    }

    pipeline.enableQuotaForCurrentStreams();
}


void InterpreterSelectQuery::executeMergeAggregated(QueryPipeline & pipeline, bool overflow_row, bool final)
{
    Block header_before_merge = pipeline.getHeader();

    ColumnNumbers keys;
    for (const auto & key : query_analyzer->aggregationKeys())
        keys.push_back(header_before_merge.getPositionByName(key.name));

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

    const Settings & settings = context->getSettingsRef();

    Aggregator::Params params(header_before_merge, keys, query_analyzer->aggregates(), overflow_row, settings.max_threads);

    auto transform_params = std::make_shared<AggregatingTransformParams>(params, final);

    if (!settings.distributed_aggregation_memory_efficient)
    {
        /// We union several sources into one, parallelizing the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<MergingAggregatedTransform>(header, transform_params, settings.max_threads);
        });
    }
    else
    {
        /// pipeline.resize(max_streams); - Seem we don't need it.
        auto num_merge_threads = settings.aggregation_memory_efficient_merge_threads
                                 ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                                 : static_cast<size_t>(settings.max_threads);

        auto pipe = createMergingAggregatedMemoryEfficientPipe(
            pipeline.getHeader(),
            transform_params,
            pipeline.getNumStreams(),
            num_merge_threads);

        pipeline.addPipe(std::move(pipe));
    }

    pipeline.enableQuotaForCurrentStreams();
}


void InterpreterSelectQuery::executeHaving(QueryPipeline & pipeline, const ExpressionActionsPtr & expression)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;

        /// TODO: do we need to save filter there?
        return std::make_shared<FilterTransform>(header, expression, getSelectQuery().having()->getColumnName(), false, on_totals);
    });
}


void InterpreterSelectQuery::executeTotalsAndHaving(QueryPipeline & pipeline, bool has_having, const ExpressionActionsPtr & expression, bool overflow_row, bool final)
{
    const Settings & settings = context->getSettingsRef();

    auto totals_having = std::make_shared<TotalsHavingTransform>(
            pipeline.getHeader(), overflow_row, expression,
            has_having ? getSelectQuery().having()->getColumnName() : "",
            settings.totals_mode, settings.totals_auto_threshold, final);

    pipeline.addTotalsHavingTransform(std::move(totals_having));
}


void InterpreterSelectQuery::executeRollupOrCube(QueryPipeline & pipeline, Modificator modificator)
{
    pipeline.resize(1);

    Block header_before_transform = pipeline.getHeader();

    ColumnNumbers keys;

    for (const auto & key : query_analyzer->aggregationKeys())
        keys.push_back(header_before_transform.getPositionByName(key.name));

    const Settings & settings = context->getSettingsRef();

    Aggregator::Params params(header_before_transform, keys, query_analyzer->aggregates(),
                              false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                              SettingUInt64(0), SettingUInt64(0),
                              settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
                              context->getTemporaryVolume(), settings.max_threads, settings.min_free_disk_space_for_temporary_data);

    auto transform_params = std::make_shared<AggregatingTransformParams>(params, true);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;

        if (modificator == Modificator::ROLLUP)
            return std::make_shared<RollupTransform>(header, std::move(transform_params));
        else
            return std::make_shared<CubeTransform>(header, std::move(transform_params));
    });
}


void InterpreterSelectQuery::executeExpression(QueryPipeline & pipeline, const ExpressionActionsPtr & expression)
{
    pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(header, expression);
    });
}


void InterpreterSelectQuery::executeOrderOptimized(QueryPipeline & pipeline, InputOrderInfoPtr input_sorting_info, UInt64 limit, SortDescription & output_order_descr)
{
    const Settings & settings = context->getSettingsRef();

    bool need_finish_sorting = (input_sorting_info->order_key_prefix_descr.size() < output_order_descr.size());
    if (pipeline.getNumStreams() > 1)
    {
        UInt64 limit_for_merging = (need_finish_sorting ? 0 : limit);
        auto transform = std::make_shared<MergingSortedTransform>(
                pipeline.getHeader(),
                pipeline.getNumStreams(),
                input_sorting_info->order_key_prefix_descr,
                settings.max_block_size, limit_for_merging);

        pipeline.addPipe({ std::move(transform) });
    }

    pipeline.enableQuotaForCurrentStreams();

    if (need_finish_sorting)
    {
        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipeline::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, output_order_descr, limit);
        });

            /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform

            pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr
            {
                return std::make_shared<FinishSortingTransform>(
                    header, input_sorting_info->order_key_prefix_descr,
                    output_order_descr, settings.max_block_size, limit);
        });
    }
}

void InterpreterSelectQuery::executeOrder(QueryPipeline & pipeline, InputOrderInfoPtr input_sorting_info)
{
    auto & query = getSelectQuery();
    SortDescription output_order_descr = getSortDescription(query, *context);
    UInt64 limit = getLimitForSorting(query, *context);

    const Settings & settings = context->getSettingsRef();

    IBlockInputStream::LocalLimits limits;
    limits.mode = IBlockInputStream::LIMITS_CURRENT;
    limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);

    if (input_sorting_info)
    {
        /* Case of sorting with optimization using sorting key.
         * We have several threads, each of them reads batch of parts in direct
         *  or reverse order of sorting key using one input stream per part
         *  and then merge them into one sorted stream.
         * At this stage we merge per-thread streams into one.
         */
        executeOrderOptimized(pipeline, input_sorting_info, limit, output_order_descr);
        return;
    }

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipeline::StreamType::Main)
            return nullptr;

        return std::make_shared<PartialSortingTransform>(header, output_order_descr, limit);
    });

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;

        auto transform = std::make_shared<LimitsCheckingTransform>(header, limits);
        return transform;
    });

    /// Merge the sorted blocks.
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;

        return std::make_shared<MergeSortingTransform>(
                header, output_order_descr, settings.max_block_size, limit,
                settings.max_bytes_before_remerge_sort / pipeline.getNumStreams(),
                settings.max_bytes_before_external_sort, context->getTemporaryVolume(),
                settings.min_free_disk_space_for_temporary_data);
    });

    /// If there are several streams, we merge them into one
    executeMergeSorted(pipeline, output_order_descr, limit);
}


void InterpreterSelectQuery::executeMergeSorted(QueryPipeline & pipeline)
{
    auto & query = getSelectQuery();
    SortDescription order_descr = getSortDescription(query, *context);
    UInt64 limit = getLimitForSorting(query, *context);

    executeMergeSorted(pipeline, order_descr, limit);
}

void InterpreterSelectQuery::executeMergeSorted(QueryPipeline & pipeline, const SortDescription & sort_description, UInt64 limit)
{
    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        const Settings & settings = context->getSettingsRef();

        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            sort_description,
            settings.max_block_size, limit);

        pipeline.addPipe({ std::move(transform) });

        pipeline.enableQuotaForCurrentStreams();
    }
}


void InterpreterSelectQuery::executeProjection(QueryPipeline & pipeline, const ExpressionActionsPtr & expression)
{
    pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr
    {
       return std::make_shared<ExpressionTransform>(header, expression);
    });
}


void InterpreterSelectQuery::executeDistinct(QueryPipeline & pipeline, bool before_order, Names columns)
{
    auto & query = getSelectQuery();
    if (query.distinct)
    {
        const Settings & settings = context->getSettingsRef();

        auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, *context);
        UInt64 limit_for_distinct = 0;

        /// If after this stage of DISTINCT ORDER BY is not executed, then you can get no more than limit_length + limit_offset of different rows.
        if (!query.orderBy() || !before_order)
            limit_for_distinct = limit_length + limit_offset;

        SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type == QueryPipeline::StreamType::Totals)
                return nullptr;

            return std::make_shared<DistinctTransform>(header, limits, limit_for_distinct, columns);
        });
    }
}


/// Preliminary LIMIT - is used in every source, if there are several sources, before they are combined.
void InterpreterSelectQuery::executePreLimit(QueryPipeline & pipeline, bool do_not_skip_offset)
{
    auto & query = getSelectQuery();
    /// If there is LIMIT
    if (query.limitLength())
    {
        auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, *context);

        if (do_not_skip_offset)
        {
            limit_length += limit_offset;
            limit_offset = 0;
        }

        auto limit = std::make_shared<LimitTransform>(pipeline.getHeader(), limit_length, limit_offset, pipeline.getNumStreams());
        pipeline.addPipe({std::move(limit)});
    }
}


void InterpreterSelectQuery::executeLimitBy(QueryPipeline & pipeline)
{
    auto & query = getSelectQuery();
    if (!query.limitByLength() || !query.limitBy())
        return;

    Names columns;
    for (const auto & elem : query.limitBy()->children)
        columns.emplace_back(elem->getColumnName());

    UInt64 length = getLimitUIntValue(query.limitByLength(), *context, "LIMIT");
    UInt64 offset = (query.limitByOffset() ? getLimitUIntValue(query.limitByOffset(), *context, "OFFSET") : 0);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;

        return std::make_shared<LimitByTransform>(header, length, offset, columns);
    });
}


namespace
{
    bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery & query)
    {
        if (query.group_by_with_totals)
            return true;

        /** NOTE You can also check that the table in the subquery is distributed, and that it only looks at one shard.
      * In other cases, totals will be computed on the initiating server of the query, and it is not necessary to read the data to the end.
      */

        if (auto query_table = extractTableExpression(query, 0))
        {
            if (const auto * ast_union = query_table->as<ASTSelectWithUnionQuery>())
            {
                for (const auto & elem : ast_union->list_of_selects->children)
                    if (hasWithTotalsInAnySubqueryInFromClause(elem->as<ASTSelectQuery &>()))
                        return true;
            }
        }

        return false;
    }
}

void InterpreterSelectQuery::executeWithFill(QueryPipeline & pipeline)
{
    auto & query = getSelectQuery();
    if (query.orderBy())
    {
        SortDescription order_descr = getSortDescription(query, *context);
        SortDescription fill_descr;
        for (auto & desc : order_descr)
        {
            if (desc.with_fill)
                fill_descr.push_back(desc);
        }

        if (fill_descr.empty())
            return;

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FillingTransform>(header, fill_descr);
        });
    }
}


void InterpreterSelectQuery::executeLimit(QueryPipeline & pipeline)
{
    auto & query = getSelectQuery();
    /// If there is LIMIT
    if (query.limitLength())
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

        if (query.group_by_with_totals && !query.orderBy())
            always_read_till_end = true;

        if (!query.group_by_with_totals && hasWithTotalsInAnySubqueryInFromClause(query))
            always_read_till_end = true;

        UInt64 limit_length;
        UInt64 limit_offset;
        std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(query, *context);

        SortDescription order_descr;
        if (query.limit_with_ties)
        {
            if (!query.orderBy())
                throw Exception("LIMIT WITH TIES without ORDER BY", ErrorCodes::LOGICAL_ERROR);
            order_descr = getSortDescription(query, *context);
        }

        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipeline::StreamType::Main)
                return nullptr;

            return std::make_shared<LimitTransform>(
                    header, limit_length, limit_offset, 1, always_read_till_end, query.limit_with_ties, order_descr);
        });
    }
}


void InterpreterSelectQuery::executeOffset(QueryPipeline & pipeline)
{
    auto & query = getSelectQuery();
    /// If there is not a LIMIT but an offset
    if (!query.limitLength() && query.limitOffset())
    {
        UInt64 limit_length;
        UInt64 limit_offset;
        std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(query, *context);

        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipeline::StreamType::Main)
                return nullptr;
            return std::make_shared<OffsetTransform>(header, limit_offset, 1);
        });
    }
}

void InterpreterSelectQuery::executeExtremes(QueryPipeline & pipeline)
{
    if (!context->getSettingsRef().extremes)
        return;

    pipeline.addExtremesTransform();
}

void InterpreterSelectQuery::executeSubqueriesInSetsAndJoins(QueryPipeline & pipeline, const SubqueriesForSets & subqueries_for_sets)
{
    if (query_info.input_order_info)
        executeMergeSorted(pipeline, query_info.input_order_info->order_key_prefix_descr, 0);

    const Settings & settings = context->getSettingsRef();

    auto creating_sets = std::make_shared<CreatingSetsTransform>(
            pipeline.getHeader(), subqueries_for_sets,
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            *context);

    pipeline.addCreatingSetsTransform(std::move(creating_sets));
}


void InterpreterSelectQuery::ignoreWithTotals()
{
    getSelectQuery().group_by_with_totals = false;
}


void InterpreterSelectQuery::initSettings()
{
    auto & query = getSelectQuery();
    if (query.settings())
        InterpreterSetQuery(query.settings(), *context).executeForCurrentContext();
}

}

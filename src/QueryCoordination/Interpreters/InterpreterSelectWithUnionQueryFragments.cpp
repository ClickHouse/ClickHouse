#include <Access/AccessControl.h>

#include <Columns/getLeastSuperColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectIntersectExceptQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <QueryCoordination/Interpreters/InterpreterSelectQueryFragments.h>
#include <QueryCoordination/Interpreters/InterpreterSelectWithUnionQueryFragments.h>
#include <QueryCoordination/Fragments/PlanFragmentBuilder.h>
#include <QueryCoordination/Coordinator.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryCoordination/FragmentMgr.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>

#include <Interpreters/InDepthNodeVisitor.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
}

InterpreterSelectWithUnionQueryFragments::InterpreterSelectWithUnionQueryFragments(
    const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_, const Names & required_result_column_names)
    : InterpreterSelectWithUnionQueryFragments(query_ptr_, Context::createCopy(context_), options_, required_result_column_names)
{
}

InterpreterSelectWithUnionQueryFragments::InterpreterSelectWithUnionQueryFragments(
    const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions & options_, const Names & required_result_column_names)
    : IInterpreterUnionOrSelectQuery(query_ptr_, context_, options_)
{
    ASTSelectWithUnionQuery * ast = query_ptr->as<ASTSelectWithUnionQuery>();
    bool require_full_header = ast->hasNonDefaultUnionMode();

    const Settings & settings = context->getSettingsRef();
    if (options.subquery_depth == 0 && (settings.limit > 0 || settings.offset > 0))
        settings_limit_offset_needed = true;

    size_t num_children = ast->list_of_selects->children.size();
    if (!num_children)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: no children in ASTSelectWithUnionQuery");

    /// Note that we pass 'required_result_column_names' to first SELECT.
    /// And for the rest, we pass names at the corresponding positions of 'required_result_column_names' in the result of first SELECT,
    ///  because names could be different.

    nested_interpreters.reserve(num_children);
    std::vector<Names> required_result_column_names_for_other_selects(num_children);

    if (!require_full_header && !required_result_column_names.empty() && num_children > 1)
    {
        /// Result header if there are no filtering by 'required_result_column_names'.
        /// We use it to determine positions of 'required_result_column_names' in SELECT clause.

        Block full_result_header = getCurrentChildResultHeader(ast->list_of_selects->children.at(0), required_result_column_names);

        std::vector<size_t> positions_of_required_result_columns(required_result_column_names.size());

        for (size_t required_result_num = 0, size = required_result_column_names.size(); required_result_num < size; ++required_result_num)
            positions_of_required_result_columns[required_result_num] = full_result_header.getPositionByName(required_result_column_names[required_result_num]);

        for (size_t query_num = 1; query_num < num_children; ++query_num)
        {
            Block full_result_header_for_current_select
                = getCurrentChildResultHeader(ast->list_of_selects->children.at(query_num), required_result_column_names);

            if (full_result_header_for_current_select.columns() != full_result_header.columns())
                throw Exception(ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH,
                                "Different number of columns in UNION ALL elements:\n{}\nand\n{}\n",
                                full_result_header.dumpNames(), full_result_header_for_current_select.dumpNames());

            required_result_column_names_for_other_selects[query_num].reserve(required_result_column_names.size());
            for (const auto & pos : positions_of_required_result_columns)
                required_result_column_names_for_other_selects[query_num].push_back(full_result_header_for_current_select.getByPosition(pos).name);
        }
    }

    if (num_children == 1 && settings_limit_offset_needed && !options.settings_limit_offset_done)
    {
        const ASTPtr first_select_ast = ast->list_of_selects->children.at(0);
        ASTSelectQuery * select_query = dynamic_cast<ASTSelectQuery *>(first_select_ast.get());
        if (!select_query)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid type in list_of_selects: {}", first_select_ast->getID());

        if (!select_query->withFill() && !select_query->limit_with_ties)
        {
            UInt64 limit_length = 0;
            UInt64 limit_offset = 0;

            const ASTPtr limit_offset_ast = select_query->limitOffset();
            if (limit_offset_ast)
            {
                limit_offset = evaluateConstantExpressionAsLiteral(limit_offset_ast, context)->as<ASTLiteral &>().value.safeGet<UInt64>();
                UInt64 new_limit_offset = settings.offset + limit_offset;
                ASTPtr new_limit_offset_ast = std::make_shared<ASTLiteral>(new_limit_offset);
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(new_limit_offset_ast));
            }
            else if (settings.offset)
            {
                ASTPtr new_limit_offset_ast = std::make_shared<ASTLiteral>(settings.offset.value);
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(new_limit_offset_ast));
            }

            const ASTPtr limit_length_ast = select_query->limitLength();
            if (limit_length_ast)
            {
                limit_length = evaluateConstantExpressionAsLiteral(limit_length_ast, context)->as<ASTLiteral &>().value.safeGet<UInt64>();

                UInt64 new_limit_length = 0;
                if (settings.offset == 0)
                    new_limit_length = std::min(limit_length, settings.limit.value);
                else if (settings.offset < limit_length)
                    new_limit_length = settings.limit ? std::min(settings.limit.value, limit_length - settings.offset.value)
                                                      : (limit_length - settings.offset.value);

                ASTPtr new_limit_length_ast = std::make_shared<ASTLiteral>(new_limit_length);
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(new_limit_length_ast));
            }
            else if (settings.limit)
            {
                ASTPtr new_limit_length_ast = std::make_shared<ASTLiteral>(settings.limit.value);
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(new_limit_length_ast));
            }

            options.settings_limit_offset_done = true;
        }
    }

    for (size_t query_num = 0; query_num < num_children; ++query_num)
    {
        const Names & current_required_result_column_names
            = query_num == 0 ? required_result_column_names : required_result_column_names_for_other_selects[query_num];

        nested_interpreters.emplace_back(
            buildCurrentChildInterpreter(ast->list_of_selects->children.at(query_num), require_full_header ? Names() : current_required_result_column_names));
        // We need to propagate the uses_view_source flag from children to the (self) parent since, if one of the children uses
        // a view source that means that the parent uses it too and can be cached globally
        uses_view_source |= nested_interpreters.back()->usesViewSource();
    }

    /// Determine structure of the result.

    if (num_children == 1)
    {
        result_header = nested_interpreters.front()->getSampleBlock();
    }
    else
    {
        Blocks headers(num_children);
        for (size_t query_num = 0; query_num < num_children; ++query_num)
        {
            headers[query_num] = nested_interpreters[query_num]->getSampleBlock();
            /// Here we check that, in case if required_result_column_names were specified,
            /// nested interpreter returns exactly it. Except if query requires full header.
            /// The code aboew is written in a way that for 0th query required_result_column_names_for_other_selects[0]
            /// is an empty list, and we should use required_result_column_names instead.
            const auto & current_required_result_column_names = (query_num == 0 && !require_full_header)
                ? required_result_column_names
                : required_result_column_names_for_other_selects[query_num];
            if (!current_required_result_column_names.empty())
            {
                const auto & header_columns = headers[query_num].getNames();
                if (current_required_result_column_names != header_columns)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Different order of columns in UNION subquery: {} and {}",
                        fmt::join(current_required_result_column_names, ", "),
                        fmt::join(header_columns, ", "));
                }

            }
        }

        result_header = getCommonHeaderForUnion(headers);
    }

    /// InterpreterSelectWithUnionQueryFragments ignores limits if all nested interpreters ignore limits.
    bool all_nested_ignore_limits = true;
    bool all_nested_ignore_quota = true;
    for (auto & interpreter : nested_interpreters)
    {
        if (!interpreter->ignoreLimits())
            all_nested_ignore_limits = false;
        if (!interpreter->ignoreQuota())
            all_nested_ignore_quota = false;
    }
    options.ignore_limits |= all_nested_ignore_limits;
    options.ignore_quota |= all_nested_ignore_quota;

}

Block InterpreterSelectWithUnionQueryFragments::getCommonHeaderForUnion(const Blocks & headers)
{
    size_t num_selects = headers.size();
    Block common_header = headers.front();
    size_t num_columns = common_header.columns();

    for (size_t query_num = 1; query_num < num_selects; ++query_num)
    {
        if (headers[query_num].columns() != num_columns)
            throw Exception(ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH,
                            "Different number of columns in UNION ALL elements:\n{}\nand\n{}\n",
                            common_header.dumpNames(), headers[query_num].dumpNames());
    }

    std::vector<const ColumnWithTypeAndName *> columns(num_selects);

    for (size_t column_num = 0; column_num < num_columns; ++column_num)
    {
        for (size_t i = 0; i < num_selects; ++i)
            columns[i] = &headers[i].getByPosition(column_num);

        ColumnWithTypeAndName & result_elem = common_header.getByPosition(column_num);
        result_elem = getLeastSuperColumn(columns);
    }

    return common_header;
}

Block InterpreterSelectWithUnionQueryFragments::getCurrentChildResultHeader(const ASTPtr & ast_ptr_, const Names & required_result_column_names)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return InterpreterSelectWithUnionQueryFragments(ast_ptr_, context, options.copy().analyze().noModify(), required_result_column_names)
            .getSampleBlock();
    else if (ast_ptr_->as<ASTSelectQuery>())
    {
        return InterpreterSelectQueryFragments(ast_ptr_, context, options.copy().analyze().noModify()).getSampleBlock();
    }
    else
        throw;
//        return InterpreterSelectIntersectExceptQuery(ast_ptr_, context, options.copy().analyze().noModify()).getSampleBlock();
}

std::unique_ptr<IInterpreterUnionOrSelectQuery>
InterpreterSelectWithUnionQueryFragments::buildCurrentChildInterpreter(const ASTPtr & ast_ptr_, const Names & current_required_result_column_names)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return std::make_unique<InterpreterSelectWithUnionQueryFragments>(ast_ptr_, context, options, current_required_result_column_names);
    else if (ast_ptr_->as<ASTSelectQuery>())
    {
        return std::make_unique<InterpreterSelectQueryFragments>(ast_ptr_, context, options, current_required_result_column_names);
    }
    else
        throw;
//        return std::make_unique<InterpreterSelectIntersectExceptQuery>(ast_ptr_, context, options);
}

InterpreterSelectWithUnionQueryFragments::~InterpreterSelectWithUnionQueryFragments() = default;

Block InterpreterSelectWithUnionQueryFragments::getSampleBlock(const ASTPtr & query_ptr_, ContextPtr context_, bool is_subquery, bool is_create_parameterized_view)
{
    if (!context_->hasQueryContext())
    {
        SelectQueryOptions options;
        if (is_subquery)
            options = options.subquery();
        if (is_create_parameterized_view)
            options = options.createParameterizedView();
        return InterpreterSelectWithUnionQueryFragments(query_ptr_, context_, std::move(options.analyze())).getSampleBlock();
    }

    auto & cache = context_->getSampleBlockCache();
    /// Using query string because query_ptr changes for every internal SELECT
    auto key = queryToString(query_ptr_);
    if (cache.find(key) != cache.end())
    {
        return cache[key];
    }

    SelectQueryOptions options;
    if (is_subquery)
        options = options.subquery();
    if (is_create_parameterized_view)
        options = options.createParameterizedView();
    return cache[key] = InterpreterSelectWithUnionQueryFragments(query_ptr_, context_, std::move(options.analyze())).getSampleBlock();
}

/// maybe abstract createFragments to here
PlanFragmentPtrs InterpreterSelectWithUnionQueryFragments::buildFragments()
{
//    size_t num_child_fragments = nested_interpreters.size();
//    const Settings & settings = context->getSettingsRef();
//
//    auto local_limits = getStorageLimits(*context, options);
//    storage_limits.emplace_back(local_limits);
//    for (auto & interpreter : nested_interpreters)
//        interpreter->addStorageLimits(storage_limits);
//
//    PlanFragmentPtr fragment;
//    /// Skip union for single interpreter.
//    if (num_child_fragments == 1)
//    {
//        auto * select_interpreter = dynamic_cast<InterpreterSelectQueryFragments *>(nested_interpreters.front().get());
//        if (select_interpreter)
//        {
//            auto nested_fragments = select_interpreter->buildFragments();
//            fragment = nested_fragments.back();
//
//            fragments.insert(fragments.end(), nested_fragments.begin(), nested_fragments.end());
//        }
//    }
//    else
//    {
//        std::vector<PlanFragmentPtr> child_fragments(num_child_fragments);
//        DataStreams data_streams(num_child_fragments);
//
//        for (size_t i = 0; i < num_child_fragments; ++i)
//        {
//            auto * select_interpreter = dynamic_cast<InterpreterSelectQueryFragments *>(nested_interpreters[i].get());
//            if (select_interpreter)
//            {
//                auto nested_fragments = select_interpreter->buildFragments();
//
//                child_fragments[i] = nested_fragments.back();
//
//                auto top_fragment = child_fragments[i];
//                if (!blocksHaveEqualStructure(top_fragment->getCurrentDataStream().header, result_header))
//                {
//                    auto actions_dag = ActionsDAG::makeConvertingActions(
//                        top_fragment->getCurrentDataStream().header.getColumnsWithTypeAndName(),
//                        result_header.getColumnsWithTypeAndName(),
//                        ActionsDAG::MatchColumnsMode::Position);
//                    auto converting_step = std::make_unique<ExpressionStep>(top_fragment->getCurrentDataStream(), std::move(actions_dag));
//                    converting_step->setStepDescription("Conversion before UNION");
//                    top_fragment->addStep(std::move(converting_step));
//                }
//
//                data_streams[i] = top_fragment->getCurrentDataStream();
//
//                fragments.insert(fragments.end(), nested_fragments.begin(), nested_fragments.end());
//            }
//        }
//
//        auto max_threads = settings.max_threads;
//        auto step = std::make_shared<UnionStep>(std::move(data_streams), max_threads);
//
//        DataPartition partition{.type = PartitionType::UNPARTITIONED};
//        PlanFragmentPtr parent_fragment = std::make_shared<PlanFragment>(context->getFragmentID(), partition, context);
//        parent_fragment->unitePlanFragments(step, child_fragments);
//
//        const auto & query = query_ptr->as<ASTSelectWithUnionQuery &>();
//        if (query.union_mode == SelectUnionMode::UNION_DISTINCT)
//        {
//            /// Add distinct transform
//            SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);
//
//            auto distinct_step = std::make_shared<DistinctStep>(
//                parent_fragment->getCurrentDataStream(),
//                limits,
//                0,
//                result_header.getNames(),
//                false,
//                settings.optimize_distinct_in_order);
//
//            parent_fragment->addStep(std::move(distinct_step));
//        }
//
//        fragment = parent_fragment;
//        fragments.emplace_back(parent_fragment);
//    }
//
//    if (settings_limit_offset_needed && !options.settings_limit_offset_done)
//    {
//        if (settings.limit > 0)
//        {
//            auto limit = std::make_unique<LimitStep>(fragment->getCurrentDataStream(), settings.limit, settings.offset, settings.exact_rows_before_limit);
//            limit->setStepDescription("LIMIT OFFSET for SETTINGS");
//            fragment->addStep(std::move(limit));
//        }
//        else
//        {
//            auto offset = std::make_unique<OffsetStep>(fragment->getCurrentDataStream(), settings.offset);
//            offset->setStepDescription("OFFSET for SETTINGS");
//            fragment->addStep(std::move(offset));
//        }
//    }
//
//    addAdditionalPostFilter(*fragment);
//
//    fragment->addInterpreterContext(context);

    QueryPlan query_plan;
    buildQueryPlan(query_plan);

    query_plan.optimize(QueryPlanOptimizationSettings::fromContext(context));

    const auto & resources = query_plan.getResources();

    PlanFragmentBuilder builder(storage_limits, context, query_plan);
    const auto & res_fragments = builder.buildFragments();

    /// query_plan resources move to fragments
    /// Extend lifetime of context, table lock, storage.
    /// TODO every fragment need context, table lock, storage ?
    for (const auto & fragment : res_fragments)
    {
        for (const auto & context_ : resources.interpreter_context)
        {
            fragment->addInterpreterContext(context_);
        }

        for (const auto & storage_holder : resources.storage_holders)
        {
            fragment->addStorageHolder(storage_holder);
        }

        for (const auto & table_lock_ : resources.table_locks)
        {
            fragment->addTableLock(table_lock_);
        }
    }

    return res_fragments;
}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};

    WriteBufferFromOwnString buf;
    IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true);
    ast_format_settings.hilite = false;
    ast_format_settings.always_quote_identifiers = true;
    ast->format(ast_format_settings);
    return buf.str();
}

BlockIO InterpreterSelectWithUnionQueryFragments::execute()
{
    BlockIO res;

    const auto & fragments = buildFragments();

    WriteBufferFromOwnString buffer;
    fragments.back()->dump(buffer);
    LOG_INFO(&Poco::Logger::get("InterpreterSelectWithUnionQueryFragments"), "Fragment dump: {}", buffer.str());

    for (const auto & fragment : fragments)
    {
        /// add fragment wait for be scheduled
        res.query_coord_state.fragments = fragments;
    }

    /// schedule fragments
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        Coordinator coord(fragments, context, formattedAST(query_ptr));
        coord.schedulePrepareDistributedPipelines();

        /// local already be scheduled
        res.query_coord_state.pipelines = std::move(coord.pipelines);
        res.query_coord_state.remote_host_connection = coord.getRemoteHostConnection();
        res.pipeline = res.pipelines.rootPipeline();

        /// TODO quota only use to root pipeline?
        setQuota(res.pipeline);
    }

    return res;
}

/// only to used generate single plan
void InterpreterSelectWithUnionQueryFragments::buildQueryPlan(QueryPlan & query_plan)
{
    size_t num_plans = nested_interpreters.size();
    const Settings & settings = context->getSettingsRef();

    auto local_limits = getStorageLimits(*context, options);
    storage_limits.emplace_back(local_limits);
    for (auto & interpreter : nested_interpreters)
        interpreter->addStorageLimits(storage_limits);

    /// Skip union for single interpreter.
    if (num_plans == 1)
    {
        nested_interpreters.front()->buildQueryPlan(query_plan);
    }
    else
    {
        std::vector<std::unique_ptr<QueryPlan>> plans(num_plans);
        DataStreams data_streams(num_plans);

        for (size_t i = 0; i < num_plans; ++i)
        {
            plans[i] = std::make_unique<QueryPlan>();
            nested_interpreters[i]->buildQueryPlan(*plans[i]);

            if (!blocksHaveEqualStructure(plans[i]->getCurrentDataStream().header, result_header))
            {
                auto actions_dag = ActionsDAG::makeConvertingActions(
                    plans[i]->getCurrentDataStream().header.getColumnsWithTypeAndName(),
                    result_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Position);
                auto converting_step = std::make_unique<ExpressionStep>(plans[i]->getCurrentDataStream(), std::move(actions_dag));
                converting_step->setStepDescription("Conversion before UNION");
                plans[i]->addStep(std::move(converting_step));
            }

            data_streams[i] = plans[i]->getCurrentDataStream();
        }

        auto max_threads = settings.max_threads;
        auto union_step = std::make_unique<UnionStep>(std::move(data_streams), max_threads);

        query_plan.unitePlans(std::move(union_step), std::move(plans));

        const auto & query = query_ptr->as<ASTSelectWithUnionQuery &>();
        if (query.union_mode == SelectUnionMode::UNION_DISTINCT)
        {
            /// Add distinct transform
            SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

            auto distinct_step = std::make_unique<DistinctStep>(
                query_plan.getCurrentDataStream(),
                limits,
                0,
                result_header.getNames(),
                false,
                settings.optimize_distinct_in_order);

            query_plan.addStep(std::move(distinct_step));
        }
    }

    if (settings_limit_offset_needed && !options.settings_limit_offset_done)
    {
        if (settings.limit > 0)
        {
            auto limit = std::make_unique<LimitStep>(query_plan.getCurrentDataStream(), settings.limit, settings.offset, settings.exact_rows_before_limit);
            limit->setStepDescription("LIMIT OFFSET for SETTINGS");
            query_plan.addStep(std::move(limit));
        }
        else
        {
            auto offset = std::make_unique<OffsetStep>(query_plan.getCurrentDataStream(), settings.offset);
            offset->setStepDescription("OFFSET for SETTINGS");
            query_plan.addStep(std::move(offset));
        }
    }

    addAdditionalPostFilter(query_plan);
    query_plan.addInterpreterContext(context);
}

void InterpreterSelectWithUnionQueryFragments::ignoreWithTotals()
{
    for (auto & interpreter : nested_interpreters)
        interpreter->ignoreWithTotals();
}

void InterpreterSelectWithUnionQueryFragments::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr /*context_*/) const
{
    for (const auto & interpreter : nested_interpreters)
    {
        const auto * select_interpreter = dynamic_cast<const InterpreterSelectQueryFragments *>(interpreter.get());
        if (select_interpreter)
        {
            auto filter = select_interpreter->getRowPolicyFilter();
            if (filter)
            {
                for (const auto & row_policy : filter->policies)
                {
                    auto name = row_policy->getFullName().toString();
                    elem.used_row_policies.emplace(std::move(name));
                }
            }
        }
    }
}

}

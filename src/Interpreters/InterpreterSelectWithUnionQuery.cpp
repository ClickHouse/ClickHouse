#include <Access/AccessControl.h>

#include <Columns/getLeastSuperColumn.h>
#include <Common/MemoryTrackerUtils.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectIntersectExceptQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/typeid_cast.h>

#include <Interpreters/InDepthNodeVisitor.h>

#include <fmt/ranges.h>


namespace DB
{
namespace Setting
{
    extern const SettingsOverflowMode distinct_overflow_mode;
    extern const SettingsUInt64 max_bytes_in_distinct;
    extern const SettingsUInt64 max_rows_in_distinct;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_threads_min_free_memory_per_thread;
    extern const SettingsBool optimize_distinct_in_order;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
}

InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_, const Names & required_result_column_names)
    : InterpreterSelectWithUnionQuery(query_ptr_, Context::createCopy(context_), options_, required_result_column_names)
{
}

InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions & options_, const Names & required_result_column_names)
    : IInterpreterUnionOrSelectQuery(query_ptr_, context_, options_)
{
    ASTSelectWithUnionQuery * ast = query_ptr->as<ASTSelectWithUnionQuery>();
    bool require_full_header = ast->hasNonDefaultUnionMode();

    size_t num_children = ast->list_of_selects->children.size();
    if (!num_children)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No children in ASTSelectWithUnionQuery");

    /// Note that we pass 'required_result_column_names' to first SELECT.
    /// And for the rest, we pass names at the corresponding positions of 'required_result_column_names' in the result of first SELECT,
    ///  because names could be different.

    nested_interpreters.reserve(num_children);
    std::vector<Names> required_result_column_names_for_other_selects(num_children);

    if (!require_full_header && !required_result_column_names.empty() && num_children > 1)
    {
        /// Result header if there are no filtering by 'required_result_column_names'.
        /// We use it to determine positions of 'required_result_column_names' in SELECT clause.

        auto full_result_header = getCurrentChildResultHeader(ast->list_of_selects->children.at(0), required_result_column_names);

        std::vector<size_t> positions_of_required_result_columns(required_result_column_names.size());

        for (size_t required_result_num = 0, size = required_result_column_names.size(); required_result_num < size; ++required_result_num)
            positions_of_required_result_columns[required_result_num] = full_result_header->getPositionByName(required_result_column_names[required_result_num]);

        for (size_t query_num = 1; query_num < num_children; ++query_num)
        {
            auto full_result_header_for_current_select
                = getCurrentChildResultHeader(ast->list_of_selects->children.at(query_num), required_result_column_names);

            if (full_result_header_for_current_select->columns() != full_result_header->columns())
                throw Exception(ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH,
                                "Different number of columns in UNION ALL elements:\n{}\nand\n{}\n",
                                full_result_header->dumpNames(), full_result_header_for_current_select->dumpNames());

            required_result_column_names_for_other_selects[query_num].reserve(required_result_column_names.size());
            for (const auto & pos : positions_of_required_result_columns)
                required_result_column_names_for_other_selects[query_num].push_back(full_result_header_for_current_select->getByPosition(pos).name);
        }
    }

    /// The `limit` / `offset` settings are applied earlier, in `applyQueryConstructionSettings`
    /// (`executeQuery`), by wrapping the top-level query as a derived table with an outer
    /// `LIMIT`/`OFFSET` and clearing the settings on the context. So there is nothing to apply here.
    /// Subquery-level `SETTINGS limit/offset` (e.g. `view(SELECT … SETTINGS limit = 5)`) is handled
    /// by the analyzer in `QueryTreeBuilder`.

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
        SharedHeaders headers(num_children);
        for (size_t query_num = 0; query_num < num_children; ++query_num)
        {
            headers[query_num] = nested_interpreters[query_num]->getSampleBlock();
            /// Here we check that, in case if required_result_column_names were specified,
            /// nested interpreter returns exactly it. Except if query requires full header.
            /// The code above is written in a way that for 0th query required_result_column_names_for_other_selects[0]
            /// is an empty list, and we should use required_result_column_names instead.
            const auto & current_required_result_column_names = (query_num == 0 && !require_full_header)
                ? required_result_column_names
                : required_result_column_names_for_other_selects[query_num];
            if (!current_required_result_column_names.empty())
            {
                const auto & header_columns = headers[query_num]->getNames();
                if (current_required_result_column_names != header_columns)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Different order of columns in UNION subquery: {} and {}",
                        fmt::join(current_required_result_column_names, ", "),
                        fmt::join(header_columns, ", "));
                }

            }
        }

        result_header = std::make_shared<const Block>(getCommonHeaderForUnion(headers));
    }

    /// InterpreterSelectWithUnionQuery ignores limits if all nested interpreters ignore limits.
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

Block InterpreterSelectWithUnionQuery::getCommonHeaderForUnion(const SharedHeaders & headers)
{
    size_t num_selects = headers.size();
    Block common_header = *headers.front();
    size_t num_columns = common_header.columns();

    for (size_t query_num = 1; query_num < num_selects; ++query_num)
    {
        if (headers[query_num]->columns() != num_columns)
            throw Exception(ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH,
                            "Different number of columns in UNION ALL elements:\n{}\nand\n{}\n",
                            common_header.dumpNames(), headers[query_num]->dumpNames());
    }

    VectorWithMemoryTracking<const ColumnWithTypeAndName *> columns(num_selects);

    for (size_t column_num = 0; column_num < num_columns; ++column_num)
    {
        for (size_t i = 0; i < num_selects; ++i)
            columns[i] = &headers[i]->getByPosition(column_num);

        ColumnWithTypeAndName & result_elem = common_header.getByPosition(column_num);
        result_elem = getLeastSuperColumn(columns);
    }

    return common_header;
}

SharedHeader InterpreterSelectWithUnionQuery::getCurrentChildResultHeader(const ASTPtr & ast_ptr_, const Names & required_result_column_names)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return InterpreterSelectWithUnionQuery(ast_ptr_, context, options.copy().analyze().noModify(), required_result_column_names)
            .getSampleBlock();
    if (ast_ptr_->as<ASTSelectQuery>())
        return InterpreterSelectQuery(ast_ptr_, context, options.copy().analyze().noModify()).getSampleBlock();
    return InterpreterSelectIntersectExceptQuery(ast_ptr_, context, options.copy().analyze().noModify()).getSampleBlock();
}

std::unique_ptr<IInterpreterUnionOrSelectQuery>
InterpreterSelectWithUnionQuery::buildCurrentChildInterpreter(const ASTPtr & ast_ptr_, const Names & current_required_result_column_names)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return std::make_unique<InterpreterSelectWithUnionQuery>(ast_ptr_, context, options, current_required_result_column_names);
    if (ast_ptr_->as<ASTSelectQuery>())
        return std::make_unique<InterpreterSelectQuery>(ast_ptr_, context, options, current_required_result_column_names);
    return std::make_unique<InterpreterSelectIntersectExceptQuery>(ast_ptr_, context, options);
}

InterpreterSelectWithUnionQuery::~InterpreterSelectWithUnionQuery() = default;

SharedHeader InterpreterSelectWithUnionQuery::getSampleBlock(const ASTPtr & query_ptr_, ContextPtr context_, bool is_subquery, bool is_create_parameterized_view)
{
    if (!context_->hasQueryContext())
    {
        SelectQueryOptions options;
        if (is_subquery)
            options = options.subquery();
        if (is_create_parameterized_view)
            options = options.createParameterizedView();
        return InterpreterSelectWithUnionQuery(query_ptr_, context_, std::move(options.analyze())).getSampleBlock();
    }

    /// Using query string because query_ptr changes for every internal SELECT
    auto key = query_ptr_->formatWithSecretsOneLine();
    {
        auto [cache, lock] = context_->getSampleBlockCache();
        if (cache->contains(key))
            return cache->at(key);
    }

    SelectQueryOptions options;
    if (is_subquery)
        options = options.subquery();
    if (is_create_parameterized_view)
        options = options.createParameterizedView();

    auto sample_block = InterpreterSelectWithUnionQuery(query_ptr_, context_, std::move(options.analyze())).getSampleBlock();
    auto [cache, lock] = context_->getSampleBlockCache();
    return (*cache)[key] = sample_block;
}


void InterpreterSelectWithUnionQuery::buildQueryPlan(QueryPlan & query_plan)
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
        SharedHeaders headers(num_plans);

        for (size_t i = 0; i < num_plans; ++i)
        {
            plans[i] = std::make_unique<QueryPlan>();
            nested_interpreters[i]->buildQueryPlan(*plans[i]);

            if (!blocksHaveEqualStructure(*plans[i]->getCurrentHeader(), *result_header))
            {
                auto actions_dag = ActionsDAG::makeConvertingActions(
                        plans[i]->getCurrentHeader()->getColumnsWithTypeAndName(),
                        result_header->getColumnsWithTypeAndName(),
                        ActionsDAG::MatchColumnsMode::Position,
                        context);
                auto converting_step = std::make_unique<ExpressionStep>(plans[i]->getCurrentHeader(), std::move(actions_dag));
                converting_step->setStepDescription("Conversion before UNION");
                plans[i]->addStep(std::move(converting_step));
            }

            headers[i] = plans[i]->getCurrentHeader();
        }

        auto max_threads = getMaxThreadsForAvailableMemory(
            settings[Setting::max_threads], settings[Setting::max_threads_min_free_memory_per_thread]);
        auto union_step = std::make_unique<UnionStep>(std::move(headers), max_threads, /* allow_narrowing = */ true);

        query_plan.unitePlans(std::move(union_step), std::move(plans));

        const auto & query = query_ptr->as<ASTSelectWithUnionQuery &>();
        if (query.union_mode == SelectUnionMode::UNION_DISTINCT)
        {
            /// Add distinct transform
            SizeLimits limits(settings[Setting::max_rows_in_distinct], settings[Setting::max_bytes_in_distinct], settings[Setting::distinct_overflow_mode]);

            auto distinct_step = std::make_unique<DistinctStep>(
                query_plan.getCurrentHeader(),
                limits,
                0,
                result_header->getNames(),
                false);

            query_plan.addStep(std::move(distinct_step));
        }
    }

    addAdditionalPostFilter(query_plan);
    query_plan.addInterpreterContext(context);
}

BlockIO InterpreterSelectWithUnionQuery::execute()
{
    BlockIO res;

    QueryPlan query_plan;
    buildQueryPlan(query_plan);

    auto builder = query_plan.buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));

    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    setQuota(res.pipeline);
    return res;
}

void InterpreterSelectWithUnionQuery::ignoreWithTotals()
{
    for (auto & interpreter : nested_interpreters)
        interpreter->ignoreWithTotals();
}

void registerInterpreterSelectWithUnionQuery(InterpreterFactory & factory);
void registerInterpreterSelectWithUnionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSelectWithUnionQuery>(args.query, args.context, args.options);
    };
    factory.registerInterpreter("InterpreterSelectWithUnionQuery", create_fn);
}

}

#include <Access/AccessControl.h>

#include <Columns/getLeastSuperColumn.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectIntersectExceptQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/QueryLog.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
namespace Setting
{
    extern const SettingsOverflowMode distinct_overflow_mode;
    extern const SettingsUInt64 max_rows_in_distinct;
    extern const SettingsUInt64 max_bytes_in_distinct;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsBool optimize_distinct_in_order;
}

namespace ErrorCodes
{
    extern const int INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH;
    extern const int LOGICAL_ERROR;
}

static Block getCommonHeader(const Blocks & headers)
{
    size_t num_selects = headers.size();
    Block common_header = headers.front();
    size_t num_columns = common_header.columns();

    for (size_t query_num = 1; query_num < num_selects; ++query_num)
    {
        if (headers[query_num].columns() != num_columns)
            throw Exception(ErrorCodes::INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH,
                            "Different number of columns in IntersectExceptQuery elements:\n {} \nand\n {}",
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

InterpreterSelectIntersectExceptQuery::InterpreterSelectIntersectExceptQuery(
    const ASTPtr & query_ptr_,
    ContextPtr context_,
    const SelectQueryOptions & options_)
    :InterpreterSelectIntersectExceptQuery(query_ptr_, Context::createCopy(context_), options_)
{
}

InterpreterSelectIntersectExceptQuery::InterpreterSelectIntersectExceptQuery(
    const ASTPtr & query_ptr_,
    ContextMutablePtr context_,
    const SelectQueryOptions & options_)
    : IInterpreterUnionOrSelectQuery(query_ptr_->clone(), context_, options_)
{
    ASTSelectIntersectExceptQuery * ast = query_ptr->as<ASTSelectIntersectExceptQuery>();
    final_operator = ast->final_operator;

    const auto & children = ast->getListOfSelects();
    size_t num_children = children.size();

    /// AST must have been changed by the visitor.
    if (final_operator == Operator::UNKNOWN || num_children != 2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "SelectIntersectExceptQuery has not been normalized (number of children: {})", num_children);

    nested_interpreters.resize(num_children);

    for (size_t i = 0; i < num_children; ++i)
    {
        nested_interpreters[i] = buildCurrentChildInterpreter(children.at(i));
        uses_view_source |= nested_interpreters[i]->usesViewSource();
    }

    Blocks headers(num_children);
    for (size_t query_num = 0; query_num < num_children; ++query_num)
        headers[query_num] = nested_interpreters[query_num]->getSampleBlock();

    result_header = getCommonHeader(headers);
}

std::unique_ptr<IInterpreterUnionOrSelectQuery>
InterpreterSelectIntersectExceptQuery::buildCurrentChildInterpreter(const ASTPtr & ast_ptr_)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return std::make_unique<InterpreterSelectWithUnionQuery>(ast_ptr_, context, SelectQueryOptions());

    if (ast_ptr_->as<ASTSelectQuery>())
        return std::make_unique<InterpreterSelectQuery>(ast_ptr_, context, SelectQueryOptions());

    if (ast_ptr_->as<ASTSelectIntersectExceptQuery>())
        return std::make_unique<InterpreterSelectIntersectExceptQuery>(ast_ptr_, context, SelectQueryOptions());

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected query: {}", ast_ptr_->getID());
}

void InterpreterSelectIntersectExceptQuery::buildQueryPlan(QueryPlan & query_plan)
{
    auto local_limits = getStorageLimits(*context, options);
    storage_limits.emplace_back(local_limits);
    for (auto & interpreter : nested_interpreters)
        interpreter->addStorageLimits(storage_limits);

    size_t num_plans = nested_interpreters.size();
    std::vector<std::unique_ptr<QueryPlan>> plans(num_plans);
    Headers headers(num_plans);

    for (size_t i = 0; i < num_plans; ++i)
    {
        plans[i] = std::make_unique<QueryPlan>();
        nested_interpreters[i]->buildQueryPlan(*plans[i]);

        if (!blocksHaveEqualStructure(plans[i]->getCurrentHeader(), result_header))
        {
            auto actions_dag = ActionsDAG::makeConvertingActions(
                    plans[i]->getCurrentHeader().getColumnsWithTypeAndName(),
                    result_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Position);
            auto converting_step = std::make_unique<ExpressionStep>(plans[i]->getCurrentHeader(), std::move(actions_dag));
            converting_step->setStepDescription("Conversion before UNION");
            plans[i]->addStep(std::move(converting_step));
        }

        headers[i] = plans[i]->getCurrentHeader();
    }

    const Settings & settings = context->getSettingsRef();
    auto step = std::make_unique<IntersectOrExceptStep>(std::move(headers), final_operator, settings[Setting::max_threads]);
    query_plan.unitePlans(std::move(step), std::move(plans));

    const auto & query = query_ptr->as<ASTSelectIntersectExceptQuery &>();
    if (query.final_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT
        || query.final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT)
    {
        /// Add distinct transform
        SizeLimits limits(settings[Setting::max_rows_in_distinct], settings[Setting::max_bytes_in_distinct], settings[Setting::distinct_overflow_mode]);

        auto distinct_step = std::make_unique<DistinctStep>(
            query_plan.getCurrentHeader(),
            limits,
            0,
            result_header.getNames(),
            false);

        query_plan.addStep(std::move(distinct_step));
    }

    addAdditionalPostFilter(query_plan);
    query_plan.addInterpreterContext(context);
}

BlockIO InterpreterSelectIntersectExceptQuery::execute()
{
    BlockIO res;

    QueryPlan query_plan;
    buildQueryPlan(query_plan);

    auto builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context));

    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    setQuota(res.pipeline);

    return res;
}

void InterpreterSelectIntersectExceptQuery::ignoreWithTotals()
{
    for (auto & interpreter : nested_interpreters)
        interpreter->ignoreWithTotals();
}

void InterpreterSelectIntersectExceptQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr /*context_*/) const
{
    for (const auto & interpreter : nested_interpreters)
    {
        if (const auto * select_interpreter = dynamic_cast<const InterpreterSelectQuery *>(interpreter.get()))
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

void registerInterpreterSelectIntersectExceptQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSelectIntersectExceptQuery>(args.query, args.context, args.options);
    };
    factory.registerInterpreter("InterpreterSelectIntersectExceptQuery", create_fn);
}

}

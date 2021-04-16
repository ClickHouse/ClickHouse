#include <Columns/getLeastSuperColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterIntersectOrExcept.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTIntersectOrExcept.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH;
}

InterpreterIntersectOrExcept::InterpreterIntersectOrExcept(const ASTPtr & query_ptr_, ContextPtr context_)
    : query_ptr(query_ptr_), context(Context::createCopy(context_))
{
    ASTIntersectOrExcept * ast = query_ptr->as<ASTIntersectOrExcept>();
    size_t num_children = ast->children.size();
    nested_interpreters.resize(num_children);
    for (size_t i = 0; i < num_children; ++i)
    {
        nested_interpreters[i] = buildCurrentChildInterpreter(ast->children[i]);
    }

    Blocks headers(num_children);
    for (size_t query_num = 0; query_num < num_children; ++query_num)
        headers[query_num] = nested_interpreters[query_num]->getSampleBlock();

    result_header = getCommonHeader(headers);
}


Block InterpreterIntersectOrExcept::getCommonHeader(const Blocks & headers)
{
    size_t num_selects = headers.size();
    Block common_header = headers.front();
    size_t num_columns = common_header.columns();

    for (size_t query_num = 1; query_num < num_selects; ++query_num)
    {
        if (headers[query_num].columns() != num_columns)
            throw Exception(
                "Different number of columns in "
                    + toString(query_ptr->as<ASTIntersectOrExcept>()->is_except ? "EXCEPT" : "INTERSECT")
                    + " elements:\n" + common_header.dumpNames() + "\nand\n"
                    + headers[query_num].dumpNames() + "\n",
                    ErrorCodes::INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH);
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


std::unique_ptr<IInterpreterUnionOrSelectQuery>
InterpreterIntersectOrExcept::buildCurrentChildInterpreter(const ASTPtr & ast_ptr_)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return std::make_unique<InterpreterSelectWithUnionQuery>(ast_ptr_, context, SelectQueryOptions());
    else
        return std::make_unique<InterpreterSelectQuery>(ast_ptr_, context, SelectQueryOptions());
}

void InterpreterIntersectOrExcept::buildQueryPlan(QueryPlan & query_plan)
{
    size_t num_plans = nested_interpreters.size();

    std::vector<std::unique_ptr<QueryPlan>> plans(num_plans);
    DataStreams data_streams(num_plans);

    for (size_t i = 0; i < num_plans; ++i)
    {
        plans[i] = std::make_unique<QueryPlan>();
        nested_interpreters[i]->buildQueryPlan(*plans[i]);
        data_streams[i] = plans[i]->getCurrentDataStream();
    }

    auto max_threads = context->getSettingsRef().max_threads;
    auto step = std::make_unique<IntersectOrExceptStep>(
        query_ptr->as<ASTIntersectOrExcept>()->is_except, std::move(data_streams), result_header, max_threads);
    query_plan.unitePlans(std::move(step), std::move(plans));
}

BlockIO InterpreterIntersectOrExcept::execute()
{
    BlockIO res;

    QueryPlan query_plan;
    buildQueryPlan(query_plan);

    auto pipeline = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context));

    res.pipeline = std::move(*pipeline);
    res.pipeline.addInterpreterContext(context);

    return res;
}
}

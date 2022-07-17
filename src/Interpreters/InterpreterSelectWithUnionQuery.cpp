#include <Columns/getLeastSuperColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/typeid_cast.h>

#include <Interpreters/InDepthNodeVisitor.h>

#include <algorithm>

namespace DB
{

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

    const Settings & settings = context->getSettingsRef();
    if (options.subquery_depth == 0 && (settings.limit > 0 || settings.offset > 0))
        settings_limit_offset_needed = true;

    size_t num_children = ast->list_of_selects->children.size();
    if (!num_children)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

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
                throw Exception("Different number of columns in UNION ALL elements:\n"
                    + full_result_header.dumpNames()
                    + "\nand\n"
                    + full_result_header_for_current_select.dumpNames() + "\n",
                    ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);

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
                limit_offset = limit_offset_ast->as<ASTLiteral &>().value.safeGet<UInt64>();
                UInt64 new_limit_offset = settings.offset + limit_offset;
                limit_offset_ast->as<ASTLiteral &>().value = Field(new_limit_offset);
            }
            else if (settings.offset)
            {
                ASTPtr new_limit_offset_ast = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(settings.offset)));
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(new_limit_offset_ast));
            }

            const ASTPtr limit_length_ast = select_query->limitLength();
            if (limit_length_ast)
            {
                limit_length = limit_length_ast->as<ASTLiteral &>().value.safeGet<UInt64>();

                UInt64 new_limit_length = 0;
                if (settings.offset == 0)
                    new_limit_length = std::min(limit_length, static_cast<UInt64>(settings.limit));
                else if (settings.offset < limit_length)
                    new_limit_length =  settings.limit ? std::min(static_cast<UInt64>(settings.limit), limit_length - settings.offset) : (limit_length - settings.offset);

                limit_length_ast->as<ASTLiteral &>().value = Field(new_limit_length);
            }
            else if (settings.limit)
            {
                ASTPtr new_limit_length_ast = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(settings.limit)));
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
            const auto & current_required_result_column_names = required_result_column_names_for_other_selects[query_num];
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

Block InterpreterSelectWithUnionQuery::getCommonHeaderForUnion(const Blocks & headers)
{
    size_t num_selects = headers.size();
    Block common_header = headers.front();
    size_t num_columns = common_header.columns();

    for (size_t query_num = 1; query_num < num_selects; ++query_num)
    {
        if (headers[query_num].columns() != num_columns)
            throw Exception("Different number of columns in UNION ALL elements:\n"
                            + common_header.dumpNames()
                            + "\nand\n"
                            + headers[query_num].dumpNames() + "\n",
                            ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);
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

Block InterpreterSelectWithUnionQuery::getCurrentChildResultHeader(const ASTPtr & ast_ptr_, const Names & required_result_column_names)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return InterpreterSelectWithUnionQuery(ast_ptr_, context, options.copy().analyze().noModify(), required_result_column_names)
            .getSampleBlock();
    else if (ast_ptr_->as<ASTSelectQuery>())
        return InterpreterSelectQuery(ast_ptr_, context, options.copy().analyze().noModify()).getSampleBlock();
    else
        return InterpreterSelectIntersectExceptQuery(ast_ptr_, context, options.copy().analyze().noModify()).getSampleBlock();
}

std::unique_ptr<IInterpreterUnionOrSelectQuery>
InterpreterSelectWithUnionQuery::buildCurrentChildInterpreter(const ASTPtr & ast_ptr_, const Names & current_required_result_column_names)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return std::make_unique<InterpreterSelectWithUnionQuery>(ast_ptr_, context, options, current_required_result_column_names);
    else if (ast_ptr_->as<ASTSelectQuery>())
        return std::make_unique<InterpreterSelectQuery>(ast_ptr_, context, options, current_required_result_column_names);
    else
        return std::make_unique<InterpreterSelectIntersectExceptQuery>(ast_ptr_, context, options);
}

InterpreterSelectWithUnionQuery::~InterpreterSelectWithUnionQuery() = default;

Block InterpreterSelectWithUnionQuery::getSampleBlock(const ASTPtr & query_ptr_, ContextPtr context_, bool is_subquery)
{
    if (!context_->hasQueryContext())
    {
        if (is_subquery)
            return InterpreterSelectWithUnionQuery(query_ptr_, context_, SelectQueryOptions().subquery().analyze()).getSampleBlock();
        else
            return InterpreterSelectWithUnionQuery(query_ptr_, context_, SelectQueryOptions().analyze()).getSampleBlock();
    }

    auto & cache = context_->getSampleBlockCache();
    /// Using query string because query_ptr changes for every internal SELECT
    auto key = queryToString(query_ptr_);
    if (cache.find(key) != cache.end())
    {
        return cache[key];
    }

    if (is_subquery)
    {
        return cache[key]
            = InterpreterSelectWithUnionQuery(query_ptr_, context_, SelectQueryOptions().subquery().analyze()).getSampleBlock();
    }
    else
    {
        return cache[key] = InterpreterSelectWithUnionQuery(query_ptr_, context_, SelectQueryOptions().analyze()).getSampleBlock();
    }
}


void InterpreterSelectWithUnionQuery::buildQueryPlan(QueryPlan & query_plan)
{
    // auto num_distinct_union = optimizeUnionList();
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

        auto max_threads = context->getSettingsRef().max_threads;
        auto union_step = std::make_unique<UnionStep>(std::move(data_streams), max_threads);

        query_plan.unitePlans(std::move(union_step), std::move(plans));

        const auto & query = query_ptr->as<ASTSelectWithUnionQuery &>();
        if (query.union_mode == SelectUnionMode::DISTINCT)
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
            auto limit = std::make_unique<LimitStep>(query_plan.getCurrentDataStream(), settings.limit, settings.offset);
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

    query_plan.addInterpreterContext(context);
}

BlockIO InterpreterSelectWithUnionQuery::execute()
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

void InterpreterSelectWithUnionQuery::ignoreWithTotals()
{
    for (auto & interpreter : nested_interpreters)
        interpreter->ignoreWithTotals();
}

}

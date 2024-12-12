#include <Access/AccessControl.h>

#include <Columns/getLeastSuperColumn.h>
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
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/typeid_cast.h>

#include <Interpreters/InDepthNodeVisitor.h>

#include <algorithm>


namespace DB
{
namespace Setting
{
    extern const SettingsOverflowMode distinct_overflow_mode;
    extern const SettingsBool exact_rows_before_limit;
    extern const SettingsUInt64 limit;
    extern const SettingsUInt64 max_bytes_in_distinct;
    extern const SettingsUInt64 max_rows_in_distinct;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 offset;
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

    const Settings & settings = context->getSettingsRef();
    if (options.subquery_depth == 0 && (settings[Setting::limit] > 0 || settings[Setting::offset] > 0))
        settings_limit_offset_needed = true;

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
                UInt64 new_limit_offset = settings[Setting::offset] + limit_offset;
                ASTPtr new_limit_offset_ast = std::make_shared<ASTLiteral>(new_limit_offset);
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(new_limit_offset_ast));
            }
            else if (settings[Setting::offset])
            {
                ASTPtr new_limit_offset_ast = std::make_shared<ASTLiteral>(settings[Setting::offset].value);
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(new_limit_offset_ast));
            }

            const ASTPtr limit_length_ast = select_query->limitLength();
            if (limit_length_ast)
            {
                limit_length = evaluateConstantExpressionAsLiteral(limit_length_ast, context)->as<ASTLiteral &>().value.safeGet<UInt64>();

                UInt64 new_limit_length = 0;
                if (settings[Setting::offset] == 0)
                    new_limit_length = std::min(limit_length, settings[Setting::limit].value);
                else if (settings[Setting::offset] < limit_length)
                    new_limit_length = settings[Setting::limit] ? std::min(settings[Setting::limit].value, limit_length - settings[Setting::offset].value)
                                                       : (limit_length - settings[Setting::offset].value);

                ASTPtr new_limit_length_ast = std::make_shared<ASTLiteral>(new_limit_length);
                select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(new_limit_length_ast));
            }
            else if (settings[Setting::limit])
            {
                ASTPtr new_limit_length_ast = std::make_shared<ASTLiteral>(settings[Setting::limit].value);
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
            /// The code above is written in a way that for 0th query required_result_column_names_for_other_selects[0]
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

Block InterpreterSelectWithUnionQuery::getCurrentChildResultHeader(const ASTPtr & ast_ptr_, const Names & required_result_column_names)
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

Block InterpreterSelectWithUnionQuery::getSampleBlock(const ASTPtr & query_ptr_, ContextPtr context_, bool is_subquery, bool is_create_parameterized_view)
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
    return cache[key] = InterpreterSelectWithUnionQuery(query_ptr_, context_, std::move(options.analyze())).getSampleBlock();
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

        auto max_threads = settings[Setting::max_threads];
        auto union_step = std::make_unique<UnionStep>(std::move(headers), max_threads);

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
                result_header.getNames(),
                false);

            query_plan.addStep(std::move(distinct_step));
        }
    }

    if (settings_limit_offset_needed && !options.settings_limit_offset_done)
    {
        if (settings[Setting::limit] > 0)
        {
            auto limit = std::make_unique<LimitStep>(
                query_plan.getCurrentHeader(), settings[Setting::limit], settings[Setting::offset], settings[Setting::exact_rows_before_limit]);
            limit->setStepDescription("LIMIT OFFSET for SETTINGS");
            query_plan.addStep(std::move(limit));
        }
        else
        {
            auto offset = std::make_unique<OffsetStep>(query_plan.getCurrentHeader(), settings[Setting::offset]);
            offset->setStepDescription("OFFSET for SETTINGS");
            query_plan.addStep(std::move(offset));
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

void InterpreterSelectWithUnionQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr /*context_*/) const
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

void registerInterpreterSelectWithUnionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSelectWithUnionQuery>(args.query, args.context, args.options);
    };
    factory.registerInterpreter("InterpreterSelectWithUnionQuery", create_fn);
}

}

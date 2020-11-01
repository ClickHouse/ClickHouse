#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Columns/getLeastSuperColumn.h>
#include <Common/typeid_cast.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/DistinctStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
    extern const int EXPECTED_ALL_OR_DISTINCT;
}

InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_, const Context & context_, const SelectQueryOptions & options_, const Names & required_result_column_names)
    : IInterpreterUnionOrSelectQuery(query_ptr_, context_, options_)
{
    const auto & ast = query_ptr->as<ASTSelectWithUnionQuery &>();

    size_t num_children = ast.list_of_selects->children.size();
    if (!num_children)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    /// We first build nested interpreters for each select query, then using this nested interpreters to build Tree Structured nested interpreter.
    /// Note that we pass 'required_result_column_names' to first SELECT.
    /// And for the rest, we pass names at the corresponding positions of 'required_result_column_names' in the result of first SELECT,
    ///  because names could be different.

    nested_interpreters.reserve(num_children);
    std::vector<Names> required_result_column_names_for_other_selects(num_children);

    if (!required_result_column_names.empty() && num_children > 1)
    {
        /// Result header if there are no filtering by 'required_result_column_names'.
        /// We use it to determine positions of 'required_result_column_names' in SELECT clause.

        Block full_result_header = getCurrentChildResultHeader(ast.list_of_selects->children.at(0), required_result_column_names);

        std::vector<size_t> positions_of_required_result_columns(required_result_column_names.size());

        for (size_t required_result_num = 0, size = required_result_column_names.size(); required_result_num < size; ++required_result_num)
            positions_of_required_result_columns[required_result_num] = full_result_header.getPositionByName(required_result_column_names[required_result_num]);

        for (size_t query_num = 1; query_num < num_children; ++query_num)
        {
            Block full_result_header_for_current_select
                = getCurrentChildResultHeader(ast.list_of_selects->children.at(query_num), required_result_column_names);

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

    for (size_t query_num = 0; query_num < num_children; ++query_num)
    {
        const Names & current_required_result_column_names
            = query_num == 0 ? required_result_column_names : required_result_column_names_for_other_selects[query_num];

        nested_interpreters.emplace_back(
            buildCurrentChildInterpreter(ast.list_of_selects->children.at(query_num), current_required_result_column_names));
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
            headers[query_num] = nested_interpreters[query_num]->getSampleBlock();

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
    if (const auto _ = ast_ptr_->as<ASTSelectWithUnionQuery>())
        return InterpreterSelectWithUnionQuery(ast_ptr_, *context, options.copy().analyze().noModify(), required_result_column_names)
            .getSampleBlock();
    else
        return InterpreterSelectQuery(ast_ptr_, *context, options.copy().analyze().noModify()).getSampleBlock();
}

std::unique_ptr<IInterpreterUnionOrSelectQuery>
InterpreterSelectWithUnionQuery::buildCurrentChildInterpreter(const ASTPtr & ast_ptr_, const Names & current_required_result_column_names)
{
    if (const auto _ = ast_ptr_->as<ASTSelectWithUnionQuery>())
        return std::make_unique<InterpreterSelectWithUnionQuery>(ast_ptr_, *context, options, current_required_result_column_names);
    else
        return std::make_unique<InterpreterSelectQuery>(ast_ptr_, *context, options, current_required_result_column_names);
}

InterpreterSelectWithUnionQuery::~InterpreterSelectWithUnionQuery() = default;

Block InterpreterSelectWithUnionQuery::getSampleBlock(const ASTPtr & query_ptr_, const Context & context_)
{
    auto & cache = context_.getSampleBlockCache();
    /// Using query string because query_ptr changes for every internal SELECT
    auto key = queryToString(query_ptr_);
    if (cache.find(key) != cache.end())
    {
        return cache[key];
    }

    return cache[key] = InterpreterSelectWithUnionQuery(query_ptr_, context_, SelectQueryOptions().analyze()).getSampleBlock();
}

size_t InterpreterSelectWithUnionQuery::optimizeUnionList()
{
    auto union_distinct_num = 0;

    auto union_default_mode = context->getSettingsRef().union_default_mode;
    auto & ast = query_ptr->as<ASTSelectWithUnionQuery &>();
    size_t num_selects = ast.list_of_selects->children.size();

    if (!num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    if (num_selects > 1)
    {
        for (auto & mode : ast.union_modes)
        {
            if (mode == ASTSelectWithUnionQuery::Mode::Unspecified)
            {
                if (union_default_mode == UnionMode::ALL)
                    mode = ASTSelectWithUnionQuery::Mode::ALL;
                else if (union_default_mode == UnionMode::DISTINCT)
                    mode = ASTSelectWithUnionQuery::Mode::DISTINCT;
                else
                    throw Exception(
                        "Expected ALL or DISTINCT in SelectWithUnion query, because setting (union_default_mode) is empty",
                        DB::ErrorCodes::EXPECTED_ALL_OR_DISTINCT);
            }
        }
        /// Optimize: if there is UNION DISTINCT, all previous UNION DISTINCT can be rewritten to UNION ALL.
        /// Therefore we have at most one UNION DISTINCT in a sequence.
        for (auto rit = ast.union_modes.rbegin(); rit != ast.union_modes.rend(); ++rit)
        {
            if (*rit == ASTSelectWithUnionQuery::Mode::DISTINCT)
            {
                /// Number of streams need to do a DISTINCT transform after unite
                union_distinct_num = ast.union_modes.rend() - rit + 1;
                for (auto mode_to_modify = ++rit; mode_to_modify != ast.union_modes.rend(); ++mode_to_modify)
                    *mode_to_modify = ASTSelectWithUnionQuery::Mode::ALL;
                break;
            }
        }
    }
    return union_distinct_num;
}

void InterpreterSelectWithUnionQuery::buildQueryPlan(QueryPlan & query_plan)
{
    auto num_distinct_union = optimizeUnionList();
    size_t num_plans = nested_interpreters.size();

    /// Skip union for single interpreter.
    if (num_plans == 1)
    {
        nested_interpreters.front()->buildQueryPlan(query_plan);
        return;
    }

    /// All UNION streams in the chain does not need to do DISTINCT transform
    if (num_distinct_union == 0)
    {
        std::vector<std::unique_ptr<QueryPlan>> plans(num_plans);
        DataStreams data_streams(num_plans);

        for (size_t i = 0; i < num_plans; ++i)
        {
            plans[i] = std::make_unique<QueryPlan>();
            nested_interpreters[i]->buildQueryPlan(*plans[i]);
            data_streams[i] = plans[i]->getCurrentDataStream();
        }

        auto max_threads = context->getSettingsRef().max_threads;
        auto union_step = std::make_unique<UnionStep>(std::move(data_streams), result_header, max_threads);

        query_plan.unitePlans(std::move(union_step), std::move(plans));
    }

    /// The first union_distinct_num UNION streams need to do a DISTINCT transform after unite
    else
    {
        QueryPlan distinct_query_plan;

        std::vector<std::unique_ptr<QueryPlan>> plans(num_distinct_union);
        DataStreams data_streams(num_distinct_union);

        for (size_t i = 0; i < num_distinct_union; ++i)
        {
            plans[i] = std::make_unique<QueryPlan>();
            nested_interpreters[i]->buildQueryPlan(*plans[i]);
            data_streams[i] = plans[i]->getCurrentDataStream();
        }

        auto max_threads = context->getSettingsRef().max_threads;
        auto union_step = std::make_unique<UnionStep>(std::move(data_streams), result_header, max_threads);

        distinct_query_plan.unitePlans(std::move(union_step), std::move(plans));

        /// Add distinct transform
        const Settings & settings = context->getSettingsRef();
        SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

        auto distinct_step
            = std::make_unique<DistinctStep>(distinct_query_plan.getCurrentDataStream(), limits, 0, result_header.getNames(), false);

        distinct_query_plan.addStep(std::move(distinct_step));

        /// No other UNION streams after DISTINCT stream
        if (num_plans == num_distinct_union)
        {
            query_plan = std::move(distinct_query_plan);
            return;
        }

        /// Build final UNION step
        std::vector<std::unique_ptr<QueryPlan>> final_plans(num_plans - num_distinct_union + 1);
        DataStreams final_data_streams(num_plans - num_distinct_union + 1);

        final_plans[0] = std::make_unique<QueryPlan>(std::move(distinct_query_plan));
        final_data_streams[0] = final_plans[0]->getCurrentDataStream();

        for (size_t i = 1; i < num_plans - num_distinct_union + 1; ++i)
        {
            final_plans[i] = std::make_unique<QueryPlan>();
            nested_interpreters[num_distinct_union + i - 1]->buildQueryPlan(*final_plans[i]);
            final_data_streams[i] = final_plans[i]->getCurrentDataStream();
        }

        auto final_union_step = std::make_unique<UnionStep>(std::move(final_data_streams), result_header, max_threads);
        query_plan.unitePlans(std::move(final_union_step), std::move(final_plans));
    }
}

BlockIO InterpreterSelectWithUnionQuery::execute()
{
    BlockIO res;

    QueryPlan query_plan;
    buildQueryPlan(query_plan);

    auto pipeline = query_plan.buildQueryPipeline();

    res.pipeline = std::move(*pipeline);
    res.pipeline.addInterpreterContext(context);

    return res;
}


void InterpreterSelectWithUnionQuery::ignoreWithTotals()
{
    for (auto & interpreter : nested_interpreters)
        interpreter->ignoreWithTotals();
}

}

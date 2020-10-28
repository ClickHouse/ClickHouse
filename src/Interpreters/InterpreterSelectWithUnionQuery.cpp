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

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
    extern const int EXPECTED_ALL_OR_DISTINCT;
}

struct CustomizeUnionModeRewrite
{
    using TypeToVisit = ASTSelectWithUnionQuery;

    const UnionMode & default_union_mode;

    void visit(ASTSelectWithUnionQuery & union_select, ASTPtr &)
    {
        size_t num_selects = union_select.list_of_selects->children.size();
        if (!num_selects)
            throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);
        if (num_selects > 1)
        {
            for (auto & mode : union_select.union_modes)
            {
                if (mode == ASTSelectWithUnionQuery::Mode::Unspecified)
                {
                    if (default_union_mode == UnionMode::ALL)
                        mode = ASTSelectWithUnionQuery::Mode::ALL;
                    else if (default_union_mode == UnionMode::DISTINCT)
                        mode = ASTSelectWithUnionQuery::Mode::DISTINCT;
                    else
                        throw Exception(
                            "Expected ALL or DISTINCT in SelectWithUnion query, because setting (union_default_mode) is empty",
                            DB::ErrorCodes::EXPECTED_ALL_OR_DISTINCT);
                }
            }
            /// Optimize: if there is UNION DISTINCT, all previous UNION DISTINCT can be rewritten to UNION ALL.
            /// Therefore we have at most one UNION DISTINCT in a sequence.
            for (auto rit = union_select.union_modes.rbegin(); rit != union_select.union_modes.rend(); ++rit)
            {
                if (*rit == ASTSelectWithUnionQuery::Mode::DISTINCT)
                {
                    /// Number of streams need to do a DISTINCT transform after unite
                    for (auto mode_to_modify = ++rit; mode_to_modify != union_select.union_modes.rend(); ++mode_to_modify)
                        *mode_to_modify = ASTSelectWithUnionQuery::Mode::ALL;
                    break;
                }
            }
        }
    }
};

using CustomizeUnionQueryOptimizeVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeUnionModeRewrite>, true>;

QueryPlan NestedInterpreter::buildQueryPlan(const std::shared_ptr<Context> & context, const Block & header)
{
    QueryPlan res;
    if (type == Type::LEAF)
    {
        if (interpreter)
        {
            interpreter->buildQueryPlan(res);
            return res;
        }
        else
            throw Exception("Interpreter is not initialized.", ErrorCodes::LOGICAL_ERROR);
    }

    if (num_distinct_union == 0)
    {
        std::vector<std::unique_ptr<QueryPlan>> plans(children.size());
        DataStreams data_streams(children.size());

        for (size_t i = 0; i < children.size(); ++i)
        {
            plans[i] = std::make_unique<QueryPlan>(children[i]->buildQueryPlan(context, header));
            data_streams[i] = plans[i]->getCurrentDataStream();
        }

        auto max_threads = context->getSettingsRef().max_threads;
        auto union_step = std::make_unique<UnionStep>(std::move(data_streams), header, max_threads);

        res.unitePlans(std::move(union_step), std::move(plans));
        return res;
    }
    /// The first union_distinct_num UNION streams need to do a DISTINCT transform after unite
    else
    {
        QueryPlan distinct_query_plan;

        std::vector<std::unique_ptr<QueryPlan>> plans(num_distinct_union);
        DataStreams data_streams(num_distinct_union);

        for (size_t i = 0; i < num_distinct_union; ++i)
        {
            plans[i] = std::make_unique<QueryPlan>(children[i]->buildQueryPlan(context, header));
            data_streams[i] = plans[i]->getCurrentDataStream();
        }

        auto max_threads = context->getSettingsRef().max_threads;
        auto union_step = std::make_unique<UnionStep>(std::move(data_streams), header, max_threads);

        distinct_query_plan.unitePlans(std::move(union_step), std::move(plans));

        /// Add distinct transform
        const Settings & settings = context->getSettingsRef();
        SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

        auto distinct_step
            = std::make_unique<DistinctStep>(distinct_query_plan.getCurrentDataStream(), limits, 0, header.getNames(), false);

        distinct_query_plan.addStep(std::move(distinct_step));

        /// No other UNION streams after DISTINCT stream
        if (num_distinct_union == children.size())
        {
            return distinct_query_plan;
        }

        /// Build final UNION step
        std::vector<std::unique_ptr<QueryPlan>> final_plans(children.size() - num_distinct_union + 1);
        DataStreams final_data_streams(children.size() - num_distinct_union + 1);

        final_plans[0] = std::make_unique<QueryPlan>(std::move(distinct_query_plan));
        final_data_streams[0] = final_plans[0]->getCurrentDataStream();

        for (size_t i = 1; i < children.size() - num_distinct_union + 1; ++i)
        {
            final_plans[i] = std::make_unique<QueryPlan>(children[num_distinct_union + i - 1]->buildQueryPlan(context, header));
            final_data_streams[i] = final_plans[i]->getCurrentDataStream();
        }

        auto final_union_step = std::make_unique<UnionStep>(std::move(final_data_streams), header, max_threads);
        res.unitePlans(std::move(final_union_step), std::move(final_plans));
        return res;
    }
}

void NestedInterpreter::ignoreWithTotals()
{
    if (type == Type::LEAF)
    {
        if (interpreter)
            interpreter->ignoreWithTotals();
        else
        {
            throw Exception("Interpreter is not initialized.", ErrorCodes::LOGICAL_ERROR);
        }
        return;
    }
    for (auto & child : children)
    {
        child->ignoreWithTotals();
    }
}


InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const SelectQueryOptions & options_,
    const Names & required_result_column_names)
    : options(options_),
    query_ptr(query_ptr_),
    context(std::make_shared<Context>(context_)),
    max_streams(context->getSettingsRef().max_threads)
{
    std::cout << "\n\n In InterpreterSelectWithUnionQuery\n\n";
    const auto & ast = query_ptr->as<ASTSelectWithUnionQuery &>();
    std::cout << "\n\n before throw\n\n";
    if (!ast.flatten_nodes_list)
        std::cout << "\n\n flatten_nodes_list is null\n\n";
    size_t total_num_selects = ast.flatten_nodes_list->children.size();
    std::cout << "\n\n after get num throw\n\n";
    if (!total_num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);
    std::cout << "\n\n after throw\n\n";

    /// Rewrite ast with settings.union_default_mode
    const auto & settings = context->getSettingsRef();
    CustomizeUnionQueryOptimizeVisitor::Data data_union_mode{settings.union_default_mode};
    CustomizeUnionQueryOptimizeVisitor(data_union_mode).visit(query_ptr);

    /// We first build nested interpreters for each select query, then using this nested interpreters to build Tree Structured nested interpreter.
    /// Note that we pass 'required_result_column_names' to first SELECT.
    /// And for the rest, we pass names at the corresponding positions of 'required_result_column_names' in the result of first SELECT,
    ///  because names could be different.

    std::vector<std::shared_ptr<InterpreterSelectQuery>> interpreters;
    interpreters.reserve(total_num_selects);
    std::vector<Names> required_result_column_names_for_other_selects(total_num_selects);
    if (!required_result_column_names.empty() && total_num_selects > 1)
    {
        /// Result header if there are no filtering by 'required_result_column_names'.
        /// We use it to determine positions of 'required_result_column_names' in SELECT clause.

        Block full_result_header
            = InterpreterSelectQuery(ast.flatten_nodes_list->children.at(0), *context, options.copy().analyze().noModify())
                  .getSampleBlock();

        std::vector<size_t> positions_of_required_result_columns(required_result_column_names.size());
        for (size_t required_result_num = 0, size = required_result_column_names.size(); required_result_num < size; ++required_result_num)
            positions_of_required_result_columns[required_result_num] = full_result_header.getPositionByName(required_result_column_names[required_result_num]);

        for (size_t query_num = 1; query_num < total_num_selects; ++query_num)
        {
            Block full_result_header_for_current_select
                = InterpreterSelectQuery(ast.flatten_nodes_list->children.at(query_num), *context, options.copy().analyze().noModify())
                      .getSampleBlock();

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

    for (size_t query_num = 0; query_num < total_num_selects; ++query_num)
    {
        const Names & current_required_result_column_names
            = query_num == 0 ? required_result_column_names : required_result_column_names_for_other_selects[query_num];

        interpreters.emplace_back(std::make_shared<InterpreterSelectQuery>(
            ast.flatten_nodes_list->children.at(query_num), *context, options, current_required_result_column_names));
    }

    /// Determine structure of the result.

    if (total_num_selects == 1)
    {
        result_header = interpreters.front()->getSampleBlock();
    }
    else
    {
        Blocks headers(total_num_selects);
        for (size_t query_num = 0; query_num < total_num_selects; ++query_num)
            headers[query_num] = interpreters[query_num]->getSampleBlock();

        result_header = getCommonHeaderForUnion(headers);
    }

    /// InterpreterSelectWithUnionQuery ignores limits if all nested interpreters ignore limits.
    bool all_nested_ignore_limits = true;
    bool all_nested_ignore_quota = true;
    for (auto & interpreter : interpreters)
    {
        if (!interpreter->ignoreLimits())
            all_nested_ignore_limits = false;
        if (!interpreter->ignoreQuota())
            all_nested_ignore_quota = false;
    }
    options.ignore_limits |= all_nested_ignore_limits;
    options.ignore_quota |= all_nested_ignore_quota;

    int index = 0;
    buildNestedTreeInterpreter(query_ptr, nested_interpreter, interpreters, index);
}

/// We build a Tree Structured nested interpreters to build QueryPlan later
/// The structure of build nested interpreters is same as AST Tree
void InterpreterSelectWithUnionQuery::buildNestedTreeInterpreter(
    const ASTPtr & ast_ptr,
    std::shared_ptr<NestedInterpreter> nested_interpreter_,
    std::vector<std::shared_ptr<InterpreterSelectQuery>> & interpreters,
    int & index)
{
    std::cout << "\n\n in build \n\n";
    if (auto inner_union = ast_ptr->as<ASTSelectWithUnionQuery>())
    {
        auto internal_intepreter = std::make_shared<NestedInterpreter>();
        const auto & union_modes = inner_union->union_modes;

        for (auto rit = union_modes.rbegin(); rit != union_modes.rend(); ++rit)
        {
            if (*rit == ASTSelectWithUnionQuery::Mode::DISTINCT)
            {
                internal_intepreter->num_distinct_union = union_modes.rend() - rit + 1;
                break;
            }
        }

        nested_interpreter_->children.push_back(internal_intepreter);

        for (auto & child : inner_union->list_of_selects->children)
            buildNestedTreeInterpreter(child, internal_intepreter, interpreters, index);
        return;
    }

    auto leaf_interpreter = std::make_shared<NestedInterpreter>();
    leaf_interpreter->type = NestedInterpreter::Type::LEAF;
    leaf_interpreter->interpreter = interpreters[index++];
    nested_interpreter_->children.push_back(leaf_interpreter);
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


InterpreterSelectWithUnionQuery::~InterpreterSelectWithUnionQuery() = default;


Block InterpreterSelectWithUnionQuery::getSampleBlock()
{
    return result_header;
}

Block InterpreterSelectWithUnionQuery::getSampleBlock(
    const ASTPtr & query_ptr,
    const Context & context)
{
    auto & cache = context.getSampleBlockCache();
    /// Using query string because query_ptr changes for every internal SELECT
    auto key = queryToString(query_ptr);
    if (cache.find(key) != cache.end())
    {
        return cache[key];
    }

    return cache[key] = InterpreterSelectWithUnionQuery(query_ptr, context, SelectQueryOptions().analyze()).getSampleBlock();
}

void InterpreterSelectWithUnionQuery::buildQueryPlan(QueryPlan & query_plan)
{
    query_plan = nested_interpreter->buildQueryPlan(context, result_header);
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
    nested_interpreter->ignoreWithTotals();
}

}

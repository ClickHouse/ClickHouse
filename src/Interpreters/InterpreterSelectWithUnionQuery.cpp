#include <Columns/getLeastSuperColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/typeid_cast.h>

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
    extern const int EXPECTED_ALL_OR_DISTINCT;
}

struct CustomizeASTSelectWithUnionQueryNormalize
{
    using TypeToVisit = ASTSelectWithUnionQuery;

    const UnionMode & union_default_mode;

    static void getSelectsFromUnionListNode(ASTPtr & ast_select, ASTs & selects)
    {
        if (auto * inner_union = ast_select->as<ASTSelectWithUnionQuery>())
        {
            for (auto & child : inner_union->list_of_selects->children)
                getSelectsFromUnionListNode(child, selects);

            return;
        }

        selects.push_back(std::move(ast_select));
    }

    void visit(ASTSelectWithUnionQuery & ast, ASTPtr &) const
    {
        auto & union_modes = ast.list_of_modes;
        ASTs selects;
        auto & select_list = ast.list_of_selects->children;

        int i;
        for (i = union_modes.size() - 1; i >= 0; --i)
        {
            /// Rewrite UNION Mode
            if (union_modes[i] == ASTSelectWithUnionQuery::Mode::Unspecified)
            {
                if (union_default_mode == UnionMode::ALL)
                    union_modes[i] = ASTSelectWithUnionQuery::Mode::ALL;
                else if (union_default_mode == UnionMode::DISTINCT)
                    union_modes[i] = ASTSelectWithUnionQuery::Mode::DISTINCT;
                else
                    throw Exception(
                        "Expected ALL or DISTINCT in SelectWithUnion query, because setting (union_default_mode) is empty",
                        DB::ErrorCodes::EXPECTED_ALL_OR_DISTINCT);
            }

            if (union_modes[i] == ASTSelectWithUnionQuery::Mode::ALL)
            {
                if (auto * inner_union = select_list[i + 1]->as<ASTSelectWithUnionQuery>())
                {
                    /// Inner_union is an UNION ALL list, just lift up
                    for (auto child = inner_union->list_of_selects->children.rbegin();
                         child != inner_union->list_of_selects->children.rend();
                         ++child)
                        selects.push_back(std::move(*child));
                }
                else
                    selects.push_back(std::move(select_list[i + 1]));
            }
            /// flatten all left nodes and current node to a UNION DISTINCT list
            else if (union_modes[i] == ASTSelectWithUnionQuery::Mode::DISTINCT)
            {
                auto distinct_list = std::make_shared<ASTSelectWithUnionQuery>();
                distinct_list->list_of_selects = std::make_shared<ASTExpressionList>();
                distinct_list->children.push_back(distinct_list->list_of_selects);

                for (int j = 0; j <= i + 1; ++j)
                {
                    getSelectsFromUnionListNode(select_list[j], distinct_list->list_of_selects->children);
                }

                distinct_list->union_mode = ASTSelectWithUnionQuery::Mode::DISTINCT;
                distinct_list->is_normalized = true;
                selects.push_back(std::move(distinct_list));
                break;
            }
        }

        /// No UNION DISTINCT or only one child in select_list
        if (i == -1)
        {
            if (auto * inner_union = select_list[0]->as<ASTSelectWithUnionQuery>())
            {
                /// Inner_union is an UNION ALL list, just lift it up
                for (auto child = inner_union->list_of_selects->children.rbegin(); child != inner_union->list_of_selects->children.rend();
                     ++child)
                    selects.push_back(std::move(*child));
            }
            else
                selects.push_back(std::move(select_list[0]));
        }

        // reverse children list
        std::reverse(selects.begin(), selects.end());

        ast.is_normalized = true;
        ast.union_mode = ASTSelectWithUnionQuery::Mode::ALL;

        ast.list_of_selects->children = std::move(selects);
    }
};

/// We need normalize children first, so we should visit AST tree bottom up
using CustomizeASTSelectWithUnionQueryNormalizeVisitor
    = InDepthNodeVisitor<OneTypeMatcher<CustomizeASTSelectWithUnionQueryNormalize>, false>;

InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_, const Context & context_, const SelectQueryOptions & options_, const Names & required_result_column_names)
    : IInterpreterUnionOrSelectQuery(query_ptr_, context_, options_)
{
    auto & ast = query_ptr->as<ASTSelectWithUnionQuery &>();

    /// Normalize AST Tree
    if (!ast.is_normalized)
    {
        CustomizeASTSelectWithUnionQueryNormalizeVisitor::Data union_default_mode{context->getSettingsRef().union_default_mode};
        CustomizeASTSelectWithUnionQueryNormalizeVisitor(union_default_mode).visit(query_ptr);

        /// After normalization, if it only has one ASTSelectWithUnionQuery child,
        /// we can lift it up, this can reduce one unnecessary recursion later.
        if (ast.list_of_selects->children.size() == 1 && ast.list_of_selects->children.at(0)->as<ASTSelectWithUnionQuery>())
        {
            query_ptr = std::move(ast.list_of_selects->children.at(0));
            ast = query_ptr->as<ASTSelectWithUnionQuery &>();
        }
    }

    size_t num_children = ast.list_of_selects->children.size();
    if (!num_children)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

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
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
        return InterpreterSelectWithUnionQuery(ast_ptr_, *context, options.copy().analyze().noModify(), required_result_column_names)
            .getSampleBlock();
    else
        return InterpreterSelectQuery(ast_ptr_, *context, options.copy().analyze().noModify()).getSampleBlock();
}

std::unique_ptr<IInterpreterUnionOrSelectQuery>
InterpreterSelectWithUnionQuery::buildCurrentChildInterpreter(const ASTPtr & ast_ptr_, const Names & current_required_result_column_names)
{
    if (ast_ptr_->as<ASTSelectWithUnionQuery>())
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


void InterpreterSelectWithUnionQuery::buildQueryPlan(QueryPlan & query_plan)
{
    // auto num_distinct_union = optimizeUnionList();
    size_t num_plans = nested_interpreters.size();

    /// Skip union for single interpreter.
    if (num_plans == 1)
    {
        nested_interpreters.front()->buildQueryPlan(query_plan);
        return;
    }

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

    const auto & query = query_ptr->as<ASTSelectWithUnionQuery &>();
    if (query.union_mode == ASTSelectWithUnionQuery::Mode::DISTINCT)
    {
        /// Add distinct transform
        const Settings & settings = context->getSettingsRef();
        SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

        auto distinct_step = std::make_unique<DistinctStep>(query_plan.getCurrentDataStream(), limits, 0, result_header.getNames(), false);

        query_plan.addStep(std::move(distinct_step));
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

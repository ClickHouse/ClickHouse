#include <Analyzer/QueryTreeBuilder.h>

#include <Common/FieldVisitorToString.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTWindowDefinition.h>

#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Databases/IDatabase.h>

#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int EXPECTED_ALL_OR_ANY;
}

namespace
{

class QueryTreeBuilder
{
public:
    explicit QueryTreeBuilder(ASTPtr query_, ContextPtr context_);

    QueryTreeNodePtr getQueryTreeNode()
    {
        return query_tree_node;
    }

private:
    QueryTreeNodePtr buildSelectOrUnionExpression(const ASTPtr & select_or_union_query, bool is_subquery, const std::string & cte_name) const;

    QueryTreeNodePtr buildSelectWithUnionExpression(const ASTPtr & select_with_union_query, bool is_subquery, const std::string & cte_name) const;

    QueryTreeNodePtr buildSelectIntersectExceptQuery(const ASTPtr & select_intersect_except_query, bool is_subquery, const std::string & cte_name) const;

    QueryTreeNodePtr buildSelectExpression(const ASTPtr & select_query, bool is_subquery, const std::string & cte_name) const;

    QueryTreeNodePtr buildSortList(const ASTPtr & order_by_expression_list) const;

    QueryTreeNodePtr buildInterpolateColumnList(const ASTPtr & interpolate_expression_list) const;

    QueryTreeNodePtr buildWindowList(const ASTPtr & window_definition_list) const;

    QueryTreeNodePtr buildExpressionList(const ASTPtr & expression_list) const;

    QueryTreeNodePtr buildExpression(const ASTPtr & expression) const;

    QueryTreeNodePtr buildWindow(const ASTPtr & window_definition) const;

    QueryTreeNodePtr buildJoinTree(const ASTPtr & tables_in_select_query) const;

    ColumnTransformersNodes buildColumnTransformers(const ASTPtr & matcher_expression, size_t start_child_index) const;

    ASTPtr query;
    ContextPtr context;
    QueryTreeNodePtr query_tree_node;

};

QueryTreeBuilder::QueryTreeBuilder(ASTPtr query_, ContextPtr context_)
    : query(query_->clone())
    , context(std::move(context_))
{
    if (query->as<ASTSelectWithUnionQuery>() ||
        query->as<ASTSelectIntersectExceptQuery>() ||
        query->as<ASTSelectQuery>())
        query_tree_node = buildSelectOrUnionExpression(query, false /*is_subquery*/, {} /*cte_name*/);
    else if (query->as<ASTExpressionList>())
        query_tree_node = buildExpressionList(query);
    else
        query_tree_node = buildExpression(query);
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectOrUnionExpression(const ASTPtr & select_or_union_query, bool is_subquery, const std::string & cte_name) const
{
    QueryTreeNodePtr query_node;

    if (select_or_union_query->as<ASTSelectWithUnionQuery>())
        query_node = buildSelectWithUnionExpression(select_or_union_query, is_subquery /*is_subquery*/, cte_name /*cte_name*/);
    else if (select_or_union_query->as<ASTSelectIntersectExceptQuery>())
        query_node = buildSelectIntersectExceptQuery(select_or_union_query, is_subquery /*is_subquery*/, cte_name /*cte_name*/);
    else if (select_or_union_query->as<ASTSelectQuery>())
        query_node = buildSelectExpression(select_or_union_query, is_subquery /*is_subquery*/, cte_name /*cte_name*/);
    else
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "SELECT or UNION query {} is not supported", select_or_union_query->formatForErrorMessage());

    return query_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectWithUnionExpression(const ASTPtr & select_with_union_query, bool is_subquery, const std::string & cte_name) const
{
    auto & select_with_union_query_typed = select_with_union_query->as<ASTSelectWithUnionQuery &>();
    auto & select_lists = select_with_union_query_typed.list_of_selects->as<ASTExpressionList &>();

    if (select_lists.children.size() == 1)
        return buildSelectOrUnionExpression(select_lists.children[0], is_subquery, cte_name);

    auto union_node = std::make_shared<UnionNode>();
    union_node->setIsSubquery(is_subquery);
    union_node->setCTEName(cte_name);
    union_node->setUnionMode(select_with_union_query_typed.union_mode);
    union_node->setUnionModes(select_with_union_query_typed.list_of_modes);
    union_node->setOriginalAST(select_with_union_query);

    size_t select_lists_children_size = select_lists.children.size();

    for (size_t i = 0; i < select_lists_children_size; ++i)
    {
        auto & select_list_node = select_lists.children[i];
        QueryTreeNodePtr query_node = buildSelectOrUnionExpression(select_list_node, false /*is_subquery*/, {} /*cte_name*/);
        union_node->getQueries().getNodes().push_back(std::move(query_node));
    }

    return union_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectIntersectExceptQuery(const ASTPtr & select_intersect_except_query, bool is_subquery, const std::string & cte_name) const
{
    auto & select_intersect_except_query_typed = select_intersect_except_query->as<ASTSelectIntersectExceptQuery &>();
    auto select_lists = select_intersect_except_query_typed.getListOfSelects();

    if (select_lists.size() == 1)
        return buildSelectExpression(select_lists[0], is_subquery, cte_name);

    auto union_node = std::make_shared<UnionNode>();
    union_node->setIsSubquery(is_subquery);
    union_node->setCTEName(cte_name);

    if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL)
        union_node->setUnionMode(SelectUnionMode::INTERSECT_ALL);
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT)
        union_node->setUnionMode(SelectUnionMode::INTERSECT_DISTINCT);
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL)
        union_node->setUnionMode(SelectUnionMode::EXCEPT_ALL);
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT)
        union_node->setUnionMode(SelectUnionMode::EXCEPT_DISTINCT);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION type is not initialized");

    union_node->setUnionModes(SelectUnionModes(select_lists.size() - 1, union_node->getUnionMode()));
    union_node->setOriginalAST(select_intersect_except_query);

    size_t select_lists_size = select_lists.size();

    for (size_t i = 0; i < select_lists_size; ++i)
    {
        auto & select_list_node = select_lists[i];
        QueryTreeNodePtr query_node = buildSelectOrUnionExpression(select_list_node, false /*is_subquery*/, {} /*cte_name*/);
        union_node->getQueries().getNodes().push_back(std::move(query_node));
    }

    return union_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectExpression(const ASTPtr & select_query, bool is_subquery, const std::string & cte_name) const
{
    const auto & select_query_typed = select_query->as<ASTSelectQuery &>();
    auto current_query_tree = std::make_shared<QueryNode>();

    current_query_tree->setIsSubquery(is_subquery);
    current_query_tree->setIsCTE(!cte_name.empty());
    current_query_tree->setCTEName(cte_name);
    current_query_tree->setIsDistinct(select_query_typed.distinct);
    current_query_tree->setIsLimitWithTies(select_query_typed.limit_with_ties);
    current_query_tree->setIsGroupByWithTotals(select_query_typed.group_by_with_totals);
    current_query_tree->setIsGroupByWithCube(select_query_typed.group_by_with_cube);
    current_query_tree->setIsGroupByWithRollup(select_query_typed.group_by_with_rollup);
    current_query_tree->setIsGroupByWithGroupingSets(select_query_typed.group_by_with_grouping_sets);
    current_query_tree->setOriginalAST(select_query);

    current_query_tree->getJoinTree() = buildJoinTree(select_query_typed.tables());

    auto select_with_list = select_query_typed.with();
    if (select_with_list)
        current_query_tree->getWithNode() = buildExpressionList(select_with_list);

    auto select_expression_list = select_query_typed.select();
    if (select_expression_list)
        current_query_tree->getProjectionNode() = buildExpressionList(select_expression_list);

    auto prewhere_expression = select_query_typed.prewhere();
    if (prewhere_expression)
        current_query_tree->getPrewhere() = buildExpression(prewhere_expression);

    auto where_expression = select_query_typed.where();
    if (where_expression)
        current_query_tree->getWhere() = buildExpression(where_expression);

    auto group_by_list = select_query_typed.groupBy();
    if (group_by_list)
    {
        auto & group_by_children = group_by_list->children;

        if (current_query_tree->isGroupByWithGroupingSets())
        {
            auto grouping_sets_list_node = std::make_shared<ListNode>();

            for (auto & grouping_sets_keys : group_by_children)
            {
                auto grouping_sets_keys_list_node = buildExpressionList(grouping_sets_keys);
                current_query_tree->getGroupBy().getNodes().emplace_back(std::move(grouping_sets_keys_list_node));
            }
        }
        else
        {
            current_query_tree->getGroupByNode() = buildExpressionList(group_by_list);
        }
    }

    auto having_expression = select_query_typed.having();
    if (having_expression)
        current_query_tree->getHaving() = buildExpression(having_expression);

    auto window_list = select_query_typed.window();
    if (window_list)
        current_query_tree->getWindowNode() = buildWindowList(window_list);

    auto select_order_by_list = select_query_typed.orderBy();
    if (select_order_by_list)
        current_query_tree->getOrderByNode() = buildSortList(select_order_by_list);

    auto interpolate_list = select_query_typed.interpolate();
    if (interpolate_list)
        current_query_tree->getInterpolate() = buildInterpolateColumnList(interpolate_list);

    auto select_limit_by_limit = select_query_typed.limitByLength();
    if (select_limit_by_limit)
        current_query_tree->getLimitByLimit() = buildExpression(select_limit_by_limit);

    auto select_limit_by_offset = select_query_typed.limitOffset();
    if (select_limit_by_offset)
        current_query_tree->getLimitByOffset() = buildExpression(select_limit_by_offset);

    auto select_limit_by = select_query_typed.limitBy();
    if (select_limit_by)
        current_query_tree->getLimitByNode() = buildExpressionList(select_limit_by);

    auto select_limit = select_query_typed.limitLength();
    if (select_limit)
        current_query_tree->getLimit() = buildExpression(select_limit);

    auto select_offset = select_query_typed.limitOffset();
    if (select_offset)
        current_query_tree->getOffset() = buildExpression(select_offset);

    return current_query_tree;
}

QueryTreeNodePtr QueryTreeBuilder::buildSortList(const ASTPtr & order_by_expression_list) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = order_by_expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        const auto & order_by_element = expression->as<const ASTOrderByElement &>();

        auto sort_direction = order_by_element.direction == 1 ? SortDirection::ASCENDING : SortDirection::DESCENDING;
        std::optional<SortDirection> nulls_sort_direction;
        if (order_by_element.nulls_direction_was_explicitly_specified)
            nulls_sort_direction = order_by_element.nulls_direction == 1 ? SortDirection::ASCENDING : SortDirection::DESCENDING;

        std::shared_ptr<Collator> collator;
        if (order_by_element.collation)
            collator = std::make_shared<Collator>(order_by_element.collation->as<ASTLiteral &>().value.get<String>());

        const auto & sort_expression_ast = order_by_element.children.at(0);
        auto sort_expression = buildExpression(sort_expression_ast);
        auto sort_node = std::make_shared<SortNode>(std::move(sort_expression),
            sort_direction,
            nulls_sort_direction,
            std::move(collator),
            order_by_element.with_fill);

        if (order_by_element.fill_from)
            sort_node->getFillFrom() = buildExpression(order_by_element.fill_from);
        if (order_by_element.fill_to)
            sort_node->getFillTo() = buildExpression(order_by_element.fill_to);
        if (order_by_element.fill_step)
            sort_node->getFillStep() = buildExpression(order_by_element.fill_step);

        list_node->getNodes().push_back(std::move(sort_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildInterpolateColumnList(const ASTPtr & interpolate_expression_list) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = interpolate_expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        const auto & interpolate_element = expression->as<const ASTInterpolateElement &>();
        auto expression_to_interpolate = std::make_shared<IdentifierNode>(Identifier(interpolate_element.column));
        auto interpolate_expression = buildExpression(interpolate_element.expr);
        auto interpolate_node = std::make_shared<InterpolateNode>(std::move(expression_to_interpolate), std::move(interpolate_expression));

        list_node->getNodes().push_back(std::move(interpolate_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildWindowList(const ASTPtr & window_definition_list) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = window_definition_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & window_list_element : expression_list_typed.children)
    {
        const auto & window_list_element_typed = window_list_element->as<const ASTWindowListElement &>();

        auto window_node = buildWindow(window_list_element_typed.definition);
        window_node->setAlias(window_list_element_typed.name);

        list_node->getNodes().push_back(std::move(window_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildExpressionList(const ASTPtr & expression_list) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        auto expression_node = buildExpression(expression);
        list_node->getNodes().push_back(std::move(expression_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildExpression(const ASTPtr & expression) const
{
    QueryTreeNodePtr result;

    if (const auto * ast_identifier = expression->as<ASTIdentifier>())
    {
        /// TODO: Identifier as query parameter
        auto identifier = Identifier(ast_identifier->name_parts);
        result = std::make_shared<IdentifierNode>(std::move(identifier));
    }
    else if (const auto * asterisk = expression->as<ASTAsterisk>())
    {
        auto column_transformers = buildColumnTransformers(expression, 0 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(column_transformers);
    }
    else if (const auto * qualified_asterisk = expression->as<ASTQualifiedAsterisk>())
    {
        /// TODO: Identifier with UUID
        /// TODO: Currently during query analysis stage we support qualified matchers with any identifier length
        /// but ASTTableIdentifier can contain only 2 parts.

        auto & qualified_identifier = qualified_asterisk->children.at(0)->as<ASTTableIdentifier &>();
        auto column_transformers = buildColumnTransformers(expression, 1 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), column_transformers);
    }
    else if (const auto * ast_literal = expression->as<ASTLiteral>())
    {
        result = std::make_shared<ConstantNode>(ast_literal->value);
    }
    else if (const auto * function = expression->as<ASTFunction>())
    {
        if (function->is_lambda_function)
        {
            const auto & lambda_arguments_and_expression = function->arguments->as<ASTExpressionList &>().children;
            auto & lambda_arguments_tuple = lambda_arguments_and_expression.at(0)->as<ASTFunction &>();

            auto lambda_arguments_nodes = std::make_shared<ListNode>();
            Names lambda_arguments;
            NameSet lambda_arguments_set;

            if (lambda_arguments_tuple.arguments)
            {
                const auto & lambda_arguments_list = lambda_arguments_tuple.arguments->as<ASTExpressionList>()->children;
                for (const auto & lambda_argument : lambda_arguments_list)
                {
                    const auto * lambda_argument_identifier = lambda_argument->as<ASTIdentifier>();

                    if (!lambda_argument_identifier)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Lambda {} argument is not identifier",
                            function->formatForErrorMessage());

                    if (lambda_argument_identifier->name_parts.size() > 1)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Lambda {} argument identifier must contain only argument name. Actual {}",
                            function->formatForErrorMessage(),
                            lambda_argument_identifier->full_name);

                    const auto & argument_name = lambda_argument_identifier->name_parts[0];
                    auto [_, inserted] = lambda_arguments_set.insert(argument_name);
                    if (!inserted)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Lambda {} multiple arguments with same name {}",
                            function->formatForErrorMessage(),
                            argument_name);

                    lambda_arguments.push_back(argument_name);
                }
            }

            const auto & lambda_expression = lambda_arguments_and_expression.at(1);
            auto lambda_expression_node = buildExpression(lambda_expression);

            result = std::make_shared<LambdaNode>(std::move(lambda_arguments), std::move(lambda_expression_node));
        }
        else
        {
            auto function_node = std::make_shared<FunctionNode>(function->name);

            if (function->parameters)
            {
                const auto & function_parameters_list = function->parameters->as<ASTExpressionList>()->children;
                for (const auto & argument : function_parameters_list)
                    function_node->getParameters().getNodes().push_back(buildExpression(argument));
            }

            if (function->arguments)
            {
                const auto & function_arguments_list = function->arguments->as<ASTExpressionList>()->children;
                for (const auto & argument : function_arguments_list)
                    function_node->getArguments().getNodes().push_back(buildExpression(argument));
            }

            if (function->is_window_function)
            {
                if (function->window_definition)
                    function_node->getWindowNode() = buildWindow(function->window_definition);
                else
                    function_node->getWindowNode() = std::make_shared<IdentifierNode>(Identifier(function->window_name));
            }

            result = function_node;
        }
    }
    else if (const auto * subquery = expression->as<ASTSubquery>())
    {
        auto subquery_query = subquery->children[0];
        auto query_node = buildSelectWithUnionExpression(subquery_query, true /*is_subquery*/, {} /*cte_name*/);

        result = query_node;
    }
    else if (const auto * with_element = expression->as<ASTWithElement>())
    {
        auto with_element_subquery = with_element->subquery->as<ASTSubquery &>().children.at(0);
        auto query_node = buildSelectWithUnionExpression(with_element_subquery, true /*is_subquery*/, with_element->name /*cte_name*/);

        result = query_node;
    }
    else if (const auto * columns_regexp_matcher = expression->as<ASTColumnsRegexpMatcher>())
    {
        auto column_transformers = buildColumnTransformers(expression, 0 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(columns_regexp_matcher->getMatcher(), std::move(column_transformers));
    }
    else if (const auto * columns_list_matcher = expression->as<ASTColumnsListMatcher>())
    {
        Identifiers column_list_identifiers;
        column_list_identifiers.reserve(columns_list_matcher->column_list->children.size());

        for (auto & column_list_child : columns_list_matcher->column_list->children)
        {
            auto & column_list_identifier = column_list_child->as<ASTIdentifier &>();
            column_list_identifiers.emplace_back(Identifier{column_list_identifier.name_parts});
        }

        auto column_transformers = buildColumnTransformers(expression, 0 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(std::move(column_list_identifiers), std::move(column_transformers));
    }
    else if (const auto * qualified_columns_regexp_matcher = expression->as<ASTQualifiedColumnsRegexpMatcher>())
    {
        auto & qualified_identifier = qualified_columns_regexp_matcher->children.at(0)->as<ASTTableIdentifier &>();
        auto column_transformers = buildColumnTransformers(expression, 1 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), qualified_columns_regexp_matcher->getMatcher(), std::move(column_transformers));
    }
    else if (const auto * qualified_columns_list_matcher = expression->as<ASTQualifiedColumnsListMatcher>())
    {
        auto & qualified_identifier = qualified_columns_list_matcher->children.at(0)->as<ASTTableIdentifier &>();

        Identifiers column_list_identifiers;
        column_list_identifiers.reserve(qualified_columns_list_matcher->column_list->children.size());

        for (auto & column_list_child : qualified_columns_list_matcher->column_list->children)
        {
            auto & column_list_identifier = column_list_child->as<ASTIdentifier &>();
            column_list_identifiers.emplace_back(Identifier{column_list_identifier.name_parts});
        }

        auto column_transformers = buildColumnTransformers(expression, 1 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), column_list_identifiers, std::move(column_transformers));
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Invalid expression. Expected identifier, literal, matcher, function, subquery. Actual {}",
            expression->formatForErrorMessage());
    }

    result->setAlias(expression->tryGetAlias());
    result->setOriginalAST(expression);

    return result;
}

QueryTreeNodePtr QueryTreeBuilder::buildWindow(const ASTPtr & window_definition) const
{
    const auto & window_definition_typed = window_definition->as<const ASTWindowDefinition &>();
    WindowFrame window_frame;

    if (!window_definition_typed.frame_is_default)
    {
        window_frame.is_default = false;
        window_frame.type = window_definition_typed.frame_type;
        window_frame.begin_type = window_definition_typed.frame_begin_type;
        window_frame.begin_preceding = window_definition_typed.frame_begin_preceding;
        window_frame.end_type = window_definition_typed.frame_end_type;
        window_frame.end_preceding = window_definition_typed.frame_end_preceding;
    }

    auto window_node = std::make_shared<WindowNode>(window_frame);
    window_node->setParentWindowName(window_definition_typed.parent_window_name);

    if (window_definition_typed.partition_by)
        window_node->getPartitionByNode() = buildExpressionList(window_definition_typed.partition_by);

    if (window_definition_typed.order_by)
        window_node->getOrderByNode() = buildSortList(window_definition_typed.order_by);

    if (window_definition_typed.frame_begin_offset)
        window_node->getFrameBeginOffsetNode() = buildExpression(window_definition_typed.frame_begin_offset);

    if (window_definition_typed.frame_end_offset)
        window_node->getFrameEndOffsetNode() = buildExpression(window_definition_typed.frame_end_offset);

    window_node->setOriginalAST(window_definition);

    return window_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildJoinTree(const ASTPtr & tables_in_select_query) const
{
    if (!tables_in_select_query)
    {
        /** If no table is specified in SELECT query we substitute system.one table.
          * SELECT * FROM system.one;
          */
        Identifier storage_identifier("system.one");
        return std::make_shared<IdentifierNode>(storage_identifier);
    }

    auto & tables = tables_in_select_query->as<ASTTablesInSelectQuery &>();

    QueryTreeNodes table_expressions;

    for (const auto & table_element_untyped : tables.children)
    {
        const auto & table_element = table_element_untyped->as<ASTTablesInSelectQueryElement &>();

        if (table_element.table_expression)
        {
            auto & table_expression = table_element.table_expression->as<ASTTableExpression &>();
            std::optional<TableExpressionModifiers> table_expression_modifiers;

            if (table_expression.final || table_expression.sample_size)
            {
                bool has_final = table_expression.final;
                std::optional<TableExpressionModifiers::Rational> sample_size_ratio;
                std::optional<TableExpressionModifiers::Rational> sample_offset_ratio;

                if (table_expression.sample_size)
                {
                    auto & ast_sample_size_ratio = table_expression.sample_size->as<ASTSampleRatio &>();
                    sample_size_ratio = ast_sample_size_ratio.ratio;

                    if (table_expression.sample_offset)
                    {
                        auto & ast_sample_offset_ratio = table_expression.sample_offset->as<ASTSampleRatio &>();
                        sample_offset_ratio = ast_sample_offset_ratio.ratio;
                    }
                }

                table_expression_modifiers = TableExpressionModifiers(has_final, sample_size_ratio, sample_offset_ratio);
            }

            if (table_expression.database_and_table_name)
            {
                auto & table_identifier_typed = table_expression.database_and_table_name->as<ASTTableIdentifier &>();
                auto storage_identifier = Identifier(table_identifier_typed.name_parts);
                QueryTreeNodePtr table_identifier_node;

                if (table_expression_modifiers)
                    table_identifier_node = std::make_shared<IdentifierNode>(storage_identifier, *table_expression_modifiers);
                else
                    table_identifier_node = std::make_shared<IdentifierNode>(storage_identifier);

                table_identifier_node->setAlias(table_identifier_typed.tryGetAlias());
                table_identifier_node->setOriginalAST(table_element.table_expression);

                table_expressions.push_back(std::move(table_identifier_node));
            }
            else if (table_expression.subquery)
            {
                auto & subquery_expression = table_expression.subquery->as<ASTSubquery &>();
                const auto & select_with_union_query = subquery_expression.children[0];

                auto node = buildSelectWithUnionExpression(select_with_union_query, true /*is_subquery*/, {} /*cte_name*/);
                node->setAlias(subquery_expression.tryGetAlias());
                node->setOriginalAST(select_with_union_query);

                if (table_expression_modifiers)
                {
                    if (auto * query_node = node->as<QueryNode>())
                        query_node->setTableExpressionModifiers(*table_expression_modifiers);
                    else if (auto * union_node = node->as<UnionNode>())
                        union_node->setTableExpressionModifiers(*table_expression_modifiers);
                    else
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected table expression subquery node. Expected union or query. Actual {}",
                            node->formatASTForErrorMessage());
                }

                table_expressions.push_back(std::move(node));
            }
            else if (table_expression.table_function)
            {
                auto & table_function_expression = table_expression.table_function->as<ASTFunction &>();

                auto node = std::make_shared<TableFunctionNode>(table_function_expression.name);

                if (table_function_expression.arguments)
                {
                    const auto & function_arguments_list = table_function_expression.arguments->as<ASTExpressionList>()->children;
                    for (const auto & argument : function_arguments_list)
                    {
                        if (argument->as<ASTSelectQuery>() || argument->as<ASTSelectWithUnionQuery>() || argument->as<ASTSelectIntersectExceptQuery>())
                            node->getArguments().getNodes().push_back(buildSelectOrUnionExpression(argument, false /*is_subquery*/, {} /*cte_name*/));
                        else
                            node->getArguments().getNodes().push_back(buildExpression(argument));
                    }
                }

                if (table_expression_modifiers)
                    node->setTableExpressionModifiers(*table_expression_modifiers);
                node->setAlias(table_function_expression.tryGetAlias());
                node->setOriginalAST(table_expression.table_function);

                table_expressions.push_back(std::move(node));
            }
            else
            {
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported table expression node {}", table_element.table_expression->formatForErrorMessage());
            }
        }

        if (table_element.table_join)
        {
            const auto & table_join = table_element.table_join->as<ASTTableJoin &>();

            auto right_table_expression = std::move(table_expressions.back());
            table_expressions.pop_back();

            auto left_table_expression = std::move(table_expressions.back());
            table_expressions.pop_back();

            QueryTreeNodePtr join_expression;

            if (table_join.using_expression_list)
                join_expression = buildExpressionList(table_join.using_expression_list);
            else if (table_join.on_expression)
                join_expression = buildExpression(table_join.on_expression);

            const auto & settings = context->getSettingsRef();
            auto join_default_strictness = settings.join_default_strictness;
            auto any_join_distinct_right_table_keys = settings.any_join_distinct_right_table_keys;

            JoinStrictness result_join_strictness = table_join.strictness;
            JoinKind result_join_kind = table_join.kind;

            if (result_join_strictness == JoinStrictness::Unspecified && (result_join_kind != JoinKind::Cross && result_join_kind != JoinKind::Comma))
            {
                if (join_default_strictness == JoinStrictness::Any)
                    result_join_strictness = JoinStrictness::Any;
                else if (join_default_strictness == JoinStrictness::All)
                    result_join_strictness = JoinStrictness::All;
                else
                    throw Exception(ErrorCodes::EXPECTED_ALL_OR_ANY,
                        "Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty");
            }

            if (any_join_distinct_right_table_keys)
            {
                if (result_join_strictness == JoinStrictness::Any && result_join_kind == JoinKind::Inner)
                {
                    result_join_strictness = JoinStrictness::Semi;
                    result_join_kind = JoinKind::Left;
                }

                if (result_join_strictness == JoinStrictness::Any)
                    result_join_strictness = JoinStrictness::RightAny;
            }
            else if (result_join_strictness == JoinStrictness::Any && result_join_kind == JoinKind::Full)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ANY FULL JOINs are not implemented");
            }

            auto join_node = std::make_shared<JoinNode>(std::move(left_table_expression),
                std::move(right_table_expression),
                std::move(join_expression),
                table_join.locality,
                result_join_strictness,
                result_join_kind);

            /** Original AST is not set because it will contain only join part and does
              * not include left table expression.
              */
            table_expressions.emplace_back(std::move(join_node));
        }

        if (table_element.array_join)
        {
            auto & array_join_expression = table_element.array_join->as<ASTArrayJoin &>();
            bool is_left_array_join = array_join_expression.kind == ASTArrayJoin::Kind::Left;

            auto last_table_expression = std::move(table_expressions.back());
            table_expressions.pop_back();

            auto array_join_expressions_list = buildExpressionList(array_join_expression.expression_list);
            auto array_join_node = std::make_shared<ArrayJoinNode>(std::move(last_table_expression), std::move(array_join_expressions_list), is_left_array_join);

            /** Original AST is not set because it will contain only array join part and does
              * not include left table expression.
              */
            table_expressions.push_back(std::move(array_join_node));
        }
    }

    if (table_expressions.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query FROM section cannot be empty");

    if (table_expressions.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query FROM section cannot have more than 1 root table expression");

    return table_expressions.back();
}


ColumnTransformersNodes QueryTreeBuilder::buildColumnTransformers(const ASTPtr & matcher_expression, size_t start_child_index) const
{
    ColumnTransformersNodes column_transformers;
    size_t children_size = matcher_expression->children.size();

    for (; start_child_index < children_size; ++start_child_index)
    {
        const auto & child = matcher_expression->children[start_child_index];

        if (auto * apply_transformer = child->as<ASTColumnsApplyTransformer>())
        {
            if (apply_transformer->lambda)
            {
                auto lambda_query_tree_node = buildExpression(apply_transformer->lambda);
                column_transformers.emplace_back(std::make_shared<ApplyColumnTransformerNode>(std::move(lambda_query_tree_node)));
            }
            else
            {
                auto function_node = std::make_shared<FunctionNode>(apply_transformer->func_name);
                if (apply_transformer->parameters)
                    function_node->getParametersNode() = buildExpressionList(apply_transformer->parameters);

                column_transformers.emplace_back(std::make_shared<ApplyColumnTransformerNode>(std::move(function_node)));
            }
        }
        else if (auto * except_transformer = child->as<ASTColumnsExceptTransformer>())
        {
            auto matcher = except_transformer->getMatcher();
            if (matcher)
            {
                column_transformers.emplace_back(std::make_shared<ExceptColumnTransformerNode>(std::move(matcher)));
            }
            else
            {
                Names except_column_names;
                except_column_names.reserve(except_transformer->children.size());

                for (auto & except_transformer_child : except_transformer->children)
                    except_column_names.push_back(except_transformer_child->as<ASTIdentifier &>().full_name);

                column_transformers.emplace_back(std::make_shared<ExceptColumnTransformerNode>(std::move(except_column_names), except_transformer->is_strict));
            }
        }
        else if (auto * replace_transformer = child->as<ASTColumnsReplaceTransformer>())
        {
            std::vector<ReplaceColumnTransformerNode::Replacement> replacements;
            replacements.reserve(replace_transformer->children.size());

            for (const auto & replace_transformer_child : replace_transformer->children)
            {
                auto & replacement = replace_transformer_child->as<ASTColumnsReplaceTransformer::Replacement &>();
                replacements.emplace_back(ReplaceColumnTransformerNode::Replacement{replacement.name, buildExpression(replacement.expr)});
            }

            column_transformers.emplace_back(std::make_shared<ReplaceColumnTransformerNode>(replacements, replace_transformer->is_strict));
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported column matcher {}", child->formatForErrorMessage());
        }
    }

    return column_transformers;
}

}

QueryTreeNodePtr buildQueryTree(ASTPtr query, ContextPtr context)
{
    QueryTreeBuilder builder(std::move(query), context);
    return builder.getQueryTreeNode();
}

}

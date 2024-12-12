#include <Analyzer/QueryTreeBuilder.h>

#include <Common/FieldVisitorToString.h>

#include <DataTypes/FieldToDataType.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryParameter.h>
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
#include <Parsers/ASTSetQuery.h>

#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/UnionNode.h>

#include <Core/Settings.h>

#include <Databases/IDatabase.h>

#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_variant_type;
    extern const SettingsBool any_join_distinct_right_table_keys;
    extern const SettingsJoinStrictness join_default_strictness;
    extern const SettingsBool enable_order_by_all;
    extern const SettingsUInt64 limit;
    extern const SettingsUInt64 offset;
    extern const SettingsBool use_variant_as_common_type;
}


namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int EXPECTED_ALL_OR_ANY;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_QUERY_PARAMETER;
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
    QueryTreeNodePtr buildSelectOrUnionExpression(const ASTPtr & select_or_union_query,
        bool is_subquery,
        const std::string & cte_name,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSelectWithUnionExpression(const ASTPtr & select_with_union_query,
        bool is_subquery,
        const std::string & cte_name,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSelectIntersectExceptQuery(const ASTPtr & select_intersect_except_query,
        bool is_subquery,
        const std::string & cte_name,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSelectExpression(const ASTPtr & select_query,
        bool is_subquery,
        const std::string & cte_name,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSortList(const ASTPtr & order_by_expression_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildInterpolateList(const ASTPtr & interpolate_expression_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildWindowList(const ASTPtr & window_definition_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildExpressionList(const ASTPtr & expression_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildExpression(const ASTPtr & expression, const ContextPtr & context) const;

    QueryTreeNodePtr buildWindow(const ASTPtr & window_definition, const ContextPtr & context) const;

    QueryTreeNodePtr buildJoinTree(const ASTPtr & tables_in_select_query, const ContextPtr & context) const;

    ColumnTransformersNodes buildColumnTransformers(const ASTPtr & matcher_expression, const ContextPtr & context) const;

    ASTPtr query;
    QueryTreeNodePtr query_tree_node;
};

QueryTreeBuilder::QueryTreeBuilder(ASTPtr query_, ContextPtr context_)
    : query(query_->clone())
{
    if (query->as<ASTSelectWithUnionQuery>() ||
        query->as<ASTSelectIntersectExceptQuery>() ||
        query->as<ASTSelectQuery>())
        query_tree_node = buildSelectOrUnionExpression(query, false /*is_subquery*/, {} /*cte_name*/, context_);
    else if (query->as<ASTExpressionList>())
        query_tree_node = buildExpressionList(query, context_);
    else
        query_tree_node = buildExpression(query, context_);
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectOrUnionExpression(const ASTPtr & select_or_union_query,
    bool is_subquery,
    const std::string & cte_name,
    const ContextPtr & context) const
{
    QueryTreeNodePtr query_node;

    if (select_or_union_query->as<ASTSelectWithUnionQuery>())
        query_node = buildSelectWithUnionExpression(select_or_union_query, is_subquery /*is_subquery*/, cte_name /*cte_name*/, context);
    else if (select_or_union_query->as<ASTSelectIntersectExceptQuery>())
        query_node = buildSelectIntersectExceptQuery(select_or_union_query, is_subquery /*is_subquery*/, cte_name /*cte_name*/, context);
    else if (select_or_union_query->as<ASTSelectQuery>())
        query_node = buildSelectExpression(select_or_union_query, is_subquery /*is_subquery*/, cte_name /*cte_name*/, context);
    else
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "SELECT or UNION query {} is not supported",
                        select_or_union_query->formatForErrorMessage());

    return query_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectWithUnionExpression(const ASTPtr & select_with_union_query,
    bool is_subquery,
    const std::string & cte_name,
    const ContextPtr & context) const
{
    auto & select_with_union_query_typed = select_with_union_query->as<ASTSelectWithUnionQuery &>();
    auto & select_lists = select_with_union_query_typed.list_of_selects->as<ASTExpressionList &>();

    if (select_lists.children.size() == 1)
        return buildSelectOrUnionExpression(select_lists.children[0], is_subquery, cte_name, context);

    auto union_node = std::make_shared<UnionNode>(Context::createCopy(context), select_with_union_query_typed.union_mode);
    union_node->setIsSubquery(is_subquery);
    union_node->setIsCTE(!cte_name.empty());
    union_node->setCTEName(cte_name);
    union_node->setOriginalAST(select_with_union_query);

    size_t select_lists_children_size = select_lists.children.size();

    for (size_t i = 0; i < select_lists_children_size; ++i)
    {
        auto & select_list_node = select_lists.children[i];
        QueryTreeNodePtr query_node = buildSelectOrUnionExpression(select_list_node, false /*is_subquery*/, {} /*cte_name*/, context);
        union_node->getQueries().getNodes().push_back(std::move(query_node));
    }

    return union_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectIntersectExceptQuery(const ASTPtr & select_intersect_except_query,
    bool is_subquery,
    const std::string & cte_name,
    const ContextPtr & context) const
{
    auto & select_intersect_except_query_typed = select_intersect_except_query->as<ASTSelectIntersectExceptQuery &>();
    auto select_lists = select_intersect_except_query_typed.getListOfSelects();

    if (select_lists.size() == 1)
        return buildSelectExpression(select_lists[0], is_subquery, cte_name, context);

    SelectUnionMode union_mode;
    if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL)
        union_mode = SelectUnionMode::INTERSECT_ALL;
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT)
        union_mode = SelectUnionMode::INTERSECT_DISTINCT;
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL)
        union_mode = SelectUnionMode::EXCEPT_ALL;
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT)
        union_mode = SelectUnionMode::EXCEPT_DISTINCT;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION type is not initialized");

    auto union_node = std::make_shared<UnionNode>(Context::createCopy(context), union_mode);
    union_node->setIsSubquery(is_subquery);
    union_node->setIsCTE(!cte_name.empty());
    union_node->setCTEName(cte_name);
    union_node->setOriginalAST(select_intersect_except_query);

    size_t select_lists_size = select_lists.size();

    for (size_t i = 0; i < select_lists_size; ++i)
    {
        auto & select_list_node = select_lists[i];
        QueryTreeNodePtr query_node = buildSelectOrUnionExpression(select_list_node, false /*is_subquery*/, {} /*cte_name*/, context);
        union_node->getQueries().getNodes().push_back(std::move(query_node));
    }

    return union_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectExpression(const ASTPtr & select_query,
    bool is_subquery,
    const std::string & cte_name,
    const ContextPtr & context) const
{
    const auto & select_query_typed = select_query->as<ASTSelectQuery &>();

    auto updated_context = Context::createCopy(context);
    auto select_settings = select_query_typed.settings();
    SettingsChanges settings_changes;

    /// We are going to remove settings LIMIT and OFFSET and
    /// further replace them with corresponding expression nodes
    UInt64 limit = 0;
    UInt64 offset = 0;

    /// Remove global settings limit and offset
    if (const auto & settings_ref = updated_context->getSettingsRef(); settings_ref[Setting::limit] || settings_ref[Setting::offset])
    {
        Settings settings = updated_context->getSettingsCopy();
        limit = settings[Setting::limit];
        offset = settings[Setting::offset];
        settings[Setting::limit] = 0;
        settings[Setting::offset] = 0;
        updated_context->setSettings(settings);
    }

    if (select_settings)
    {
        auto & set_query = select_settings->as<ASTSetQuery &>();

        /// Remove expression settings limit and offset
        if (auto * limit_field = set_query.changes.tryGet("limit"))
        {
            limit = limit_field->safeGet<UInt64>();
            set_query.changes.removeSetting("limit");
        }
        if (auto * offset_field = set_query.changes.tryGet("offset"))
        {
            offset = offset_field->safeGet<UInt64>();
            set_query.changes.removeSetting("offset");
        }

        if (!set_query.changes.empty())
        {
            updated_context->applySettingsChanges(set_query.changes);
            settings_changes = set_query.changes;
        }
    }

    const auto enable_order_by_all = updated_context->getSettingsRef()[Setting::enable_order_by_all];

    auto current_query_tree = std::make_shared<QueryNode>(std::move(updated_context), std::move(settings_changes));

    current_query_tree->setIsSubquery(is_subquery);
    current_query_tree->setIsCTE(!cte_name.empty());
    current_query_tree->setCTEName(cte_name);
    current_query_tree->setIsRecursiveWith(select_query_typed.recursive_with);
    current_query_tree->setIsDistinct(select_query_typed.distinct);
    current_query_tree->setIsLimitWithTies(select_query_typed.limit_with_ties);
    current_query_tree->setIsGroupByWithTotals(select_query_typed.group_by_with_totals);
    current_query_tree->setIsGroupByWithCube(select_query_typed.group_by_with_cube);
    current_query_tree->setIsGroupByWithRollup(select_query_typed.group_by_with_rollup);
    current_query_tree->setIsGroupByWithGroupingSets(select_query_typed.group_by_with_grouping_sets);
    current_query_tree->setIsGroupByAll(select_query_typed.group_by_all);
    /// order_by_all flag in AST is set w/o consideration of `enable_order_by_all` setting
    /// since SETTINGS section has not been parsed yet, - so, check the setting here
    if (enable_order_by_all)
        current_query_tree->setIsOrderByAll(select_query_typed.order_by_all);
    current_query_tree->setOriginalAST(select_query);

    auto current_context = current_query_tree->getContext();

    current_query_tree->getJoinTree() = buildJoinTree(select_query_typed.tables(), current_context);

    auto select_with_list = select_query_typed.with();
    if (select_with_list)
    {
        current_query_tree->getWithNode() = buildExpressionList(select_with_list, current_context);

        if (select_query_typed.recursive_with)
        {
            for (auto & with_node : current_query_tree->getWith().getNodes())
            {
                auto * with_union_node = with_node->as<UnionNode>();
                if (!with_union_node)
                    continue;

                with_union_node->setIsRecursiveCTE(true);
            }
        }
    }

    auto select_expression_list = select_query_typed.select();
    if (select_expression_list)
        current_query_tree->getProjectionNode() = buildExpressionList(select_expression_list, current_context);

    auto prewhere_expression = select_query_typed.prewhere();
    if (prewhere_expression)
        current_query_tree->getPrewhere() = buildExpression(prewhere_expression, current_context);

    auto where_expression = select_query_typed.where();
    if (where_expression)
        current_query_tree->getWhere() = buildExpression(where_expression, current_context);

    auto group_by_list = select_query_typed.groupBy();
    if (group_by_list)
    {
        auto & group_by_children = group_by_list->children;

        if (current_query_tree->isGroupByWithGroupingSets())
        {
            auto grouping_sets_list_node = std::make_shared<ListNode>();

            for (auto & grouping_sets_keys : group_by_children)
            {
                auto grouping_sets_keys_list_node = buildExpressionList(grouping_sets_keys, current_context);
                current_query_tree->getGroupBy().getNodes().emplace_back(std::move(grouping_sets_keys_list_node));
            }
        }
        else
        {
            current_query_tree->getGroupByNode() = buildExpressionList(group_by_list, current_context);
        }
    }

    auto having_expression = select_query_typed.having();
    if (having_expression)
        current_query_tree->getHaving() = buildExpression(having_expression, current_context);

    auto window_list = select_query_typed.window();
    if (window_list)
        current_query_tree->getWindowNode() = buildWindowList(window_list, current_context);

    auto qualify_expression = select_query_typed.qualify();
    if (qualify_expression)
        current_query_tree->getQualify() = buildExpression(qualify_expression, current_context);

    auto select_order_by_list = select_query_typed.orderBy();
    if (select_order_by_list)
        current_query_tree->getOrderByNode() = buildSortList(select_order_by_list, current_context);

    auto interpolate_list = select_query_typed.interpolate();
    if (interpolate_list)
        current_query_tree->getInterpolate() = buildInterpolateList(interpolate_list, current_context);

    auto select_limit_by_limit = select_query_typed.limitByLength();
    if (select_limit_by_limit)
        current_query_tree->getLimitByLimit() = buildExpression(select_limit_by_limit, current_context);

    auto select_limit_by_offset = select_query_typed.limitByOffset();
    if (select_limit_by_offset)
        current_query_tree->getLimitByOffset() = buildExpression(select_limit_by_offset, current_context);

    auto select_limit_by = select_query_typed.limitBy();
    if (select_limit_by)
        current_query_tree->getLimitByNode() = buildExpressionList(select_limit_by, current_context);

    /// Combine limit expression with limit and offset settings into final limit expression
    /// The sequence of application is the following - offset expression, limit expression, offset setting, limit setting.
    /// Since offset setting is applied after limit expression, but we want to transfer settings into expression
    /// we must decrease limit expression by offset setting and then add offset setting to offset expression.
    ///    select_limit - limit expression
    ///    limit        - limit setting
    ///    offset       - offset setting
    ///
    /// if select_limit
    ///   -- if offset >= select_limit                (expr 0)
    ///      then (0) (0 rows)
    ///   -- else if limit > 0                        (expr 1)
    ///      then min(select_limit - offset, limit)   (expr 2)
    ///   -- else
    ///      then (select_limit - offset)             (expr 3)
    /// else if limit > 0
    ///    then limit
    ///
    /// offset = offset + of_expr
    auto select_limit = select_query_typed.limitLength();
    if (select_limit)
    {
        /// Shortcut
        if (offset == 0 && limit == 0)
        {
            current_query_tree->getLimit() = buildExpression(select_limit, current_context);
        }
        else
        {
            /// expr 3
            auto expr_3 = std::make_shared<FunctionNode>("minus");
            expr_3->getArguments().getNodes().push_back(buildExpression(select_limit, current_context));
            expr_3->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(offset));

            /// expr 2
            auto expr_2 = std::make_shared<FunctionNode>("least");
            expr_2->getArguments().getNodes().push_back(expr_3->clone());
            expr_2->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(limit));

            /// expr 0
            auto expr_0 = std::make_shared<FunctionNode>("greaterOrEquals");
            expr_0->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(offset));
            expr_0->getArguments().getNodes().push_back(buildExpression(select_limit, current_context));

            /// expr 1
            auto expr_1 = std::make_shared<ConstantNode>(limit > 0);

            auto function_node = std::make_shared<FunctionNode>("multiIf");
            function_node->getArguments().getNodes().push_back(expr_0);
            function_node->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(0));
            function_node->getArguments().getNodes().push_back(expr_1);
            function_node->getArguments().getNodes().push_back(expr_2);
            function_node->getArguments().getNodes().push_back(expr_3);

            current_query_tree->getLimit() = std::move(function_node);
        }
    }
    else if (limit > 0)
        current_query_tree->getLimit() = std::make_shared<ConstantNode>(limit);

    /// Combine offset expression with offset setting into final offset expression
    auto select_offset = select_query_typed.limitOffset();
    if (select_offset && offset)
    {
        auto function_node = std::make_shared<FunctionNode>("plus");
        function_node->getArguments().getNodes().push_back(buildExpression(select_offset, current_context));
        function_node->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(offset));
        current_query_tree->getOffset() = std::move(function_node);
    }
    else if (offset)
        current_query_tree->getOffset() = std::make_shared<ConstantNode>(offset);
    else if (select_offset)
        current_query_tree->getOffset() = buildExpression(select_offset, current_context);

    return current_query_tree;
}

QueryTreeNodePtr QueryTreeBuilder::buildSortList(const ASTPtr & order_by_expression_list, const ContextPtr & context) const
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
        if (order_by_element.getCollation())
            collator = std::make_shared<Collator>(order_by_element.getCollation()->as<ASTLiteral &>().value.safeGet<String &>());

        const auto & sort_expression_ast = order_by_element.children.at(0);
        auto sort_expression = buildExpression(sort_expression_ast, context);
        auto sort_node = std::make_shared<SortNode>(std::move(sort_expression),
            sort_direction,
            nulls_sort_direction,
            std::move(collator),
            order_by_element.with_fill);

        if (order_by_element.getFillFrom())
            sort_node->getFillFrom() = buildExpression(order_by_element.getFillFrom(), context);
        if (order_by_element.getFillTo())
            sort_node->getFillTo() = buildExpression(order_by_element.getFillTo(), context);
        if (order_by_element.getFillStep())
            sort_node->getFillStep() = buildExpression(order_by_element.getFillStep(), context);

        list_node->getNodes().push_back(std::move(sort_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildInterpolateList(const ASTPtr & interpolate_expression_list, const ContextPtr & context) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = interpolate_expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        const auto & interpolate_element = expression->as<const ASTInterpolateElement &>();
        auto expression_to_interpolate = std::make_shared<IdentifierNode>(Identifier(interpolate_element.column));
        auto interpolate_expression = buildExpression(interpolate_element.expr, context);
        auto interpolate_node = std::make_shared<InterpolateNode>(std::move(expression_to_interpolate), std::move(interpolate_expression));

        list_node->getNodes().push_back(std::move(interpolate_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildWindowList(const ASTPtr & window_definition_list, const ContextPtr & context) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = window_definition_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & window_list_element : expression_list_typed.children)
    {
        const auto & window_list_element_typed = window_list_element->as<const ASTWindowListElement &>();

        auto window_node = buildWindow(window_list_element_typed.definition, context);
        window_node->setAlias(window_list_element_typed.name);

        list_node->getNodes().push_back(std::move(window_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildExpressionList(const ASTPtr & expression_list, const ContextPtr & context) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        auto expression_node = buildExpression(expression, context);
        list_node->getNodes().push_back(std::move(expression_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildExpression(const ASTPtr & expression, const ContextPtr & context) const
{
    QueryTreeNodePtr result;

    if (const auto * ast_identifier = expression->as<ASTIdentifier>())
    {
        auto identifier = Identifier(ast_identifier->name_parts);
        result = std::make_shared<IdentifierNode>(std::move(identifier));
    }
    else if (const auto * table_identifier = expression->as<ASTTableIdentifier>())
    {
        auto identifier = Identifier(table_identifier->name_parts);
        result = std::make_shared<IdentifierNode>(std::move(identifier));
    }
    else if (const auto * asterisk = expression->as<ASTAsterisk>())
    {
        auto column_transformers = buildColumnTransformers(asterisk->transformers, context);
        result = std::make_shared<MatcherNode>(std::move(column_transformers));
    }
    else if (const auto * qualified_asterisk = expression->as<ASTQualifiedAsterisk>())
    {
        auto & qualified_identifier = qualified_asterisk->qualifier->as<ASTIdentifier &>();
        auto column_transformers = buildColumnTransformers(qualified_asterisk->transformers, context);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), std::move(column_transformers));
    }
    else if (const auto * ast_literal = expression->as<ASTLiteral>())
    {
        if (context->getSettingsRef()[Setting::allow_experimental_variant_type] && context->getSettingsRef()[Setting::use_variant_as_common_type])
            result = std::make_shared<ConstantNode>(ast_literal->value, applyVisitor(FieldToDataType<LeastSupertypeOnError::Variant>(), ast_literal->value));
        else
            result = std::make_shared<ConstantNode>(ast_literal->value);
    }
    else if (const auto * function = expression->as<ASTFunction>())
    {
        if (function->is_lambda_function || isASTLambdaFunction(*function))
        {
            const auto & lambda_arguments_and_expression = function->arguments->as<ASTExpressionList &>().children;
            auto & lambda_arguments_tuple = lambda_arguments_and_expression.at(0)->as<ASTFunction &>();

            auto lambda_arguments_nodes = std::make_shared<ListNode>();
            Names lambda_arguments;
            NameSet lambda_arguments_set;

            if (lambda_arguments_tuple.arguments)
            {
                const auto & lambda_arguments_list = lambda_arguments_tuple.arguments->as<ASTExpressionList &>().children;
                for (const auto & lambda_argument : lambda_arguments_list)
                {
                    const auto * lambda_argument_identifier = lambda_argument->as<ASTIdentifier>();

                    if (!lambda_argument_identifier)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Lambda {} argument is not identifier",
                            function->formatForErrorMessage());

                    if (lambda_argument_identifier->name_parts.size() > 1)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Lambda {} argument identifier must contain single part. Actual {}",
                            function->formatForErrorMessage(),
                            lambda_argument_identifier->full_name);

                    const auto & argument_name = lambda_argument_identifier->name_parts[0];
                    auto [_, inserted] = lambda_arguments_set.insert(argument_name);
                    if (!inserted)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Lambda {} multiple arguments with same name {}",
                            function->formatForErrorMessage(),
                            argument_name);

                    lambda_arguments.push_back(argument_name);
                }
            }

            const auto & lambda_expression = lambda_arguments_and_expression.at(1);
            auto lambda_expression_node = buildExpression(lambda_expression, context);

            result = std::make_shared<LambdaNode>(std::move(lambda_arguments), std::move(lambda_expression_node));
        }
        else
        {
            auto function_node = std::make_shared<FunctionNode>(function->name);
            function_node->setNullsAction(function->nulls_action);

            if (function->parameters)
            {
                const auto & function_parameters_list = function->parameters->as<ASTExpressionList>()->children;
                for (const auto & argument : function_parameters_list)
                    function_node->getParameters().getNodes().push_back(buildExpression(argument, context));
            }

            if (function->arguments)
            {
                const auto & function_arguments_list = function->arguments->as<ASTExpressionList>()->children;
                for (const auto & argument : function_arguments_list)
                    function_node->getArguments().getNodes().push_back(buildExpression(argument, context));
            }

            if (function->is_window_function)
            {
                if (function->window_definition)
                    function_node->getWindowNode() = buildWindow(function->window_definition, context);
                else
                    function_node->getWindowNode() = std::make_shared<IdentifierNode>(Identifier(function->window_name));
            }

            result = std::move(function_node);
        }
    }
    else if (const auto * subquery = expression->as<ASTSubquery>())
    {
        auto subquery_query = subquery->children[0];
        auto query_node = buildSelectWithUnionExpression(subquery_query, true /*is_subquery*/, {} /*cte_name*/, context);

        result = std::move(query_node);
    }
    else if (const auto * select_with_union_query = expression->as<ASTSelectWithUnionQuery>())
    {
        auto query_node = buildSelectWithUnionExpression(expression, false /*is_subquery*/, {} /*cte_name*/, context);
        result = std::move(query_node);
    }
    else if (const auto * with_element = expression->as<ASTWithElement>())
    {
        auto with_element_subquery = with_element->subquery->as<ASTSubquery &>().children.at(0);
        auto query_node = buildSelectWithUnionExpression(with_element_subquery, true /*is_subquery*/, with_element->name /*cte_name*/, context);

        result = std::move(query_node);
    }
    else if (const auto * columns_regexp_matcher = expression->as<ASTColumnsRegexpMatcher>())
    {
        auto column_transformers = buildColumnTransformers(columns_regexp_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(columns_regexp_matcher->getPattern(), std::move(column_transformers));
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

        auto column_transformers = buildColumnTransformers(columns_list_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(std::move(column_list_identifiers), std::move(column_transformers));
    }
    else if (const auto * qualified_columns_regexp_matcher = expression->as<ASTQualifiedColumnsRegexpMatcher>())
    {
        auto & qualified_identifier = qualified_columns_regexp_matcher->qualifier->as<ASTIdentifier &>();
        auto column_transformers = buildColumnTransformers(qualified_columns_regexp_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), qualified_columns_regexp_matcher->getPattern(), std::move(column_transformers));
    }
    else if (const auto * qualified_columns_list_matcher = expression->as<ASTQualifiedColumnsListMatcher>())
    {
        auto & qualified_identifier = qualified_columns_list_matcher->qualifier->as<ASTIdentifier &>();

        Identifiers column_list_identifiers;
        column_list_identifiers.reserve(qualified_columns_list_matcher->column_list->children.size());

        for (auto & column_list_child : qualified_columns_list_matcher->column_list->children)
        {
            auto & column_list_identifier = column_list_child->as<ASTIdentifier &>();
            column_list_identifiers.emplace_back(Identifier{column_list_identifier.name_parts});
        }

        auto column_transformers = buildColumnTransformers(qualified_columns_list_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), std::move(column_list_identifiers), std::move(column_transformers));
    }
    else if (const auto * query_parameter = expression->as<ASTQueryParameter>())
    {
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER,
            "Query parameter {} was not set",
            backQuote(query_parameter->name));
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

QueryTreeNodePtr QueryTreeBuilder::buildWindow(const ASTPtr & window_definition, const ContextPtr & context) const
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
        window_node->getPartitionByNode() = buildExpressionList(window_definition_typed.partition_by, context);

    if (window_definition_typed.order_by)
        window_node->getOrderByNode() = buildSortList(window_definition_typed.order_by, context);

    if (window_definition_typed.frame_begin_offset)
        window_node->getFrameBeginOffsetNode() = buildExpression(window_definition_typed.frame_begin_offset, context);

    if (window_definition_typed.frame_end_offset)
        window_node->getFrameEndOffsetNode() = buildExpression(window_definition_typed.frame_end_offset, context);

    window_node->setOriginalAST(window_definition);

    return window_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildJoinTree(const ASTPtr & tables_in_select_query, const ContextPtr & context) const
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

                auto node = buildSelectWithUnionExpression(select_with_union_query, true /*is_subquery*/, {} /*cte_name*/, context);
                node->setAlias(subquery_expression.tryGetAlias());
                node->setOriginalAST(select_with_union_query);

                if (table_expression_modifiers)
                {
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Table expression modifiers {} are not supported for subquery {}",
                        table_expression_modifiers->formatForErrorMessage(),
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
                    const auto & function_arguments_list = table_function_expression.arguments->as<ASTExpressionList &>().children;
                    for (const auto & argument : function_arguments_list)
                    {
                        if (!node->getSettingsChanges().empty())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' has arguments after SETTINGS",
                                table_function_expression.formatForErrorMessage());

                        if (argument->as<ASTSelectQuery>() || argument->as<ASTSelectWithUnionQuery>() || argument->as<ASTSelectIntersectExceptQuery>())
                            node->getArguments().getNodes().push_back(buildSelectOrUnionExpression(argument, false /*is_subquery*/, {} /*cte_name*/, context));
                        else if (const auto * ast_set = argument->as<ASTSetQuery>())
                            node->setSettingsChanges(ast_set->changes);
                        else
                            node->getArguments().getNodes().push_back(buildExpression(argument, context));
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
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported table expression node {}",
                                table_element.table_expression->formatForErrorMessage());
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
                join_expression = buildExpressionList(table_join.using_expression_list, context);
            else if (table_join.on_expression)
                join_expression = buildExpression(table_join.on_expression, context);

            const auto & settings = context->getSettingsRef();
            auto join_default_strictness = settings[Setting::join_default_strictness];
            auto any_join_distinct_right_table_keys = settings[Setting::any_join_distinct_right_table_keys];

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
            join_node->setOriginalAST(table_element.table_join);

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

            auto array_join_expressions_list = buildExpressionList(array_join_expression.expression_list, context);
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


ColumnTransformersNodes QueryTreeBuilder::buildColumnTransformers(const ASTPtr & matcher_expression, const ContextPtr & context) const
{
    ColumnTransformersNodes column_transformers;

    if (!matcher_expression)
        return column_transformers;

    for (const auto & child : matcher_expression->children)
    {
        if (auto * apply_transformer = child->as<ASTColumnsApplyTransformer>())
        {
            if (apply_transformer->lambda)
            {
                auto lambda_query_tree_node = buildExpression(apply_transformer->lambda, context);
                column_transformers.emplace_back(std::make_shared<ApplyColumnTransformerNode>(std::move(lambda_query_tree_node)));
            }
            else
            {
                auto function_node = std::make_shared<FunctionNode>(apply_transformer->func_name);
                if (apply_transformer->parameters)
                    function_node->getParametersNode() = buildExpressionList(apply_transformer->parameters, context);

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
                replacements.emplace_back(ReplaceColumnTransformerNode::Replacement{replacement.name, buildExpression(replacement.children[0], context)});
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

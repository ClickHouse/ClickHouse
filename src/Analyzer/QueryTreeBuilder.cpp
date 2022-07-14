#include <Analyzer/QueryTreeBuilder.h>

#include <Common/FieldVisitorToString.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
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

#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/QueryNode.h>
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
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

class QueryTreeBuilder : public WithContext
{
public:
    QueryTreeBuilder(ASTPtr query_, ContextPtr context_);

    QueryTreeNodePtr getQueryTreeNode()
    {
        return query_tree_node;
    }

private:
    QueryTreeNodePtr getSelectWithUnionExpression(const ASTPtr & select_with_union_query, bool is_scalar_query) const;

    QueryTreeNodePtr getSelectExpression(const ASTPtr & select_query, bool is_scalar_query) const;

    QueryTreeNodePtr getExpressionList(const ASTPtr & expression_list) const;

    QueryTreeNodePtr getExpression(const ASTPtr & expression) const;

    QueryTreeNodePtr getFromNode(const ASTPtr & tables_in_select_query) const;

    ColumnTransformersNodes getColumnTransformers(const ASTPtr & matcher_expression, size_t start_child_index) const;

    StoragePtr resolveStorage(const Identifier & storage_identifier) const;

    ASTPtr query;
    QueryTreeNodePtr query_tree_node;

};

QueryTreeBuilder::QueryTreeBuilder(ASTPtr query_, ContextPtr context_)
    : WithContext(context_)
    , query(query_->clone())
{
    if (query->as<ASTSelectWithUnionQuery>())
        query_tree_node = getSelectWithUnionExpression(query, false /*is_scalar_query*/);
    else if (query->as<ASTSelectQuery>())
        query_tree_node = getSelectExpression(query, false /*is_scalar_query*/);
    else if (query->as<ASTExpressionList>())
        query_tree_node = getExpressionList(query);
    else
        query_tree_node = getExpression(query);
}

QueryTreeNodePtr QueryTreeBuilder::getSelectWithUnionExpression(const ASTPtr & select_with_union_query, bool is_scalar_query) const
{
    auto & select_with_union_query_typed = select_with_union_query->as<ASTSelectWithUnionQuery &>();
    auto & select_lists = select_with_union_query_typed.list_of_selects->as<ASTExpressionList &>();

    if (select_lists.children.size() == 1)
    {
        return getSelectExpression(select_with_union_query->children[0]->children[0], is_scalar_query);
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "UNION is not supported");
        // auto union_expression = UnionExpression::create(is_scalar_query);

        // union_expression->getModes() = select_with_union_query_typed.list_of_modes;
        // union_expression->getModesSet() = select_with_union_query_typed.set_of_modes;

        // auto & select_expressions = union_expression->getSelectExpressions();
        // select_expressions.reserve(select_lists.children.size());

        // for (const auto & select : select_lists.children)
        // {
        //     auto expression = getSelectExpression(select, false);
        //     select_expressions.emplace_back(std::move(expression));
        // }

        // return union_expression;
    }
}

QueryTreeNodePtr QueryTreeBuilder::getSelectExpression(const ASTPtr & select_query, bool) const
{
    const auto & select_query_typed = select_query->as<ASTSelectQuery &>();
    auto current_query_tree = std::make_shared<QueryNode>();
    current_query_tree->getFrom() = getFromNode(select_query_typed.tables());
    current_query_tree->setOriginalAST(select_query);

    auto select_with_list = select_query_typed.with();
    if (select_with_list)
    {
        auto & select_with_list_typed = select_with_list->as<ASTExpressionList &>();
        for (auto & expression_part : select_with_list_typed.children)
        {
            auto expression_node = getExpression(expression_part);
            current_query_tree->getWith().getNodes().push_back(expression_node);
        }
    }

    auto select_expression_list = select_query_typed.select();
    if (select_expression_list)
    {
        auto & select_expression_list_typed = select_expression_list->as<ASTExpressionList &>();

        for (auto & expression_part : select_expression_list_typed.children)
        {
            auto expression_node = getExpression(expression_part);
            current_query_tree->getProjection().getNodes().push_back(expression_node);
        }
    }

    auto prewhere_expression = select_query_typed.prewhere();
    if (prewhere_expression)
        current_query_tree->getPrewhere() = getExpression(prewhere_expression);

    auto where_expression = select_query_typed.where();
    if (where_expression)
        current_query_tree->getWhere() = getExpression(where_expression);

    return current_query_tree;
}

QueryTreeNodePtr QueryTreeBuilder::getExpressionList(const ASTPtr & expression_list) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        auto expression_node = getExpression(expression);
        list_node->getNodes().push_back(std::move(expression_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::getExpression(const ASTPtr & expression) const
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
        auto column_transformers = getColumnTransformers(expression, 0 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(column_transformers);
    }
    else if (const auto * qualified_asterisk = expression->as<ASTQualifiedAsterisk>())
    {
        /// TODO: Identifier with UUID
        /// TODO: Currently during query analysis stage we support qualified matchers with any identifier length
        /// but ASTTableIdentifier can contain only 2 parts.

        auto & qualified_identifier = qualified_asterisk->children.at(0)->as<ASTTableIdentifier &>();
        auto column_transformers = getColumnTransformers(expression, 1 /*start_child_index*/);
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
            auto lambda_expression_node = getExpression(lambda_expression);

            result = std::make_shared<LambdaNode>(std::move(lambda_arguments), std::move(lambda_expression_node));
        }
        else
        {
            auto function_node = std::make_shared<FunctionNode>(function->name);

            if (function->parameters)
            {
                const auto & function_parameters_list = function->parameters->as<ASTExpressionList>()->children;
                for (const auto & argument : function_parameters_list)
                    function_node->getParameters().getNodes().push_back(getExpression(argument));
            }

            if (function->arguments)
            {
                const auto & function_arguments_list = function->arguments->as<ASTExpressionList>()->children;
                for (const auto & argument : function_arguments_list)
                    function_node->getArguments().getNodes().push_back(getExpression(argument));
            }

            result = function_node;
        }
    }
    else if (const auto * subquery = expression->as<ASTSubquery>())
    {
        auto subquery_query = subquery->children[0];
        auto query_node = getSelectWithUnionExpression(subquery_query, false);
        result = query_node;
    }
    else if (const auto * columns_regexp_matcher = expression->as<ASTColumnsRegexpMatcher>())
    {
        auto column_transformers = getColumnTransformers(expression, 0 /*start_child_index*/);
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

        auto column_transformers = getColumnTransformers(expression, 0 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(std::move(column_list_identifiers), std::move(column_transformers));
    }
    else if (const auto * qualified_columns_regexp_matcher = expression->as<ASTQualifiedColumnsRegexpMatcher>())
    {
        auto & qualified_identifier = qualified_columns_regexp_matcher->children.at(0)->as<ASTTableIdentifier &>();
        auto column_transformers = getColumnTransformers(expression, 1 /*start_child_index*/);
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

        auto column_transformers = getColumnTransformers(expression, 1 /*start_child_index*/);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), column_list_identifiers, std::move(column_transformers));
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Only literals and constants are supported as expression. Actual {}", expression->formatForErrorMessage());
    }

    result->setAlias(expression->tryGetAlias());
    result->setOriginalAST(expression);

    return result;
}

QueryTreeNodePtr QueryTreeBuilder::getFromNode(const ASTPtr & tables_in_select_query) const
{
    if (!tables_in_select_query)
    {
        /** If no table is specified in SELECT query we substitude system.one table.
          * SELECT * FROM system.one;
          */
        Identifier storage_identifier("system.one");
        auto table = resolveStorage(storage_identifier);
        return std::make_shared<TableNode>(table, getContext());
    }

    auto & tables = tables_in_select_query->as<ASTTablesInSelectQuery &>();
    if (tables.children.size() > 1)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Multiple tables are not supported");

    for (const auto & table_element_untyped : tables.children)
    {
        const auto & table_element = table_element_untyped->as<ASTTablesInSelectQueryElement &>();

        if (table_element.table_expression)
        {
            auto * table_expression = table_element.table_expression->as<ASTTableExpression>();

            if (table_expression->database_and_table_name)
            {
                /// Add CTE support
                auto & table_identifier_typed = table_expression->database_and_table_name->as<ASTTableIdentifier &>();
                auto storage_identifier = Identifier(table_identifier_typed.name_parts);
                auto table = resolveStorage(storage_identifier);
                auto node = std::make_shared<TableNode>(table, getContext());

                node->setAlias(table_identifier_typed.tryGetAlias());
                node->setOriginalAST(table_element.table_expression);

                return node;
            }
            // else if (auto * subquery_expression = table_expression->subquery->as<ASTSubquery>())
            // {
            //     const auto & select_with_union_query = subquery_expression->children[0];
            //     auto expression = getSelectWithUnionExpression(select_with_union_query, false /*scalar query*/);
            //     expression->setAlias(subquery_expression->tryGetAlias());

            //     if (auto * select_expression = expression->as<SelectExpression>())
            //         scope->addInnerScope(select_expression->getScope());

            //     table_expressions.emplace_back(std::move(expression));
            // }
            else
            {
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Only table is supported");
            }
        }

        // if (table_element.table_join)
        // {
        //     const auto & table_join = table_element.table_join->as<ASTTableJoin>();

        //     auto right_table_expression = std::move(table_expressions.back());
        //     table_expressions.pop_back();

        //     auto left_table_expression = std::move(table_expressions.back());
        //     table_expressions.pop_back();

        //     auto join_expression = JoinExpression::create();
        //     join_expression->getLeftTableExpression() = left_table_expression;
        //     join_expression->getRightTableExpression() = right_table_expression;

        //     if (table_join->using_expression_list)
        //         join_expression->getUsingExpressions() = getExpressionElements(table_join->using_expression_list, scope);

        //     if (table_join->on_expression)
        //     {
        //         join_expression->getOnExpression() = getExpressionElement(table_join->on_expression, scope);
        //     }

        //     table_expressions.emplace_back(std::move(join_expression));
        // }

        // if (table_element.array_join)
        // {
        //     auto array_join_array_expression = table_element.array_join->children[0]->children[0];
        //     auto expression_element = getExpressionElement(array_join_array_expression, scope);
        //     expression_element->setAlias(array_join_array_expression->tryGetAlias());

        //     auto last_table_expression = std::move(table_expressions.back());
        //     table_expressions.pop_back();

        //     auto array_join_expression = ArrayJoinExpression::create();
        //     array_join_expression->getLeftTableExpression() = std::move(last_table_expression);
        //     array_join_expression->getArrayExpression() = std::move(expression_element);

        //     table_expressions.push_back(array_join_expression);
        // }
    }

    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "From section is unsupported");
    // if (table_expressions.empty())
    //     throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryAnalyzer from cannot be empty");

    // return table_expressions.back();
}


ColumnTransformersNodes QueryTreeBuilder::getColumnTransformers(const ASTPtr & matcher_expression, size_t start_child_index) const
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
                auto lambda_query_tree_node = getExpression(apply_transformer->lambda);
                column_transformers.emplace_back(std::make_shared<ApplyColumnTransformerNode>(std::move(lambda_query_tree_node)));
            }
            else
            {
                auto function_node = std::make_shared<FunctionNode>(apply_transformer->func_name);
                if (apply_transformer->parameters)
                    function_node->getParametersNode() = getExpressionList(apply_transformer->parameters);

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

                column_transformers.emplace_back(std::make_shared<ExceptColumnTransformerNode>(std::move(except_column_names)));
            }
        }
        else if (auto * replace_transformer = child->as<ASTColumnsReplaceTransformer>())
        {
            std::vector<ReplaceColumnTransformerNode::Replacement> replacements;
            replacements.reserve(replace_transformer->children.size());

            for (const auto & replace_transformer_child : replace_transformer->children)
            {
                auto & replacement = replace_transformer_child->as<ASTColumnsReplaceTransformer::Replacement &>();
                replacements.emplace_back(ReplaceColumnTransformerNode::Replacement{replacement.name, getExpression(replacement.expr)});
            }

            column_transformers.emplace_back(std::make_shared<ReplaceColumnTransformerNode>(replacements));
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported column matcher {}", child->formatForErrorMessage());
        }
    }

    return column_transformers;
}

StoragePtr QueryTreeBuilder::resolveStorage(const Identifier & storage_identifier) const
{
    size_t parts_size = storage_identifier.getPartsSize();
    if (parts_size < 1 || parts_size > 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table identifier should consist of 1 or 2 parts");

    std::string database_name;
    std::string table_name;

    if (storage_identifier.isCompound())
    {
        database_name = storage_identifier[0];
        table_name = storage_identifier[1];
    }
    else
    {
        table_name = storage_identifier[0];
    }

    auto current_context = getContext();
    if (database_name.empty())
        database_name = current_context->getCurrentDatabase();

    auto & database_catalog = DatabaseCatalog::instance();
    auto database = database_catalog.tryGetDatabase(database_name);
    if (!database)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} doesn't exists", database_name);

    auto table = database->tryGetTable(table_name, current_context);

    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} doesn't exists", table_name);

    return table;
}

QueryTreeNodePtr buildQueryTree(ASTPtr query, ContextPtr context)
{
    QueryTreeBuilder builder(query, context);
    return builder.getQueryTreeNode();
}

}

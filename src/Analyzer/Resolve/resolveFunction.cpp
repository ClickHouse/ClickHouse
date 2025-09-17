#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/WindowNode.h>

#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/SetUtils.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/exists.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/misc.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/grouping.h>
#include <Storages/StorageJoin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_IDENTIFIER;
    extern const int SYNTAX_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

namespace Setting
{
    extern const SettingsBool execute_exists_as_scalar_subquery;
    extern const SettingsBool format_display_secrets_in_show_and_select;
    extern const SettingsBool transform_null_in;
    extern const SettingsBool force_grouping_standard_compatibility;
    extern const SettingsBool validate_enum_literals_in_operators;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsOverflowMode set_overflow_mode;
}

namespace
{
void checkFunctionNodeHasEmptyNullsAction(FunctionNode const & node)
{
    if (node.getNullsAction() != NullsAction::EMPTY)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Function with name {} cannot use {} NULLS",
            backQuote(node.getFunctionName()),
            node.getNullsAction() == NullsAction::IGNORE_NULLS ? "IGNORE" : "RESPECT");
}
}

/** Resolve function node in scope.
  * During function node resolve, function node can be replaced with another expression (if it match lambda or sql user defined function),
  * with constant (if it allow constant folding), or with expression list. It is caller responsibility to handle such cases appropriately.
  *
  * Steps:
  * 1. Resolve function parameters. Validate that each function parameter must be constant node.
  * 2. Try to lookup function as lambda in current scope. If it is lambda we can skip `in` and `count` special handling.
  * 3. If function is count function, that take unqualified ASTERISK matcher, remove it from its arguments. Example: SELECT count(*) FROM test_table;
  * 4. If function is `IN` function, then right part of `IN` function is replaced as subquery.
  * 5. Resolve function arguments list, lambda expressions are allowed as function arguments.
  * For `IN` function table expressions are allowed as function arguments.
  * 6. Initialize argument_columns, argument_types, function_lambda_arguments_indexes arrays from function arguments.
  * 7. If function name identifier was not resolved as function in current scope, try to lookup lambda from sql user defined functions factory.
  * 8. If function was resolve as lambda from step 2 or 7, then resolve lambda using function arguments and replace function node with lambda result.
  * After than function node is resolved.
  * 9. If function was not resolved during step 6 as lambda, then try to resolve function as window function or executable user defined function
  * or ordinary function or aggregate function.
  *
  * If function is resolved as window function or executable user defined function or aggregate function, function node is resolved
  * no additional special handling is required.
  *
  * 8. If function was resolved as non aggregate function. Then if some of function arguments are lambda expressions, their result types need to be initialized and
  * they must be resolved.
  * 9. If function is suitable for constant folding, try to perform constant folding for function node.
  */
ProjectionNames QueryAnalyzer::resolveFunction(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    FunctionNodePtr function_node_ptr = std::static_pointer_cast<FunctionNode>(node);
    auto function_name = function_node_ptr->getFunctionName();

    /// Resolve function parameters

    auto parameters_projection_names = resolveExpressionNodeList(
        function_node_ptr->getParametersNode(),
        scope,
        false /*allow_lambda_expression*/,
        false /*allow_table_expression*/);

    /// Convert function parameters into constant parameters array

    Array parameters;

    auto & parameters_nodes = function_node_ptr->getParameters().getNodes();
    parameters.reserve(parameters_nodes.size());

    for (auto & parameter_node : parameters_nodes)
    {
        const auto * constant_node = parameter_node->as<ConstantNode>();
        if (!constant_node)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter for function '{}' expected to have constant value. Actual: {}. In scope {}",
            function_name,
            parameter_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

        parameters.push_back(constant_node->getValue());
    }

    //// If function node is not window function try to lookup function node name as lambda identifier.
    QueryTreeNodePtr lambda_expression_untyped;
    if (!function_node_ptr->isWindowFunction())
    {
        auto function_lookup_result = tryResolveIdentifier({Identifier{function_name}, IdentifierLookupContext::FUNCTION}, scope);
        lambda_expression_untyped = function_lookup_result.resolved_identifier;
    }

    bool is_special_function_in = false;
    bool is_special_function_dict_get = false;
    bool is_special_function_join_get = false;
    bool is_special_function_exists = false;
    bool is_special_function_if = false;

    if (!lambda_expression_untyped)
    {
        is_special_function_in = isNameOfInFunction(function_name);
        is_special_function_dict_get = functionIsDictGet(function_name);
        is_special_function_join_get = functionIsJoinGet(function_name);
        is_special_function_exists = function_name == "exists";
        is_special_function_if = function_name == "if";

        auto function_name_lowercase = Poco::toLower(function_name);

        /** Special handling for count and countState functions.
          *
          * Example: SELECT count(*) FROM test_table
          * Example: SELECT countState(*) FROM test_table;
          */
        if (function_node_ptr->getArguments().getNodes().size() == 1 &&
            (function_name_lowercase == "count" || function_name_lowercase == "countstate"))
        {
            auto * matcher_node = function_node_ptr->getArguments().getNodes().front()->as<MatcherNode>();
            if (matcher_node && matcher_node->isUnqualified())
                function_node_ptr->getArguments().getNodes().clear();
        }
    }

    /** Special functions dictGet and its variations and joinGet can be executed when first argument is identifier.
      * Example: SELECT dictGet(identifier, 'value', toUInt64(0));
      *
      * Try to resolve identifier as expression identifier and if it is resolved use it.
      * Example: WITH 'dict_name' AS identifier SELECT dictGet(identifier, 'value', toUInt64(0));
      *
      * Otherwise replace identifier with identifier full name constant.
      * Validation that dictionary exists or table exists will be performed during function `getReturnType` method call.
      */
    if ((is_special_function_dict_get || is_special_function_join_get) &&
        !function_node_ptr->getArguments().getNodes().empty() &&
        function_node_ptr->getArguments().getNodes()[0]->getNodeType() == QueryTreeNodeType::IDENTIFIER)
    {
        auto & first_argument = function_node_ptr->getArguments().getNodes()[0];
        auto & first_argument_identifier = first_argument->as<IdentifierNode &>();
        auto identifier = first_argument_identifier.getIdentifier();

        IdentifierLookup identifier_lookup{identifier, IdentifierLookupContext::EXPRESSION};
        auto resolve_result = tryResolveIdentifier(identifier_lookup, scope);

        if (resolve_result.isResolved())
        {
            first_argument = std::move(resolve_result.resolved_identifier);
        }
        else
        {
            size_t parts_size = identifier.getPartsSize();
            if (parts_size < 1 || parts_size > 2)
                throw Exception(ErrorCodes::INVALID_IDENTIFIER,
                    "Expected {} function first argument identifier to contain 1 or 2 parts. Actual '{}'. In scope {}",
                    function_name,
                    identifier.getFullName(),
                    scope.scope_node->formatASTForErrorMessage());

            if (is_special_function_dict_get)
            {
                scope.context->getExternalDictionariesLoader().assertDictionaryStructureExists(identifier.getFullName(), scope.context);
            }
            else
            {
                auto table_node = IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(identifier, scope.context).resolved_identifier;
                if (!table_node)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} first argument expected table identifier '{}'. In scope {}",
                        function_name,
                        identifier.getFullName(),
                        scope.scope_node->formatASTForErrorMessage());

                auto & table_node_typed = table_node->as<TableNode &>();
                if (!std::dynamic_pointer_cast<StorageJoin>(table_node_typed.getStorage()))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} table '{}' should have engine StorageJoin. In scope {}",
                        function_name,
                        identifier.getFullName(),
                        scope.scope_node->formatASTForErrorMessage());
            }

            first_argument = std::make_shared<ConstantNode>(identifier.getFullName());
        }
    }

    if (is_special_function_if && !function_node_ptr->getArguments().getNodes().empty())
    {
        checkFunctionNodeHasEmptyNullsAction(*function_node_ptr);
        /** Handle special case with constant If function, even if some of the arguments are invalid.
          *
          * SELECT if(hasColumnInTable('system', 'numbers', 'not_existing_column'), not_existing_column, 5) FROM system.numbers;
          */
        auto & if_function_arguments = function_node_ptr->getArguments().getNodes();
        auto if_function_condition = if_function_arguments[0];
        resolveExpressionNode(if_function_condition, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        auto constant_condition = tryExtractConstantFromConditionNode(if_function_condition);

        if (constant_condition.has_value() && if_function_arguments.size() == 3)
        {
            QueryTreeNodePtr constant_if_result_node;
            QueryTreeNodePtr possibly_invalid_argument_node;

            if (*constant_condition)
            {
                possibly_invalid_argument_node = if_function_arguments[2];
                constant_if_result_node = if_function_arguments[1];
            }
            else
            {
                possibly_invalid_argument_node = if_function_arguments[1];
                constant_if_result_node = if_function_arguments[2];
            }

            bool apply_constant_if_optimization = false;

            try
            {
                resolveExpressionNode(possibly_invalid_argument_node,
                    scope,
                    false /*allow_lambda_expression*/,
                    false /*allow_table_expression*/);
            }
            catch (...)
            {
                apply_constant_if_optimization = true;
            }

            if (apply_constant_if_optimization)
            {
                auto result_projection_names = resolveExpressionNode(constant_if_result_node,
                    scope,
                    false /*allow_lambda_expression*/,
                    false /*allow_table_expression*/);
                node = std::move(constant_if_result_node);
                return result_projection_names;
            }
        }
    }

    if (is_special_function_exists)
    {
        checkFunctionNodeHasEmptyNullsAction(*function_node_ptr);
        /// Rewrite EXISTS (subquery) into EXISTS (SELECT 1 FROM (subquery) LIMIT 1).
        const auto & exists_subquery_argument = function_node_ptr->getArguments().getNodes().at(0);

        auto constant_data_type = std::make_shared<DataTypeUInt64>();
        auto new_exists_subquery = std::make_shared<QueryNode>(Context::createCopy(scope.context));

        new_exists_subquery->setIsSubquery(true);
        new_exists_subquery->getProjection().getNodes().push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
        new_exists_subquery->getJoinTree() = exists_subquery_argument;
        new_exists_subquery->getLimit() = std::make_shared<ConstantNode>(1UL, constant_data_type);

        QueryTreeNodePtr new_exists_argument = new_exists_subquery;

        auto exists_arguments_projection_names = resolveExpressionNode(
            new_exists_argument,
            scope,
            true /*allow_lambda_expression*/,
            true /*allow_table_expression*/
        );

        if (new_exists_subquery->isCorrelated())
        {
            function_node_ptr->getArguments().getNodes() = {
                std::move(new_exists_argument)
            };

            /// Subquery is correlated and EXISTS can not be replaced by IN function.
            /// EXISTS function will be replated by JOIN during query planning.
            auto function_exists = std::make_shared<FunctionExists>();
            function_node_ptr->resolveAsFunction(
                std::make_shared<FunctionToFunctionBaseAdaptor>(
                    function_exists, DataTypes{}, function_exists->getReturnTypeImpl({})
                )
            );

            return { calculateFunctionProjectionName(node, parameters_projection_names, exists_arguments_projection_names) };
        }
        else
        {
            if (only_analyze || !scope.context->getSettingsRef()[Setting::execute_exists_as_scalar_subquery])
            {
                /// Rewrite EXISTS (subquery) into 1 IN (SELECT 1 FROM (subquery) LIMIT 1).
                QueryTreeNodePtr constant = std::make_shared<ConstantNode>(1UL, constant_data_type);

                function_node_ptr = std::make_shared<FunctionNode>("in");
                function_node_ptr->getArguments().getNodes() = {
                    constant,
                    std::move(new_exists_argument)
                };

                node = function_node_ptr;
                function_name = "in";
                is_special_function_in = true;
            }
            else
            {
                evaluateScalarSubqueryIfNeeded(new_exists_argument, scope, true);
                auto res_col = ColumnUInt8::create();
                const auto * const_node = new_exists_argument->as<ConstantNode>();
                res_col->getData().push_back(const_node->getColumn()->isNullAt(0) ? 0 : 1);
                ConstantValue const_value(std::move(res_col), std::make_shared<DataTypeUInt8>());
                auto tme_const_node = std::make_shared<ConstantNode>(std::move(const_value), std::move(node));
                auto res = tme_const_node->getValueStringRepresentation();
                node = std::move(tme_const_node);
                return {std::move(res)};
            }
        }
    }

    /// Resolve function arguments
    bool allow_table_expressions = is_special_function_in || is_special_function_exists;
    auto arguments_projection_names = resolveExpressionNodeList(
        function_node_ptr->getArgumentsNode(),
        scope,
        true /*allow_lambda_expression*/,
        allow_table_expressions /*allow_table_expression*/);

    /// Mask arguments if needed
    if (!scope.context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select])
    {
        if (FunctionSecretArgumentsFinder::Result secret_arguments = FunctionSecretArgumentsFinderTreeNode(*function_node_ptr).getResult(); secret_arguments.count)
        {
            auto & argument_nodes = function_node_ptr->getArgumentsNode()->as<ListNode &>().getNodes();

            for (size_t n = secret_arguments.start; n < secret_arguments.start + secret_arguments.count; ++n)
            {
                if (auto * constant = argument_nodes[n]->as<ConstantNode>())
                {
                    auto mask = scope.projection_mask_map->insert({constant->getTreeHash(), scope.projection_mask_map->size() + 1}).first->second;
                    constant->setMaskId(mask);
                    arguments_projection_names[n] = "[HIDDEN id: " + std::to_string(mask) + "]";
                }
            }
        }
    }

    auto & function_node = *function_node_ptr;

    /// Replace right IN function argument if it is table or table function with subquery that read ordinary columns
    if (is_special_function_in)
    {
        checkFunctionNodeHasEmptyNullsAction(function_node);
        if (scope.context->getSettingsRef()[Setting::transform_null_in])
        {
            static constexpr std::array<std::pair<std::string_view, std::string_view>, 4> in_function_to_replace_null_in_function_map =
            {{
                {"in", "nullIn"},
                {"notIn", "notNullIn"},
                {"globalIn", "globalNullIn"},
                {"globalNotIn", "globalNotNullIn"},
            }};

            for (const auto & [in_function_name, in_function_name_to_replace] : in_function_to_replace_null_in_function_map)
            {
                if (function_name == in_function_name)
                {
                    function_name = in_function_name_to_replace;
                    break;
                }
            }
        }

        auto & function_in_arguments_nodes = function_node.getArguments().getNodes();
        if (function_in_arguments_nodes.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' expects 2 arguments", function_name);

        auto & in_second_argument = function_in_arguments_nodes[1];
        if (isCorrelatedQueryOrUnionNode(function_in_arguments_nodes[0]) || isCorrelatedQueryOrUnionNode(function_in_arguments_nodes[1]))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Correlated subqueries are not supported as IN function arguments yet, but found in expression: {}",
                node->formatASTForErrorMessage());
        auto * table_node = in_second_argument->as<TableNode>();
        auto * table_function_node = in_second_argument->as<TableFunctionNode>();

        if (table_node)
        {
            /// If table is already prepared set, we do not replace it with subquery.
            /// If table is not a StorageSet, we'll create plan to build set in the Planner.
        }
        else if (table_function_node)
        {
            const auto & storage_snapshot = table_function_node->getStorageSnapshot();
            auto columns_to_select = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary));

            size_t columns_to_select_size = columns_to_select.size();

            auto column_nodes_to_select = std::make_shared<ListNode>();
            column_nodes_to_select->getNodes().reserve(columns_to_select_size);

            NamesAndTypes projection_columns;
            projection_columns.reserve(columns_to_select_size);

            for (auto & column : columns_to_select)
            {
                column_nodes_to_select->getNodes().emplace_back(std::make_shared<ColumnNode>(column, in_second_argument));
                projection_columns.emplace_back(column.name, column.type);
            }

            auto in_second_argument_query_node = std::make_shared<QueryNode>(Context::createCopy(scope.context));
            in_second_argument_query_node->setIsSubquery(true);
            in_second_argument_query_node->getProjectionNode() = std::move(column_nodes_to_select);
            in_second_argument_query_node->getJoinTree() = std::move(in_second_argument);
            in_second_argument_query_node->resolveProjectionColumns(std::move(projection_columns));

            in_second_argument = std::move(in_second_argument_query_node);
        }
        else
        {
            /// Replace storage with values storage of insertion block
            if (StoragePtr storage = scope.context->getViewSource())
            {
                QueryTreeNodePtr table_expression = in_second_argument;

                /// Process possibly nested sub-selects
                while (table_expression)
                {
                    if (auto * query_node = table_expression->as<QueryNode>())
                        table_expression = extractLeftTableExpression(query_node->getJoinTree());
                    else if (auto * union_node = table_expression->as<UnionNode>())
                        table_expression = union_node->getQueries().getNodes().at(0);
                    else
                        break;
                }

                TableNode * table_expression_table_node = table_expression ? table_expression->as<TableNode>() : nullptr;

                if (table_expression_table_node &&
                    table_expression_table_node->getStorageID().getFullNameNotQuoted() == storage->getStorageID().getFullNameNotQuoted())
                {
                    auto replacement_table_expression_table_node = table_expression_table_node->clone();
                    replacement_table_expression_table_node->as<TableNode &>().updateStorage(storage, scope.context);
                    in_second_argument = in_second_argument->cloneAndReplace(table_expression, std::move(replacement_table_expression_table_node));
                }
            }
        }

        /// Edge case when the first argument of IN is scalar subquery.
        auto & in_first_argument = function_in_arguments_nodes[0];
        auto first_argument_type = in_first_argument->getNodeType();
        if (first_argument_type == QueryTreeNodeType::QUERY || first_argument_type == QueryTreeNodeType::UNION)
        {
            IdentifierResolveScope & subquery_scope = createIdentifierResolveScope(in_first_argument, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            evaluateScalarSubqueryIfNeeded(in_first_argument, subquery_scope);
        }
    }

    /// Initialize function argument columns

    ColumnsWithTypeAndName argument_columns;
    DataTypes argument_types;
    bool all_arguments_constants = true;
    std::vector<size_t> function_lambda_arguments_indexes;

    auto & function_arguments = function_node.getArguments().getNodes();
    size_t function_arguments_size = function_arguments.size();

    for (size_t function_argument_index = 0; function_argument_index < function_arguments_size; ++function_argument_index)
    {
        auto & function_argument = function_arguments[function_argument_index];

        ColumnWithTypeAndName argument_column;
        argument_column.name = arguments_projection_names[function_argument_index];

        /** If function argument is lambda, save lambda argument index and initialize argument type as DataTypeFunction
          * where function argument types are initialized with empty arrays of lambda arguments size.
          */
        const auto * lambda_node = function_argument->as<const LambdaNode>();
        if (lambda_node)
        {
            size_t lambda_arguments_size = lambda_node->getArguments().getNodes().size();
            argument_column.type = std::make_shared<DataTypeFunction>(DataTypes(lambda_arguments_size, nullptr), nullptr);
            function_lambda_arguments_indexes.push_back(function_argument_index);
        }
        else if (is_special_function_in && function_argument_index == 1)
        {
            argument_column.type = std::make_shared<DataTypeSet>();
        }
        else
        {
            argument_column.type = function_argument->getResultType();
        }

        if (!argument_column.type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Function '{}' argument is not resolved. In scope {}",
                function_name,
                scope.scope_node->formatASTForErrorMessage());

        bool argument_is_constant = false;
        const auto * constant_node = function_argument->as<ConstantNode>();
        if (constant_node)
        {
            argument_column.column = constant_node->getColumn();
            argument_column.type = constant_node->getResultType();
            argument_is_constant = true;
        }
        else if (const auto * get_scalar_function_node = function_argument->as<FunctionNode>();
                get_scalar_function_node && get_scalar_function_node->getFunctionName() == "__getScalar")
        {
            /// Allow constant folding through getScalar
            const auto * get_scalar_const_arg = get_scalar_function_node->getArguments().getNodes().at(0)->as<ConstantNode>();
            if (get_scalar_const_arg && scope.context->hasQueryContext())
            {
                auto query_context = scope.context->getQueryContext();
                auto scalar_string = toString(get_scalar_const_arg->getValue());
                if (query_context->hasScalar(scalar_string))
                {
                    auto scalar = query_context->getScalar(scalar_string);
                    argument_column.column = ColumnConst::create(scalar.getByPosition(0).column, 1);
                    argument_column.type = get_scalar_function_node->getResultType();
                    argument_is_constant = true;
                }
            }
        }

        all_arguments_constants &= argument_is_constant;

        argument_types.push_back(argument_column.type);
        argument_columns.emplace_back(std::move(argument_column));
    }

    /// Calculate function projection name
    ProjectionNames result_projection_names = { calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names) };

    /** Try to resolve function as
      * 1. Lambda function in current scope. Example: WITH (x -> x + 1) AS lambda SELECT lambda(1);
      * 2. Lambda function from sql user defined functions.
      * 3. Special `untuple` function.
      * 4. Special `grouping` function.
      * 5. Window function.
      * 6. Executable user defined function.
      * 7. Ordinary function.
      * 8. Aggregate function.
      *
      * TODO: Provide better error hints.
      */
    if (!function_node.isWindowFunction())
    {
        if (!lambda_expression_untyped)
            lambda_expression_untyped = tryGetLambdaFromSQLUserDefinedFunctions(function_node.getFunctionName(), scope.context);

        /** If function is resolved as lambda.
          * Clone lambda before resolve.
          * Initialize lambda arguments as function arguments.
          * Resolve lambda and then replace function node with resolved lambda expression body.
          * Example: WITH (x -> x + 1) AS lambda SELECT lambda(value) FROM test_table;
          * Result: SELECT value + 1 FROM test_table;
          */
        if (lambda_expression_untyped)
        {
            auto * lambda_expression = lambda_expression_untyped->as<LambdaNode>();
            if (!lambda_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function identifier '{}' must be resolved as lambda. Actual: {}. In scope {}",
                    function_node.getFunctionName(),
                    lambda_expression_untyped->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            checkFunctionNodeHasEmptyNullsAction(function_node);

            if (!parameters.empty())
            {
                throw Exception(
                    ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS, "Function {} is not parametric", function_node.formatASTForErrorMessage());
            }

            auto lambda_expression_clone = lambda_expression_untyped->clone();

            IdentifierResolveScope & lambda_scope = createIdentifierResolveScope(lambda_expression_clone, &scope /*parent_scope*/);
            ProjectionNames lambda_projection_names = resolveLambda(lambda_expression_untyped, lambda_expression_clone, function_arguments, lambda_scope);

            auto & resolved_lambda = lambda_expression_clone->as<LambdaNode &>();
            node = resolved_lambda.getExpression();

            if (node->getNodeType() == QueryTreeNodeType::LIST)
                result_projection_names = std::move(lambda_projection_names);

            return result_projection_names;
        }

        if (function_name == "untuple")
        {
            /// Special handling of `untuple` function

            if (function_arguments.size() != 1)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Function 'untuple' must have 1 argument. In scope {}",
                    scope.scope_node->formatASTForErrorMessage());

            checkFunctionNodeHasEmptyNullsAction(function_node);

            const auto & untuple_argument = function_arguments[0];
            /// Handle this special case first as `getResultType()` might return nullptr
            if (untuple_argument->as<LambdaNode>())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function untuple can't have lambda-expressions as arguments");

            auto result_type = untuple_argument->getResultType();
            const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(result_type.get());
            if (!tuple_data_type)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Function 'untuple' argument must have compound type. Actual type {}. In scope {}",
                    result_type->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            const auto & element_names = tuple_data_type->getElementNames();

            auto result_list = std::make_shared<ListNode>();
            result_list->getNodes().reserve(element_names.size());

            for (const auto & element_name : element_names)
            {
                auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
                tuple_element_function->getArguments().getNodes().push_back(untuple_argument);
                tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

                QueryTreeNodePtr function_query_node = tuple_element_function;
                resolveFunction(function_query_node, scope);

                result_list->getNodes().push_back(std::move(function_query_node));
            }

            const auto & untuple_argument_projection_name = arguments_projection_names.at(0);
            result_projection_names.clear();

            for (const auto & element_name : element_names)
            {
                if (node->hasAlias())
                    result_projection_names.push_back(node->getAlias() + '.' + element_name);
                else
                    result_projection_names.push_back(fmt::format("tupleElement({}, '{}')", untuple_argument_projection_name, element_name));
            }

            node = std::move(result_list);
            return result_projection_names;
        }
        if (function_name == "grouping")
        {
            /// It is responsibility of planner to perform additional handling of grouping function
            if (function_arguments_size == 0)
                throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function GROUPING expects at least one argument");
            if (function_arguments_size > 64)
                throw Exception(
                    ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                    "Function GROUPING can have up to 64 arguments, but {} provided",
                    function_arguments_size);
            checkFunctionNodeHasEmptyNullsAction(function_node);

            bool force_grouping_standard_compatibility = scope.context->getSettingsRef()[Setting::force_grouping_standard_compatibility];
            auto grouping_function = std::make_shared<FunctionGrouping>(force_grouping_standard_compatibility);
            auto grouping_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_function));
            function_node.resolveAsFunction(grouping_function_adaptor->build(argument_columns));

            return result_projection_names;
        }
    }

    if (function_node.isWindowFunction())
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
        {
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Aggregate function with name '{}' does not exist. In scope {}{}",
                            function_name, scope.scope_node->formatASTForErrorMessage(),
                            getHintsErrorMessageSuffix(AggregateFunctionFactory::instance().getHints(function_name)));
        }

        if (!function_lambda_arguments_indexes.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Window function '{}' does not support lambda arguments",
                function_name);

        auto action = function_node_ptr->getNullsAction();
        std::string aggregate_function_name = rewriteAggregateFunctionNameIfNeeded(function_name, action, scope.context);

        AggregateFunctionProperties properties;
        auto aggregate_function
            = AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, parameters, properties);

        function_node.resolveAsWindowFunction(std::move(aggregate_function));

        bool window_node_is_identifier = function_node.getWindowNode()->getNodeType() == QueryTreeNodeType::IDENTIFIER;
        ProjectionName window_projection_name = resolveWindow(function_node.getWindowNode(), scope);

        if (function_name == "lag" || function_name == "lead")
        {
            auto & frame = function_node.getWindowNode()->as<WindowNode>()->getWindowFrame();
            if (!frame.is_default)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Window function '{}' does not expect window frame to be explicitly specified. In expression {}",
                    function_name,
                    function_node.formatASTForErrorMessage());
            }

            frame = WindowFrame{
                .is_default = false,
                .type = WindowFrame::FrameType::ROWS,
                .begin_type = WindowFrame::BoundaryType::Unbounded,
                .begin_preceding = true,
                .end_type = WindowFrame::BoundaryType::Unbounded,
                .end_preceding = false,
            };
        }

        if (window_node_is_identifier)
            result_projection_names[0] += " OVER " + window_projection_name;
        else
            result_projection_names[0] += " OVER (" + window_projection_name + ')';

        return result_projection_names;
    }

    FunctionOverloadResolverPtr function = UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, scope.context, parameters); /// NOLINT(readability-static-accessed-through-instance)
    bool is_executable_udf = true;

    IdentifierResolveScope::ResolvedFunctionsCache * function_cache = nullptr;

    if (!function)
    {
        /// This is a hack to allow a query like `select randConstant(), randConstant(), randConstant()`.
        /// Function randConstant() would return the same value for the same arguments (in scope).

        auto hash = function_node_ptr->getTreeHash();
        function_cache = &scope.functions_cache[hash];
        if (!function_cache->resolver)
            function_cache->resolver = FunctionFactory::instance().tryGet(function_name, scope.context);

        function = function_cache->resolver;

        is_executable_udf = false;
    }

    if (function)
    {
        checkFunctionNodeHasEmptyNullsAction(function_node);
    }
    else
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
        {
            std::vector<std::string> possible_function_names;

            auto function_names = UserDefinedExecutableFunctionFactory::instance().getRegisteredNames(scope.context); /// NOLINT(readability-static-accessed-through-instance)
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = UserDefinedSQLFunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = FunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = AggregateFunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            for (auto & [name, lambda_node] : scope.aliases.alias_name_to_lambda_node)
            {
                if (lambda_node->getNodeType() == QueryTreeNodeType::LAMBDA)
                    possible_function_names.push_back(name);
            }

            auto hints = NamePrompter<2>::getHints(function_name, possible_function_names);

            throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
                "Function with name {} does not exist. In scope {}{}",
                backQuote(function_name),
                scope.scope_node->formatASTForErrorMessage(),
                getHintsErrorMessageSuffix(hints));
        }

        if (!function_lambda_arguments_indexes.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Aggregate function {} does not support lambda arguments",
                backQuote(function_name));

        auto action = function_node_ptr->getNullsAction();
        std::string aggregate_function_name = rewriteAggregateFunctionNameIfNeeded(function_name, action, scope.context);

        AggregateFunctionProperties properties;
        auto aggregate_function
            = AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, parameters, properties);

        function_node.resolveAsAggregateFunction(std::move(aggregate_function));

        return result_projection_names;
    }

    /// Executable UDFs may have parameters. They are checked in UserDefinedExecutableFunctionFactory.
    if (!parameters.empty() && !is_executable_udf)
    {
        throw Exception(ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS, "Function {} is not parametric", function_name);
    }

    /** For lambda arguments we need to initialize lambda argument types DataTypeFunction using `getLambdaArgumentTypes` function.
      * Then each lambda arguments are initialized with columns, where column source is lambda.
      * This information is important for later steps of query processing.
      * Example: SELECT arrayMap(x -> x + 1, [1, 2, 3]).
      * lambda node x -> x + 1 identifier x is resolved as column where source is lambda node.
      */
    bool has_lambda_arguments = !function_lambda_arguments_indexes.empty();
    if (has_lambda_arguments)
    {
        function->getLambdaArgumentTypes(argument_types);

        ProjectionNames lambda_projection_names;
        for (auto & function_lambda_argument_index : function_lambda_arguments_indexes)
        {
            auto & lambda_argument = function_arguments[function_lambda_argument_index];
            auto lambda_to_resolve = lambda_argument->clone();
            auto & lambda_to_resolve_typed = lambda_to_resolve->as<LambdaNode &>();

            const auto & lambda_argument_names = lambda_to_resolve_typed.getArgumentNames();
            size_t lambda_arguments_size = lambda_to_resolve_typed.getArguments().getNodes().size();

            const auto * function_data_type = typeid_cast<const DataTypeFunction *>(argument_types[function_lambda_argument_index].get());
            if (!function_data_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function '{}' expected function data type for lambda argument with index {}. Actual: {}. In scope {}",
                    function_name,
                    function_lambda_argument_index,
                    argument_types[function_lambda_argument_index]->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            const auto & function_data_type_argument_types = function_data_type->getArgumentTypes();
            size_t function_data_type_arguments_size = function_data_type_argument_types.size();
            if (function_data_type_arguments_size != lambda_arguments_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Function '{}"
                                "' function data type for lambda argument with index {} arguments size mismatch. "
                                "Actual: {}. Expected {}. In scope {}",
                                function_name,
                                function_data_type_arguments_size,
                                lambda_arguments_size,
                                argument_types[function_lambda_argument_index]->getName(),
                                scope.scope_node->formatASTForErrorMessage());

            QueryTreeNodes lambda_arguments;
            lambda_arguments.reserve(lambda_arguments_size);

            IdentifierResolveScope & lambda_scope = createIdentifierResolveScope(lambda_to_resolve, &scope /*parent_scope*/);
            for (size_t i = 0; i < lambda_arguments_size; ++i)
            {
                const auto & argument_type = function_data_type_argument_types[i];
                auto column_name_and_type = NameAndTypePair{lambda_argument_names[i], argument_type};
                lambda_arguments.push_back(std::make_shared<ColumnNode>(std::move(column_name_and_type), lambda_to_resolve));
            }

            lambda_projection_names = resolveLambda(lambda_argument, lambda_to_resolve, lambda_arguments, lambda_scope);

            if (auto * lambda_list_node_result = lambda_to_resolve_typed.getExpression()->as<ListNode>())
            {
                size_t lambda_list_node_result_nodes_size = lambda_list_node_result->getNodes().size();

                if (lambda_list_node_result_nodes_size != 1)
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Lambda as function argument resolved as list node with size {}. Expected 1. In scope {}",
                        lambda_list_node_result_nodes_size,
                        lambda_to_resolve->formatASTForErrorMessage());

                lambda_to_resolve_typed.getExpression() = lambda_list_node_result->getNodes().front();
            }

            if (arguments_projection_names.at(function_lambda_argument_index) == PROJECTION_NAME_PLACEHOLDER)
            {
                size_t lambda_projection_names_size =lambda_projection_names.size();
                if (lambda_projection_names_size != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Lambda argument inside function expected to have 1 projection name. Actual: {}",
                        lambda_projection_names_size);

                WriteBufferFromOwnString lambda_argument_projection_name_buffer;
                lambda_argument_projection_name_buffer << "lambda(";
                lambda_argument_projection_name_buffer << "tuple(";

                size_t lambda_argument_names_size = lambda_argument_names.size();

                for (size_t i = 0; i < lambda_argument_names_size; ++i)
                {
                    const auto & lambda_argument_name = lambda_argument_names[i];
                    lambda_argument_projection_name_buffer << lambda_argument_name;

                    if (i + 1 != lambda_argument_names_size)
                        lambda_argument_projection_name_buffer << ", ";
                }

                lambda_argument_projection_name_buffer << "), ";
                lambda_argument_projection_name_buffer << lambda_projection_names[0];
                lambda_argument_projection_name_buffer << ")";

                lambda_projection_names.clear();

                arguments_projection_names[function_lambda_argument_index] = lambda_argument_projection_name_buffer.str();
            }

            auto lambda_resolved_type = std::make_shared<DataTypeFunction>(function_data_type_argument_types, lambda_to_resolve_typed.getExpression()->getResultType());
            lambda_to_resolve_typed.resolve(lambda_resolved_type);

            argument_types[function_lambda_argument_index] = lambda_resolved_type;
            argument_columns[function_lambda_argument_index].type = lambda_resolved_type;
            function_arguments[function_lambda_argument_index] = std::move(lambda_to_resolve);
        }

        /// Recalculate function projection name after lambda resolution
        result_projection_names = { calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names) };
    }

    /** Create SET column for special function IN to allow constant folding
      * if left and right arguments are constants.
      *
      * Example: SELECT * FROM test_table LIMIT 1 IN 1;
      */
    if (is_special_function_in)
    {
        const auto * first_argument_constant_node = function_arguments[0]->as<ConstantNode>();
        const auto * second_argument_constant_node = function_arguments[1]->as<ConstantNode>();

        if (first_argument_constant_node && second_argument_constant_node)
        {
            const auto & first_argument_constant_type = first_argument_constant_node->getResultType();
            const auto second_argument_constant_literal = second_argument_constant_node->getValue();
            const auto & second_argument_constant_type = second_argument_constant_node->getResultType();

            const auto & settings = scope.context->getSettingsRef();

            auto result_block = getSetElementsForConstantValue(
                first_argument_constant_type, second_argument_constant_literal, second_argument_constant_type,
                GetSetElementParams{
                    .transform_null_in = settings[Setting::transform_null_in],
                    .forbid_unknown_enum_values = settings[Setting::validate_enum_literals_in_operators],
                });


            SizeLimits size_limits_for_set = {settings[Setting::max_rows_in_set], settings[Setting::max_bytes_in_set], settings[Setting::set_overflow_mode]};

            auto hash = function_arguments[1]->getTreeHash({ .ignore_cte = true });
            auto ast = function_arguments[1]->toAST();
            auto future_set = std::make_shared<FutureSetFromTuple>(hash, std::move(ast), std::move(result_block), settings[Setting::transform_null_in], size_limits_for_set);

            /// Create constant set column for constant folding

            auto column_set = ColumnSet::create(1, std::move(future_set));
            argument_columns[1].column = ColumnConst::create(std::move(column_set), 1);
        }

        argument_columns[1].type = std::make_shared<DataTypeSet>();
    }

    ConstantNodePtr constant_node;

    try
    {
        FunctionBasePtr function_base;
        if (function_cache)
        {
            auto & cached_function = function_cache->function_base;
            if (!cached_function)
                cached_function = function->build(argument_columns);

            function_base = cached_function;
        }
        else
            function_base = function->build(argument_columns);

        bool allow_constant_folding = true;

        auto * nearest_join_query_scope = scope.joins_count > 0 ? scope.getNearestQueryScope() : nullptr;
        auto * nearest_join_query_scope_query_node = nearest_join_query_scope ? nearest_join_query_scope->scope_node->as<QueryNode>() : nullptr;
        const auto * join_node = nearest_join_query_scope_query_node ? nearest_join_query_scope_query_node->getJoinTree()->as<JoinNode>() : nullptr;
        if (join_node && join_node->getStrictness() == JoinStrictness::Asof &&
            scope.expressions_in_resolve_process_stack.has(join_node->getJoinExpression().get()))
        {
            /// Disable constant folding for ASOF JOIN ON expressions.
            /// In ASOF JOIN, comparison functions like >= or <= are not evaluated normally.
            /// They instead indicate which columns should be used for finding the closest matching rows.
            /// Even though whole expression is constant, code handling ASOF JOIN may expect presence of comparison function,
            /// and consider query as malformed if we replace it to constant.
            allow_constant_folding = false;
        }

        /** If function is suitable for constant folding try to convert it to constant.
          * Example: SELECT plus(1, 1);
          * Result: SELECT 2;
          */
        if (allow_constant_folding && function_base->isSuitableForConstantFolding())
        {
            auto result_type = function_base->getResultType();
            auto executable_function = function_base->prepare(argument_columns);

            ColumnPtr column;

            if (all_arguments_constants)
            {
                size_t num_rows = 1;
                if (!argument_columns.empty())
                    num_rows = argument_columns.front().column->size();
                column = executable_function->execute(argument_columns, result_type, num_rows, true);
            }
            else
            {
                column = function_base->getConstantResultForNonConstArguments(argument_columns, result_type);
            }

            if (column && column->getDataType() != result_type->getColumnType())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected return type from {}. Expected {}. Got {}",
                    function->getName(),
                    result_type->getColumnType(),
                    column->getDataType());

            /** Do not perform constant folding if there are aggregate or arrayJoin functions inside function.
              * Example: SELECT toTypeName(sum(number)) FROM numbers(10);
              */
            if (column && isColumnConst(*column) && !typeid_cast<const ColumnConst *>(column.get())->getDataColumn().isDummy() &&
                !hasAggregateFunctionNodes(node) && !hasFunctionNode(node, "arrayJoin") &&
                /// Sanity check: do not convert large columns to constants
                column->byteSize() < 1_MiB)
            {
                /// Replace function node with result constant node
                constant_node = std::make_shared<ConstantNode>(ConstantValue{ std::move(column), std::move(result_type) }, node);
            }
        }

        function_node.resolveAsFunction(std::move(function_base));
    }
    catch (Exception & e)
    {
        e.addMessage("In scope {}", scope.scope_node->formatASTForErrorMessage());
        throw;
    }

    if (constant_node)
        node = std::move(constant_node);

    return result_projection_names;
}
}

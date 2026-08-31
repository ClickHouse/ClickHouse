#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <DataTypes/DataTypeString.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>

#include <Analyzer/ColumnNode.h>
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

#include <Access/EnabledRowPolicies.h>

#include <Common/FieldVisitorConvertToNumber.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>

#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/hasNullable.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/exists.h>
#include <Columns/validateColumnType.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/misc.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionFactory.h>
#include <Functions/grouping.h>
#include <Storages/StorageJoin.h>

#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>

#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>


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
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsBool enable_function_early_short_circuit;
    extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;
    extern const SettingsBool execute_exists_as_scalar_subquery;
    extern const SettingsBool format_display_secrets_in_show_and_select;
    extern const SettingsBool transform_null_in;
    extern const SettingsBool force_grouping_standard_compatibility;
    extern const SettingsBool validate_enum_literals_in_operators;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsOverflowMode set_overflow_mode;
    extern const SettingsBool allow_experimental_correlated_subqueries;
    extern const SettingsBool rewrite_in_to_join;
    extern const SettingsMap additional_table_filters;
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

/** Finds a decisive constant in the direct prefix of an AND/OR expression before its
  * arguments are analyzed. Nested calls are resolved independently so scoped lambdas and UDFs
  * cannot be mistaken for builtin logical functions. False decides AND, true decides OR.
  */
std::optional<bool> getEarlyShortCircuitResultForAndOr(
    const QueryTreeNodePtr & node,
    const String & function_name_to_fold)
{
    const auto * function_node = node->as<FunctionNode>();
    if (!function_node
        || function_node->getFunctionName() != function_name_to_fold
        || !function_node->getParameters().getNodes().empty()
        || function_node->getNullsAction() != NullsAction::EMPTY
        || function_node->isWindowFunction())
        return {};

    const bool decisive_value = function_name_to_fold == "or";
    for (const auto & argument : function_node->getArguments().getNodes())
    {
        std::optional<bool> argument_value;
        if (const auto * constant_node = argument->as<ConstantNode>())
        {
            const auto & type = constant_node->getResultType();
            const auto value = constant_node->getValue();
            if (isNativeNumber(removeNullable(type)) && !value.isNull())
                argument_value = applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
        }

        /// Short-circuiting is prefix-based. An unresolved argument before the decisive constant
        /// is live and must be analyzed/executed, so this optimization cannot cross it.
        if (!argument_value)
            return {};

        if (*argument_value == decisive_value)
            return decisive_value;
    }

    return {};
}

bool hasNestedQueryOrUnion(const IQueryTreeNode & node)
{
    for (const auto & child : node.getChildren())
    {
        if (!child)
            continue;

        const auto child_type = child->getNodeType();
        if (child_type == QueryTreeNodeType::QUERY || child_type == QueryTreeNodeType::UNION)
            return true;

        if (hasNestedQueryOrUnion(*child))
            return true;
    }

    return false;
}

bool isFunctionAliasInScope(const String & name, const IdentifierResolveScope & scope)
{
    for (const auto * current_scope = &scope; current_scope; current_scope = current_scope->parent_scope)
    {
        if (current_scope->aliases.alias_name_to_lambda_node.contains(name)
            || current_scope->global_with_aliases.alias_name_to_lambda_node.contains(name))
            return true;
    }

    return false;
}

bool isSafeCountScalarSubqueryForEarlyShortCircuit(
    const QueryNode & query,
    const IdentifierResolveScope & scope);

bool hasUnsafeFunctionForEarlyShortCircuit(
    const QueryTreeNodePtr & node,
    const ContextPtr & context,
    const IdentifierResolveScope & scope,
    bool inside_safe_count_scalar_subquery = false)
{
    if (const auto * query = node->as<QueryNode>())
        inside_safe_count_scalar_subquery = isSafeCountScalarSubqueryForEarlyShortCircuit(*query, scope);

    if (const auto * function = node->as<FunctionNode>())
    {
        if (isFunctionAliasInScope(function->getFunctionName(), scope))
            return true;

        /// throwIf is eligible for runtime lazy execution itself, but when it is inside a
        /// non-lazy comparison the comparison evaluates it eagerly. Never erase it speculatively.
        if (function->getFunctionName() == "throwIf")
            return true;

        auto resolver = FunctionFactory::instance().tryGet(function->getFunctionName(), context);
        if (!resolver)
        {
            /// An aggregate count is only safe inside a scalar subquery that has already passed
            /// the strict count-subquery preflight. Any other unknown name may be a
            /// SQL/executable UDF whose body is not visible here.
            if (function->getFunctionName() != "count" || !inside_safe_count_scalar_subquery)
                return true;
        }
        else if (!resolver->isDeterministic() || !resolver->isDeterministicInScopeOfQuery())
            return true;
    }

    for (const auto & child : node->getChildren())
        if (child && hasUnsafeFunctionForEarlyShortCircuit(child, context, scope, inside_safe_count_scalar_subquery))
            return true;

    return false;
}

bool isEarlyShortCircuitScalarPlaceholder(const QueryTreeNodePtr & node)
{
    const auto * column = node->as<ColumnNode>();
    return column && column->getColumnName().starts_with("_subquery_");
}

bool isComparisonOfEarlyShortCircuitScalar(const FunctionNode & function)
{
    const auto & name = function.getFunctionName();
    const bool is_comparison = name == "equals" || name == "notEquals"
        || name == "less" || name == "greater"
        || name == "lessOrEquals" || name == "greaterOrEquals";
    if (!is_comparison)
        return false;

    const auto & arguments = function.getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    const bool first_is_scalar = isEarlyShortCircuitScalarPlaceholder(arguments[0]);
    const bool second_is_scalar = isEarlyShortCircuitScalarPlaceholder(arguments[1]);
    if (first_is_scalar == second_is_scalar)
        return false;

    const auto & other_argument = arguments[first_is_scalar ? 1 : 0];
    const auto * other_constant = other_argument->as<ConstantNode>();
    return other_constant && other_constant->isDeterministic() && !other_constant->hasSourceExpression();
}

bool hasUnsafeEarlyShortCircuitScalarUsage(const QueryTreeNodePtr & node, bool placeholder_is_allowed = false)
{
    if (isEarlyShortCircuitScalarPlaceholder(node))
        return !placeholder_is_allowed;

    if (const auto * constant = node->as<ConstantNode>(); constant && constant->hasSourceExpression())
        return hasUnsafeEarlyShortCircuitScalarUsage(constant->getSourceExpression());

    const auto * function = node->as<FunctionNode>();
    const bool is_safe_comparison = function && isComparisonOfEarlyShortCircuitScalar(*function);
    for (const auto & child : node->getChildren())
        if (child && hasUnsafeEarlyShortCircuitScalarUsage(child, is_safe_comparison))
            return true;

    return false;
}

bool hasFunctionNotSuitableForEarlyShortCircuit(const QueryTreeNodePtr & node, bool is_root = true)
{
    if (const auto * constant = node->as<ConstantNode>(); constant && constant->hasSourceExpression())
        return hasFunctionNotSuitableForEarlyShortCircuit(constant->getSourceExpression(), is_root);

    if (const auto * function = node->as<FunctionNode>())
    {
        /// The root is the logical function being folded. It is short-circuit by definition,
        /// although it deliberately reports false for lazy execution of itself.
        const auto & function_name = function->getFunctionName();
        const bool is_nested_logical = function_name == "and" || function_name == "or";
        if (!is_root && !is_nested_logical)
        {
            if (auto function_base = function->getFunction())
            {
                DataTypesWithConstInfo arguments;
                const auto & argument_nodes = function->getArguments().getNodes();
                arguments.reserve(argument_nodes.size());

                for (const auto & argument : argument_nodes)
                    arguments.push_back({argument->getResultType(), argument->as<ConstantNode>() != nullptr});

                if (!function_base->isSuitableForShortCircuitArgumentsExecution(arguments)
                    && !isComparisonOfEarlyShortCircuitScalar(*function))
                    return true;
            }
        }
    }

    for (const auto & child : node->getChildren())
        if (child && hasFunctionNotSuitableForEarlyShortCircuit(child, false))
            return true;

    return false;
}

void copySecretMasksByPosition(
    QueryTreeNodePtr resolved_node,
    const QueryTreeNodePtr & source_node,
    std::map<IQueryTreeNode::Hash, size_t> & projection_mask_map)
{
    const auto mask_source_subtree = [&projection_mask_map](const auto & self, const QueryTreeNodePtr & node) -> void
    {
        if (auto * constant = node->as<ConstantNode>())
        {
            const auto hash = constant->getTreeHash();
            const auto mask = projection_mask_map.insert({hash, projection_mask_map.size() + 1}).first->second;
            constant->setMaskId(mask);
        }

        for (const auto & child : node->getChildren())
            if (child)
                self(self, child);
    };

    while (resolved_node->getNodeType() != source_node->getNodeType())
    {
        const auto * resolved_constant = resolved_node->as<ConstantNode>();
        if (!resolved_constant || !resolved_constant->hasSourceExpression())
            return;

        if (resolved_constant->isMasked())
        {
            /// A folded masked constant can keep a non-constant source expression. Once it is
            /// unwrapped to align the node types, all constants in that source expression must
            /// stay hidden as well.
            mask_source_subtree(mask_source_subtree, source_node);
            return;
        }

        resolved_node = resolved_constant->getSourceExpression();
    }

    if (const auto * resolved_constant = resolved_node->as<ConstantNode>(); resolved_constant && resolved_constant->isMasked())
    {
        if (auto * source_constant = source_node->as<ConstantNode>())
        {
            const auto hash = source_constant->getTreeHash();
            const auto mask = projection_mask_map.insert({hash, projection_mask_map.size() + 1}).first->second;
            source_constant->setMaskId(mask);
        }
    }

    const auto & resolved_children = resolved_node->getChildren();
    const auto & source_children = source_node->getChildren();
    if (resolved_children.size() != source_children.size())
        return;

    for (size_t i = 0; i < resolved_children.size(); ++i)
        if (resolved_children[i] && source_children[i])
            copySecretMasksByPosition(resolved_children[i], source_children[i], projection_mask_map);
}

bool isTableIdentifierShadowedInScope(const IdentifierNode & identifier_node, const IdentifierResolveScope & scope)
{
    const auto & identifier = identifier_node.getIdentifier();
    const auto & full_name = identifier.getFullName();
    const auto & first_name = identifier.front();

    for (const auto * current_scope = &scope; current_scope; current_scope = current_scope->parent_scope)
    {
        if (current_scope->cte_name_to_query_node.contains(full_name)
            || current_scope->cte_name_to_query_node.contains(first_name)
            || current_scope->aliases.alias_name_to_table_expression_node.contains(full_name)
            || current_scope->aliases.alias_name_to_table_expression_node.contains(first_name)
            || current_scope->global_with_aliases.alias_name_to_table_expression_node.contains(full_name)
            || current_scope->global_with_aliases.alias_name_to_table_expression_node.contains(first_name))
            return true;
    }

    return false;
}

bool isUnsafeCountScalarSource(const QueryTreeNodePtr & join_tree, const IdentifierResolveScope & scope)
{
    QueryTreeNodePtr resolved_table = join_tree;
    if (const auto * identifier = join_tree->as<IdentifierNode>())
    {
        auto resolve_result = IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(
            identifier->getIdentifier(), scope.context);
        resolved_table = std::move(resolve_result.resolved_identifier);
    }

    const auto * table = resolved_table ? resolved_table->as<TableNode>() : nullptr;
    if (!table || !table->getStorage())
        return true;

    const auto & storage = table->getStorage();
    /// The speculative pass cannot inspect fan-out or forwarding storage children. In particular,
    /// Merge applies each matching child's view and row-policy behavior later, while Alias does
    /// not forward isView() and evaluates policies on its target. Remote storages can likewise
    /// apply shard-local policies and filters not visible in initiator-side metadata.
    ///
    /// Keep the opt-in fast path limited to local physical tables: Memory and MergeTree-family
    /// engines. All other storages fail closed rather than requiring per-engine semantic proofs.
    const bool is_local_physical_table = storage->getName() == "Memory" || storage->isMergeTree();
    return !is_local_physical_table || storage->isView() || storage->isRemote() || storage->getName() == "Alias";
}

bool hasLateAttachedTableFilter(
    const QueryTreeNodePtr & join_tree,
    const ContextPtr & query_context,
    const IdentifierResolveScope & scope)
{
    /// Additional filters are parsed only by the planner, after the speculative type-only
    /// analysis. They can be configured by either the outer query or the scalar subquery.
    /// Any configured filter may affect the selected table, so fail closed.
    if (!scope.context->getSettingsRef()[Setting::additional_table_filters].value.empty()
        || !query_context->getSettingsRef()[Setting::additional_table_filters].value.empty())
        return true;

    QueryTreeNodePtr resolved_table = join_tree;
    if (const auto * identifier = join_tree->as<IdentifierNode>())
    {
        auto resolve_result = IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(
            identifier->getIdentifier(), scope.context);
        resolved_table = std::move(resolve_result.resolved_identifier);
    }

    const auto * table = resolved_table ? resolved_table->as<TableNode>() : nullptr;
    if (!table || !table->getStorage())
        return true;

    const auto & storage_id = table->getStorage()->getStorageID();
    if (!storage_id.hasDatabase())
        return true;

    const auto has_nontrivial_row_policy = [&](const ContextPtr & context)
    {
        const auto row_policy_filter = context->getRowPolicyFilter(
            storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        return row_policy_filter && !row_policy_filter->isAlwaysTrue();
    };

    /// A scalar query can have its own context. Check both contexts even though they normally
    /// share access rights, because an inherited setting/profile can change the effective policy.
    return has_nontrivial_row_policy(scope.context)
        || (query_context != scope.context && has_nontrivial_row_policy(query_context));
}

bool isSafeCountScalarSubqueryForEarlyShortCircuit(
    const QueryNode & query,
    const IdentifierResolveScope & scope)
{
    const auto & join_tree = query.getJoinTreeNode();
    if (!join_tree
        || (join_tree->getNodeType() != QueryTreeNodeType::TABLE
            && join_tree->getNodeType() != QueryTreeNodeType::IDENTIFIER)
        || (join_tree->getNodeType() == QueryTreeNodeType::IDENTIFIER
            && isTableIdentifierShadowedInScope(join_tree->as<IdentifierNode &>(), scope))
        || isUnsafeCountScalarSource(join_tree, scope)
        || hasLateAttachedTableFilter(join_tree, query.getContext(), scope)
        || hasNestedQueryOrUnion(query)
        || query.hasWith()
        || query.hasPrewhere()
        || query.hasWhere()
        || query.hasGroupBy()
        || query.hasHaving()
        || query.hasWindow()
        || query.hasQualify()
        || query.hasOrderBy()
        || query.hasLimitBy()
        || query.hasLimit()
        || query.hasOffset())
        return false;

    const auto & projection = query.getProjection().getNodes();
    if (projection.size() != 1)
        return false;

    const auto * function = projection.front()->as<FunctionNode>();
    if (!function
        || function->getFunctionName() != "count"
        || !function->getParameters().getNodes().empty())
        return false;

    const auto & arguments = function->getArguments().getNodes();
    if (arguments.empty())
        return true;

    if (arguments.size() != 1)
        return false;

    const auto * matcher = arguments.front()->as<MatcherNode>();
    return matcher && matcher->isUnqualified();
}

bool containsQueryOrUnion(const QueryTreeNodePtr & node)
{
    const auto type = node->getNodeType();
    if (type == QueryTreeNodeType::QUERY || type == QueryTreeNodeType::UNION)
        return true;

    for (const auto & child : node->getChildren())
        if (child && containsQueryOrUnion(child))
            return true;

    return false;
}

bool containsCountFunction(const QueryTreeNodePtr & node)
{
    if (const auto * function = node->as<FunctionNode>(); function && function->getFunctionName() == "count")
        return true;

    if (const auto * constant = node->as<ConstantNode>(); constant && constant->hasSourceExpression())
        if (containsCountFunction(constant->getSourceExpression()))
            return true;

    for (const auto & child : node->getChildren())
        if (child && containsCountFunction(child))
            return true;

    return false;
}

bool comparisonWithScalarHasNonLiteralOtherSide(const FunctionNode & function)
{
    const auto & name = function.getFunctionName();
    const bool is_comparison = name == "equals" || name == "notEquals"
        || name == "less" || name == "greater"
        || name == "lessOrEquals" || name == "greaterOrEquals";
    if (!is_comparison)
        return false;

    const auto & arguments = function.getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    const bool first_is_scalar = containsQueryOrUnion(arguments[0]) || containsCountFunction(arguments[0]);
    const bool second_is_scalar = containsQueryOrUnion(arguments[1]) || containsCountFunction(arguments[1]);
    if (first_is_scalar == second_is_scalar)
        return false;

    const auto * other_constant = arguments[first_is_scalar ? 1 : 0]->as<ConstantNode>();
    return !other_constant || !other_constant->isDeterministic() || other_constant->hasSourceExpression();
}

bool isStrictSafeLogicalTree(
    const QueryTreeNodePtr & node,
    const IdentifierResolveScope & scope)
{
    if (const auto * constant = node->as<ConstantNode>())
        return constant->isDeterministic() && !constant->hasSourceExpression()
            && isNativeNumber(removeNullable(constant->getResultType()));

    const auto * function = node->as<FunctionNode>();
    if (!function)
        return false;

    const auto & name = function->getFunctionName();
    if (name == "and" || name == "or")
    {
        for (const auto & argument : function->getArguments().getNodes())
            if (!isStrictSafeLogicalTree(argument, scope))
                return false;
        return true;
    }

    const bool is_comparison = name == "equals" || name == "notEquals"
        || name == "less" || name == "greater"
        || name == "lessOrEquals" || name == "greaterOrEquals";
    if (!is_comparison)
        return false;

    const auto & arguments = function->getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    const auto * first_query = arguments[0]->as<QueryNode>();
    const auto * second_query = arguments[1]->as<QueryNode>();
    if ((first_query != nullptr) == (second_query != nullptr))
        return false;

    const auto * scalar_query = first_query ? first_query : second_query;
    const auto & other_argument = arguments[first_query ? 1 : 0];
    const auto * other_constant = other_argument->as<ConstantNode>();
    return isSafeCountScalarSubqueryForEarlyShortCircuit(*scalar_query, scope)
        && other_constant && other_constant->isDeterministic() && !other_constant->hasSourceExpression();
}

bool hasScopeDependentNodesForEarlyShortCircuit(
    const QueryTreeNodePtr & node,
    const IdentifierResolveScope & scope)
{
    const auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY)
        return !isSafeCountScalarSubqueryForEarlyShortCircuit(node->as<QueryNode &>(), scope);
    if (node_type == QueryTreeNodeType::UNION)
        return true;

    if (node_type != QueryTreeNodeType::FUNCTION
        && node_type != QueryTreeNodeType::CONSTANT
        && node_type != QueryTreeNodeType::LIST)
        return true;

    if (const auto * function = node->as<FunctionNode>();
        function && comparisonWithScalarHasNonLiteralOtherSide(*function))
        return true;

    for (const auto & child : node->getChildren())
        if (child && hasScopeDependentNodesForEarlyShortCircuit(child, scope))
            return true;

    return false;
}
}

/// Checks if node is a NULL constant
static bool isNullConstant(const QueryTreeNodePtr & node)
{
    if (const auto * const_node = node->as<ConstantNode>())
        return const_node->getValue().isNull();
    return false;
}

/// Use the supertype of the LHS and all tuple elements, to support cases like
/// `toUInt8(232) IN (1000, number)`. A NULL literal has type `Nullable(Nothing)`
/// and should not narrow the array element type.
static DataTypePtr getLeastSupertypeForInArrayElements(
    const QueryTreeNodes & array_elements,
    const QueryTreeNodePtr & in_first_argument,
    bool left_is_null)
{
    DataTypes arg_types;
    arg_types.reserve(array_elements.size() + 1);

    if (!left_is_null)
        arg_types.push_back(in_first_argument->getResultType());

    for (const auto & arg : array_elements)
        arg_types.push_back(arg->getResultType());

    return tryGetLeastSupertype(arg_types);
}

template <typename CastNodeToType>
static std::shared_ptr<ListNode> makeInArrayArgumentsList(
    const QueryTreeNodes & array_elements,
    DataTypePtr common_type,
    bool rhs_has_null,
    bool compare_nulls,
    IdentifierResolveScope & scope,
    CastNodeToType && cast_node_to_type)
{
    auto array_arguments_list = std::make_shared<ListNode>();
    if (!common_type)
    {
        for (const auto & arg : array_elements)
            array_arguments_list->getNodes().push_back(arg);
        return array_arguments_list;
    }

    /// `has` compares the array elements against the left-hand side value, so the element type has
    /// to be able to hold `NULL` when the right-hand side can contain `NULL` values or when `NULL`
    /// values must not match. Whether `NULL` values match is a property of the resolved function
    /// (`nullIn` compares `NULL`s, `in` does not), not of the `transform_null_in` setting, which
    /// only renames `in` to `nullIn` before this rewrite. Types that cannot be inside `Nullable`,
    /// such as `Array(...)` or `Map(...)`, are left as they are - the `Nullable` wrapper would be
    /// rejected when the column is created. `Tuple(...)` is excluded explicitly, because it reports
    /// that it can be inside `Nullable` while a `Nullable(Tuple(...))` column cannot be created by
    /// default.
    if ((rhs_has_null || !compare_nulls)
        && !isTuple(common_type))
        common_type = makeNullableOrLowCardinalityNullableSafe(common_type);

    for (const auto & arg : array_elements)
        array_arguments_list->getNodes().push_back(cast_node_to_type(arg, common_type, scope));

    return array_arguments_list;
}

/// Creates a NOT function node wrapping the given node (caller must resolve it)
static QueryTreeNodePtr createNotWrapper(QueryTreeNodePtr node)
{
    auto not_fn = std::make_shared<FunctionNode>("not");
    not_fn->getArguments().getNodes().push_back(node);
    return not_fn;
}

static bool isNegativeInFunctionName(std::string_view function_name)
{
    return function_name == "notIn" || function_name == "globalNotIn" || function_name == "notNullIn" || function_name == "globalNotNullIn";
}

static bool inFunctionComparesNulls(std::string_view function_name)
{
    return function_name == "nullIn" || function_name == "globalNullIn" || function_name == "notNullIn"
        || function_name == "globalNotNullIn";
}

static QueryTreeNodePtr makeTupleHasNoNullElementsPredicate(const QueryTreeNodePtr & tuple_value, size_t tuple_size)
{
    QueryTreeNodePtr result;
    for (size_t i = 0; i != tuple_size; ++i)
    {
        auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
        tuple_element_function->getArguments().getNodes().push_back(tuple_value->clone());
        tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(i + 1)));

        auto is_null_function = std::make_shared<FunctionNode>("isNull");
        is_null_function->getArguments().getNodes().push_back(tuple_element_function);

        auto element_is_not_null = std::make_shared<FunctionNode>("not");
        element_is_not_null->getArguments().getNodes().push_back(is_null_function);

        if (result)
        {
            auto and_function = std::make_shared<FunctionNode>("and");
            and_function->getArguments().getNodes() = {std::move(result), std::move(element_is_not_null)};
            result = std::move(and_function);
        }
        else
        {
            result = std::move(element_is_not_null);
        }
    }

    return result;
}

/// A subquery on the right of IN whose single result column is an Array exactly one dimension
/// deeper than the left argument is the set of the elements of those arrays, exactly like an
/// array literal or an array-returning function on the right of IN (e.g.
/// `x IN (SELECT groupArray(x) ...)` means `x` is tested against the set `{groupArray(x)...}`).
/// If the second argument of IN is such a subquery, wrap its column with arrayJoin so its elements
/// become the set elements: `SELECT arrayJoin(<column>) FROM (<subquery>)`; otherwise leave it
/// unchanged. Without this the whole array is treated as a single opaque set value and the left
/// argument is coerced into the array type, giving a confusing "Array does not start with '['
/// character" or type-mismatch error.
/// `in_second_argument` must be a resolved QueryNode/UnionNode and `in_first_argument` resolved.
/// Shared by both the regular IN handling and the `rewrite_in_to_join` EXISTS rewrite.
static void flattenArraySubqueryOnRightOfIn(
    QueryTreeNodePtr & in_second_argument,
    const QueryTreeNodePtr & in_first_argument,
    const ContextPtr & context)
{
    if (!(in_second_argument->as<QueryNode>() || in_second_argument->as<UnionNode>())
        || in_first_argument->getNodeType() == QueryTreeNodeType::LAMBDA)
        return;

    const auto * subquery_query_node = in_second_argument->as<QueryNode>();
    NamesAndTypes subquery_projection_columns = subquery_query_node
        ? subquery_query_node->getProjectionColumns()
        : in_second_argument->as<UnionNode>()->computeProjectionColumns();

    const auto * rhs_array_type = subquery_projection_columns.size() == 1
        ? typeid_cast<const DataTypeArray *>(subquery_projection_columns.front().type.get())
        : nullptr;
    if (!rhs_array_type)
        return;

    const auto & lhs_type = in_first_argument->getResultType();
    const auto * lhs_array_type = typeid_cast<const DataTypeArray *>(lhs_type.get());
    const size_t lhs_depth = lhs_array_type ? lhs_array_type->getNumberOfDimensions() : 0;

    if (rhs_array_type->getNumberOfDimensions() != lhs_depth + 1)
        return;

    /// Wrap the subquery in `SELECT arrayJoin(<column>) FROM (<subquery>)`.
    auto array_column_node = std::make_shared<ColumnNode>(subquery_projection_columns.front(), static_pointer_cast<ITableExpressionNode>(in_second_argument));

    auto array_join_function = std::make_shared<FunctionNode>("arrayJoin");
    array_join_function->getArguments().getNodes().push_back(std::move(array_column_node));
    resolveOrdinaryFunctionNodeByName(*array_join_function, "arrayJoin", context);

    auto element_type = array_join_function->getResultType();
    auto element_name = "arrayJoin(" + subquery_projection_columns.front().name + ")";

    auto projection_list = std::make_shared<ListNode>();
    projection_list->getNodes().push_back(std::move(array_join_function));

    auto flattened_subquery = std::make_shared<QueryNode>(Context::createCopy(context));
    flattened_subquery->setIsSubquery(true);
    flattened_subquery->getProjectionNode() = std::move(projection_list);
    flattened_subquery->getJoinTreeNode() = std::move(in_second_argument);
    flattened_subquery->resolveProjectionColumns(NamesAndTypes{{element_name, element_type}});

    in_second_argument = std::move(flattened_subquery);
}

/// Same as `flattenArraySubqueryOnRightOfIn`, but for a table expression on the right of IN whose
/// columns are described by a storage snapshot instead of a subquery's projection (a `TableNode`,
/// e.g. `x IN some_array_table`). If the table has a single Array column exactly one dimension
/// deeper than the left argument, wrap it as a `SELECT column FROM table` subquery and flatten it,
/// so its elements become the set elements — consistent with an array subquery, an array literal, or an
/// array-returning function. Tables with a non-Array column, a wrong depth, or more than one column
/// are left untouched, so the common `x IN table` path (and any `StorageSet` fast path) is unchanged.
static void flattenArrayTableExpressionOnRightOfIn(
    QueryTreeNodePtr & in_second_argument,
    const QueryTreeNodePtr & in_first_argument,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    if (in_first_argument->getNodeType() == QueryTreeNodeType::LAMBDA)
        return;

    auto columns = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary));
    if (columns.size() != 1)
        return;

    const auto & column = columns.front();
    const auto * rhs_array_type = typeid_cast<const DataTypeArray *>(column.type.get());
    if (!rhs_array_type)
        return;

    const auto & lhs_type = in_first_argument->getResultType();
    const auto * lhs_array_type = typeid_cast<const DataTypeArray *>(lhs_type.get());
    const size_t lhs_depth = lhs_array_type ? lhs_array_type->getNumberOfDimensions() : 0;

    if (rhs_array_type->getNumberOfDimensions() != lhs_depth + 1)
        return;

    /// Wrap the table as a `SELECT column FROM table` subquery, then flatten it like an array subquery.
    auto column_node = std::make_shared<ColumnNode>(column, static_pointer_cast<ITableExpressionNode>(in_second_argument));

    auto projection_list = std::make_shared<ListNode>();
    projection_list->getNodes().push_back(std::move(column_node));

    auto subquery = std::make_shared<QueryNode>(Context::createCopy(context));
    subquery->setIsSubquery(true);
    subquery->getProjectionNode() = std::move(projection_list);
    subquery->getJoinTreeNode() = in_second_argument;
    subquery->resolveProjectionColumns(NamesAndTypes{column});

    in_second_argument = std::move(subquery);

    flattenArraySubqueryOnRightOfIn(in_second_argument, in_first_argument, context);
}

/// Builds and resolves `IF(isNull(element), NULL, has(array, element))`
QueryTreeNodePtr QueryAnalyzer::makeNullSafeHas(
    QueryTreeNodePtr array_arg,    // [1,2,number]
    QueryTreeNodePtr element_arg,  // x (e.g. NULL)
    IdentifierResolveScope & scope)
{
    auto is_null_fn = std::make_shared<FunctionNode>("isNull");
    is_null_fn->getArguments().getNodes().push_back(element_arg);

    auto has_fn = std::make_shared<FunctionNode>("has");
    has_fn->getArguments().getNodes().push_back(array_arg);
    has_fn->getArguments().getNodes().push_back(element_arg);

    QueryTreeNodePtr in_result = has_fn;
    /// `has` treats tuple values with equal `NULL` elements as a match, while `IN`
    /// with `transform_null_in = 0` skips such tuple values. Guard tuple LHS
    /// elements to preserve `IN` semantics in the row-wise rewrite.
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(element_arg->getResultType()).get());
        tuple_type && !tuple_type->getElements().empty())
    {
        auto and_fn = std::make_shared<FunctionNode>("and");
        and_fn->getArguments().getNodes() =
        {
            makeTupleHasNoNullElementsPredicate(element_arg, tuple_type->getElements().size()),
            std::move(in_result),
        };
        in_result = std::move(and_fn);
    }

    auto null_const = std::make_shared<ConstantNode>(
        Field{},
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()));

    auto raw_if = std::make_shared<FunctionNode>("if");
    raw_if->getArguments().getNodes() = {is_null_fn, null_const, in_result};

    QueryTreeNodePtr if_node = raw_if;
    resolveFunction(if_node, scope);

    return if_node;
}

/// Builds has() expression with proper null handling and NOT wrapping for IN rewrites
ProjectionNames QueryAnalyzer::buildHasExpression(
    QueryTreeNodePtr & node,
    QueryTreeNodePtr array_arg,
    QueryTreeNodePtr element_arg,
    bool is_not_in,
    bool compare_nulls,
    const ProjectionNames & arguments_projection_names,
    const ProjectionNames & parameters_projection_names,
    IdentifierResolveScope & scope)
{
    auto proj = calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names);

    if (!compare_nulls)
    {
        QueryTreeNodePtr result_node = makeNullSafeHas(array_arg, element_arg, scope);
        if (is_not_in)
        {
            result_node = createNotWrapper(result_node);
            resolveFunction(result_node, scope);
        }
        node = result_node;
        return ProjectionNames{proj};
    }

    auto has_fn = std::make_shared<FunctionNode>("has");
    has_fn->getArguments().getNodes() = {array_arg, element_arg};
    QueryTreeNodePtr result_node = has_fn;
    resolveFunction(result_node, scope);

    if (is_not_in)
    {
        result_node = createNotWrapper(result_node);
        resolveFunction(result_node, scope);
    }
    node = result_node;
    return ProjectionNames{proj};
}

QueryTreeNodes QueryAnalyzer::getArrayElementsForInTupleArguments(
    const QueryTreeNodes & tuple_args,
    const QueryTreeNodePtr & in_first_argument,
    IdentifierResolveScope & scope,
    bool expand_single_tuple_value)
{
    if (!expand_single_tuple_value
        || tuple_args.size() != 1
        || isTuple(removeNullable(in_first_argument->getResultType()))
        || !isTuple(removeNullable(tuple_args[0]->getResultType())))
        return tuple_args;

    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(tuple_args[0]->getResultType()).get());
    QueryTreeNodes array_elements;
    array_elements.reserve(tuple_type->getElements().size());

    for (size_t i = 0; i != tuple_type->getElements().size(); ++i)
    {
        auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
        tuple_element_function->getArguments().getNodes().push_back(tuple_args[0]);
        tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(i + 1)));

        QueryTreeNodePtr tuple_element = tuple_element_function;
        resolveFunction(tuple_element, scope);
        array_elements.push_back(std::move(tuple_element));
    }

    return array_elements;
}

/// Builds the row-wise comparison for a one-element IN set: the compare-nulls functions
/// (`nullIn`, `notNullIn`) map to isDistinctFrom/isNotDistinctFrom, the others map to
/// ifNull(equals/notEquals(...), default), with a NULL result for a NULL LHS value.
/// The caller must resolve the returned node.
static QueryTreeNodePtr buildScalarInComparison(
    const QueryTreeNodePtr & left_argument,
    const QueryTreeNodePtr & right_argument,
    bool is_not_in,
    bool compare_nulls)
{
    if (compare_nulls)
    {
        auto comparison_fn = std::make_shared<FunctionNode>(is_not_in ? "isDistinctFrom" : "isNotDistinctFrom");
        comparison_fn->getArguments().getNodes() = {left_argument, right_argument};
        return comparison_fn;
    }

    auto eq_fn = std::make_shared<FunctionNode>(is_not_in ? "notEquals" : "equals");
    eq_fn->getArguments().getNodes() = {left_argument, right_argument};

    auto default_val = std::make_shared<ConstantNode>(is_not_in ? Field{1u} : Field{0u});
    auto ifnull_fn = std::make_shared<FunctionNode>("ifNull");
    ifnull_fn->getArguments().getNodes() = {eq_fn, default_val};

    if (!isNullableOrLowCardinalityNullable(left_argument->getResultType()))
        return ifnull_fn;

    auto is_null_fn = std::make_shared<FunctionNode>("isNull");
    is_null_fn->getArguments().getNodes().push_back(left_argument);

    auto null_const = std::make_shared<ConstantNode>(
        Field{},
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()));

    auto raw_if = std::make_shared<FunctionNode>("if");
    raw_if->getArguments().getNodes() = {is_null_fn, null_const, ifnull_fn};
    return raw_if;
}

/// converts tuple to array with proper type handling
QueryTreeNodePtr QueryAnalyzer::convertTupleToArray(
    const QueryTreeNodes & tuple_args,
    const QueryTreeNodePtr & in_first_argument,
    IdentifierResolveScope & scope,
    bool expand_single_tuple_value,
    bool compare_nulls)
{
    QueryTreeNodes array_elements = getArrayElementsForInTupleArguments(tuple_args, in_first_argument, scope, expand_single_tuple_value);

    bool left_is_null = isNullConstant(in_first_argument);

    bool rhs_has_null = std::any_of(array_elements.begin(), array_elements.end(),
        [](const auto & arg)
        { return isNullConstant(arg) || isNullableOrLowCardinalityNullable(arg->getResultType()); });

    DataTypePtr common_type = getLeastSupertypeForInArrayElements(array_elements, in_first_argument, left_is_null);
    /// If no supertype exists, keep the old behaviour for non-NULL left-hand side
    /// values and let per-element `CAST` handle or reject the mismatch. For NULL
    /// left-hand side values, let `array` infer the right-hand side type on its own.
    if (!common_type && !left_is_null)
        common_type = in_first_argument->getResultType();

    auto array_arguments_list = makeInArrayArgumentsList(array_elements, common_type, rhs_has_null, compare_nulls, scope,
        [this](const QueryTreeNodePtr & node, const DataTypePtr & target_type, IdentifierResolveScope & function_scope)
        {
            return castNodeToType(node, target_type, function_scope);
        });
    auto array_function_node = std::make_shared<FunctionNode>("array");
    array_function_node->getArgumentsNode() = array_arguments_list;
    QueryTreeNodePtr array_node = array_function_node;
    resolveExpressionNode(array_node, scope, false /*allow_lambda_expression*/, true /*allow_table_expression*/);

    return array_node;
}

/// casts node to target type with appropriate method (toString for strings, CAST for others)
QueryTreeNodePtr QueryAnalyzer::castNodeToType(
    const QueryTreeNodePtr & node,
    const DataTypePtr & target_type,
    IdentifierResolveScope & scope)
{
    if (node->getResultType()->equals(*target_type))
        return node;

    auto cast_node = std::make_shared<FunctionNode>("CAST");
    auto cast_args = std::make_shared<ListNode>();
    cast_args->getNodes().push_back(node);
    cast_args->getNodes().push_back(
        std::make_shared<ConstantNode>(target_type->getName(), std::make_shared<DataTypeString>()));
    cast_node->getArgumentsNode() = cast_args;

    QueryTreeNodePtr result = cast_node;
    resolveFunction(result, scope);
    return result;
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
ProjectionNames QueryAnalyzer::resolveFunction(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_niladic_functions)
{
    FunctionNodePtr function_node_ptr = std::static_pointer_cast<FunctionNode>(node);
    auto function_name = function_node_ptr->getFunctionName();

    /// Resolve function parameters

    auto parameters_projection_names = resolveExpressionNodeList(
        function_node_ptr->getParametersNode(),
        scope,
        false /*allow_lambda_expression*/,
        false /*allow_table_expression*/,
        allow_niladic_functions);

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
        auto function_lookup_result = tryResolveIdentifier({Identifier{function_name}, IdentifierLookupContext::FUNCTION}, scope, { .allow_to_resolve_niladic_functions =  allow_niladic_functions });
        lambda_expression_untyped = function_lookup_result.resolved_identifier;
    }

    /** Early short-circuit optimization for ordinary builtin AND/OR functions. Perform this
      * only after checking scoped lambdas and registered UDFs, so a builtin cannot bypass a
      * user-defined function with the same name.
      */
    if (!early_short_circuit_type_inference_in_process
        && scope.context->getSettingsRef()[Setting::enable_function_early_short_circuit]
        && scope.context->getSettingsRef()[Setting::short_circuit_function_evaluation] != ShortCircuitFunctionEvaluation::DISABLE
        && (function_name == "and" || function_name == "or")
        && parameters.empty()
        && function_node_ptr->getNullsAction() == NullsAction::EMPTY
        && !function_node_ptr->isWindowFunction()
        /// JOIN planning unwraps root constant source expressions. Keep JOIN ON expressions on
        /// the regular path so a preserved scalar-subquery source is never sent to the planner.
        && !scope.resolving_join_on_expression
        && !lambda_expression_untyped
        && !UserDefinedSQLFunctionFactory::instance().tryGet(function_name)
        && !UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, scope.context, parameters)) /// NOLINT(readability-static-accessed-through-instance)
    {
        auto short_circuit_result = getEarlyShortCircuitResultForAndOr(node, function_name);
        const bool is_strict_safe_logical_tree = isStrictSafeLogicalTree(node, scope);
        if (short_circuit_result
            && !hasScopeDependentNodesForEarlyShortCircuit(node, scope)
            && !hasUnsafeFunctionForEarlyShortCircuit(node, scope.context, scope))
        {
            /// Resolve a clone in type-only mode. Scalar subqueries are analyzed but not executed,
            /// which gives the logical expression its real Nullable/Bool result type. It also
            /// discovers aggregates and arrayJoin before they can be erased by the early fold.
            auto source_expression = node->clone();
            auto node_for_type_inference = node->clone();

            /// Speculative resolution must not cache placeholders or leave in-progress stack
            /// entries in the live analyzer when it falls back. Use a dedicated QueryAnalyzer and
            /// an isolated cache-disabled scope; parent-scope dependencies fall back immediately.
            IdentifierResolveScope type_inference_scope = scope;
            type_inference_scope.parent_scope = nullptr;
            type_inference_scope.identifier_in_lookup_process.clear();
            type_inference_scope.clearIdentifierCache();
            type_inference_scope.disableIdentifierCachePermanently();
            type_inference_scope.expression_argument_name_to_node.clear();
            type_inference_scope.aliases = {};
            type_inference_scope.global_with_aliases = {};
            type_inference_scope.cte_name_to_query_node.clear();
            type_inference_scope.table_expression_data_for_alias_resolution = nullptr;
            type_inference_scope.join_using_columns.clear();
            type_inference_scope.table_expression_node_to_data.clear();
            type_inference_scope.registered_table_expression_nodes.clear();
            type_inference_scope.expression_join_tree_node.reset();
            type_inference_scope.projection_mask_map
                = std::make_shared<std::map<IQueryTreeNode::Hash, size_t>>(*scope.projection_mask_map);

            QueryAnalyzer type_inference_analyzer(/*only_analyze_=*/ false);
            type_inference_analyzer.early_short_circuit_type_inference_in_process = true;
            type_inference_analyzer.subquery_counter = subquery_counter;

            bool type_inference_succeeded = false;
            ProjectionNames type_inference_projection_names;
            try
            {
                type_inference_projection_names = type_inference_analyzer.resolveExpressionNode(
                    node_for_type_inference,
                    type_inference_scope,
                    false /*allow_lambda_expression*/,
                    false /*allow_table_expression*/,
                    false /*ignore_alias*/,
                    allow_niladic_functions);
                type_inference_succeeded = !type_inference_analyzer.early_short_circuit_type_inference_failed;
            }
            catch (...)
            {
                /// Ok. Some functions require the value of a constant argument to infer or validate
                /// their result (for example, tupleElement's index). A type-only scalar placeholder
                /// cannot provide it, so fall back to the regular path which evaluates the scalar.
                type_inference_succeeded = false;
            }

            const bool post_resolution_is_safe = !hasFunctionNode(node_for_type_inference, "arrayJoin")
                && !hasUnsafeEarlyShortCircuitScalarUsage(node_for_type_inference)
                && !hasFunctionNotSuitableForEarlyShortCircuit(node_for_type_inference);
            if (type_inference_succeeded && (is_strict_safe_logical_tree || post_resolution_is_safe))
            {
                auto result_type = node_for_type_inference->getResultType();
                auto result_column = result_type->createColumnConst(1, static_cast<UInt8>(*short_circuit_result));
                copySecretMasksByPosition(node_for_type_inference, source_expression, *scope.projection_mask_map);

                ConstantValue constant_value{ std::move(result_column), std::move(result_type) };
                node = std::make_shared<ConstantNode>(
                    std::move(constant_value), std::move(source_expression), true /*is_deterministic*/);
                subquery_counter = type_inference_analyzer.subquery_counter;
                return type_inference_projection_names;
            }
        }
    }

    bool is_special_function_in = false;
    bool is_special_function_dict_get = false;
    bool is_special_function_join_get = false;
    bool is_special_function_exists = false;
    bool is_special_function_if = false;
    bool is_special_function_multi_if = false;

    if (!lambda_expression_untyped)
    {
        is_special_function_in = isNameOfInFunction(function_name);
        is_special_function_dict_get = functionIsDictGet(function_name);
        is_special_function_join_get = functionIsJoinGet(function_name);
        is_special_function_exists = function_name == "exists";
        is_special_function_if = function_name == "if";
        is_special_function_multi_if = function_name == "multiIf";

        /** Special handling for count and countState functions (including with combinators like countIf, countIfState, etc.).
          *
          * Example: SELECT count(*) FROM test_table
          * Example: SELECT countState(*) FROM test_table;
          *
          * To determine if it's safe to remove the asterisk, we check the transformsArgumentTypes() method
          * of each combinator. If any combinator transforms argument types (returns true), it's not safe to remove the asterisk.
          */
        String base_function_name = function_name;
        bool safe_to_remove_asterisk = true;

        while (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(base_function_name))
        {
            if (combinator->transformsArgumentTypes())
            {
                safe_to_remove_asterisk = false;
                break;
            }

            base_function_name = base_function_name.substr(0, base_function_name.size() - combinator->getName().size());
        }

        auto base_function_name_lowercase = Poco::toLower(base_function_name);
        auto function_name_lowercase = Poco::toLower(function_name);

        /// Only remove asterisks for exactly "count" or "countstate" (possibly with combinators),
        /// not for other functions like "countDistinct" which is a separate function
        /// countDistinct gets transformed to uniqExact and requires arguments
        bool is_count_function = (base_function_name_lowercase == "count" || base_function_name_lowercase == "countstate");
        bool is_count_variant = is_count_function && function_name_lowercase.starts_with(base_function_name_lowercase);
        bool is_not_count_distinct = function_name_lowercase != "countdistinct";

        if (safe_to_remove_asterisk && is_count_variant && is_not_count_distinct)
        {
            auto & arguments = function_node_ptr->getArguments().getNodes();

            std::erase_if(arguments, [](const QueryTreeNodePtr & argument)
            {
                auto * matcher_node = argument->as<MatcherNode>();
                return matcher_node && matcher_node->isUnqualified();
            });
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
        auto resolve_result = tryResolveIdentifier(identifier_lookup, scope, { .allow_to_resolve_niladic_functions =  allow_niladic_functions });

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
        resolveExpressionNode(if_function_condition, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/, allow_niladic_functions);

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
                    false /*allow_table_expression*/,
                    allow_niladic_functions);
            }
            catch (const Exception &)
            {
                apply_constant_if_optimization = true;
            }

            if (apply_constant_if_optimization)
            {
                auto result_projection_names = resolveExpressionNode(constant_if_result_node,
                    scope,
                    false /*allow_lambda_expression*/,
                    false /*allow_table_expression*/,
                    allow_niladic_functions);
                node = std::move(constant_if_result_node);
                return result_projection_names;
            }
        }
    }

    /** Handle multiIf analogously to the `if` special-case above: when every condition up to
      * the selected branch is a compile-time constant and resolving the statically unreachable
      * branches throws, replace the whole multiIf node with its live branch. Mirrors the `if`
      * special-case: the fall-through path leaves the node intact so that
      * `FunctionMultiIf::build` performs normal common-supertype unification — replacing
      * unconditionally would bypass it (e.g. `toTypeName(multiIf(1, toUInt8(1), toUInt16(2)))`
      * must stay `UInt16`, not collapse to `UInt8`).
      *
      * multiIf(cond1, val1, cond2, val2, ..., condN, valN, else): arity is odd and >= 3.
      * Walk conditions left to right. When a condition is a true constant the corresponding
      * value becomes the live branch; when a condition is a false constant the paired value is
      * dead. If every condition is a false constant the else branch is live. Once a
      * non-constant condition is seen we fall through to the generic path.
      */
    if (is_special_function_multi_if && !function_node_ptr->getArguments().getNodes().empty())
    {
        auto & multi_if_args = function_node_ptr->getArguments().getNodes();
        const size_t arg_count = multi_if_args.size();

        /// If arity is malformed let the generic path report the error as usual.
        if (arg_count >= 3 && (arg_count % 2) == 1)
        {
            checkFunctionNodeHasEmptyNullsAction(*function_node_ptr);

            const size_t num_pairs = arg_count / 2;
            const size_t else_index = arg_count - 1;

            bool found_true_branch = false;
            size_t winner_index = else_index;
            bool stopped_on_nonconstant = false;

            for (size_t pair = 0; pair < num_pairs; ++pair)
            {
                /// Snapshot, not reference: `resolveExpressionNode` rebinds matchers in place.
                QueryTreeNodePtr cond_node = multi_if_args[2 * pair];
                resolveExpressionNode(cond_node,
                    scope,
                    false /*allow_lambda_expression*/,
                    false /*allow_table_expression*/,
                    allow_niladic_functions);

                auto constant_condition = tryExtractConstantFromConditionNode(cond_node);
                if (!constant_condition.has_value())
                {
                    stopped_on_nonconstant = true;
                    break;
                }

                if (*constant_condition)
                {
                    found_true_branch = true;
                    winner_index = 2 * pair + 1;
                    break;
                }
                /// Constant false: the paired value is unreachable at run time.
            }

            /// Only fold when the winner is statically determined. Otherwise fall through to
            /// the generic path which resolves the remaining arguments and lets
            /// `FunctionMultiIf::build` run normal type unification.
            if (!stopped_on_nonconstant)
            {
                /// Snapshot the live branch and every dead branch into local shared_ptr
                /// copies before calling `resolveExpressionNode` on them. Mirrors the `if`
                /// special case above: we must not index back into `multi_if_args` after
                /// nested resolution, because resolving a branch may rewrite neighbouring
                /// slots (matcher expansion, sub-node replacement, etc.) and leave a stale
                /// pointer in the original vector slot.
                QueryTreeNodePtr live_branch_copy = multi_if_args[winner_index];
                std::vector<QueryTreeNodePtr> dead_branch_copies;
                dead_branch_copies.reserve(arg_count);

                for (size_t pair = 0; pair < num_pairs; ++pair)
                {
                    const size_t cond_idx = 2 * pair;
                    const size_t val_idx = cond_idx + 1;

                    /// The winner's own slot is resolved separately below.
                    if (val_idx == winner_index)
                        continue;

                    /// Earlier pairs already had their condition resolved (constant false);
                    /// only the paired value is dead.
                    if (val_idx < winner_index)
                    {
                        dead_branch_copies.push_back(multi_if_args[val_idx]);
                        continue;
                    }

                    /// Pairs after the winner: both the condition and the value are
                    /// unreachable and have not been resolved yet.
                    dead_branch_copies.push_back(multi_if_args[cond_idx]);
                    dead_branch_copies.push_back(multi_if_args[val_idx]);
                }

                /// When a true condition wins, the else slot is dead too.
                if (found_true_branch)
                    dead_branch_copies.push_back(multi_if_args[else_index]);

                /// Try to resolve every dead branch. If any throws we apply the fold,
                /// mirroring the `if` special case: only then do we replace the node and
                /// swallow the analysis error in the unreachable branch. If every dead
                /// branch resolves cleanly we fall through so that normal type inference
                /// (common-supertype unification) still runs on the full `multiIf`.
                bool apply_constant_multi_if_optimization = false;
                for (auto & dead_branch : dead_branch_copies)
                {
                    try
                    {
                        resolveExpressionNode(dead_branch,
                            scope,
                            false /*allow_lambda_expression*/,
                            false /*allow_table_expression*/,
                            allow_niladic_functions);
                    }
                    catch (const Exception &)
                    {
                        apply_constant_multi_if_optimization = true;
                    }
                }

                if (apply_constant_multi_if_optimization)
                {
                    /// Resolve the live branch via the local copy and replace the whole
                    /// multiIf node with it.
                    auto result_projection_names = resolveExpressionNode(live_branch_copy,
                        scope,
                        false /*allow_lambda_expression*/,
                        false /*allow_table_expression*/,
                        allow_niladic_functions);
                    node = std::move(live_branch_copy);
                    return result_projection_names;
                }
                /// All dead branches resolved cleanly: fall through to the generic path so that
                /// `FunctionMultiIf::build` can perform common-supertype unification.
            }
        }
    }

    /// Replace IN (subquery)
    /// NOTE: the resulting subquery in the argument of EXISTS will have correlated column x, that's why this rewriting has to be before handling
    /// EXISTS which is done below in 'if (is_special_function_exists)' case.
    /// NOTE: the rewrite is skipped inside `PREWHERE`: the rewritten form is a correlated subquery,
    /// and `PREWHERE` is evaluated by the reading step, which cannot execute one (the planner rejects
    /// it with `ILLEGAL_PREWHERE`). Keeping the plain `IN` there makes `PREWHERE x IN (subquery)`
    /// behave exactly like its `WHERE` spelling instead of failing.
    /// See https://github.com/ClickHouse/ClickHouse/issues/114026.
    /// Also skip when `transform_null_in` is enabled, because the `EXISTS` rewrite is not null-aware
    /// and would make `WHERE` diverge from `PREWHERE` (which keeps the null-aware `nullIn` path).
    if (is_special_function_in &&
        (function_name == "in" || function_name == "notIn") &&
        scope.context->getSettingsRef()[Setting::rewrite_in_to_join] &&
        !scope.context->getSettingsRef()[Setting::transform_null_in] &&
        !scope.in_prewhere)
    {
        const bool is_function_not_in = function_name == "notIn";

        auto & function_in_arguments_nodes = function_node_ptr->getArguments().getNodes();
        if (function_in_arguments_nodes.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' expects 2 arguments", function_name);

        QueryTreeNodePtr in_first_argument = function_in_arguments_nodes[0]->clone();

        /// Resolve first argument of IN to determine if it is constant or not. In case of constant we will not do any rewriting
        resolveExpressionNode(
            in_first_argument,
            scope,
            true /*allow_lambda_expression*/,
            true /*allow_table_expression*/,
            allow_niladic_functions
        );

        if (!in_first_argument->as<ConstantNode>())
        {
            auto in_second_argument = function_in_arguments_nodes[1]->clone();

            /// Resolve second argument of IN to determine if it is a subquery.
            resolveExpressionNode(
                in_second_argument,
                scope,
                true /*allow_lambda_expression*/,
                true /*allow_table_expression*/,
                allow_niladic_functions
            );

            if (in_second_argument->as<QueryNode>())
            {
                /// The rewrite below produces a correlated subquery, so it requires the setting.
                /// Checked here (not at the gate) so constant/tuple `IN` is never rejected.
                if (!scope.context->getSettingsRef()[Setting::allow_experimental_correlated_subqueries])
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "Setting 'rewrite_in_to_join' requires 'allow_experimental_correlated_subqueries' to also be enabled");

                /// An array subquery on the right of IN is the set of its elements (see
                /// `flattenArraySubqueryOnRightOfIn`). Flatten it with arrayJoin before building the
                /// EXISTS rewrite, so the comparison below is `x = <element>` rather than `x = <array>`.
                /// Otherwise this branch would keep the reported bug alive whenever `rewrite_in_to_join`
                /// is enabled, making the new behavior depend on an unrelated setting.
                flattenArraySubqueryOnRightOfIn(in_second_argument, in_first_argument, scope.context);

                /// Rewrite 'x IN subquery' to 'EXISTS (SELECT 1 FROM (SELECT * AS _unique_name_ FROM subquery) WHERE x = _unique_name_ LIMIT 1)'

                /// Rename subquery projection to a unique name to avoid collisions with names from outer scope
                /// E.g. when rewriting "SELECT number IN (SELECT * FROM numbers(3)) FROM numbers(5)" the inner
                /// query "SELECT * FROM numbers(3)" returns column `number` which will collide with outer column `number`
                auto subquery_node = std::move(in_second_argument);

                String unique_column_name = "__subquery_column_" + toString(UUIDHelpers::generateV4());

                /// Re-resolve subquery columns setting the unique alias
                auto subquery_projection_columns = subquery_node->as<QueryNode>()->getProjectionColumns();
                subquery_node->as<QueryNode>()->clearProjectionColumns();
                if (subquery_projection_columns.size() == 1)
                {
                    subquery_node->as<QueryNode>()->setProjectionAliasesToOverride({unique_column_name});
                    subquery_node->as<QueryNode>()->resolveProjectionColumns(subquery_projection_columns);
                }
                else
                {
                    /// It there are multiple columns, wrap them in a Tuple()
                    auto projection = subquery_node->as<QueryNode>()->getProjection().clone();

                    QueryTreeNodePtr wrapper_tuple_node = std::make_shared<FunctionNode>("tuple");
                    wrapper_tuple_node->as<FunctionNode>()->getArguments().getNodes() = std::move(projection->as<ListNode>()->getNodes());
                    resolveFunction(wrapper_tuple_node, scope);

                    /// Replace the original projection columns with one Tuple column
                    subquery_node->as<QueryNode>()->getProjection().getNodes() = { std::move(wrapper_tuple_node) };
                    DataTypes wrapper_tuple_element_types;
                    for (const auto & c : subquery_projection_columns)
                        wrapper_tuple_element_types.push_back(c.type);
                    auto wrapper_tuple_data_type = std::make_shared<DataTypeTuple>(wrapper_tuple_element_types);
                    /// Return the Tuple under unique name
                    subquery_node->as<QueryNode>()->resolveProjectionColumns(NamesAndTypes{{unique_column_name, wrapper_tuple_data_type}});
                }

                /// SELECT * AS _unique_name_ FROM subquery
                auto internal_exists_subquery = std::make_shared<QueryNode>(Context::createCopy(scope.context));
                internal_exists_subquery->setIsSubquery(true);
                internal_exists_subquery->getProjection().getNodes().push_back(std::make_shared<IdentifierNode>(Identifier{unique_column_name}));
                internal_exists_subquery->getJoinTreeNode() = std::move(subquery_node);

                /// SELECT 1 FROM (SELECT * AS _unique_name_ FROM subquery) WHERE a = _unique_name_ LIMIT 1
                auto new_exists_subquery = std::make_shared<QueryNode>(Context::createCopy(scope.context));
                {
                    auto constant_data_type = std::make_shared<DataTypeUInt64>();
                    new_exists_subquery->setIsSubquery(true);
                    new_exists_subquery->getProjection().getNodes().push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
                    new_exists_subquery->getJoinTreeNode() = std::move(internal_exists_subquery);

                    auto equals_function_node_ptr = std::make_shared<FunctionNode>("equals");

                    auto copy_of_in_first_parameter = function_in_arguments_nodes[0];

                    auto subquery_projection = std::make_shared<IdentifierNode>(Identifier{unique_column_name});

                    equals_function_node_ptr->getArguments().getNodes() = {
                        std::move(copy_of_in_first_parameter), /// x
                        std::move(subquery_projection) /// `_unique_name_` from subquery
                    };

                    new_exists_subquery->getWhere() = std::move(equals_function_node_ptr);
                    new_exists_subquery->getLimit() = std::make_shared<ConstantNode>(1UL, constant_data_type);
                }

                auto exists_function_node_ptr = std::make_shared<FunctionNode>("exists");
                exists_function_node_ptr->getArguments().getNodes() = {
                    std::move(new_exists_subquery)
                };

                if (is_function_not_in)
                {
                    /// NOT IN is rewritten to NOT EXISTS
                    function_node_ptr = std::make_shared<FunctionNode>("not");
                    function_node_ptr->getArguments().getNodes() = {
                        std::move(exists_function_node_ptr)
                    };

                    node = function_node_ptr;
                    function_name = "not";
                    is_special_function_in = false;
                    is_special_function_exists = false;
                }
                else
                {
                    function_node_ptr = exists_function_node_ptr;
                    node = function_node_ptr;
                    function_name = "exists";
                    is_special_function_in = false;
                    is_special_function_exists = true;
                }
            }
        }
    }

    if (is_special_function_exists)
    {
        checkFunctionNodeHasEmptyNullsAction(*function_node_ptr);

        /// Rewrite EXISTS (subquery) into EXISTS (SELECT 1 FROM (subquery) LIMIT 1).
        const auto & exists_subquery_argument = function_node_ptr->getArguments().getNodes().at(0);

        auto exists_subquery_argument_node_type = exists_subquery_argument->getNodeType();
        if (exists_subquery_argument_node_type != QueryTreeNodeType::QUERY
            && exists_subquery_argument_node_type != QueryTreeNodeType::UNION)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function 'exists' expects a subquery argument. Actual: {}. In scope {}",
                exists_subquery_argument->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }

        auto constant_data_type = std::make_shared<DataTypeUInt64>();
        auto new_exists_subquery = std::make_shared<QueryNode>(Context::createCopy(scope.context));

        new_exists_subquery->setIsSubquery(true);
        new_exists_subquery->getProjection().getNodes().push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
        new_exists_subquery->getJoinTreeNode() = exists_subquery_argument;
        new_exists_subquery->getLimit() = std::make_shared<ConstantNode>(1UL, constant_data_type);

        QueryTreeNodePtr new_exists_argument = new_exists_subquery;

        auto exists_arguments_projection_names = resolveExpressionNode(
            new_exists_argument,
            scope,
            true /*allow_lambda_expression*/,
            true /*allow_table_expression*/,
            allow_niladic_functions
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
                res_col->getData().push_back(static_cast<UInt8>(const_node->getColumn()->isNullAt(0) ? 0 : 1));
                ConstantValue const_value(ColumnConst::create(std::move(res_col), 1), std::make_shared<DataTypeUInt8>());
                auto tme_const_node = std::make_shared<ConstantNode>(std::move(const_value), std::move(node));
                auto res = tme_const_node->getValueStringRepresentation();
                node = std::move(tme_const_node);
                return {std::move(res)};
            }
        }
    }

    /** Convert a bare function name in the first argument position to a lambda expression,
      * but only when the parent function is a higher-order function that accepts lambdas.
      * Example: arrayMap(toUpper, arr) is converted to arrayMap(x -> toUpper(x), arr).
      *
      * The transformation is gated by `isHigherOrderFunction`, a non-throwing capability
      * check. This avoids relying on `getLambdaArgumentTypes` to throw on non-higher-order
      * functions, which would terminate the process under `CLICKHOUSE_TERMINATE_ON_ANY_EXCEPTION`
      * even though the exception is caught (the exception constructor itself terminates).
      *
      * The lambda arity is taken from the inner function:
      * - Built-in, executable, and WebAssembly UDFs: `getNumberOfArguments` of the
      *   resolver (zero means variadic; WebAssembly UDFs are always fixed-arity).
      * - SQL UDFs: the number of lambda parameters in the `CREATE FUNCTION` AST.
      * For variadic inner functions (e.g. `concat`), fall back to the number of array
      * arguments (`argument_nodes_size - 1`). This works for the common higher-order
      * functions (`arrayMap`, `arrayFilter`, `arrayFold`, …) where the lambda arity
      * equals the number of arrays. For higher-order functions with fixed non-array
      * parameters (e.g. `arrayPartialSort`), variadic inner functions may need an
      * explicit lambda.
      */
    {
        auto & argument_nodes = function_node_ptr->getArguments().getNodes();
        size_t argument_nodes_size = argument_nodes.size();

        /// Higher-order functions always expect the lambda as the first argument.
        if (argument_nodes_size >= 2)
        {
            auto * identifier_node = argument_nodes[0]->as<IdentifierNode>();
            if (identifier_node)
            {
                const auto & identifier = identifier_node->getIdentifier();
                if (identifier.getPartsSize() == 1)
                {
                    /// Check the parent first. This avoids probing UDF registries (which take
                    /// the external UDF loader mutex) on every ordinary call like `plus(a, b)`
                    /// where the first argument happens to be an identifier.
                    auto parent_resolver = FunctionFactory::instance().tryGet(function_name, scope.context);

                    if (parent_resolver && parent_resolver->isHigherOrderFunction())
                    {
                        const auto & identifier_name = identifier.getFullName();

                        /// These checks don't create tree nodes, so they don't affect node ID
                        /// numbering. We must not throw from this rewrite-candidate check — it
                        /// runs before column/alias resolution, so a throw would break the
                        /// documented "column/alias names take priority" contract and would also
                        /// be disruptive for queries run with `terminate_on_any_exception` enabled.
                        ///
                        /// Built-in, executable, and WebAssembly UDFs are all `IFunction`
                        /// implementations exposed as regular `FunctionOverloadResolverPtr`s,
                        /// just stored in different factories — so they share the resolver-arity
                        /// path below. SQL UDFs are not `IFunction`s; their body is an arbitrary
                        /// SQL expression inlined at analysis time, so arity is read from the
                        /// stored `CREATE FUNCTION` AST.
                        auto inner_resolver = FunctionFactory::instance().tryGet(identifier_name, scope.context);
                        if (!inner_resolver && UserDefinedExecutableFunctionFactory::has(identifier_name, scope.context))
                        {
                            /// `has` first: `tryGet` instantiates `UserDefinedFunction` with empty
                            /// parameters, whose constructor throws `BAD_ARGUMENTS` when the UDF
                            /// declares command parameters. Such UDFs are not eligible for the
                            /// lambda rewrite anyway (we have no parameters to supply), so swallow
                            /// `BAD_ARGUMENTS` and let identifier resolution proceed.
                            try
                            {
                                inner_resolver = UserDefinedExecutableFunctionFactory::tryGet(identifier_name, scope.context);
                            }
                            catch (const Exception & e)
                            {
                                if (e.code() != ErrorCodes::BAD_ARGUMENTS)
                                    throw;
                            }
                        }
                        if (!inner_resolver)
                        {
                            /// Use `tryGet` (returns nullptr if missing) instead of `has` + `get`:
                            /// a `has` + `get` sequence has a TOCTOU race with concurrent
                            /// `DROP FUNCTION`, where `get` would throw `RESOURCE_NOT_FOUND`
                            /// and preempt the documented "column/alias names take priority"
                            /// behavior. This rewrite probe must stay strictly non-throwing.
                            inner_resolver = UserDefinedWebAssemblyFunctionFactory::instance().tryGet(identifier_name, scope.context);
                        }

                        ASTPtr sql_udf_ast;
                        ASTPtr wasm_udf_ast;
                        if (!inner_resolver)
                        {
                            auto stored_udf_ast = UserDefinedSQLFunctionFactory::instance().tryGet(identifier_name);
                            if (stored_udf_ast && stored_udf_ast->as<ASTCreateSQLFunctionQuery>())
                                sql_udf_ast = std::move(stored_udf_ast);
                            /// A `CREATE FUNCTION ... LANGUAGE WASM` definition is kept in the same storage and
                            /// outlives the engine that runs it: after a restart with
                            /// `allow_experimental_webassembly_udf` turned off, or on a build without a
                            /// WebAssembly engine at all, the definition is still stored while the registry
                            /// probed above is empty. Rewrite the reference from the stored definition anyway,
                            /// so that resolving the rewritten call reports that WebAssembly support is
                            /// unavailable instead of failing as an unknown identifier.
                            else if (stored_udf_ast && stored_udf_ast->as<ASTCreateWasmFunctionQuery>())
                                wasm_udf_ast = std::move(stored_udf_ast);
                        }

                        if (inner_resolver || sql_udf_ast || wasm_udf_ast)
                        {
                            /// Determine arity from the inner function itself. This handles
                            /// cases like `arrayMap(plus, arr1, arr2)` where `plus` has a
                            /// fixed arity of 2, regardless of how many array args are passed.
                            size_t inner_arity = inner_resolver ? inner_resolver->getNumberOfArguments() : 0;

                            /// SQL UDFs are not registered in `FunctionFactory` because they are not
                            /// `IFunction` implementations: their body is an arbitrary SQL expression
                            /// inlined at analysis time by `UserDefinedSQLFunctionVisitor`, not evaluated
                            /// by a runtime resolver. So when the inner function is a SQL UDF we extract
                            /// arity directly from the stored `CREATE FUNCTION` AST.
                            if (!inner_resolver && sql_udf_ast)
                            {
                                if (const auto * lambda = sql_udf_ast->as<ASTCreateSQLFunctionQuery>())
                                {
                                    if (lambda->function_core)
                                    {
                                        if (const auto * lambda_expr = lambda->function_core->as<ASTFunction>())
                                        {
                                            if (lambda_expr->name == "lambda" && lambda_expr->arguments
                                                && lambda_expr->arguments->children.size() >= 2)
                                            {
                                                const auto * tuple_ast = lambda_expr->arguments->children[0]->as<ASTFunction>();
                                                if (tuple_ast && tuple_ast->arguments)
                                                    inner_arity = tuple_ast->arguments->children.size();
                                            }
                                        }
                                    }
                                }
                            }

                            /// A WebAssembly UDF declares its arguments in the `CREATE FUNCTION` statement,
                            /// so the stored definition carries the arity even when nothing can run it.
                            if (const auto * wasm_udf = wasm_udf_ast ? wasm_udf_ast->as<ASTCreateWasmFunctionQuery>() : nullptr)
                                inner_arity = wasm_udf->getNumberOfArguments();

                            /// Determine the lambda arity:
                            /// - Inner function with fixed arity: use it directly.
                            /// - Variadic inner function (e.g. `concat`): fall back to the
                            ///   number of array arguments, which is correct for the common
                            ///   higher-order functions (`arrayMap`, `arrayFilter`, `arrayFold`).
                            ///   Note: this fallback does not auto-unpack tuples — for variadic
                            ///   inner functions with a single `Array(Tuple(...))` argument
                            ///   (e.g. `arrayMap(concat, [('a','b'), ('c','d')])`) the rewrite
                            ///   produces a unary lambda, which is not equivalent to the binary
                            ///   `(x, y) -> concat(x, y)` an explicit lambda would yield after
                            ///   tuple destructuring. Use an explicit lambda for that case.
                            /// - Fixed-arity zero-argument inner function (e.g. `UTCTimestamp`):
                            ///   the rewrite makes no sense — a zero-arg function can't be
                            ///   applied to lambda arguments — leave the call unchanged.
                            size_t lambda_arity = 0;
                            if (inner_arity > 0)
                                lambda_arity = inner_arity;
                            else if (inner_resolver && inner_resolver->isVariadic())
                                lambda_arity = argument_nodes_size - 1;

                            if (lambda_arity > 0)
                            {
                                /// Now check if the identifier resolves as a column or alias.
                                /// This is deferred to here because tryResolveIdentifier may allocate
                                /// tree nodes that affect node ID numbering.
                                auto expression_resolve_result = tryResolveIdentifier(
                                    {identifier, IdentifierLookupContext::EXPRESSION}, scope, {});

                                if (!expression_resolve_result.isResolved())
                                {
                                    auto function_resolve_result = tryResolveIdentifier(
                                        {identifier, IdentifierLookupContext::FUNCTION}, scope, {});

                                    if (!function_resolve_result.isResolved())
                                    {
                                        Names lambda_arg_names;
                                        lambda_arg_names.reserve(lambda_arity);

                                        auto func_call = std::make_shared<FunctionNode>(identifier_name);
                                        auto & func_call_args = func_call->getArguments().getNodes();
                                        func_call_args.reserve(lambda_arity);

                                        for (size_t j = 0; j < lambda_arity; ++j)
                                        {
                                            String arg_name = "__function_ref_arg_" + std::to_string(j);
                                            lambda_arg_names.push_back(arg_name);
                                            func_call_args.push_back(
                                                std::make_shared<IdentifierNode>(Identifier{arg_name}));
                                        }

                                        auto lambda_args_node = std::make_shared<LambdaArgumentsNode>(std::move(lambda_arg_names));
                                        argument_nodes[0] = std::make_shared<LambdaNode>(
                                            std::move(lambda_args_node), std::move(func_call), false /*is_operator*/);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Resolve function arguments
    bool allow_table_expressions = is_special_function_in || is_special_function_exists;
    auto arguments_projection_names = resolveExpressionNodeList(
        function_node_ptr->getArgumentsNode(),
        scope,
        true /*allow_lambda_expression*/,
        allow_table_expressions /*allow_table_expression*/,
        allow_niladic_functions);

    /// Mask arguments if needed
    if (!scope.context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select])
    {
        if (FunctionSecretArgumentsFinder::Result secret_arguments = FunctionSecretArgumentsFinderTreeNode(*function_node_ptr).getResult(); secret_arguments.hasSecrets())
        {
            auto & argument_nodes = function_node_ptr->getArgumentsNode()->as<ListNode &>().getNodes();

            /// This tree is used for execution, so the value itself cannot be rewritten; only the
            /// display mask of its constants can be set. `setMaskId` is a display flag, so it hides
            /// the literal in projection names, `EXPLAIN QUERY TREE` and the `EXPLAIN actions = 1`
            /// ActionsDAG (see PlannerActionsVisitor) without changing what is executed.
            auto assign_mask = [&](ConstantNode & constant)
            {
                auto mask = scope.projection_mask_map->insert({constant.getTreeHash(), scope.projection_mask_map->size() + 1}).first->second;
                constant.setMaskId(mask);
                return mask;
            };
            /// A secret value can be an expression, not a bare literal (e.g. an `encrypt` key built as
            /// `leftPad('...', 16, '*')`, including one inlined from a SQL UDF body). Hide every
            /// constant inside it so no fragment of the secret leaks; returns whether any literal was
            /// hidden. A slot that carries no literal (a plaintext like `toString(number)` or a key
            /// held in a column) exposes nothing in the query text, so it is left as is.
            std::function<bool(const QueryTreeNodePtr &)> mask_secret_constants = [&](const QueryTreeNodePtr & subtree)
            {
                if (auto * constant = subtree->as<ConstantNode>())
                {
                    assign_mask(*constant);
                    return true;
                }
                bool masked_any = false;
                for (const auto & child : subtree->getChildren())
                    if (child)
                        masked_any |= mask_secret_constants(child);
                return masked_any;
            };

            forEachSecretArgumentNode(
                argument_nodes,
                secret_arguments,
                [&](size_t n, QueryTreeNodePtr & secret_node)
                {
                    if (auto * constant = secret_node->as<ConstantNode>())
                        arguments_projection_names[n] = "[HIDDEN id: " + std::to_string(assign_mask(*constant)) + "]";
                    else if (mask_secret_constants(secret_node))
                        arguments_projection_names[n] = "[HIDDEN]";
                });
        }
    }

    /** Bind an unqualified dictionary name to the current database.
      *
      * The dictionary name of `dictGet` and its variations is resolved against the current database of
      * the server that evaluates the function. A shard of a `Distributed` table evaluates it in a session
      * whose current database comes from the cluster configuration - `default` unless `<default_database>`
      * is set - and not from the initiator, so an unqualified name shipped to a shard either fails to
      * resolve or, worse, silently resolves to a different dictionary that happens to have the same name.
      * Bind the name here, while the current database of the initiator is still known. The old analyzer
      * does the same in `AddDefaultDatabaseVisitor` for the query it sends to the shards.
      *
      * `arguments_projection_names` is already calculated at this point, so the column name of the
      * expression stays exactly as it was written by the user.
      *
      * `qualifyDictionaryNameWithDatabase` leaves the name alone when it is already qualified, when it
      * belongs to an XML dictionary, and when no such dictionary exists in the current database - in the
      * last case the name may still be meant for a dictionary that only exists on the shards.
      */
    if (is_special_function_dict_get)
    {
        auto & dict_get_arguments = function_node_ptr->getArguments().getNodes();
        if (!dict_get_arguments.empty())
        {
            const auto * dictionary_name_node = dict_get_arguments[0]->as<ConstantNode>();
            if (dictionary_name_node && dictionary_name_node->getValue().getType() == Field::Types::String)
            {
                const auto & dictionary_name = dictionary_name_node->getValue().safeGet<String>();
                auto qualified_dictionary_name = scope.context->getExternalDictionariesLoader()
                    .qualifyDictionaryNameWithDatabase(dictionary_name, scope.context).getFullName();

                if (qualified_dictionary_name != dictionary_name)
                    dict_get_arguments[0] = std::make_shared<ConstantNode>(qualified_dictionary_name);
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

        auto & in_first_argument = function_in_arguments_nodes[0];
        auto & in_second_argument = function_in_arguments_nodes[1];
        if (isCorrelatedQueryOrUnionNode(function_in_arguments_nodes[0]) || isCorrelatedQueryOrUnionNode(function_in_arguments_nodes[1]))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Correlated subqueries are not supported as IN function arguments yet, but found in expression: {}",
                node->formatASTForErrorMessage());

        /// Table expressions are only allowed as the second (right) argument of IN.
        /// A table on the left side is not a value expression, so reject it with a clear
        /// error instead of failing later when getResultType is called on the table node.
        auto first_argument_type = in_first_argument->getNodeType();
        if (first_argument_type == QueryTreeNodeType::TABLE || first_argument_type == QueryTreeNodeType::TABLE_FUNCTION)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function '{}' is a table expression '{}', but it must be a value expression. In scope {}",
                function_name,
                in_first_argument->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        /// Edge case when the first argument of IN is scalar subquery.
        if (first_argument_type == QueryTreeNodeType::QUERY || first_argument_type == QueryTreeNodeType::UNION)
        {
            IdentifierResolveScope & subquery_scope = createIdentifierResolveScope(in_first_argument, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            evaluateScalarSubqueryIfNeeded(in_first_argument, subquery_scope);
        }

        auto * table_node = in_second_argument->as<TableNode>();
        auto * table_function_node = in_second_argument->as<TableFunctionNode>();

        if (table_node)
        {
            /// If table is already prepared set, we do not replace it with subquery.
            /// If table is not a StorageSet, we'll create plan to build set in the Planner.
            ///
            /// If its single column is an Array one dimension deeper than the left argument,
            /// interpret it as the set of the array's elements, exactly like an array subquery,
            /// an array literal, or an array-returning function on the right side of IN.
            flattenArrayTableExpressionOnRightOfIn(in_second_argument, in_first_argument, table_node->getStorageSnapshot(), scope.context);
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
                column_nodes_to_select->getNodes().emplace_back(std::make_shared<ColumnNode>(column, static_pointer_cast<ITableExpressionNode>(in_second_argument)));
                projection_columns.emplace_back(column.name, column.type);
            }

            auto in_second_argument_query_node = std::make_shared<QueryNode>(Context::createCopy(scope.context));
            in_second_argument_query_node->setIsSubquery(true);
            in_second_argument_query_node->getProjectionNode() = std::move(column_nodes_to_select);
            in_second_argument_query_node->getJoinTreeNode() = std::move(in_second_argument);
            in_second_argument_query_node->resolveProjectionColumns(std::move(projection_columns));

            in_second_argument = std::move(in_second_argument_query_node);

            /// If the wrapped subquery's single column is an Array one dimension deeper than the left
            /// argument, interpret it as the set of the array's elements, exactly like an array
            /// subquery, an array literal, or an array-returning function on the right side of IN.
            flattenArraySubqueryOnRightOfIn(in_second_argument, in_first_argument, scope.context);
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
                        table_expression = extractLeftTableExpression(query_node->getJoinTreeNodeTyped());
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
                    in_second_argument = in_second_argument->cloneAndReplace(
                        static_pointer_cast<ITableExpressionNode>(table_expression),
                        static_pointer_cast<ITableExpressionNode>(std::move(replacement_table_expression_table_node)));
                }
            }

            /// If the subquery's single column is an Array one dimension deeper than the left
            /// argument, flatten it with arrayJoin so its elements become the set elements.
            flattenArraySubqueryOnRightOfIn(in_second_argument, in_first_argument, scope.context);

            const bool is_not_in = isNegativeInFunctionName(function_name);
            const bool compare_nulls = inFunctionComparesNulls(function_name);
            auto & fn_args = function_node.getArguments().getNodes();

            /// A lambda on the left of IN has no result type until `getLambdaArgumentTypes` rejects it
            /// with a proper error further below, so none of the row-wise rewrites here may inspect it.
            const bool left_argument_is_lambda = in_first_argument->getNodeType() == QueryTreeNodeType::LAMBDA;

            bool expand_single_tuple_value = false;
            bool wrapped_column_rhs = false;

            /// If the second argument of IN is a bare column reference (e.g. from `IN (col)` where the
            /// parentheses were stripped by the parser), decide how to treat it by its type.
            if (auto * in_second_argument_column = in_second_argument->as<ColumnNode>())
            {
                if (!left_argument_is_lambda)
                {
                    /// An Array-typed column on the right of IN is the set of its elements, exactly like an
                    /// array literal or an array-returning function, so rewrite `x IN arr` to `has(arr, x)` -
                    /// but only when the array is exactly one dimension deeper than the left argument, so that
                    /// the element type of `has` matches the left argument (e.g. a scalar and `Array(scalar)`,
                    /// or `Array(T)` and `Array(Array(T))`). When the depths are equal (e.g. `Array(T) IN
                    /// Array(T)`), the column is a single set element and must be handled as `x = col` below.
                    /// Without this, the column would be wrapped in tuple() and treated as a single set element,
                    /// giving a wrong (always-false) result for stringifiable elements or an error otherwise.
                    const auto * rhs_array_type = typeid_cast<const DataTypeArray *>(in_second_argument_column->getColumnType().get());
                    if (rhs_array_type)
                    {
                        const auto & lhs_type = in_first_argument->getResultType();
                        const auto * lhs_array_type = typeid_cast<const DataTypeArray *>(lhs_type.get());
                        const size_t lhs_depth = lhs_array_type ? lhs_array_type->getNumberOfDimensions() : 0;

                        if (rhs_array_type->getNumberOfDimensions() == lhs_depth + 1)
                            return buildHasExpression(
                                node,
                                in_second_argument,
                                in_first_argument,
                                is_not_in,
                                compare_nulls,
                                arguments_projection_names,
                                parameters_projection_names,
                                scope);
                    }

                    const bool left_is_tuple = isTuple(removeNullable(in_first_argument->getResultType()));
                    const bool right_is_tuple = isTuple(removeNullable(in_second_argument_column->getColumnType()));
                    expand_single_tuple_value = !left_is_tuple && right_is_tuple;

                    /// A scalar column with a tuple LHS is a one-element set whose only execution
                    /// strategy is the direct row-wise comparison, like the scalar function RHS of
                    /// Case 3 below. Wrapping it in tuple() would send it through the tuple-set
                    /// rewrite, where the least supertype of the tuple LHS and a NULL column becomes
                    /// `Nullable(Tuple(...))` and fails, while the function-node analog
                    /// `(1, 2) IN (materialize(NULL))` returns 0. A string RHS takes the
                    /// cast-to-LHS-type fallback instead: the constant `Set` path parses such a set
                    /// element into the tuple type, so a non-parseable value raises the same parsing
                    /// error (e.g. `('a', 'b') IN (_table)` over a `merge` table) instead of
                    /// `NO_COMMON_TYPE`. The cast target stays non-`Nullable`: `Nullable(Tuple)`
                    /// columns are gated by `allow_experimental_nullable_tuple_type`, and the
                    /// constant `Set` path throws for a non-parseable tuple element rather than
                    /// skipping it, so a throwing `CAST` matches it.
                    if (left_is_tuple && !right_is_tuple)
                    {
                        auto proj = calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names);
                        QueryTreeNodePtr right_argument = in_second_argument;
                        const auto & left_type = in_first_argument->getResultType();
                        const auto & right_type = in_second_argument_column->getColumnType();
                        if (isStringOrFixedString(removeNullable(removeLowCardinality(right_type)))
                            && !tryGetLeastSupertype(DataTypes{left_type, right_type}))
                            right_argument = castNodeToType(right_argument, left_type, scope);
                        node = buildScalarInComparison(fn_args[0], right_argument, is_not_in, compare_nulls);
                        resolveFunction(node, scope);
                        return ProjectionNames{proj};
                    }
                }

                /// Any other single column value is a one-element set; wrap it in tuple() so it can be
                /// handled by the tuple → has() rewrite below.
                wrapped_column_rhs = true;
                auto tuple_function = std::make_shared<FunctionNode>("tuple");
                tuple_function->getArguments().getNodes().push_back(std::move(in_second_argument));
                in_second_argument = std::move(tuple_function);
                resolveFunction(in_second_argument, scope);
            }

            /// If it's a function node like array(..) or tuple(..), consider rewriting them to 'has':
            if (auto * non_const_set_candidate = in_second_argument->as<FunctionNode>())
            {
                const auto & candidate_name = non_const_set_candidate->getFunctionName();

                /// the type of the second argument
                bool is_array_type = (candidate_name == "array") ||
                    (non_const_set_candidate->isResolved() && isArray(non_const_set_candidate->getResultType()));
                bool is_tuple_function = (candidate_name == "tuple");
                bool is_tuple_type = is_tuple_function ||
                    (non_const_set_candidate->isResolved() && isTuple(removeNullable(non_const_set_candidate->getResultType())));
                bool is_not_array_or_tuple_type = non_const_set_candidate->isResolved() &&
                    !isArray(non_const_set_candidate->getResultType()) &&
                    !isTuple(non_const_set_candidate->getResultType());
                if (!is_tuple_function
                    && is_tuple_type
                    && !left_argument_is_lambda
                    && !isTuple(removeNullable(in_first_argument->getResultType())))
                    expand_single_tuple_value = true;

                /// None of the rewrites below may run for a lambda on the left-hand side, because they all
                /// inspect its result type, which an unresolved lambda does not have. Fall through instead
                /// and let getLambdaArgumentTypes() reject the lambda with a proper error.

                /// Case 1: array(..) or any function returning Array type -> rewrite to has()
                if (is_array_type && !left_argument_is_lambda)
                    return buildHasExpression(
                        node,
                        fn_args[1],
                        fn_args[0],
                        is_not_in,
                        compare_nulls,
                        arguments_projection_names,
                        parameters_projection_names,
                        scope);

                /// Case 2: tuple(..) -> convert to array, then rewrite to has()
                if (is_tuple_type && !left_argument_is_lambda)
                {
                    QueryTreeNodes tuple_args;
                    if (is_tuple_function)
                    {
                        const auto & left_type = in_first_argument->getResultType();
                        const bool left_is_tuple = isTuple(removeNullable(left_type));
                        const auto & candidate_arguments = non_const_set_candidate->getArguments().getNodes();
                        const auto * nullable_left_type = typeid_cast<const DataTypeNullable *>(left_type.get());
                        const auto * nullable_left_tuple_type = nullable_left_type
                            ? typeid_cast<const DataTypeTuple *>(nullable_left_type->getNestedType().get())
                            : nullptr;
                        const bool rhs_tuple_all_null = nullable_left_tuple_type
                            && !wrapped_column_rhs
                            && !candidate_arguments.empty()
                            && std::all_of(candidate_arguments.begin(), candidate_arguments.end(),
                                [](const auto & arg) { return arg->getResultType()->onlyNull(); });
                        const bool tuple_function_is_set = wrapped_column_rhs
                            || !left_is_tuple
                            || rhs_tuple_all_null
                            || std::any_of(candidate_arguments.begin(), candidate_arguments.end(),
                                [](const auto & arg) { return isTuple(removeNullable(arg->getResultType())); });

                        if (tuple_function_is_set)
                        {
                            tuple_args = candidate_arguments;
                            /// For a `Nullable(Tuple(...))` LHS, the constant `Set` path interprets an
                            /// explicit all-`NULL` RHS tuple as both top-level `NULL` set elements and,
                            /// when its elements are nullable, the tuple value itself. Preserve both
                            /// interpretations in the row-wise rewrite.
                            if (rhs_tuple_all_null
                                && compare_nulls
                                && candidate_arguments.size() == nullable_left_tuple_type->getElements().size()
                                && std::all_of(nullable_left_tuple_type->getElements().begin(), nullable_left_tuple_type->getElements().end(),
                                    [](const auto & type) { return type->isNullable(); }))
                                tuple_args.push_back(fn_args[1]);
                        }
                        else
                            tuple_args = {fn_args[1]};
                    }
                    else
                        tuple_args = {fn_args[1]};
                    /// A left-hand side of type `Nullable(Nothing)`, such as `materialize(NULL)`,
                    /// is `NULL` in every row, so it follows the same rewrites as a literal `NULL`.
                    const bool left_is_null = isNullConstant(in_first_argument) || in_first_argument->getResultType()->onlyNull();

                    /// Preserve NULL result for NULL IN (tuple) when NULLs are not compared.
                    /// When NULLs are compared, fall through to the regular `has` rewrite
                    /// so tuple-valued RHS expressions are expanded before NULL matching.
                    if (left_is_null && !compare_nulls)
                    {
                        auto proj = calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names);
                        node = std::make_shared<ConstantNode>(Field{},
                            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()));
                        return ProjectionNames{proj};
                    }

                    if (left_is_null && compare_nulls && std::any_of(tuple_args.begin(), tuple_args.end(), isNullConstant))
                    {
                        auto proj = calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names);
                        node = std::make_shared<ConstantNode>(is_not_in ? Field{0u} : Field{1u}, std::make_shared<DataTypeUInt8>());
                        return ProjectionNames{proj};
                    }

                    /// The constant `Set` path decides `NULL IN (...)` under compare-nulls semantics
                    /// purely by `NULL` presence among the set elements, without requiring a common
                    /// element type. Mirror it row-wise as `or(isNull(e1), ..., isNull(en))` instead
                    /// of building an `array(...)` of the elements, which could fail with
                    /// `NO_COMMON_TYPE` for a heterogeneous RHS.
                    if (left_is_null && compare_nulls)
                    {
                        auto proj = calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names);

                        QueryTreeNodes set_elements = getArrayElementsForInTupleArguments(tuple_args, in_first_argument, scope, expand_single_tuple_value);

                        if (set_elements.empty())
                        {
                            node = std::make_shared<ConstantNode>(is_not_in ? Field{1u} : Field{0u}, std::make_shared<DataTypeUInt8>());
                            return ProjectionNames{proj};
                        }

                        QueryTreeNodePtr null_presence;
                        for (auto & element : set_elements)
                        {
                            auto is_null_fn = std::make_shared<FunctionNode>("isNull");
                            is_null_fn->getArguments().getNodes().push_back(element);

                            if (null_presence)
                            {
                                auto or_fn = std::make_shared<FunctionNode>("or");
                                or_fn->getArguments().getNodes() = {std::move(null_presence), std::move(is_null_fn)};
                                null_presence = std::move(or_fn);
                            }
                            else
                            {
                                null_presence = std::move(is_null_fn);
                            }
                        }

                        if (is_not_in)
                        {
                            auto not_fn = std::make_shared<FunctionNode>("not");
                            not_fn->getArguments().getNodes().push_back(std::move(null_presence));
                            null_presence = std::move(not_fn);
                        }

                        node = std::move(null_presence);
                        resolveFunction(node, scope);
                        return ProjectionNames{proj};
                    }

                    /// convert tuple to array and rewrite to has()
                    QueryTreeNodePtr array_arg = convertTupleToArray(tuple_args, in_first_argument, scope, expand_single_tuple_value, compare_nulls);
                    return buildHasExpression(
                        node,
                        array_arg,
                        in_first_argument,
                        is_not_in,
                        compare_nulls,
                        arguments_projection_names,
                        parameters_projection_names,
                        scope);
                }

                /// Case 3: scalar-returning function -> rewrite to a row-wise comparison.
                if (is_not_array_or_tuple_type && !left_argument_is_lambda)
                {
                    auto proj = calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names);

                    /// The comparison functions below need a common supertype of both sides. When none
                    /// exists, the RHS is still a one-element set, so mirror the cast-to-LHS-type
                    /// fallback of the tuple/array rewrite (a failed `CAST` to a `Nullable` target
                    /// produces `NULL`, like the constant `Set` path skipping unrepresentable
                    /// elements). The `Nullable` target is used when the RHS can be `NULL` or when
                    /// `NULL` values must not match - a property of the resolved function (`nullIn`
                    /// compares `NULL`s, `in` does not), not of the `transform_null_in` setting. A
                    /// tuple LHS keeps the direct comparison, matching the scalar rewrite of the
                    /// old analyzer. A pair of numbers without a lossless supertype, such as `Int64`
                    /// and `Float64`, keeps the direct comparison too: the comparison functions
                    /// compare numbers accurately, while a `CAST` of the RHS to the LHS type would
                    /// truncate the value (`CAST(-0.6 AS Int64)` is `0`) and break the `Set` contract
                    /// of the constant path.
                    QueryTreeNodePtr right_argument = fn_args[1];
                    const auto & left_type = in_first_argument->getResultType();
                    if (!left_type->onlyNull() && !isTuple(removeNullable(left_type)))
                    {
                        const auto & right_type = non_const_set_candidate->getResultType();
                        const bool is_number_comparison = isNumber(removeNullable(removeLowCardinality(left_type)))
                            && isNumber(removeNullable(removeLowCardinality(right_type)));
                        if (!is_number_comparison && !tryGetLeastSupertype(DataTypes{left_type, right_type}))
                        {
                            DataTypePtr cast_elements_to = left_type;
                            if (isNullableOrLowCardinalityNullable(right_type) || !compare_nulls)
                                cast_elements_to = makeNullableOrLowCardinalityNullableSafe(cast_elements_to);
                            right_argument = castNodeToType(right_argument, cast_elements_to, scope);
                        }
                    }

                    node = buildScalarInComparison(fn_args[0], right_argument, is_not_in, compare_nulls);
                    resolveFunction(node, scope);
                    return ProjectionNames{proj};
                }
            }
        }

    }

    /// Initialize function argument columns

    ColumnsWithTypeAndName argument_columns;
    DataTypes argument_types;
    bool all_arguments_constants = true;
    bool all_arguments_are_deterministic = true;
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
            size_t lambda_arguments_size = lambda_node->getArguments().getNames().size();
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
        bool argument_is_deterministic = true;
        const auto * constant_node = function_argument->as<ConstantNode>();
        if (constant_node)
        {
            argument_column.column = constant_node->getColumn();
            argument_column.type = constant_node->getResultType();
            argument_is_deterministic = constant_node->isDeterministic();
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
                auto scalar_string = fieldToString(get_scalar_const_arg->getValue());
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
        all_arguments_are_deterministic &= argument_is_deterministic;

        argument_types.push_back(argument_column.type);
        argument_columns.emplace_back(std::move(argument_column));
    }

    /// Calculate function projection name
    ProjectionNames result_projection_names = { calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names) };

    ASTPtr user_defined_function = nullptr;
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
        user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function_name);

        if (!lambda_expression_untyped && user_defined_function)
            /// Try to substitute user defined SQL expression
            lambda_expression_untyped = tryGetLambdaFromUserDefinedSQLFunctions(user_defined_function, scope.context);

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
            DataTypePtr result_type_without_nullable = removeNullable(result_type);
            const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(result_type_without_nullable.get());
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
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Aggregate function with name '{}' does not exist{}. In scope {}",
                            function_name,
                            getHintsErrorMessageSuffix(AggregateFunctionFactory::instance().getHints(function_name)),
                            scope.scope_node->formatASTForErrorMessage());
        }

        if (!function_lambda_arguments_indexes.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Window function '{}' does not support lambda arguments",
                function_name);

        auto action = function_node_ptr->getNullsAction();
        std::string aggregate_function_name = rewriteAggregateFunctionNameIfNeeded(function_name, action, scope.context);

        AggregateFunctionProperties properties;
        auto aggregate_function
            = AggregateFunctionFactory::instance().get(
                aggregate_function_name,
                action,
                argument_types,
                parameters,
                properties,
                AggregateFunctionStateVariant::Window);

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
        }

        if (window_node_is_identifier)
            result_projection_names[0] += " OVER " + window_projection_name;
        else
            result_projection_names[0] += " OVER (" + window_projection_name + ')';

        return result_projection_names;
    }

    FunctionOverloadResolverPtr function = UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, scope.context, parameters); /// NOLINT(readability-static-accessed-through-instance)
    /// Executable UDFs may have parameters. They are checked in UserDefinedExecutableFunctionFactory.
    bool can_have_parameters = (function != nullptr);

    if (!function)
    {
        if (const auto * create_function_query = typeid_cast<const ASTCreateWasmFunctionQuery *>(user_defined_function.get()))
        {
            UNUSED(create_function_query);
            UserDefinedWebAssemblyFunctionFactory::checkWebAssemblyIsAvailable(scope.context);
            function = UserDefinedWebAssemblyFunctionFactory::instance().get(function_name, scope.context);
        }
    }

    FunctionBasePtr * function_base_cache = nullptr;

    if (!function)
    {
        function = FunctionFactory::instance().tryGet(function_name, scope.context);
        can_have_parameters = false;

        /// This is a hack to allow a query like `select randConstant(), randConstant(), randConstant()`.
        /// A non-deterministic function like `randConstant` returns a different value on every `build`,
        /// so syntactically-identical calls must share the same built `FunctionBase` to fold to the same
        /// constant. We deduplicate by tree hash to achieve that.
        ///
        /// Deterministic functions never need this (same arguments always produce the same result), and
        /// `getTreeHash` walks the whole argument subtree, dominating analysis of deeply nested expressions.
        /// So the hash and the cache are computed only for non-deterministic functions.
        ///
        /// The cache is global across the whole query, so only a function that is stable inside a query
        /// may be shared. A function that is not deterministic in the scope of a query (`getSetting`,
        /// `getSettingOrDefault`, `blockNumber`, `rowNumberInAllBlocks`, ...) must NOT be shared: its
        /// result depends on the scope it is evaluated in - e.g. `SETTINGS` can change what `getSetting`
        /// returns for every scope - and a stateful function keeps counting inside the single instance
        /// the cache hands out.
        ///
        /// The hash ignores aliases. An alias renames an expression and never changes the value the
        /// `FunctionBase` captures, so `randConstant() AS x, randConstant() AS y` must share what
        /// `randConstant(), randConstant()` shares. What separates two calls is their arguments, which
        /// the hash still covers: `randConstant(1)` and `randConstant(2)` keep their own values, and that
        /// is the documented way to ask for two different constants in one query.
        if (function && !function->isDeterministic() && !function->isStateful()
            && function->isDeterministicInScopeOfQuery())
        {
            auto hash = function_node_ptr->getTreeHash({ .compare_aliases = false });
            function_base_cache = &functions_cache[hash];
        }
    }

    if (function)
    {
        checkFunctionNodeHasEmptyNullsAction(function_node);
    }
    else
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
        {
            VectorWithMemoryTracking<std::string> possible_function_names;

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
                "Function with name {} does not exist{}. In scope {}",
                backQuote(function_name),
                getHintsErrorMessageSuffix(hints),
                scope.scope_node->formatASTForErrorMessage());
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

    if (!parameters.empty() && !can_have_parameters)
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

        /** Validate every lambda argument BEFORE resolving any lambda body. getLambdaArgumentTypes
          * only fills in the placeholder argument types for positions that actually expect a lambda;
          * where it does not (e.g. arrayFold's accumulator: arrayFold(lambda, arr, another_lambda)),
          * the placeholder DataTypeFunction keeps null argument types. Those nulls must be rejected up
          * front: a later lambda that stays unresolved can be copied into an earlier lambda's argument
          * type, so resolving the earlier lambda's body first would take the non-lambda path and
          * dereference the null return type (FunctionArrayMapped::getReturnTypeImpl).
          */
        for (auto & function_lambda_argument_index : function_lambda_arguments_indexes)
        {
            const auto * function_data_type = typeid_cast<const DataTypeFunction *>(argument_types[function_lambda_argument_index].get());
            if (!function_data_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function '{}' expected function data type for lambda argument with index {}. Actual: {}. In scope {}",
                    function_name,
                    function_lambda_argument_index,
                    argument_types[function_lambda_argument_index]->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            for (const auto & lambda_argument_type : function_data_type->getArgumentTypes())
                if (!lambda_argument_type)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function '{}' does not expect a lambda expression as argument {}. In scope {}",
                        function_name,
                        function_lambda_argument_index + 1,
                        scope.scope_node->formatASTForErrorMessage());
        }

        ProjectionNames lambda_projection_names;
        for (auto & function_lambda_argument_index : function_lambda_arguments_indexes)
        {
            auto & lambda_argument = function_arguments[function_lambda_argument_index];
            auto lambda_to_resolve = lambda_argument->clone();
            auto & lambda_to_resolve_typed = lambda_to_resolve->as<LambdaNode &>();

            const auto & lambda_argument_names = lambda_to_resolve_typed.getArguments().getNames();
            size_t lambda_arguments_size = lambda_argument_names.size();

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

            /** Check that getLambdaArgumentTypes actually resolved the types for this lambda.
              * If the argument types are still null, the function did not expect a lambda at this position.
              * This can happen when a lambda is passed where a concrete value is expected,
              * e.g. arrayFold(lambda, array, another_lambda_instead_of_initial_value).
              */
            for (size_t i = 0; i < function_data_type_arguments_size; ++i)
            {
                if (!function_data_type_argument_types[i])
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function '{}' does not expect a lambda expression as argument {}. In scope {}",
                        function_name,
                        function_lambda_argument_index + 1,
                        scope.scope_node->formatASTForErrorMessage());
            }

            QueryTreeNodes lambda_arguments;
            lambda_arguments.reserve(lambda_arguments_size);

            IdentifierResolveScope & lambda_scope = createIdentifierResolveScope(lambda_to_resolve, &scope /*parent_scope*/);
            for (size_t i = 0; i < lambda_arguments_size; ++i)
            {
                const auto & argument_type = function_data_type_argument_types[i];
                auto column_name_and_type = NameAndTypePair{lambda_argument_names[i], argument_type};
                lambda_arguments.push_back(std::make_shared<ColumnNode>(std::move(column_name_and_type), lambda_to_resolve_typed.getArgumentsTyped()));
            }

            /// Record the resolved argument types on the lambda arguments node so the planner
            /// can reconstruct (name, type) pairs without the per-argument column list.
            lambda_to_resolve_typed.getArguments().resolve(function_data_type_argument_types);

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
            const auto & second_argument_constant_column = second_argument_constant_node->getColumn();
            const auto & second_argument_constant_type = second_argument_constant_node->getResultType();

            const auto & settings = scope.context->getSettingsRef();

            auto result_block = getSetElementsForConstantValue(
                first_argument_constant_type, second_argument_constant_column, second_argument_constant_type,
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
        /** Do not use cache for functions with lambda arguments.
          * The cache key (tree hash) is computed before lambdas are resolved,
          * so the same AST structure with different resolved lambda types
          * would incorrectly share the cached function base.
          */
        if (function_base_cache && !has_lambda_arguments)
        {
            auto & cached_function = *function_base_cache;
            if (!cached_function)
                cached_function = function->build(argument_columns);

            function_base = cached_function;
        }
        else
            function_base = function->build(argument_columns);

        bool allow_constant_folding = true;

        auto * nearest_join_query_scope = scope.joins_count > 0 ? scope.getNearestQueryScope() : nullptr;
        auto * nearest_join_query_scope_query_node = nearest_join_query_scope ? nearest_join_query_scope->scope_node->as<QueryNode>() : nullptr;
        const auto * join_node = nearest_join_query_scope_query_node ? nearest_join_query_scope_query_node->getJoinTreeNode()->as<JoinNode>() : nullptr;
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

                /// All constant (literal) columns in block are added with size 1.
                /// But if there was no columns in block before executing a function, the result has size 0.
                /// Change the size to 1.
                if (column && column->empty() && isColumnConst(*column))
                    column = column->cloneResized(1);
            }
            else
            {
                column = function_base->getConstantResultForNonConstArguments(argument_columns, result_type);
            }

            if (column && !columnMatchesType(*column, *result_type))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected return type from {}. Expected {}. Got {}",
                    function->getName(),
                    result_type->getName(),
                    column->getName());

            const bool is_deterministic = all_arguments_are_deterministic && function->isDeterministic();

            /** Do not perform constant folding if there are aggregate or arrayJoin functions inside function.
              * Example: SELECT toTypeName(sum(number)) FROM numbers(10);
              */
            const auto * column_const = column ? typeid_cast<const ColumnConst *>(column.get()) : nullptr;
            if (column_const && !column_const->getDataColumn().isDummy() &&
                !hasAggregateFunctionNodes(node) && !hasFunctionNode(node, "arrayJoin") &&
                /// Sanity check: do not convert large columns to constants
                column->byteSize() < 1_MiB)
            {
                /// Replace function node with result constant node
                constant_node = std::make_shared<ConstantNode>(ConstantValue{ column_const->getPtr(), std::move(result_type) }, node, is_deterministic);
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

    /// A resolved FunctionNode must produce exactly one projection name. Surface any violation here
    /// instead of letting it cascade into a generic LOGICAL_ERROR in the expression-list resolver.
    chassert(result_projection_names.size() == 1);

    return result_projection_names;
}
}

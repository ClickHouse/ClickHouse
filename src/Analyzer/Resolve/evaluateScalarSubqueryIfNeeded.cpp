#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnTuple.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Planner/Utils.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Chunk.h>

#include <Core/Settings.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledQuota.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/ProfileEvents.h>

#include <Poco/String.h>
#include <fmt/format.h>

#include <algorithm>
#include <ranges>

namespace ProfileEvents
{
    extern const Event ScalarSubqueriesGlobalCacheHit;
    extern const Event ScalarSubqueriesLocalCacheHit;
    extern const Event ScalarSubqueriesCacheMiss;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

namespace Setting
{
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsBool extremes;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsString implicit_table_at_top_level;
    extern const SettingsUInt64 use_structure_from_insertion_table_in_table_functions;
    extern const SettingsBool enable_scalar_subquery_optimization;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool optimize_merge_scalar_subqueries;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 max_columns_to_read;
    extern const SettingsUInt64 max_temporary_columns;
    extern const SettingsUInt64 max_temporary_non_const_columns;
}

namespace
{

bool subtreeHasViewSource(const IQueryTreeNode * node, const Context & context)
{
    if (!node)
        return false;

    if (const auto * table_node = node->as<TableNode>())
    {
        if (table_node->getStorageID().getFullNameNotQuoted() == context.getViewSource()->getStorageID().getFullNameNotQuoted())
            return true;
    }

    for (const auto & child : node->getChildren())
        if (subtreeHasViewSource(child.get(), context))
            return true;

    return false;
}

void wrapScalarBlockWithNullableOrTuple(Block & block)
{
    block = materializeBlock(block);
    if (block.columns() == 1)
    {
        auto & column = block.getByPosition(0);
        /// Here we wrap type to nullable if we can.
        /// It is needed cause if subquery return no rows, it's result will be Null.
        /// In case of many columns, do not check it cause tuple can't be nullable.
        if (!column.type->isNullable() && column.type->canBeInsideNullable())
        {
            column.type = makeNullable(column.type);
            column.column = makeNullable(column.column);
        }
    }
    else
    {
        /** Make unique column names for tuple.
          *
          * Example: SELECT (SELECT 2 AS x, x)
          */
        makeUniqueColumnNamesInBlock(block);
        block = Block({{
            ColumnTuple::create(block.getColumns()),
            std::make_shared<DataTypeTuple>(block.getDataTypes(), block.getNames()),
            "tuple"
        }});
    }
}

ContextMutablePtr createSubqueryExecutionContext(const ContextPtr & source_context)
{
    auto subquery_context = Context::createCopy(source_context);

    Settings subquery_settings = source_context->getSettingsCopy();
    subquery_settings[Setting::max_result_rows] = 1;
    subquery_settings[Setting::extremes] = false;
    subquery_settings[Setting::implicit_table_at_top_level] = "";
    /// When execute `INSERT INTO t WITH ... SELECT ...`, it may lead to `Unknown columns`
    /// exception with this settings enabled(https://github.com/ClickHouse/ClickHouse/issues/52494).
    subquery_settings[Setting::use_structure_from_insertion_table_in_table_functions] = false;
    subquery_settings[Setting::allow_experimental_parallel_reading_from_replicas] = 0;
    subquery_context->setSettings(subquery_settings);

    return subquery_context;
}

bool hasSimpleSelectQueryClauses(const QueryNode & query)
{
    return !query.isCorrelated()
        && !query.hasSettingsChanges()
        && !query.isCTE()
        && !query.isMaterialized()
        && !query.isRecursiveWith()
        && !query.isDistinct()
        && !query.isGroupByAll()
        && !query.isGroupByWithTotals()
        && !query.isGroupByWithRollup()
        && !query.isGroupByWithCube()
        && !query.isGroupByWithGroupingSets()
        && !query.isOrderByAll()
        && !query.isLimitByAll()
        && !query.isLimitAfterAll()
        && !query.isLimitWithTies()
        && !query.isShuffle()
        && !query.hasWith()
        && !query.hasPrewhere()
        && query.hasWhere()
        && !query.hasGroupBy()
        && !query.hasHaving()
        && !query.hasWindow()
        && !query.hasQualify()
        && !query.hasOrderBy()
        && !query.hasInterpolate()
        && !query.hasLimitByLimit()
        && !query.hasLimitByOffset()
        && !query.hasLimitBy()
        && !query.hasLimit()
        && !query.hasLimitAfter()
        && !query.hasLimitUntil()
        && !query.hasOffset();
}

bool hasSupportedTableModifiers(const std::optional<TableExpressionModifiers> & modifiers)
{
    if (!modifiers)
        return true;

    return !modifiers->hasSampleSizeRatio()
        && !modifiers->hasSampleOffsetRatio()
        && !modifiers->hasStream();
}

bool isNarrowColumnType(const DataTypePtr & type)
{
    auto unwrapped = removeNullable(removeLowCardinality(type));
    if (unwrapped->isValueRepresentedByNumber())
        return true;

    return isFixedString(unwrapped) && unwrapped->getSizeOfValueInMemory() <= 32;
}

bool areTypesSafeToCompare(const DataTypePtr & lhs, const DataTypePtr & rhs)
{
    auto t0 = removeNullable(removeLowCardinality(lhs));
    auto t1 = removeNullable(removeLowCardinality(rhs));

    if (t0->onlyNull() || t1->onlyNull())
        return !t0->hasDynamicSubcolumnsData() && !t1->hasDynamicSubcolumnsData();

    if ((isInteger(t0) || isFloat(t0)) && (isInteger(t1) || isFloat(t1)))
        return true;

    /// Rescaling a decimal or a date/time to a different scale can overflow on rows that the
    /// subquery's own filter would have excluded, so only same-scale decimals may be compared.
    if (isDecimal(t0) && isDecimal(t1))
        return getDecimalScale(*t0) == getDecimalScale(*t1)
            && !WhichDataType(t0).isDecimal256() && !WhichDataType(t1).isDecimal256();

    if (isDecimal(t0) || isDecimal(t1))
        return false;

    if (isStringOrFixedString(t0) && isStringOrFixedString(t1))
        return true;

    return t0->equals(*t1) && !t0->hasDynamicSubcolumnsData();
}

bool isAllowedAggregateName(const String & name_lowercase, bool & is_count)
{
    if (name_lowercase == "count" || name_lowercase == "countornull")
    {
        is_count = true;
        return true;
    }

    is_count = false;
    return name_lowercase == "sum" || name_lowercase == "sumornull"
        || name_lowercase == "avg" || name_lowercase == "avgornull"
        || name_lowercase == "min" || name_lowercase == "minornull"
        || name_lowercase == "max" || name_lowercase == "maxornull";
}

bool isSafeLogicalOrNullFunction(const String & name_lowercase)
{
    return name_lowercase == "and" || name_lowercase == "or" || name_lowercase == "xor"
        || name_lowercase == "not" || name_lowercase == "isnull" || name_lowercase == "isnotnull";
}

bool isSafeComparisonFunction(const String & name_lowercase)
{
    return name_lowercase == "equals" || name_lowercase == "notequals"
        || name_lowercase == "less" || name_lowercase == "greater"
        || name_lowercase == "lessorequals" || name_lowercase == "greaterorequals";
}

bool isSafeResolvedExpression(
    const QueryTreeNodePtr & expr,
    const TableNode & expected_table,
    const ColumnsDescription & columns_desc,
    bool & all_columns_narrow)
{
    if (!expr)
        return false;

    if (const auto * constant = expr->as<ConstantNode>())
        return constant->isDeterministic() && !constant->hasSourceExpression();

    if (const auto * column = expr->as<ColumnNode>())
    {
        if (column->getColumnSourceOrNull().get() != &expected_table || column->hasExpression())
            return false;

        const auto & col_name = column->getColumnName();
        if (!columns_desc.hasPhysical(col_name))
            return false;

        if (const auto default_desc = columns_desc.getDefault(col_name); default_desc && default_desc->expression)
            return false;

        if (!isNarrowColumnType(column->getColumnType()))
            all_columns_narrow = false;

        return true;
    }

    const auto * function = expr->as<FunctionNode>();
    if (!function || !function->isOrdinaryFunction() || !function->isResolved() || function->hasWindow())
        return false;

    if (!function->getParameters().getNodes().empty() || function->getNullsAction() != NullsAction::EMPTY)
        return false;

    if (auto fn_base = function->getFunction(); !fn_base || !fn_base->isDeterministic() || !fn_base->isDeterministicInScopeOfQuery())
        return false;

    const String lower_name = Poco::toLower(function->getFunctionName());
    const auto & args = function->getArguments().getNodes();

    if (isSafeComparisonFunction(lower_name))
    {
        if (args.size() != 2)
            return false;
        if (!areTypesSafeToCompare(args[0]->getResultType(), args[1]->getResultType()))
            return false;
    }
    else if (isSafeLogicalOrNullFunction(lower_name))
    {
        if (args.empty())
            return false;
    }
    else
    {
        return false;
    }

    return std::ranges::all_of(args, [&](const QueryTreeNodePtr & child)
    {
        return isSafeResolvedExpression(child, expected_table, columns_desc, all_columns_narrow);
    });
}

void splitConjunctionsQueryTree(const QueryTreeNodePtr & node, QueryTreeNodes & out)
{
    if (!node)
        return;

    if (const auto * fn = node->as<FunctionNode>();
        fn && fn->isOrdinaryFunction() && fn->getFunctionName() == "and" && fn->getParameters().getNodes().empty())
    {
        for (const auto & child : fn->getArguments().getNodes())
            splitConjunctionsQueryTree(child, out);
        return;
    }

    out.push_back(node);
}

bool expressionReferencesAnyColumn(const QueryTreeNodePtr & node, const std::unordered_set<String> & target_columns)
{
    if (!node || target_columns.empty())
        return false;

    if (const auto * col = node->as<ColumnNode>())
        return target_columns.contains(col->getColumnName());

    for (const auto & child : node->getChildren())
        if (child && expressionReferencesAnyColumn(child, target_columns))
            return true;

    return false;
}

std::unordered_set<String> collectIndexAndKeyColumns(const StorageInMemoryMetadata & metadata)
{
    std::unordered_set<String> result;
    const auto key_column_lists = {
        metadata.getColumnsRequiredForPartitionKey(),
        metadata.getColumnsRequiredForSortingKey(),
        metadata.getColumnsRequiredForPrimaryKey(),
        metadata.getColumnsRequiredForSampling()};
    for (const auto & names : key_column_lists)
    {
        result.insert(names.begin(), names.end());
    }

    for (const auto & index : metadata.getSecondaryIndices())
    {
        result.insert(index.column_names.begin(), index.column_names.end());
        if (index.expression)
        {
            auto required = index.expression->getRequiredColumns();
            result.insert(required.begin(), required.end());
        }
    }

    return result;
}

struct ResolvedScalarCandidate
{
    QueryTreeNodePtrWithHash node_with_hash;
    String str_hash;
    TableNodePtr table_node;
    QueryTreeNodePtr agg_node_ptr;
    const FunctionNode * agg_node = nullptr;
    DataTypePtr original_result_type;
    QueryTreeNodePtr where_expr;
    QueryTreeNodes conjuncts;
    bool all_columns_narrow = true;
    String sub_group_key;
};

/// Identifies the table a candidate reads, including the `FINAL` modifier.
String computeCandidateTableKey(const TableNode & table_node)
{
    String key = table_node.isTemporaryTable()
        ? ("T:" + table_node.getTemporaryTableName())
        : ("C:" + table_node.getStorageID().getFullNameNotQuoted());

    if (const auto & modifiers = table_node.getTableExpressionModifiers(); modifiers && modifiers->hasFinal())
        key += ":FINAL";

    return key;
}

String computeCandidateSubGroupKey(
    const TableNode & table_node,
    const QueryTreeNodes & conjuncts,
    bool all_columns_narrow,
    const std::unordered_set<String> & index_columns)
{
    String key = computeCandidateTableKey(table_node);

    std::vector<String> hash_parts;
    QueryTreeNodePtrWithHashIgnoreAliasesSet seen;

    for (const auto & conjunct : conjuncts)
    {
        if (all_columns_narrow && !expressionReferencesAnyColumn(conjunct, index_columns))
            continue;

        QueryTreeNodePtrWithHashIgnoreAliases item(conjunct);
        if (seen.insert(item).second)
            hash_parts.push_back(fmt::format("{:016x}{:016x}", item.hash.high64, item.hash.low64));
    }

    std::ranges::sort(hash_parts);
    key += all_columns_narrow ? "|K:" : "|W:";
    for (const auto & part : hash_parts)
    {
        key += part;
        key += ',';
    }

    return key;
}

std::optional<ResolvedScalarCandidate> extractResolvedScalarCandidate(
    const QueryTreeNodePtr & resolved_node,
    const QueryTreeNodePtrWithHash & node_with_hash,
    bool view_source_in_subqueries)
{
    const auto * query = resolved_node->as<QueryNode>();
    if (!query || !query->isResolved() || !hasSimpleSelectQueryClauses(*query))
        return std::nullopt;

    const auto & query_ctx = query->getContext();
    if (getViewSourceIfRead(query_ctx, view_source_in_subqueries) && subtreeHasViewSource(query, *query_ctx))
        return std::nullopt;

    auto table_node = std::dynamic_pointer_cast<TableNode>(query->getJoinTreeNode());
    if (!table_node || table_node->isMaterializedCTE() || !hasSupportedTableModifiers(table_node->getTableExpressionModifiers()))
        return std::nullopt;

    const auto & storage = table_node->getStorage();
    const auto & snapshot = table_node->getStorageSnapshot();
    if (!storage || !snapshot || !snapshot->metadata)
        return std::nullopt;

    const bool is_merge_tree = dynamic_cast<const MergeTreeData *>(storage.get()) != nullptr;
    const bool is_memory = storage->getName() == "Memory";
    if (!is_merge_tree && !is_memory)
        return std::nullopt;

    const auto & metadata = *snapshot->metadata;
    if (metadata.hasProjections())
        return std::nullopt;

    const auto & projection_nodes = query->getProjection().getNodes();
    if (projection_nodes.size() != 1 || query->getProjectionColumns().size() != 1)
        return std::nullopt;

    const auto & agg_node_ptr = projection_nodes[0];
    const auto * agg_node = agg_node_ptr->as<FunctionNode>();
    if (!agg_node || !agg_node->isAggregateFunction() || !agg_node->isResolved() || agg_node->hasWindow())
        return std::nullopt;

    if (!agg_node->getParameters().getNodes().empty() || agg_node->getNullsAction() != NullsAction::EMPTY)
        return std::nullopt;

    bool is_count = false;
    if (!isAllowedAggregateName(Poco::toLower(agg_node->getFunctionName()), is_count))
        return std::nullopt;

    const auto & agg_args = agg_node->getArguments().getNodes();
    if (is_count ? (agg_args.size() > 1) : (agg_args.size() != 1))
        return std::nullopt;

    const auto & columns_desc = metadata.getColumns();
    bool all_columns_narrow = true;

    for (const auto & arg : agg_args)
    {
        if (!isSafeResolvedExpression(arg, *table_node, columns_desc, all_columns_narrow))
            return std::nullopt;
    }

    if (!isSafeResolvedExpression(query->getWhere(), *table_node, columns_desc, all_columns_narrow))
        return std::nullopt;

    QueryTreeNodes conjuncts;
    splitConjunctionsQueryTree(query->getWhere(), conjuncts);

    const auto index_columns = collectIndexAndKeyColumns(metadata);
    String sub_group_key = computeCandidateSubGroupKey(*table_node, conjuncts, all_columns_narrow, index_columns);

    return ResolvedScalarCandidate{
        .node_with_hash = node_with_hash,
        .str_hash = DB::toString(node_with_hash.hash),
        .table_node = std::move(table_node),
        .agg_node_ptr = agg_node_ptr,
        .agg_node = agg_node,
        .original_result_type = query->getProjectionColumns()[0].type,
        .where_expr = query->getWhere(),
        .conjuncts = std::move(conjuncts),
        .all_columns_narrow = all_columns_narrow,
        .sub_group_key = std::move(sub_group_key),
    };
}

bool hasAnyExpressionAlias(const QueryTreeNodePtr & node)
{
    if (!node)
        return false;
    if (node->hasAlias())
        return true;

    return std::ranges::any_of(node->getChildren(), [](const QueryTreeNodePtr & child)
    {
        return child && hasAnyExpressionAlias(child);
    });
}

bool isNameShadowedInScopeChain(const String & name, const IdentifierResolveScope * start_scope, bool check_functions)
{
    for (const auto * current = start_scope; current != nullptr; current = current->parent_scope)
    {
        if (check_functions)
        {
            if (current->aliases.alias_name_to_lambda_node.contains(name)
                || current->global_with_aliases.alias_name_to_lambda_node.contains(name))
                return true;
        }
        else
        {
            if (current->aliases.alias_name_to_expression_node.contains(name)
                || current->global_with_aliases.alias_name_to_expression_node.contains(name)
                || current->cte_name_to_query_node.contains(name))
                return true;
        }
    }

    return false;
}

bool isCustomFunctionName(const String & function_name, const ContextPtr & context, const IdentifierResolveScope * scope_chain)
{
    if (isNameShadowedInScopeChain(function_name, scope_chain, /*check_functions=*/ true))
        return true;
    if (UserDefinedSQLFunctionFactory::instance().tryGet(function_name))
        return true;
    if (UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, context)) /// NOLINT(readability-static-accessed-through-instance)
        return true;
    return false;
}

DataTypePtr validateUnresolvedExpressionAndInferType(
    const QueryTreeNodePtr & expr,
    const TableNode & target_table,
    const String & table_qualifier,
    bool table_has_alias,
    const ColumnsDescription & columns_desc,
    const ContextPtr & query_context,
    const IdentifierResolveScope * parent_scope)
{
    if (!expr)
        return nullptr;

    if (const auto * constant = expr->as<ConstantNode>())
    {
        if (!constant->isDeterministic() || constant->hasSourceExpression())
            return nullptr;
        return constant->getResultType();
    }

    if (const auto * id_node = expr->as<IdentifierNode>())
    {
        const auto & id = id_node->getIdentifier();
        if (id.empty() || isNameShadowedInScopeChain(id[0], parent_scope, /*check_functions=*/ false))
            return nullptr;

        String col_name;
        if (id.getPartsSize() == 1)
        {
            col_name = id[0];
        }
        else if (id.getPartsSize() == 2)
        {
            if (id[0] != table_qualifier)
                return nullptr;
            col_name = id[1];
        }
        else if (id.getPartsSize() == 3 && !table_has_alias && !target_table.isTemporaryTable())
        {
            const auto & storage_id = target_table.getStorageID();
            if (id[0] != storage_id.getDatabaseName() || id[1] != storage_id.getTableName())
                return nullptr;
            col_name = id[2];
        }
        else
        {
            return nullptr;
        }

        if (!columns_desc.hasPhysical(col_name))
            return nullptr;

        if (const auto default_desc = columns_desc.getDefault(col_name); default_desc && default_desc->expression)
            return nullptr;

        if (!target_table.isTemporaryTable())
        {
            const auto & storage_id = target_table.getStorageID();
            const auto access_rights = query_context->getAccess()->getAccessRightsWithImplicit();
            if (!access_rights->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, col_name))
                return nullptr;
        }

        return columns_desc.getPhysical(col_name).type;
    }

    const auto * fn = expr->as<FunctionNode>();
    if (!fn || fn->hasWindow() || !fn->getParameters().getNodes().empty() || fn->getNullsAction() != NullsAction::EMPTY)
        return nullptr;

    if (isCustomFunctionName(fn->getFunctionName(), query_context, parent_scope))
        return nullptr;

    const String lower_name = Poco::toLower(fn->getFunctionName());
    const auto & args = fn->getArguments().getNodes();

    if (isSafeComparisonFunction(lower_name))
    {
        if (args.size() != 2)
            return nullptr;

        auto lhs_type = validateUnresolvedExpressionAndInferType(
            args[0], target_table, table_qualifier, table_has_alias, columns_desc, query_context, parent_scope);
        auto rhs_type = validateUnresolvedExpressionAndInferType(
            args[1], target_table, table_qualifier, table_has_alias, columns_desc, query_context, parent_scope);
        if (!lhs_type || !rhs_type || !areTypesSafeToCompare(lhs_type, rhs_type))
            return nullptr;

        return std::make_shared<DataTypeUInt8>();
    }

    if (lower_name == "not" || lower_name == "isnull" || lower_name == "isnotnull")
    {
        if (args.size() != 1)
            return nullptr;

        auto arg_type = validateUnresolvedExpressionAndInferType(
            args[0], target_table, table_qualifier, table_has_alias, columns_desc, query_context, parent_scope);
        return arg_type ? std::make_shared<DataTypeUInt8>() : nullptr;
    }

    if (lower_name == "and" || lower_name == "or" || lower_name == "xor")
    {
        if (args.size() < 2)
            return nullptr;

        for (const auto & child : args)
        {
            auto child_type = validateUnresolvedExpressionAndInferType(
                child, target_table, table_qualifier, table_has_alias, columns_desc, query_context, parent_scope);
            if (!child_type || !isNarrowColumnType(child_type))
                return nullptr;
        }

        return std::make_shared<DataTypeUInt8>();
    }

    return nullptr;
}

bool isUnresolvedSiblingSafeToResolve(
    const QueryNode & sibling,
    const TableNode & target_table,
    const ColumnsDescription & columns_desc,
    const IdentifierResolveScope * parent_scope)
{
    if (!hasSimpleSelectQueryClauses(sibling))
        return false;

    const auto * table_id_node = sibling.getJoinTreeNode()->as<IdentifierNode>();
    if (!table_id_node)
        return false;

    if (table_id_node->getTableExpressionModifiers() != target_table.getTableExpressionModifiers())
        return false;

    const auto & table_id = table_id_node->getIdentifier();
    const auto & sibling_ctx = sibling.getContext();

    if (table_id.getPartsSize() == 1)
    {
        if (parent_scope)
        {
            for (const auto * s = parent_scope; s != nullptr; s = s->parent_scope)
                if (s->cte_name_to_query_node.contains(table_id[0]))
                    return false;
        }

        if (target_table.isTemporaryTable())
        {
            if (table_id[0] != target_table.getTemporaryTableName())
                return false;
        }
        else
        {
            if (sibling_ctx->tryResolveStorageID(StorageID{"", table_id[0]}, Context::ResolveExternal))
                return false;

            const auto & expected_id = target_table.getStorageID();
            if (expected_id.getDatabaseName() != sibling_ctx->getCurrentDatabase()
                || expected_id.getTableName() != table_id[0])
                return false;
        }
    }
    else if (table_id.getPartsSize() == 2 && !target_table.isTemporaryTable())
    {
        const auto & expected_id = target_table.getStorageID();
        if (expected_id.getDatabaseName() != table_id[0] || expected_id.getTableName() != table_id[1])
            return false;
    }
    else
    {
        return false;
    }

    if (hasAnyExpressionAlias(sibling.getProjectionNode()) || hasAnyExpressionAlias(sibling.getWhere()))
        return false;

    const auto & proj_nodes = sibling.getProjection().getNodes();
    if (proj_nodes.size() != 1)
        return false;

    const auto * agg_fn = proj_nodes[0]->as<FunctionNode>();
    if (!agg_fn || agg_fn->hasWindow() || !agg_fn->getParameters().getNodes().empty() || agg_fn->getNullsAction() != NullsAction::EMPTY)
        return false;

    if (isCustomFunctionName(agg_fn->getFunctionName(), sibling_ctx, parent_scope))
        return false;

    bool is_count = false;
    const String lower_agg = Poco::toLower(agg_fn->getFunctionName());
    if (!isAllowedAggregateName(lower_agg, is_count))
        return false;

    const String table_qualifier = table_id_node->hasAlias() ? table_id_node->getAlias() : table_id.back();
    const bool table_has_alias = table_id_node->hasAlias();

    const auto & agg_args = agg_fn->getArguments().getNodes();
    if (is_count)
    {
        if (agg_args.size() > 1)
            return false;
        if (agg_args.size() == 1)
        {
            if (const auto * matcher = agg_args[0]->as<MatcherNode>())
            {
                if (!matcher->isAsteriskMatcher() || !matcher->isUnqualified() || !matcher->getColumnTransformers().getNodes().empty())
                    return false;
            }
            else if (!validateUnresolvedExpressionAndInferType(
                         agg_args[0], target_table, table_qualifier, table_has_alias, columns_desc, sibling_ctx, parent_scope))
            {
                return false;
            }
        }
    }
    else
    {
        if (agg_args.size() != 1)
            return false;

        auto arg_type = validateUnresolvedExpressionAndInferType(
            agg_args[0], target_table, table_qualifier, table_has_alias, columns_desc, sibling_ctx, parent_scope);
        if (!arg_type)
            return false;

        auto unwrapped_arg = removeNullable(removeLowCardinality(arg_type));
        if ((lower_agg.starts_with("sum") || lower_agg.starts_with("avg")) && (!isNumber(unwrapped_arg) || isBool(unwrapped_arg)))
            return false;
    }

    auto where_type = validateUnresolvedExpressionAndInferType(
        sibling.getWhere(), target_table, table_qualifier, table_has_alias, columns_desc, sibling_ctx, parent_scope);
    return where_type && isNarrowColumnType(where_type);
}

void collectCandidateSubqueryNodes(
    const QueryTreeNodePtr & node,
    const ContextPtr & context,
    const IdentifierResolveScope * parent_scope,
    QueryTreeNodes & out)
{
    if (!node)
        return;

    const auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY)
    {
        out.push_back(node);
        return;
    }

    if (node_type == QueryTreeNodeType::UNION
        || node_type == QueryTreeNodeType::LAMBDA
        || node_type == QueryTreeNodeType::WINDOW
        || node_type == QueryTreeNodeType::TABLE
        || node_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        return;
    }

    if (const auto * fn = node->as<FunctionNode>())
    {
        if (fn->hasWindow())
            return;

        const auto & fn_name = fn->getFunctionName();
        const String lower_name = Poco::toLower(fn_name);
        if (lower_name == "exists" || lower_name == "notexists" || isNameOfInFunction(fn_name))
            return;

        if (!fn->isResolved() && isCustomFunctionName(fn_name, context, parent_scope))
            return;

        /// Do not pre-evaluate inside constant-short-circuitable AND/OR trees where the analyzer's
        /// early short-circuit optimization skips executing the dead scalar subquery branch.
        if (!fn->isResolved() && (lower_name == "and" || lower_name == "or"))
        {
            const bool has_const_child = std::ranges::any_of(fn->getArguments().getNodes(), [](const QueryTreeNodePtr & arg)
            {
                return arg && arg->as<ConstantNode>() != nullptr;
            });
            if (has_const_child)
                return;
        }

        for (const auto & arg : fn->getArguments().getNodes())
            collectCandidateSubqueryNodes(arg, context, parent_scope, out);
        return;
    }

    for (const auto & child : node->getChildren())
        collectCandidateSubqueryNodes(child, context, parent_scope, out);
}

void rebindColumnsToTable(const QueryTreeNodePtr & node, const TableExpressionNodePtr & target_table)
{
    if (!node)
        return;

    if (auto * col = node->as<ColumnNode>())
    {
        col->setColumnSource(target_table);
        return;
    }

    for (const auto & child : node->getChildren())
        if (child)
            rebindColumnsToTable(child, target_table);
}

QueryTreeNodePtr cloneAndRebindToTable(const QueryTreeNodePtr & node, const TableExpressionNodePtr & target_table)
{
    auto cloned = node->clone();
    cloned->removeAlias();
    rebindColumnsToTable(cloned, target_table);
    return cloned;
}

QueryTreeNodePtr ensureBoolCondition(QueryTreeNodePtr cond, const ContextPtr & context)
{
    const auto & type = cond->getResultType();
    if (!type->lowCardinality() && isUInt8(removeNullable(type)))
        return cond;

    return createResolvedFunction(context, "toBool", {std::move(cond)});
}

QueryTreeNodePtr combineConditions(
    const QueryTreeNodes & conditions,
    const String & op_name,
    const TableExpressionNodePtr & target_table,
    const ContextPtr & context)
{
    if (conditions.empty())
        return nullptr;

    if (conditions.size() == 1)
        return cloneAndRebindToTable(conditions[0], target_table);

    QueryTreeNodes args;
    args.reserve(conditions.size());
    for (const auto & cond : conditions)
        args.push_back(ensureBoolCondition(cloneAndRebindToTable(cond, target_table), context));

    return createResolvedFunction(context, op_name, std::move(args));
}

struct SplitQueryTreeConditions
{
    QueryTreeNodes shared;
    std::vector<QueryTreeNodes> own;
};

SplitQueryTreeConditions splitSharedQueryTreeConjuncts(const std::vector<ResolvedScalarCandidate> & group)
{
    SplitQueryTreeConditions result;
    result.own.resize(group.size());

    QueryTreeNodePtrWithHashIgnoreAliasesSet shared_set;
    for (const auto & conjunct : group.front().conjuncts)
    {
        QueryTreeNodePtrWithHashIgnoreAliases key(conjunct);
        if (shared_set.contains(key))
            continue;

        const bool in_all = std::ranges::all_of(group, [&](const ResolvedScalarCandidate & candidate)
        {
            return std::ranges::any_of(candidate.conjuncts, [&](const QueryTreeNodePtr & other)
            {
                return QueryTreeNodePtrWithHashIgnoreAliases(other) == key;
            });
        });

        if (in_all)
        {
            shared_set.insert(key);
            result.shared.push_back(conjunct);
        }
    }

    for (size_t i = 0; i < group.size(); ++i)
    {
        for (const auto & conjunct : group[i].conjuncts)
        {
            if (!shared_set.contains(QueryTreeNodePtrWithHashIgnoreAliases(conjunct)))
                result.own[i].push_back(conjunct);
        }
    }

    return result;
}

QueryTreeNodePtr buildMergedWhereQueryTree(
    const SplitQueryTreeConditions & split,
    const TableExpressionNodePtr & target_table,
    const ContextPtr & context)
{
    QueryTreeNodePtr shared = combineConditions(split.shared, "and", target_table, context);

    QueryTreeNodes unique_alternatives;
    QueryTreeNodePtrWithHashIgnoreAliasesSet seen_alternatives;

    for (const auto & own : split.own)
    {
        if (own.empty())
            return shared;

        auto branch = combineConditions(own, "and", target_table, context);
        QueryTreeNodePtrWithHashIgnoreAliases branch_key(branch);
        if (seen_alternatives.insert(branch_key).second)
            unique_alternatives.push_back(std::move(branch));
    }

    QueryTreeNodePtr disjunction;
    if (unique_alternatives.size() == 1)
    {
        disjunction = std::move(unique_alternatives[0]);
    }
    else
    {
        QueryTreeNodes bool_alternatives;
        bool_alternatives.reserve(unique_alternatives.size());
        for (auto & alt : unique_alternatives)
            bool_alternatives.push_back(ensureBoolCondition(std::move(alt), context));
        disjunction = createResolvedFunction(context, "or", std::move(bool_alternatives));
    }

    if (!shared)
        return disjunction;

    return createResolvedFunction(
        context,
        "and",
        {ensureBoolCondition(std::move(shared), context), ensureBoolCondition(std::move(disjunction), context)});
}

}

bool QueryAnalyzer::tryEvaluateMergedScalarSubqueries(
    const QueryTreeNodePtr & first_node,
    const QueryTreeNodePtrWithHash & first_node_with_hash,
    IdentifierResolveScope & scope,
    bool can_use_global_scalars)
{
    if (!scope.parent_scope)
        return false;

    const auto & current_settings = scope.context->getSettingsRef();
    /// A merged scan reads the columns of all subqueries in one step, so limits that are checked per
    /// read step could reject it while every original subquery fits.
    if (!current_settings[Setting::optimize_merge_scalar_subqueries]
        || current_settings[Setting::empty_result_for_aggregation_by_empty_set]
        || current_settings[Setting::max_columns_to_read]
        || current_settings[Setting::max_temporary_columns]
        || current_settings[Setting::max_temporary_non_const_columns]
        /// Table filters are keyed by table alias, and the merged scan keeps only one member's
        /// table expression, so a member could lose or inherit another member's filter.
        || !current_settings[Setting::additional_table_filters].value.empty())
        return false;

    auto * enclosing_query_scope = scope.parent_scope->getNearestQueryScope();
    if (!enclosing_query_scope || !enclosing_query_scope->scope_node)
        return false;

    const auto * enclosing_query = enclosing_query_scope->scope_node->as<QueryNode>();
    if (!enclosing_query)
        return false;

    auto first_candidate = extractResolvedScalarCandidate(first_node, first_node_with_hash, view_source_in_subqueries);
    if (!first_candidate)
        return false;

    auto & scalars_cache = can_use_global_scalars ? scalar_subquery_to_scalar_value_global : scalar_subquery_to_scalar_value_local;
    const auto & target_table = *first_candidate->table_node;

    /// Resolve the siblings over this table once per enclosing query and bucket them by group key.
    /// Re-resolving every sibling for each new group key would make analysis quadratic in the number
    /// of scalar subqueries that cannot be merged (for example, ones over distinct primary key ranges).
    auto [table_candidates_it, inserted] = mergeable_scalar_subqueries.try_emplace(
        std::make_pair(static_cast<const IQueryTreeNode *>(enclosing_query), computeCandidateTableKey(target_table)));
    auto & table_candidates = table_candidates_it->second;

    if (inserted)
    {
        QueryTreeNodes raw_subquery_nodes;
        collectCandidateSubqueryNodes(
            enclosing_query->getProjectionNode(),
            enclosing_query->getContext(),
            scope.parent_scope,
            raw_subquery_nodes);

        const auto & columns_desc = target_table.getStorageSnapshot()->metadata->getColumns();

        for (const auto & raw_subquery : raw_subquery_nodes)
        {
            if (raw_subquery.get() == first_node.get())
                continue;

            const auto * sibling_query = raw_subquery->as<QueryNode>();
            if (!sibling_query)
                continue;

            if (!sibling_query->isResolved() && !isUnresolvedSiblingSafeToResolve(*sibling_query, target_table, columns_desc, scope.parent_scope))
                continue;

            QueryTreeNodePtr resolved_sibling = raw_subquery->clone();
            resolved_sibling->removeAlias();

            if (!sibling_query->isResolved())
            {
                auto & cloned_query = resolved_sibling->as<QueryNode &>();
                cloned_query.getMutableContext() = Context::createCopy(cloned_query.getContext());
                if (scope.parent_scope->context)
                    cloned_query.getMutableContext()->setDistributed(scope.parent_scope->context->isDistributed());

                QueryAnalyzer sibling_analyzer(
                    /*only_analyze_=*/ false,
                    positional_arguments_already_resolved,
                    replaced_storage,
                    view_source_in_subqueries);

                IdentifierResolveScope sibling_scope(resolved_sibling, nullptr /*parent_scope*/);
                sibling_scope.subquery_depth = scope.subquery_depth;
                sibling_analyzer.resolveQuery(resolved_sibling, sibling_scope);
                sibling_analyzer.restoreReplacedTableStoragesIfNeeded(resolved_sibling);
            }

            QueryTreeNodePtrWithHash sibling_with_hash(resolved_sibling);
            if (auto candidate = extractResolvedScalarCandidate(resolved_sibling, sibling_with_hash, view_source_in_subqueries))
                table_candidates[candidate->sub_group_key].push_back(std::move(resolved_sibling));
        }
    }

    auto group_candidates_it = table_candidates.find(first_candidate->sub_group_key);
    if (group_candidates_it == table_candidates.end())
        return false;

    /// A group is merged at most once: its other members are in the scalar cache afterwards.
    const QueryTreeNodes group_siblings = std::move(group_candidates_it->second);
    table_candidates.erase(group_candidates_it);

    std::vector<ResolvedScalarCandidate> group;
    QueryTreeNodePtrWithHashSet seen_candidates;
    seen_candidates.insert(first_candidate->node_with_hash);
    group.push_back(std::move(*first_candidate));

    for (const auto & resolved_sibling : group_siblings)
    {
        QueryTreeNodePtrWithHash sibling_with_hash(resolved_sibling);
        if (seen_candidates.contains(sibling_with_hash) || scalars_cache.contains(sibling_with_hash))
            continue;

        if (can_use_global_scalars && scope.context->hasQueryContext()
            && scope.context->getQueryContext()->hasComputedScalar(DB::toString(sibling_with_hash.hash)))
            continue;

        auto candidate = extractResolvedScalarCandidate(resolved_sibling, sibling_with_hash, view_source_in_subqueries);
        chassert(candidate);
        seen_candidates.insert(candidate->node_with_hash);
        group.push_back(std::move(*candidate));
    }

    if (group.size() < 2)
        return false;

    ProfileEvents::increment(ProfileEvents::ScalarSubqueriesCacheMiss);

    auto & context = scope.context;
    auto subquery_context = createSubqueryExecutionContext(context);

    auto merged_query = std::make_shared<QueryNode>(subquery_context);
    merged_query->setIsSubquery(true);

    auto merged_table_node = std::static_pointer_cast<TableNode>(group.front().table_node->clone());
    merged_table_node->removeAlias();
    merged_query->getJoinTreeNode() = merged_table_node;

    const auto split_conditions = splitSharedQueryTreeConjuncts(group);
    if (auto merged_where = buildMergedWhereQueryTree(split_conditions, merged_table_node, subquery_context))
        merged_query->getWhere() = std::move(merged_where);

    auto & merged_projection = merged_query->getProjection().getNodes();
    merged_projection.reserve(group.size());

    NamesAndTypes merged_projection_columns;
    merged_projection_columns.reserve(group.size());

    for (size_t i = 0; i < group.size(); ++i)
    {
        const auto & candidate = group[i];
        QueryTreeNodePtr proj_expr;

        if (split_conditions.own[i].empty())
        {
            proj_expr = cloneAndRebindToTable(candidate.agg_node_ptr, merged_table_node);
        }
        else
        {
            auto own_cond = ensureBoolCondition(
                combineConditions(split_conditions.own[i], "and", merged_table_node, subquery_context),
                subquery_context);

            const String if_agg_name = candidate.agg_node->getFunctionName() + "If";
            auto fn_node = std::make_shared<FunctionNode>(if_agg_name);
            auto & fn_args = fn_node->getArguments().getNodes();

            const auto & orig_args = candidate.agg_node->getArguments().getNodes();
            fn_args.reserve(orig_args.size() + 1);
            for (const auto & orig_arg : orig_args)
                fn_args.push_back(cloneAndRebindToTable(orig_arg, merged_table_node));
            fn_args.push_back(std::move(own_cond));

            resolveAggregateFunctionNodeByName(*fn_node, if_agg_name);
            proj_expr = std::move(fn_node);

            if (!proj_expr->getResultType()->equals(*candidate.original_result_type))
                proj_expr = buildCastFunction(proj_expr, candidate.original_result_type, subquery_context);
        }

        merged_projection_columns.emplace_back(fmt::format("__merged_scalar_{}", i), proj_expr->getResultType());
        merged_projection.push_back(std::move(proj_expr));
    }

    merged_query->resolveProjectionColumns(std::move(merged_projection_columns));

    QueryTreeNodePtr merged_query_tree = std::move(merged_query);
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, scope.subquery_depth, true /*is_subquery*/);
    options.only_analyze = false;
    options.view_source_in_subqueries = view_source_in_subqueries;
    options.forceMaterializeCTE();

    QueryTreePassManager query_tree_pass_manager(subquery_context);
    addQueryTreePasses(query_tree_pass_manager, options.only_analyze);
    query_tree_pass_manager.run(merged_query_tree);

    auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(merged_query_tree, subquery_context, options);
    auto & query_plan = interpreter->getQueryPlan();

    if (!interpreter->ignoreQuota())
    {
        if (auto quota = subquery_context->getQuota())
            quota->checkExceededProfileEvents(subquery_context->getNormalizedQueryHash());
        if (auto process_list_elem = subquery_context->getProcessListElement())
            process_list_elem->markNonExemptScalarSubqueryExecuted();
    }

    QueryPlanOptimizationSettings optimization_settings(subquery_context);
    BuildQueryPipelineSettings build_pipeline_settings(subquery_context);
    query_plan.setConcurrencyControl(subquery_context->getSettingsRef()[Setting::use_concurrency_control]);

    auto pipeline_builder = std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings));

    BlockIO io;
    io.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    io.pipeline.setQuota(subquery_context->getQuota());
    io.pipeline.setNormalizedQueryHash(subquery_context->getNormalizedQueryHash());
    io.pipeline.setProgressCallback(context->getProgressCallback());
    io.pipeline.setProcessListElement(context->getProcessListElement());
    io.pipeline.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

    PullingAsyncPipelineExecutor executor(io.pipeline);
    if (auto cancel_cb = context->hasQueryContext() ? context->getQueryContext()->getInteractiveCancelCallback() : nullptr)
        executor.setCancelCallback(std::move(cancel_cb), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));

    Chunk chunk;
    while (chunk.getNumRows() == 0 && executor.pull(chunk))
    {
    }

    if (chunk.getNumRows() == 0)
    {
        const auto & sample_block = interpreter->getSampleBlock();
        for (size_t i = 0; i < group.size(); ++i)
        {
            DataTypePtr type = sample_block->getByPosition(i).type;
            if (!type->isNullable())
            {
                if (!type->canBeInsideNullable())
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                        "Scalar subquery returned empty result of type {} which cannot be Nullable",
                        type->getName());
                type = makeNullable(type);
            }

            auto scalar_column = type->createColumn();
            scalar_column->insert(Null());

            Block single_block;
            single_block.insert({std::move(scalar_column), type, "null"});

            scalars_cache.emplace(group[i].node_with_hash, single_block);
            if (can_use_global_scalars && context->hasQueryContext())
                context->getQueryContext()->addScalar(group[i].str_hash, single_block);
        }
    }
    else
    {
        if (chunk.getNumRows() != 1)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

        Chunk tmp_chunk;
        while (tmp_chunk.getNumRows() == 0 && executor.pull(tmp_chunk))
        {
        }

        if (tmp_chunk.getNumRows() != 0)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

        Block full_block = materializeBlock(executor.getHeader().cloneWithColumns(chunk.getColumns()));
        if (full_block.columns() != group.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Merged scalar subquery expected {} columns, got {}",
                group.size(),
                full_block.columns());

        for (size_t i = 0; i < group.size(); ++i)
        {
            Block single_block{full_block.getByPosition(i)};
            wrapScalarBlockWithNullableOrTuple(single_block);

            scalars_cache.emplace(group[i].node_with_hash, single_block);
            if (can_use_global_scalars && context->hasQueryContext())
                context->getQueryContext()->addScalar(group[i].str_hash, single_block);
        }
    }

    logProcessorProfile(context, io.pipeline.getProcessors());
    return true;
}

/// Evaluate scalar subquery and perform constant folding if scalar subquery does not have constant value
void QueryAnalyzer::evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool execute_for_exists)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();
    if (!query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node must have query or union type. Actual: {} {}",
            node->getNodeTypeName(),
            node->formatASTForErrorMessage());

    bool is_correlated_subquery = (query_node != nullptr && query_node->isCorrelated())
                                || (union_node != nullptr && union_node->isCorrelated());
    if (is_correlated_subquery)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot evaluate correlated scalar subquery");

    /// The subquery is executed now, so decide here whether its replaced tables read with parallel replicas.
    restoreReplacedTableStoragesIfNeeded(node);

    auto & context = scope.context;

    /// Scalar subqueries in table function or parameterized view arguments are executed even in
    /// only-analyze mode. The table function / view is resolved into a storage during analysis
    /// (its header is needed), so it requires real argument values rather than type-only
    /// placeholders. Otherwise a placeholder would be passed, e.g. an empty URL to `s3`:
    /// CREATE TABLE t ENGINE = MergeTree ORDER BY () AS
    /// WITH (SELECT path FROM table_with_paths) AS path SELECT * FROM s3(path, NOSIGN);
    /// or a default (empty) value substituted for a parameterized view parameter.
    /// Early short-circuit inference is different: it must never execute speculative work. If a
    /// table function or view needs the actual scalar value, resolution fails and the optimization
    /// falls back to the regular analyzer.
    const bool only_analyze_subquery = early_short_circuit_type_inference_in_process
        || (only_analyze
            && !table_function_arguments_in_resolve_process
            && !parameterized_view_arguments_in_resolve_process);

    if (early_short_circuit_type_inference_in_process && (execute_for_exists || !query_node))
    {
        /// Type-only analysis cannot validate scalar cardinality or determine the runtime value
        /// of EXISTS. Keep resolving the clone for its type, but prevent the early fold.
        early_short_circuit_type_inference_failed = true;
    }

    Block scalar_block;

    auto node_without_alias = node->clone();
    node_without_alias->removeAlias();

    QueryTreeNodePtrWithHash node_with_hash(node_without_alias);
    auto str_hash = DB::toString(node_with_hash.hash);

    bool can_use_global_scalars = !only_analyze_subquery && !(context->getViewSource() && subtreeHasViewSource(node_without_alias.get(), *context));

    auto & scalars_cache = early_short_circuit_type_inference_in_process
        ? scalar_subquery_to_scalar_value_type_only
        : (can_use_global_scalars ? scalar_subquery_to_scalar_value_global : scalar_subquery_to_scalar_value_local);

    if (scalars_cache.contains(node_with_hash))
    {
        if (can_use_global_scalars)
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
        else
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesLocalCacheHit);

        scalar_block = scalars_cache.at(node_with_hash);
    }
    else if (context->hasQueryContext() && can_use_global_scalars && context->getQueryContext()->hasComputedScalar(str_hash))
    {
        scalar_block = context->getQueryContext()->getScalar(str_hash);
        scalar_subquery_to_scalar_value_global.emplace(node_with_hash, scalar_block);
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
    }
    /// In only-analyze mode a subquery is executed only when it is a table function or parameterized
    /// view argument; merging would also execute its `SELECT`-list siblings, which are only analyzed.
    else if (!only_analyze && !only_analyze_subquery && !execute_for_exists && !early_short_circuit_type_inference_in_process
        && query_node != nullptr
        && tryEvaluateMergedScalarSubqueries(node, node_with_hash, scope, can_use_global_scalars))
    {
        scalar_block = scalars_cache.at(node_with_hash);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesCacheMiss);
        auto subquery_context = createSubqueryExecutionContext(context);

        auto query_tree = node->clone();
        /// Update context for the QueryTree, because apparently Planner would use this context.
        if (auto * new_query_node = query_tree->as<QueryNode>())
            new_query_node->getMutableContext() = subquery_context;
        if (auto * new_union_node = query_tree->as<UnionNode>())
            new_union_node->getMutableContext() = subquery_context;

        /// The Planner reads the context from every node, so parallel replicas must be disabled
        /// in nested QueryNode/UnionNode contexts too, not just on the top node. Same recursive
        /// walk as createLocalPlanForParallelReplicas.
        std::vector<IQueryTreeNode *> nodes_to_visit;
        for (const auto & child : query_tree->getChildren())
            if (child)
                nodes_to_visit.push_back(child.get());
        while (!nodes_to_visit.empty())
        {
            auto * current = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * nested_query_node = current->as<QueryNode>())
            {
                auto nested_context = Context::createCopy(nested_query_node->getContext());
                nested_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                nested_query_node->getMutableContext() = std::move(nested_context);
            }
            else if (auto * nested_union_node = current->as<UnionNode>())
            {
                auto nested_context = Context::createCopy(nested_union_node->getContext());
                nested_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                nested_union_node->getMutableContext() = std::move(nested_context);
            }

            for (const auto & child : current->getChildren())
                if (child)
                    nodes_to_visit.push_back(child.get());
        }

        auto options = SelectQueryOptions(QueryProcessingStage::Complete, scope.subquery_depth, true /*is_subquery*/);
        options.only_analyze = only_analyze_subquery;
        options.view_source_in_subqueries = view_source_in_subqueries;

        /// Scalar subqueries may reference materialized CTEs that haven't been populated yet.
        /// Force CTE materialization in the sub-plan so that the pipeline execution
        /// populates the CTE StorageMemory tables before the scalar subquery reads from them.
        /// This is analogous to addBuildSubqueriesForSetsStepIfNeeded using forceMaterializeCTE()
        /// for set subqueries that reference CTEs.
        options.forceMaterializeCTE();

        QueryTreePassManager query_tree_pass_manager(subquery_context);
        addQueryTreePasses(query_tree_pass_manager, options.only_analyze);
        query_tree_pass_manager.run(query_tree);

        /// The inner query of an ordinary view referenced by the view query reads the table itself,
        /// not the inserted block.
        if (auto storage = getViewSourceIfRead(subquery_context, view_source_in_subqueries))
            replaceStorageInQueryTree(query_tree, subquery_context, storage);
        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_tree, subquery_context, options);

        if (only_analyze_subquery)
        {
            /// If query is only analyzed, then constants are not correct.
            scalar_block = *interpreter->getSampleBlock();
            for (auto & column : scalar_block)
            {
                if (column.column->empty())
                {
                    auto mut_col = column.column->cloneEmpty();
                    /// Not `NULL`: the placeholder is still evaluated in the enclosing expression, and
                    /// e.g. a cast to a non-Nullable type would throw. `Nothing` has no other value.
                    auto nested_type = removeNullable(removeLowCardinality(column.type));
                    if (isNothing(nested_type))
                        mut_col->insertDefault();
                    else
                        mut_col->insert(nested_type->getDefault());
                    column.column = std::move(mut_col);
                }
            }

            wrapScalarBlockWithNullableOrTuple(scalar_block);
        }
        else
        {
            BlockIO io;
            auto & query_plan = interpreter->getQueryPlan();
            bool skip_execution_for_exists = false;

            /// A scalar subquery is executed during the analysis of the query, before the quotas of the query are checked,
            /// so an exhausted quota over profile events is checked here, before the subquery consumes any resources.
            /// The query plan has been built above, so `ignoreQuota` is known (e.g. for exempt tables in `system`).
            /// The query is then charged with what the subquery consumes even if it is exempt itself (see
            /// `QueryStatus::executed_non_exempt_scalar_subquery`).
            if (!interpreter->ignoreQuota())
            {
                if (auto quota = subquery_context->getQuota())
                    quota->checkExceededProfileEvents(subquery_context->getNormalizedQueryHash());
                if (auto process_list_elem = subquery_context->getProcessListElement())
                    process_list_elem->markNonExemptScalarSubqueryExecuted();
            }

            if (execute_for_exists)
            {
                if (optimizePlanForExists(query_plan))
                    skip_execution_for_exists = true;
                else
                {
                    auto limit_step = std::make_unique<LimitStep>(query_plan.getCurrentHeader(), 1, 0);
                    query_plan.addStep(std::move(limit_step));
                }
            }

            if (!skip_execution_for_exists)
            {
                QueryPlanOptimizationSettings optimization_settings(subquery_context);
                BuildQueryPipelineSettings build_pipeline_settings(subquery_context);

                query_plan.setConcurrencyControl(subquery_context->getSettingsRef()[Setting::use_concurrency_control]);

                auto pipeline_builder = std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings));

                io.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
                io.pipeline.setQuota(subquery_context->getQuota());
                io.pipeline.setNormalizedQueryHash(subquery_context->getNormalizedQueryHash());
            }

            std::optional<PullingAsyncPipelineExecutor> executor;
            Chunk chunk;

            if (!skip_execution_for_exists)
            {
                io.pipeline.setProgressCallback(context->getProgressCallback());
                io.pipeline.setProcessListElement(context->getProcessListElement());
                io.pipeline.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

                executor.emplace(io.pipeline);
                if (auto cancel_cb = context->hasQueryContext() ? context->getQueryContext()->getInteractiveCancelCallback() : nullptr)
                    executor->setCancelCallback(std::move(cancel_cb), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));
                while (chunk.getNumRows() == 0 && executor->pull(chunk))
                {
                }
            }

            if (chunk.getNumRows() == 0)
            {
                DataTypePtr type;
                if (execute_for_exists)
                    type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
                else
                {
                    const auto & sample_block = interpreter->getSampleBlock();
                    auto types = sample_block->getDataTypes();
                    if (types.size() != 1)
                        types = {std::make_shared<DataTypeTuple>(types)};

                    type = types[0];
                    if (!type->isNullable())
                    {
                        if (!type->canBeInsideNullable())
                            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                                "Scalar subquery returned empty result of type {} which cannot be Nullable",
                                type->getName());

                        type = makeNullable(type);
                    }
                }

                auto scalar_column = type->createColumn();
                if (skip_execution_for_exists)
                    scalar_column->insert(1);
                else
                    scalar_column->insert(Null());
                scalar_block.insert({std::move(scalar_column), type, (execute_for_exists ? "exists" : "null")});
            }
            else
            {
                if (chunk.getNumRows() != 1)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                Chunk tmp_chunk;
                while (tmp_chunk.getNumRows() == 0 && executor->pull(tmp_chunk))
                {
                }

                if (tmp_chunk.getNumRows() != 0)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                if (execute_for_exists)
                {
                    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
                    auto scalar_column = type->createColumn();
                    scalar_column->insert(1);
                    scalar_block.insert({std::move(scalar_column), type, "exists"});
                }
                else
                {
                    auto block = executor->getHeader().cloneWithColumns(chunk.getColumns());
                    wrapScalarBlockWithNullableOrTuple(block);
                    scalar_block = std::move(block);
                }
            }

            logProcessorProfile(context, io.pipeline.getProcessors());
        }

        scalars_cache.emplace(node_with_hash, scalar_block);
        if (can_use_global_scalars && context->hasQueryContext())
            context->getQueryContext()->addScalar(str_hash, scalar_block);
    }

    const auto & scalar_column_with_type = scalar_block.safeGetByPosition(0);
    const auto & scalar_type = scalar_column_with_type.type;

    if (early_short_circuit_type_inference_in_process && !execute_for_exists)
    {
        /// During early short-circuit type inference the scalar value is intentionally not
        /// executed, so do not expose the arbitrary default from the sample block as a constant.
        /// A non-constant placeholder preserves the real type. Functions that require the actual
        /// constant value will reject it, causing the optimization to fall back to normal analysis.
        node = std::make_shared<ColumnNode>(
            NameAndTypePair{"_subquery_" + std::to_string(subquery_counter - 1), scalar_type},
            TableExpressionNodeWeakPtr{});
        return;
    }

    const auto * scalar_type_name = scalar_block.safeGetByPosition(0).type->getFamilyName();
    static const std::set<std::string_view> useless_literal_types = {"Array", "Tuple", "AggregateFunction", "Function", "Set", "LowCardinality"};
    auto * nearest_query_scope = scope.getNearestQueryScope();

    /// Always convert to literals when there is no query context, or when resolving a
    /// parameterized view argument (its value must fold to a literal to be matched against
    /// the view's query parameters, see `parameterized_view_arguments_in_resolve_process`).
    /// The EXISTS caller also requires a ConstantNode. In type-only analysis its sample value is
    /// only a placeholder; `early_short_circuit_type_inference_failed` prevents folding from it.
    if (execute_for_exists || !context->getSettingsRef()[Setting::enable_scalar_subquery_optimization]
        || !useless_literal_types.contains(scalar_type_name) || !context->hasQueryContext()
        || !nearest_query_scope || parameterized_view_arguments_in_resolve_process)
    {
        ConstantValue constant_value{ ConstantValue::wrapToColumnConst(scalar_column_with_type.column), scalar_type };
        auto constant_node = std::make_shared<ConstantNode>(constant_value, node);

        if (scalar_column_with_type.column->isNullAt(0))
        {
            node = buildCastFunction(constant_node, constant_node->getResultType(), context);
            node = std::make_shared<ConstantNode>(std::move(constant_value), node);
        }
        else
            node = std::move(constant_node);

        return;
    }

    auto & nearest_query_scope_query_node = nearest_query_scope->scope_node->as<QueryNode &>();
    auto & mutable_context = nearest_query_scope_query_node.getMutableContext();

    auto scalar_query_hash_string = DB::toString(node_with_hash.hash) + (only_analyze_subquery ? "_analyze" : "");

    if (mutable_context->hasQueryContext())
        mutable_context->getQueryContext()->addScalar(scalar_query_hash_string, scalar_block);

    mutable_context->addScalar(scalar_query_hash_string, scalar_block);

    std::string get_scalar_function_name = "__getScalar";

    auto scalar_query_hash_constant_node = std::make_shared<ConstantNode>(std::move(scalar_query_hash_string), std::make_shared<DataTypeString>());

    auto get_scalar_function_node = std::make_shared<FunctionNode>(get_scalar_function_name);
    get_scalar_function_node->getArguments().getNodes().push_back(std::move(scalar_query_hash_constant_node));

    auto get_scalar_function = FunctionFactory::instance().get(get_scalar_function_name, mutable_context);
    get_scalar_function_node->resolveAsFunction(get_scalar_function->build(get_scalar_function_node->getArgumentColumns()));

    node = std::move(get_scalar_function_node);
}

}

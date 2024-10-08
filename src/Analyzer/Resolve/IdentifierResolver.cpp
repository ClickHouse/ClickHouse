#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Storages/IStorage.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/Context.h>

#include <Analyzer/Utils.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/Resolve/ReplaceColumnsVisitor.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool single_join_prefer_left_table;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int AMBIGUOUS_IDENTIFIER;
    extern const int INVALID_IDENTIFIER;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

QueryTreeNodePtr IdentifierResolver::convertJoinedColumnTypeToNullIfNeeded(
    const QueryTreeNodePtr & resolved_identifier,
    const JoinKind & join_kind,
    std::optional<JoinTableSide> resolved_side,
    IdentifierResolveScope & scope)
{
    if (resolved_identifier->getNodeType() == QueryTreeNodeType::COLUMN &&
        JoinCommon::canBecomeNullable(resolved_identifier->getResultType()) &&
        (isFull(join_kind) ||
        (isLeft(join_kind) && resolved_side && *resolved_side == JoinTableSide::Right) ||
        (isRight(join_kind) && resolved_side && *resolved_side == JoinTableSide::Left)))
    {
        auto nullable_resolved_identifier = resolved_identifier->clone();
        auto & resolved_column = nullable_resolved_identifier->as<ColumnNode &>();
        auto new_result_type = makeNullableOrLowCardinalityNullable(resolved_column.getColumnType());
        resolved_column.setColumnType(new_result_type);
        if (resolved_column.hasExpression())
        {
            auto & resolved_expression = resolved_column.getExpression();
            if (!resolved_expression->getResultType()->equals(*new_result_type))
                resolved_expression = buildCastFunction(resolved_expression, new_result_type, scope.context, true);
        }
        if (!nullable_resolved_identifier->isEqual(*resolved_identifier))
            scope.join_columns_with_changed_types[nullable_resolved_identifier] = resolved_identifier;
        return nullable_resolved_identifier;
    }
    return nullptr;
}

bool IdentifierResolver::isExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::CONSTANT || node_type == QueryTreeNodeType::COLUMN || node_type == QueryTreeNodeType::FUNCTION
        || node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

bool IdentifierResolver::isFunctionExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::LAMBDA;
}

bool IdentifierResolver::isSubqueryNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

bool IdentifierResolver::isTableExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::TABLE || node_type == QueryTreeNodeType::TABLE_FUNCTION ||
        isSubqueryNodeType(node_type);
}

DataTypePtr IdentifierResolver::getExpressionNodeResultTypeOrNull(const QueryTreeNodePtr & query_tree_node)
{
    auto node_type = query_tree_node->getNodeType();

    switch (node_type)
    {
        case QueryTreeNodeType::CONSTANT:
            [[fallthrough]];
        case QueryTreeNodeType::COLUMN:
        {
            return query_tree_node->getResultType();
        }
        case QueryTreeNodeType::FUNCTION:
        {
            auto & function_node = query_tree_node->as<FunctionNode &>();
            if (function_node.isResolved())
                return function_node.getResultType();
            break;
        }
        default:
        {
            break;
        }
    }

    return nullptr;
}

/// Get valid identifiers for typo correction from compound expression
void IdentifierResolver::collectCompoundExpressionValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const DataTypePtr & compound_expression_type,
    const Identifier & valid_identifier_prefix,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    IDataType::forEachSubcolumn([&](const auto &, const auto & name, const auto &)
    {
        Identifier subcolumn_indentifier(name);
        size_t new_identifier_size = valid_identifier_prefix.getPartsSize() + subcolumn_indentifier.getPartsSize();

        if (new_identifier_size == unresolved_identifier.getPartsSize())
        {
            auto new_identifier = valid_identifier_prefix;
            for (const auto & part : subcolumn_indentifier)
                new_identifier.push_back(part);

            valid_identifiers_result.insert(std::move(new_identifier));
        }
    }, ISerialization::SubstreamData(compound_expression_type->getDefaultSerialization()));
}

/// Get valid identifiers for typo correction from table expression
void IdentifierResolver::collectTableExpressionValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const QueryTreeNodePtr & table_expression,
    const AnalysisTableExpressionData & table_expression_data,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    for (const auto & [column_name, column_node] : table_expression_data.column_name_to_column_node)
    {
        Identifier column_identifier(column_name);
        if (unresolved_identifier.getPartsSize() == column_identifier.getPartsSize())
            valid_identifiers_result.insert(column_identifier);

        collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
            column_node->getColumnType(),
            column_identifier,
            valid_identifiers_result);

        if (table_expression->hasAlias())
        {
            Identifier column_identifier_with_alias({table_expression->getAlias()});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_alias.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_alias.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_alias);

            collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_alias,
                valid_identifiers_result);
        }

        if (!table_expression_data.table_name.empty())
        {
            Identifier column_identifier_with_table_name({table_expression_data.table_name});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_table_name.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_table_name.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_table_name);

            collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_table_name,
                valid_identifiers_result);
        }

        if (!table_expression_data.database_name.empty() && !table_expression_data.table_name.empty())
        {
            Identifier column_identifier_with_table_name_and_database_name({table_expression_data.database_name, table_expression_data.table_name});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_table_name_and_database_name.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_table_name_and_database_name.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_table_name_and_database_name);

            collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_table_name_and_database_name,
                valid_identifiers_result);
        }
    }
}

/// Get valid identifiers for typo correction from scope without looking at parent scopes
void IdentifierResolver::collectScopeValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const IdentifierResolveScope & scope,
    bool allow_expression_identifiers,
    bool allow_function_identifiers,
    bool allow_table_expression_identifiers,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    bool identifier_is_short = unresolved_identifier.isShort();
    bool identifier_is_compound = unresolved_identifier.isCompound();

    if (allow_expression_identifiers)
    {
        for (const auto & [name, expression] : *scope.aliases.alias_name_to_expression_node)
        {
            assert(expression);
            auto expression_identifier = Identifier(name);
            valid_identifiers_result.insert(expression_identifier);

            auto result_type = getExpressionNodeResultTypeOrNull(expression);

            if (identifier_is_compound && result_type)
            {
                collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                    result_type,
                    expression_identifier,
                    valid_identifiers_result);
            }
        }

        for (const auto & [table_expression, table_expression_data] : scope.table_expression_node_to_data)
        {
            collectTableExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                table_expression,
                table_expression_data,
                valid_identifiers_result);
        }
    }

    if (identifier_is_short)
    {
        if (allow_function_identifiers)
        {
            for (const auto & [name, _] : *scope.aliases.alias_name_to_expression_node)
                valid_identifiers_result.insert(Identifier(name));
        }

        if (allow_table_expression_identifiers)
        {
            for (const auto & [name, _] : scope.aliases.alias_name_to_table_expression_node)
                valid_identifiers_result.insert(Identifier(name));
        }
    }

    for (const auto & [argument_name, expression] : scope.expression_argument_name_to_node)
    {
        assert(expression);
        auto expression_node_type = expression->getNodeType();

        if (allow_expression_identifiers && isExpressionNodeType(expression_node_type))
        {
            auto expression_identifier = Identifier(argument_name);
            valid_identifiers_result.insert(expression_identifier);

            auto result_type = getExpressionNodeResultTypeOrNull(expression);

            if (identifier_is_compound && result_type)
            {
                collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                    result_type,
                    expression_identifier,
                    valid_identifiers_result);
            }
        }
        else if (identifier_is_short && allow_function_identifiers && isFunctionExpressionNodeType(expression_node_type))
        {
            valid_identifiers_result.insert(Identifier(argument_name));
        }
        else if (allow_table_expression_identifiers && isTableExpressionNodeType(expression_node_type))
        {
            valid_identifiers_result.insert(Identifier(argument_name));
        }
    }
}

void IdentifierResolver::collectScopeWithParentScopesValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const IdentifierResolveScope & scope,
    bool allow_expression_identifiers,
    bool allow_function_identifiers,
    bool allow_table_expression_identifiers,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    const IdentifierResolveScope * current_scope = &scope;

    while (current_scope)
    {
        collectScopeValidIdentifiersForTypoCorrection(unresolved_identifier,
            *current_scope,
            allow_expression_identifiers,
            allow_function_identifiers,
            allow_table_expression_identifiers,
            valid_identifiers_result);

        current_scope = current_scope->parent_scope;
    }
}

std::vector<String> IdentifierResolver::collectIdentifierTypoHints(const Identifier & unresolved_identifier, const std::unordered_set<Identifier> & valid_identifiers)
{
    std::vector<String> prompting_strings;
    prompting_strings.reserve(valid_identifiers.size());

    for (const auto & valid_identifier : valid_identifiers)
        prompting_strings.push_back(valid_identifier.getFullName());

    return NamePrompter<1>::getHints(unresolved_identifier.getFullName(), prompting_strings);
}

static FunctionNodePtr wrapExpressionNodeInFunctionWithSecondConstantStringArgument(
    QueryTreeNodePtr expression,
    std::string function_name,
    std::string second_argument,
    const ContextPtr & context)
{
    auto function_node = std::make_shared<FunctionNode>(std::move(function_name));

    auto constant_node_type = std::make_shared<DataTypeString>();
    auto constant_value = std::make_shared<ConstantValue>(std::move(second_argument), std::move(constant_node_type));

    ColumnsWithTypeAndName argument_columns;
    argument_columns.push_back({nullptr, expression->getResultType(), {}});
    argument_columns.push_back({constant_value->getType()->createColumnConst(1, constant_value->getValue()), constant_value->getType(), {}});

    auto function = FunctionFactory::instance().tryGet(function_node->getFunctionName(), context);
    auto function_base = function->build(argument_columns);

    auto constant_node = std::make_shared<ConstantNode>(std::move(constant_value));

    auto & get_subcolumn_function_arguments_nodes = function_node->getArguments().getNodes();

    get_subcolumn_function_arguments_nodes.reserve(2);
    get_subcolumn_function_arguments_nodes.push_back(std::move(expression));
    get_subcolumn_function_arguments_nodes.push_back(std::move(constant_node));

    function_node->resolveAsFunction(std::move(function_base));
    return function_node;
}

static FunctionNodePtr wrapExpressionNodeInSubcolumn(QueryTreeNodePtr expression, std::string subcolumn_name, const ContextPtr & context)
{
    return wrapExpressionNodeInFunctionWithSecondConstantStringArgument(expression, "getSubcolumn", subcolumn_name, context);
}

static FunctionNodePtr wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression, std::string subcolumn_name, const ContextPtr & context)
{
    return wrapExpressionNodeInFunctionWithSecondConstantStringArgument(expression, "tupleElement", subcolumn_name, context);
}

/** Wrap expression node in tuple element function calls for nested paths.
  * Example: Expression node: compound_expression. Nested path: nested_path_1.nested_path_2.
  * Result: tupleElement(tupleElement(compound_expression, 'nested_path_1'), 'nested_path_2').
  */
QueryTreeNodePtr IdentifierResolver::wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression_node, IdentifierView nested_path, const ContextPtr & context)
{
    size_t nested_path_parts_size = nested_path.getPartsSize();
    for (size_t i = 0; i < nested_path_parts_size; ++i)
    {
        std::string nested_path_part(nested_path[i]);
        expression_node = DB::wrapExpressionNodeInTupleElement(std::move(expression_node), std::move(nested_path_part), context);
    }

    return expression_node;
}

/// Resolve identifier functions implementation

/// Try resolve table identifier from database catalog
QueryTreeNodePtr IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(const Identifier & table_identifier, ContextPtr context)
{
    size_t parts_size = table_identifier.getPartsSize();
    if (parts_size < 1 || parts_size > 2)
        throw Exception(ErrorCodes::INVALID_IDENTIFIER,
            "Expected table identifier to contain 1 or 2 parts. Actual '{}'",
            table_identifier.getFullName());

    std::string database_name;
    std::string table_name;

    if (table_identifier.isCompound())
    {
        database_name = table_identifier[0];
        table_name = table_identifier[1];
    }
    else
    {
        table_name = table_identifier[0];
    }

    StorageID storage_id(database_name, table_name);
    storage_id = context->resolveStorageID(storage_id);
    bool is_temporary_table = storage_id.getDatabaseName() == DatabaseCatalog::TEMPORARY_DATABASE;

    StoragePtr storage;
    TableLockHolder storage_lock;

    if (is_temporary_table)
        storage = DatabaseCatalog::instance().getTable(storage_id, context);
    else if (auto refresh_task = context->getRefreshSet().tryGetTaskForInnerTable(storage_id))
    {
        /// If table is the target of a refreshable materialized view, it needs additional
        /// synchronization to make sure we see all of the data (e.g. if refresh happened on another replica).
        std::tie(storage, storage_lock) = refresh_task->getAndLockTargetTable(storage_id, context);
    }
    else
        storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);

    if (!storage && storage_id.hasUUID())
    {
        // If `storage_id` has UUID, it is possible that the UUID is removed from `DatabaseCatalog` after `context->resolveStorageID(storage_id)`
        // We try to get the table with the database name and the table name.
        auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.getDatabaseName());
        if (database)
            storage = database->tryGetTable(table_name, context);
    }
    if (!storage)
        return {};

    if (!storage_lock)
        storage_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
    auto storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
    auto result = std::make_shared<TableNode>(std::move(storage), std::move(storage_lock), std::move(storage_snapshot));
    if (is_temporary_table)
        result->setTemporaryTableName(table_name);

    return result;
}

/// Resolve identifier from compound expression
/// If identifier cannot be resolved throw exception or return nullptr if can_be_not_found is true
QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromCompoundExpression(const Identifier & expression_identifier,
    size_t identifier_bind_size,
    const QueryTreeNodePtr & compound_expression,
    String compound_expression_source,
    IdentifierResolveScope & scope,
    bool can_be_not_found)
{
    Identifier compound_expression_identifier;
    for (size_t i = 0; i < identifier_bind_size; ++i)
        compound_expression_identifier.push_back(expression_identifier[i]);

    IdentifierView nested_path(expression_identifier);
    nested_path.popFirst(identifier_bind_size);

    auto expression_type = compound_expression->getResultType();

    if (!expression_type->hasSubcolumn(nested_path.getFullName()))
    {
        if (auto * column = compound_expression->as<ColumnNode>())
        {
            const DataTypePtr & column_type = column->getColumn().getTypeInStorage();
            if (column_type->getTypeId() == TypeIndex::ObjectDeprecated)
            {
                const auto & object_type = checkAndGetDataType<DataTypeObjectDeprecated>(*column_type);
                if (object_type.getSchemaFormat() == "json" && object_type.hasNullableSubcolumns())
                {
                    QueryTreeNodePtr constant_node_null = std::make_shared<ConstantNode>(Field());
                    return constant_node_null;
                }
            }
        }

        if (can_be_not_found)
            return {};

        std::unordered_set<Identifier> valid_identifiers;
        collectCompoundExpressionValidIdentifiersForTypoCorrection(expression_identifier,
            expression_type,
            compound_expression_identifier,
            valid_identifiers);

        auto hints = collectIdentifierTypoHints(expression_identifier, valid_identifiers);

        String compound_expression_from_error_message;
        if (!compound_expression_source.empty())
        {
            compound_expression_from_error_message += " from ";
            compound_expression_from_error_message += compound_expression_source;
        }

        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
            "Identifier {} nested path {} cannot be resolved from type {}{}. In scope {}{}",
            expression_identifier,
            nested_path,
            expression_type->getName(),
            compound_expression_from_error_message,
            scope.scope_node->formatASTForErrorMessage(),
            getHintsErrorMessageSuffix(hints));
    }

    return wrapExpressionNodeInSubcolumn(compound_expression, std::string(nested_path.getFullName()), scope.context);
}

/** Resolve identifier from expression arguments.
  *
  * Expression arguments can be initialized during lambda analysis or they could be provided externally.
  * Expression arguments must be already resolved nodes. This is client responsibility to resolve them.
  *
  * Example: SELECT arrayMap(x -> x + 1, [1,2,3]);
  * For lambda x -> x + 1, `x` is lambda expression argument.
  *
  * Resolve strategy:
  * 1. Try to bind identifier to scope argument name to node map.
  * 2. If identifier is binded but expression context and node type are incompatible return nullptr.
  *
  * It is important to support edge cases, where we lookup for table or function node, but argument has same name.
  * Example: WITH (x -> x + 1) AS func, (func -> func(1) + func) AS lambda SELECT lambda(1);
  *
  * 3. If identifier is compound and identifier lookup is in expression context use `tryResolveIdentifierFromCompoundExpression`.
  */
QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    auto it = scope.expression_argument_name_to_node.find(identifier_lookup.identifier.getFullName());
    bool resolve_full_identifier = it != scope.expression_argument_name_to_node.end();

    if (!resolve_full_identifier)
    {
        const auto & identifier_bind_part = identifier_lookup.identifier.front();

        it = scope.expression_argument_name_to_node.find(identifier_bind_part);
        if (it == scope.expression_argument_name_to_node.end())
            return {};
    }

    auto node_type = it->second->getNodeType();
    if (identifier_lookup.isExpressionLookup() && !isExpressionNodeType(node_type))
        return {};
    if (identifier_lookup.isTableExpressionLookup() && !isTableExpressionNodeType(node_type))
        return {};
    if (identifier_lookup.isFunctionLookup() && !isFunctionExpressionNodeType(node_type))
        return {};

    if (!resolve_full_identifier && identifier_lookup.identifier.isCompound() && identifier_lookup.isExpressionLookup())
        return tryResolveIdentifierFromCompoundExpression(identifier_lookup.identifier, 1 /*identifier_bind_size*/, it->second, {}, scope);

    return it->second;
}

bool IdentifierResolver::tryBindIdentifierToAliases(const IdentifierLookup & identifier_lookup, const IdentifierResolveScope & scope)
{
    return scope.aliases.find(identifier_lookup, ScopeAliases::FindOption::FIRST_NAME) != nullptr || scope.aliases.array_join_aliases.contains(identifier_lookup.identifier.front());
}

/** Resolve identifier from table columns.
  *
  * 1. If table column nodes are empty or identifier is not expression lookup return nullptr.
  * 2. If identifier full name match table column use column. Save information that we resolve identifier using full name.
  * 3. Else if identifier binds to table column, use column.
  * 4. Try to resolve column ALIAS expression if it exists.
  * 5. If identifier was compound and was not resolved using full name during step 1 use `tryResolveIdentifierFromCompoundExpression`.
  * This can be the case with compound ALIAS columns.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64, value Tuple(id UInt64, value String), alias_value ALIAS value.id) ENGINE=TinyLog;
  */
QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromTableColumns(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    if (scope.column_name_to_column_node.empty() || !identifier_lookup.isExpressionLookup())
        return {};

    const auto & identifier = identifier_lookup.identifier;
    auto it = scope.column_name_to_column_node.find(identifier.getFullName());
    bool full_column_name_match = it != scope.column_name_to_column_node.end();

    if (!full_column_name_match)
    {
        it = scope.column_name_to_column_node.find(identifier_lookup.identifier[0]);
        if (it == scope.column_name_to_column_node.end())
            return {};
    }

    QueryTreeNodePtr result = it->second;

    if (!full_column_name_match && identifier.isCompound())
        return tryResolveIdentifierFromCompoundExpression(identifier_lookup.identifier, 1 /*identifier_bind_size*/, it->second, {}, scope);

    return result;
}

bool IdentifierResolver::tryBindIdentifierToTableExpression(const IdentifierLookup & identifier_lookup,
    const QueryTreeNodePtr & table_expression_node,
    const IdentifierResolveScope & scope)
{
    auto table_expression_node_type = table_expression_node->getNodeType();

    if (table_expression_node_type != QueryTreeNodeType::TABLE &&
        table_expression_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
        table_expression_node_type != QueryTreeNodeType::QUERY &&
        table_expression_node_type != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
        "Unexpected table expression. Expected table, table function, query or union node. Actual {}. In scope {}",
        table_expression_node->formatASTForErrorMessage(),
        scope.scope_node->formatASTForErrorMessage());

    const auto & identifier = identifier_lookup.identifier;
    const auto & path_start = identifier.getParts().front();

    const auto & table_expression_data = scope.getTableExpressionDataOrThrow(table_expression_node);

    const auto & table_name = table_expression_data.table_name;
    const auto & database_name = table_expression_data.database_name;

    if (identifier_lookup.isTableExpressionLookup())
    {
        size_t parts_size = identifier_lookup.identifier.getPartsSize();
        if (parts_size != 1 && parts_size != 2)
            throw Exception(ErrorCodes::INVALID_IDENTIFIER,
                "Expected identifier '{}' to contain 1 or 2 parts to be resolved as table expression. In scope {}",
                identifier_lookup.identifier.getFullName(),
                table_expression_node->formatASTForErrorMessage());

        if (parts_size == 1 && path_start == table_name)
            return true;
        if (parts_size == 2 && path_start == database_name && identifier[1] == table_name)
            return true;
        return false;
    }

    if (table_expression_data.hasFullIdentifierName(IdentifierView(identifier)) || table_expression_data.canBindIdentifier(IdentifierView(identifier)))
        return true;

    if (identifier.getPartsSize() == 1)
        return false;

    if ((!table_name.empty() && path_start == table_name) || (table_expression_node->hasAlias() && path_start == table_expression_node->getAlias()))
        return true;

    if (identifier.getPartsSize() == 2)
        return false;

    if (!database_name.empty() && path_start == database_name && identifier[1] == table_name)
        return true;

    return false;
}

bool IdentifierResolver::tryBindIdentifierToTableExpressions(const IdentifierLookup & identifier_lookup,
    const QueryTreeNodePtr & table_expression_node_to_ignore,
    const IdentifierResolveScope & scope)
{
    bool can_bind_identifier_to_table_expression = false;

    for (const auto & [table_expression_node, _] : scope.table_expression_node_to_data)
    {
        if (table_expression_node.get() == table_expression_node_to_ignore.get())
            continue;

        can_bind_identifier_to_table_expression = tryBindIdentifierToTableExpression(identifier_lookup, table_expression_node, scope);
        if (can_bind_identifier_to_table_expression)
            break;
    }

    return can_bind_identifier_to_table_expression;
}

QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromStorage(
    const Identifier & identifier,
    const QueryTreeNodePtr & table_expression_node,
    const AnalysisTableExpressionData & table_expression_data,
    IdentifierResolveScope & scope,
    size_t identifier_column_qualifier_parts,
    bool can_be_not_found)
{
    auto identifier_without_column_qualifier = identifier;
    identifier_without_column_qualifier.popFirst(identifier_column_qualifier_parts);

    /** Compound identifier cannot be resolved directly from storage if storage is not table.
        *
        * Example: SELECT test_table.id.value1.value2 FROM test_table;
        * In table storage column test_table.id.value1.value2 will exists.
        *
        * Example: SELECT test_subquery.compound_expression.value FROM (SELECT compound_expression AS value) AS test_subquery;
        * Here there is no column with name test_subquery.compound_expression.value, and additional wrap in tuple element is required.
        */

    QueryTreeNodePtr result_expression;
    bool match_full_identifier = false;

    const auto & identifier_full_name = identifier_without_column_qualifier.getFullName();

    ColumnNodePtr result_column_node;
    bool can_resolve_directly_from_storage = false;
    bool is_subcolumn = false;
    if (auto it = table_expression_data.column_name_to_column_node.find(identifier_full_name); it != table_expression_data.column_name_to_column_node.end())
    {
        can_resolve_directly_from_storage = true;
        is_subcolumn = table_expression_data.subcolumn_names.contains(identifier_full_name);
        result_column_node = it->second;
    }
    /// Check if it's a dynamic subcolumn
    else if (table_expression_data.supports_subcolumns)
    {
        auto [column_name, dynamic_subcolumn_name] = Nested::splitName(identifier_full_name);
        auto jt = table_expression_data.column_name_to_column_node.find(column_name);
        if (jt != table_expression_data.column_name_to_column_node.end() && jt->second->getColumnType()->hasDynamicSubcolumns())
        {
            if (auto dynamic_subcolumn_type = jt->second->getColumnType()->tryGetSubcolumnType(dynamic_subcolumn_name))
            {
                result_column_node = std::make_shared<ColumnNode>(NameAndTypePair{identifier_full_name, dynamic_subcolumn_type}, jt->second->getColumnSource());
                can_resolve_directly_from_storage = true;
                is_subcolumn = true;
            }
        }
    }

    if (can_resolve_directly_from_storage && is_subcolumn)
    {
        /** In the case when we have an ARRAY JOIN, we should not resolve subcolumns directly from storage.
          * For example, consider the following SQL query:
          * SELECT ProfileEvents.Values FROM system.query_log ARRAY JOIN ProfileEvents
          * In this case, ProfileEvents.Values should also be array joined, not directly resolved from storage.
          */
        auto * nearest_query_scope = scope.getNearestQueryScope();
        auto * nearest_query_scope_query_node = nearest_query_scope ? nearest_query_scope->scope_node->as<QueryNode>() : nullptr;
        if (nearest_query_scope_query_node && nearest_query_scope_query_node->getJoinTree()->getNodeType() == QueryTreeNodeType::ARRAY_JOIN)
            can_resolve_directly_from_storage = false;
    }

    if (can_resolve_directly_from_storage)
    {
        match_full_identifier = true;
        result_expression = result_column_node;
    }
    else
    {
        auto it = table_expression_data.column_name_to_column_node.find(identifier_without_column_qualifier.at(0));
        if (it != table_expression_data.column_name_to_column_node.end())
            result_expression = it->second;
    }

    bool clone_is_needed = true;

    String table_expression_source = table_expression_data.table_expression_description;
    if (!table_expression_data.table_expression_name.empty())
        table_expression_source += " with name " + table_expression_data.table_expression_name;

    if (result_expression && !match_full_identifier && identifier_without_column_qualifier.isCompound())
    {
        size_t identifier_bind_size = identifier_column_qualifier_parts + 1;
        result_expression = tryResolveIdentifierFromCompoundExpression(identifier,
            identifier_bind_size,
            result_expression,
            table_expression_source,
            scope,
            can_be_not_found);
        if (can_be_not_found && !result_expression)
            return {};
        clone_is_needed = false;
    }

    if (!result_expression)
    {
        QueryTreeNodes nested_column_nodes;
        DataTypes nested_types;
        Array nested_names_array;

        for (const auto & [column_name, _] : table_expression_data.column_names_and_types)
        {
            Identifier column_name_identifier_without_last_part(column_name);
            auto column_name_identifier_last_part = column_name_identifier_without_last_part.getParts().back();
            column_name_identifier_without_last_part.popLast();

            if (identifier_without_column_qualifier.getFullName() != column_name_identifier_without_last_part.getFullName())
                continue;

            auto column_node_it = table_expression_data.column_name_to_column_node.find(column_name);
            if (column_node_it == table_expression_data.column_name_to_column_node.end())
                continue;

            const auto & column_node = column_node_it->second;
            const auto & column_type = column_node->getColumnType();
            const auto * column_type_array = typeid_cast<const DataTypeArray *>(column_type.get());
            if (!column_type_array)
                continue;

            nested_column_nodes.push_back(column_node);
            nested_types.push_back(column_type_array->getNestedType());
            nested_names_array.push_back(Field(std::move(column_name_identifier_last_part)));
        }

        if (!nested_types.empty())
        {
            auto nested_function_node = std::make_shared<FunctionNode>("nested");
            auto & nested_function_node_arguments = nested_function_node->getArguments().getNodes();

            auto nested_function_names_array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
            auto nested_function_names_constant_node = std::make_shared<ConstantNode>(std::move(nested_names_array),
                std::move(nested_function_names_array_type));
            nested_function_node_arguments.push_back(std::move(nested_function_names_constant_node));
            nested_function_node_arguments.insert(nested_function_node_arguments.end(),
                nested_column_nodes.begin(),
                nested_column_nodes.end());

            auto nested_function = FunctionFactory::instance().get(nested_function_node->getFunctionName(), scope.context);
            nested_function_node->resolveAsFunction(nested_function->build(nested_function_node->getArgumentColumns()));

            clone_is_needed = false;
            result_expression = std::move(nested_function_node);
        }
    }

    if (!result_expression)
    {
        if (can_be_not_found)
            return {};
        std::unordered_set<Identifier> valid_identifiers;
        collectTableExpressionValidIdentifiersForTypoCorrection(identifier,
            table_expression_node,
            table_expression_data,
            valid_identifiers);

        auto hints = collectIdentifierTypoHints(identifier, valid_identifiers);

        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Identifier '{}' cannot be resolved from {}. In scope {}{}",
            identifier.getFullName(),
            table_expression_source,
            scope.scope_node->formatASTForErrorMessage(),
            getHintsErrorMessageSuffix(hints));
    }

    if (clone_is_needed)
        result_expression = result_expression->clone();

    auto qualified_identifier = identifier;

    for (size_t i = 0; i < identifier_column_qualifier_parts; ++i)
    {
        auto qualified_identifier_with_removed_part = qualified_identifier;
        qualified_identifier_with_removed_part.popFirst();

        if (qualified_identifier_with_removed_part.empty())
            break;

        IdentifierLookup column_identifier_lookup = {qualified_identifier_with_removed_part, IdentifierLookupContext::EXPRESSION};
        if (tryBindIdentifierToAliases(column_identifier_lookup, scope))
            break;

        if (table_expression_data.should_qualify_columns &&
            tryBindIdentifierToTableExpressions(column_identifier_lookup, table_expression_node, scope))
            break;

        qualified_identifier = std::move(qualified_identifier_with_removed_part);
    }

    auto qualified_identifier_full_name = qualified_identifier.getFullName();
    node_to_projection_name.emplace(result_expression, std::move(qualified_identifier_full_name));

    return result_expression;
}

QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromTableExpression(const IdentifierLookup & identifier_lookup,
    const QueryTreeNodePtr & table_expression_node,
    IdentifierResolveScope & scope)
{
    auto table_expression_node_type = table_expression_node->getNodeType();

    if (table_expression_node_type != QueryTreeNodeType::TABLE &&
        table_expression_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
        table_expression_node_type != QueryTreeNodeType::QUERY &&
        table_expression_node_type != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Unexpected table expression. Expected table, table function, query or union node. Actual {}. In scope {}",
            table_expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

    const auto & identifier = identifier_lookup.identifier;
    const auto & path_start = identifier.getParts().front();

    auto & table_expression_data = scope.getTableExpressionDataOrThrow(table_expression_node);

    if (identifier_lookup.isTableExpressionLookup())
    {
        size_t parts_size = identifier_lookup.identifier.getPartsSize();
        if (parts_size != 1 && parts_size != 2)
            throw Exception(ErrorCodes::INVALID_IDENTIFIER,
                "Expected identifier '{}' to contain 1 or 2 parts to be resolved as table expression. In scope {}",
                identifier_lookup.identifier.getFullName(),
                table_expression_node->formatASTForErrorMessage());

        const auto & table_name = table_expression_data.table_name;
        const auto & database_name = table_expression_data.database_name;

        if (parts_size == 1 && path_start == table_name)
            return table_expression_node;
        if (parts_size == 2 && path_start == database_name && identifier[1] == table_name)
            return table_expression_node;
        return {};
    }

     /** If identifier first part binds to some column start or table has full identifier name. Then we can try to find whole identifier in table.
       * 1. Try to bind identifier first part to column in table, if true get full identifier from table or throw exception.
       * 2. Try to bind identifier first part to table name or storage alias, if true remove first part and try to get full identifier from table or throw exception.
       * Storage alias works for subquery, table function as well.
       * 3. Try to bind identifier first parts to database name and table name, if true remove first two parts and try to get full identifier from table or throw exception.
       */
    if (table_expression_data.hasFullIdentifierName(IdentifierView(identifier)))
        return tryResolveIdentifierFromStorage(identifier, table_expression_node, table_expression_data, scope, 0 /*identifier_column_qualifier_parts*/);

    if (table_expression_data.canBindIdentifier(IdentifierView(identifier)))
    {
        /** This check is insufficient to determine whether and identifier can be resolved from table expression.
          * A further check will be performed in `tryResolveIdentifierFromStorage` to see if we have such a subcolumn.
          * In cases where the subcolumn cannot be found we want to have `nullptr` instead of exception.
          * So, we set `can_be_not_found = true` to have an attempt to resolve the identifier from another table expression.
          * Example: `SELECT t.t from (SELECT 1 as t) AS a FULL JOIN (SELECT 1 as t) as t ON a.t = t.t;`
          * Initially, we will try to resolve t.t from `a` because `t.` is bound to `1 as t`. However, as it is not a nested column, we will need to resolve it from the second table expression.
          */
        auto resolved_identifier = tryResolveIdentifierFromStorage(identifier, table_expression_node, table_expression_data, scope, 0 /*identifier_column_qualifier_parts*/, true /*can_be_not_found*/);
        if (resolved_identifier)
            return resolved_identifier;
    }

    if (identifier.getPartsSize() == 1)
        return {};

    const auto & table_name = table_expression_data.table_name;
    if ((!table_name.empty() && path_start == table_name) || (table_expression_node->hasAlias() && path_start == table_expression_node->getAlias()))
        return tryResolveIdentifierFromStorage(identifier, table_expression_node, table_expression_data, scope, 1 /*identifier_column_qualifier_parts*/);

    if (identifier.getPartsSize() == 2)
        return {};

    const auto & database_name = table_expression_data.database_name;
    if (!database_name.empty() && path_start == database_name && identifier[1] == table_name)
        return tryResolveIdentifierFromStorage(identifier, table_expression_node, table_expression_data, scope, 2 /*identifier_column_qualifier_parts*/);

    return {};
}

QueryTreeNodePtr checkIsMissedObjectJSONSubcolumn(const QueryTreeNodePtr & left_resolved_identifier,
                                                  const QueryTreeNodePtr & right_resolved_identifier)
{
    if (left_resolved_identifier && right_resolved_identifier && left_resolved_identifier->getNodeType() == QueryTreeNodeType::CONSTANT
        && right_resolved_identifier->getNodeType() == QueryTreeNodeType::CONSTANT)
    {
        auto & left_resolved_column = left_resolved_identifier->as<ConstantNode &>();
        auto & right_resolved_column = right_resolved_identifier->as<ConstantNode &>();
        if (left_resolved_column.getValueStringRepresentation() == "NULL" && right_resolved_column.getValueStringRepresentation() == "NULL")
            return left_resolved_identifier;
    }
    else if (left_resolved_identifier && left_resolved_identifier->getNodeType() == QueryTreeNodeType::CONSTANT)
    {
        auto & left_resolved_column = left_resolved_identifier->as<ConstantNode &>();
        if (left_resolved_column.getValueStringRepresentation() == "NULL")
            return left_resolved_identifier;
    }
    else if (right_resolved_identifier && right_resolved_identifier->getNodeType() == QueryTreeNodeType::CONSTANT)
    {
        auto & right_resolved_column = right_resolved_identifier->as<ConstantNode &>();
        if (right_resolved_column.getValueStringRepresentation() == "NULL")
            return right_resolved_identifier;
    }
    return {};
}

/// Compare resolved identifiers considering columns that become nullable after JOIN
bool resolvedIdenfiersFromJoinAreEquals(
    const QueryTreeNodePtr & left_resolved_identifier,
    const QueryTreeNodePtr & right_resolved_identifier,
    const IdentifierResolveScope & scope)
{
    auto left_original_node = ReplaceColumnsVisitor::findTransitiveReplacement(left_resolved_identifier, scope.join_columns_with_changed_types);
    const auto & left_resolved_to_compare = left_original_node ? left_original_node : left_resolved_identifier;

    auto right_original_node = ReplaceColumnsVisitor::findTransitiveReplacement(right_resolved_identifier, scope.join_columns_with_changed_types);
    const auto & right_resolved_to_compare = right_original_node ? right_original_node : right_resolved_identifier;

    return left_resolved_to_compare->isEqual(*right_resolved_to_compare, IQueryTreeNode::CompareOptions{.compare_aliases = false});
}

QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromJoin(const IdentifierLookup & identifier_lookup,
    const QueryTreeNodePtr & table_expression_node,
    IdentifierResolveScope & scope)
{
    const auto & from_join_node = table_expression_node->as<const JoinNode &>();
    auto left_resolved_identifier = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, from_join_node.getLeftTableExpression(), scope);
    auto right_resolved_identifier = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, from_join_node.getRightTableExpression(), scope);

    if (!identifier_lookup.isExpressionLookup())
    {
        if (left_resolved_identifier && right_resolved_identifier)
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "JOIN {} ambiguous identifier {}. In scope {}",
                table_expression_node->formatASTForErrorMessage(),
                identifier_lookup.dump(),
                scope.scope_node->formatASTForErrorMessage());

        return left_resolved_identifier ? left_resolved_identifier : right_resolved_identifier;
    }

    bool join_node_in_resolve_process = scope.table_expressions_in_resolve_process.contains(table_expression_node.get());

    std::unordered_map<std::string, ColumnNodePtr> join_using_column_name_to_column_node;

    if (!join_node_in_resolve_process && from_join_node.isUsingJoinExpression())
    {
        auto & join_using_list = from_join_node.getJoinExpression()->as<ListNode &>();
        for (auto & join_using_node : join_using_list.getNodes())
        {
            auto & column_node = join_using_node->as<ColumnNode &>();
            join_using_column_name_to_column_node.emplace(column_node.getColumnName(), std::static_pointer_cast<ColumnNode>(join_using_node));
        }
    }

    auto check_nested_column_not_in_using = [&join_using_column_name_to_column_node, &identifier_lookup](const QueryTreeNodePtr & node)
    {
        /** tldr: When an identifier is resolved into the function `nested` or `getSubcolumn`, and
          * some column in its argument is in the USING list and its type has to be updated, we throw an error to avoid overcomplication.
          *
          * Identifiers can be resolved into functions in case of nested or subcolumns.
          * For example `t.t.t` can be resolved into `getSubcolumn(t, 't.t')` function in case of `t` is `Tuple`.
          * So, `t` in USING list is resolved from JOIN itself and has supertype of columns from left and right table.
          * But `t` in `getSubcolumn` argument is still resolved from table and we need to update its type.
          *
          * Example:
          *
          * SELECT t.t FROM (
          *     SELECT ((1, 's'), 's') :: Tuple(t Tuple(t UInt32, s1 String), s1 String) as t
          * ) AS a FULL JOIN (
          *     SELECT ((1, 's'), 's') :: Tuple(t Tuple(t Int32, s2 String), s2 String) as t
          * ) AS b USING t;
          *
          * Result type of `t` is `Tuple(Tuple(Int64, String), String)` (different type and no names for subcolumns),
          * so it may be tricky to have a correct type for `t.t` that is resolved into getSubcolumn(t, 't').
          *
          * It can be more complicated in case of Nested subcolumns, in that case in query:
          *     SELECT t FROM ... JOIN ... USING (t.t)
          * Here, `t` is resolved into function `nested(['t', 's'], t.t, t.s) so, `t.t` should be from JOIN and `t.s` should be from table.
          *
          * Updating type accordingly is pretty complicated, so just forbid such cases.
          *
          * While it still may work for storages that support selecting subcolumns directly without `getSubcolumn` function:
          *     SELECT t, t.t, toTypeName(t), toTypeName(t.t) FROM t1 AS a FULL JOIN t2 AS b USING t.t;
          * We just support it as a best-effort: `t` will have original type from table, but `t.t` will have super-type from JOIN.
          * Probably it's good to prohibit such cases as well, but it's not clear how to check it in general case.
          */
        if (node->getNodeType() != QueryTreeNodeType::FUNCTION)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node type {}, expected function node", node->getNodeType());

        const auto & function_argument_nodes = node->as<FunctionNode &>().getArguments().getNodes();
        for (const auto & argument_node : function_argument_nodes)
        {
            if (argument_node->getNodeType() == QueryTreeNodeType::COLUMN)
            {
                const auto & column_name = argument_node->as<ColumnNode &>().getColumnName();
                if (join_using_column_name_to_column_node.contains(column_name))
                    throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                        "Cannot select subcolumn for identifier '{}' while joining using column '{}'",
                            identifier_lookup.identifier, column_name);
            }
            else if (argument_node->getNodeType() == QueryTreeNodeType::CONSTANT)
            {
                continue;
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node type {} for argument node in {}",
                    argument_node->getNodeType(), node->formatASTForErrorMessage());
            }
        }
    };

    std::optional<JoinTableSide> resolved_side;
    QueryTreeNodePtr resolved_identifier;

    JoinKind join_kind = from_join_node.getKind();

    /// If columns from left or right table were missed Object(Nullable('json')) subcolumns, they will be replaced
    /// to ConstantNode(NULL), which can't be cast to ColumnNode, so we resolve it here.
    if (auto missed_subcolumn_identifier = checkIsMissedObjectJSONSubcolumn(left_resolved_identifier, right_resolved_identifier))
        return missed_subcolumn_identifier;

    if (left_resolved_identifier && right_resolved_identifier)
    {
        auto using_column_node_it = join_using_column_name_to_column_node.end();
        if (left_resolved_identifier->getNodeType() == QueryTreeNodeType::COLUMN && right_resolved_identifier->getNodeType() == QueryTreeNodeType::COLUMN)
        {
            auto & left_resolved_column = left_resolved_identifier->as<ColumnNode &>();
            auto & right_resolved_column = right_resolved_identifier->as<ColumnNode &>();
            if (left_resolved_column.getColumnName() == right_resolved_column.getColumnName())
                using_column_node_it = join_using_column_name_to_column_node.find(left_resolved_column.getColumnName());
        }
        else
        {
            if (left_resolved_identifier->getNodeType() != QueryTreeNodeType::COLUMN)
                check_nested_column_not_in_using(left_resolved_identifier);
            if (right_resolved_identifier->getNodeType() != QueryTreeNodeType::COLUMN)
                check_nested_column_not_in_using(right_resolved_identifier);
        }

        if (using_column_node_it != join_using_column_name_to_column_node.end())
        {
            JoinTableSide using_column_inner_column_table_side = isRight(join_kind) ? JoinTableSide::Right : JoinTableSide::Left;
            auto & using_column_node = using_column_node_it->second->as<ColumnNode &>();
            auto & using_expression_list = using_column_node.getExpression()->as<ListNode &>();

            size_t inner_column_node_index = using_column_inner_column_table_side == JoinTableSide::Left ? 0 : 1;
            const auto & inner_column_node = using_expression_list.getNodes().at(inner_column_node_index);

            auto result_column_node = inner_column_node->clone();
            auto & result_column = result_column_node->as<ColumnNode &>();
            result_column.setColumnType(using_column_node.getColumnType());

            const auto & join_using_left_column = using_expression_list.getNodes().at(0);
            if (!result_column_node->isEqual(*join_using_left_column))
                scope.join_columns_with_changed_types[result_column_node] = join_using_left_column;

            resolved_identifier = std::move(result_column_node);
        }
        else if (resolvedIdenfiersFromJoinAreEquals(left_resolved_identifier, right_resolved_identifier, scope))
        {
            const auto & identifier_path_part = identifier_lookup.identifier.front();
            auto * left_resolved_identifier_column = left_resolved_identifier->as<ColumnNode>();
            auto * right_resolved_identifier_column = right_resolved_identifier->as<ColumnNode>();

            if (left_resolved_identifier_column && right_resolved_identifier_column)
            {
                const auto & left_column_source_alias = left_resolved_identifier_column->getColumnSource()->getAlias();
                const auto & right_column_source_alias = right_resolved_identifier_column->getColumnSource()->getAlias();

                /** If column from right table was resolved using alias, we prefer column from right table.
                  *
                  * Example: SELECT dummy FROM system.one JOIN system.one AS A ON A.dummy = system.one.dummy;
                  *
                  * If alias is specified for left table, and alias is not specified for right table and identifier was resolved
                  * without using left table alias, we prefer column from right table.
                  *
                  * Example: SELECT dummy FROM system.one AS A JOIN system.one ON A.dummy = system.one.dummy;
                  *
                  * Otherwise we prefer column from left table.
                  */
                bool column_resolved_using_right_alias = identifier_path_part == right_column_source_alias;
                bool column_resolved_without_using_left_alias = !left_column_source_alias.empty()
                                                                && right_column_source_alias.empty()
                                                                && identifier_path_part != left_column_source_alias;
                if (column_resolved_using_right_alias || column_resolved_without_using_left_alias)
                {
                    resolved_side = JoinTableSide::Right;
                    resolved_identifier = right_resolved_identifier;
                }
                else
                {
                    resolved_side = JoinTableSide::Left;
                    resolved_identifier = left_resolved_identifier;
                }
            }
            else
            {
                resolved_side = JoinTableSide::Left;
                resolved_identifier = left_resolved_identifier;
            }
        }
        else if (scope.joins_count == 1 && scope.context->getSettingsRef()[Setting::single_join_prefer_left_table])
        {
            resolved_side = JoinTableSide::Left;
            resolved_identifier = left_resolved_identifier;
        }
        else
        {
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "JOIN {} ambiguous identifier '{}'. In scope {}",
                table_expression_node->formatASTForErrorMessage(),
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }
    else if (left_resolved_identifier)
    {
        resolved_side = JoinTableSide::Left;
        resolved_identifier = left_resolved_identifier;

        if (left_resolved_identifier->getNodeType() != QueryTreeNodeType::COLUMN)
        {
            check_nested_column_not_in_using(left_resolved_identifier);
        }
        else
        {
            auto & left_resolved_column = left_resolved_identifier->as<ColumnNode &>();
            auto using_column_node_it = join_using_column_name_to_column_node.find(left_resolved_column.getColumnName());
            if (using_column_node_it != join_using_column_name_to_column_node.end() &&
                !using_column_node_it->second->getColumnType()->equals(*left_resolved_column.getColumnType()))
            {
                auto left_resolved_column_clone = std::static_pointer_cast<ColumnNode>(left_resolved_column.clone());
                left_resolved_column_clone->setColumnType(using_column_node_it->second->getColumnType());
                resolved_identifier = std::move(left_resolved_column_clone);

                if (!resolved_identifier->isEqual(*using_column_node_it->second))
                    scope.join_columns_with_changed_types[resolved_identifier] = using_column_node_it->second;
            }
        }
    }
    else if (right_resolved_identifier)
    {
        resolved_side = JoinTableSide::Right;
        resolved_identifier = right_resolved_identifier;

        if (right_resolved_identifier->getNodeType() != QueryTreeNodeType::COLUMN)
        {
            check_nested_column_not_in_using(right_resolved_identifier);
        }
        else
        {
            auto & right_resolved_column = right_resolved_identifier->as<ColumnNode &>();
            auto using_column_node_it = join_using_column_name_to_column_node.find(right_resolved_column.getColumnName());
            if (using_column_node_it != join_using_column_name_to_column_node.end() &&
                !using_column_node_it->second->getColumnType()->equals(*right_resolved_column.getColumnType()))
            {
                auto right_resolved_column_clone = std::static_pointer_cast<ColumnNode>(right_resolved_column.clone());
                right_resolved_column_clone->setColumnType(using_column_node_it->second->getColumnType());
                resolved_identifier = std::move(right_resolved_column_clone);
                if (!resolved_identifier->isEqual(*using_column_node_it->second))
                    scope.join_columns_with_changed_types[resolved_identifier] = using_column_node_it->second;
            }
        }
    }

    if (join_node_in_resolve_process || !resolved_identifier)
        return resolved_identifier;

    if (scope.join_use_nulls)
    {
        auto projection_name_it = node_to_projection_name.find(resolved_identifier);
        auto nullable_resolved_identifier = convertJoinedColumnTypeToNullIfNeeded(resolved_identifier, join_kind, resolved_side, scope);
        if (nullable_resolved_identifier)
        {
            resolved_identifier = nullable_resolved_identifier;
            /// Set the same projection name for new nullable node
            if (projection_name_it != node_to_projection_name.end())
            {
                node_to_projection_name.emplace(resolved_identifier, projection_name_it->second);
            }
        }
    }

    return resolved_identifier;
}

QueryTreeNodePtr IdentifierResolver::matchArrayJoinSubcolumns(
    const QueryTreeNodePtr & array_join_column_inner_expression,
    const ColumnNode & array_join_column_expression_typed,
    const QueryTreeNodePtr & resolved_expression,
    IdentifierResolveScope & scope)
{
    const auto * resolved_function = resolved_expression->as<FunctionNode>();
    if (!resolved_function || resolved_function->getFunctionName() != "getSubcolumn")
        return {};

    const auto * array_join_parent_column = array_join_column_inner_expression.get();

    /** If both resolved and array-joined expressions are subcolumns, try to match them:
      * For example, in `SELECT t.map.values FROM (SELECT * FROM tbl) ARRAY JOIN t.map`
      * Identifier `t.map.values` is resolved into `getSubcolumn(t, 'map.values')` and t.map is resolved into `getSubcolumn(t, 'map')`
      * Since we need to perform array join on `getSubcolumn(t, 'map')`, `t.map.values` should become `getSubcolumn(getSubcolumn(t, 'map'), 'values')`
      *
      * Note: It doesn't work when subcolumn in ARRAY JOIN is transformed by another expression, for example
      * SELECT c.map, c.map.values FROM (SELECT * FROM tbl) ARRAY JOIN mapApply(x -> x, t.map);
      */
    String array_join_subcolumn_prefix;
    auto * array_join_column_inner_expression_function = array_join_column_inner_expression->as<FunctionNode>();
    if (array_join_column_inner_expression_function &&
        array_join_column_inner_expression_function->getFunctionName() == "getSubcolumn")
    {
        const auto & argument_nodes = array_join_column_inner_expression_function->getArguments().getNodes();
        if (argument_nodes.size() == 2 && argument_nodes.at(1)->getNodeType() == QueryTreeNodeType::CONSTANT)
        {
            const auto & constant_node = argument_nodes.at(1)->as<ConstantNode &>();
            const auto & constant_node_value = constant_node.getValue();
            if (constant_node_value.getType() == Field::Types::String)
            {
                array_join_subcolumn_prefix = constant_node_value.safeGet<String>() + ".";
                array_join_parent_column = argument_nodes.at(0).get();
            }
        }
    }

    const auto & argument_nodes = resolved_function->getArguments().getNodes();
    if (argument_nodes.size() != 2 && !array_join_parent_column->isEqual(*argument_nodes.at(0)))
        return {};

    const auto * second_argument = argument_nodes.at(1)->as<ConstantNode>();
    if (!second_argument || second_argument->getValue().getType() != Field::Types::String)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected constant string as second argument of getSubcolumn function {}", resolved_function->dumpTree());

    const auto & resolved_subcolumn_path = second_argument->getValue().safeGet<String &>();
    if (!startsWith(resolved_subcolumn_path, array_join_subcolumn_prefix))
        return {};

    auto column_node = std::make_shared<ColumnNode>(array_join_column_expression_typed.getColumn(), array_join_column_expression_typed.getColumnSource());

    return wrapExpressionNodeInSubcolumn(std::move(column_node), resolved_subcolumn_path.substr(array_join_subcolumn_prefix.size()), scope.context);
}

QueryTreeNodePtr IdentifierResolver::tryResolveExpressionFromArrayJoinExpressions(const QueryTreeNodePtr & resolved_expression,
    const QueryTreeNodePtr & table_expression_node,
    IdentifierResolveScope & scope)
{
    const auto & array_join_node = table_expression_node->as<const ArrayJoinNode &>();
    const auto & array_join_column_expressions_list = array_join_node.getJoinExpressions();
    const auto & array_join_column_expressions_nodes = array_join_column_expressions_list.getNodes();

    QueryTreeNodePtr array_join_resolved_expression;

    /** Special case when qualified or unqualified identifier point to array join expression without alias.
      *
      * CREATE TABLE test_table (id UInt64, value String, value_array Array(UInt8)) ENGINE=TinyLog;
      * SELECT id, value, value_array, test_table.value_array, default.test_table.value_array FROM test_table ARRAY JOIN value_array;
      *
      * value_array, test_table.value_array, default.test_table.value_array must be resolved into array join expression.
      */
    for (const auto & array_join_column_expression : array_join_column_expressions_nodes)
    {
        auto & array_join_column_expression_typed = array_join_column_expression->as<ColumnNode &>();
        if (array_join_column_expression_typed.hasAlias())
            continue;

        auto & array_join_column_inner_expression = array_join_column_expression_typed.getExpressionOrThrow();
        auto * array_join_column_inner_expression_function = array_join_column_inner_expression->as<FunctionNode>();

        if (array_join_column_inner_expression_function &&
            array_join_column_inner_expression_function->getFunctionName() == "nested" &&
            array_join_column_inner_expression_function->getArguments().getNodes().size() > 1 &&
            isTuple(array_join_column_expression_typed.getResultType()))
        {
            const auto & nested_function_arguments = array_join_column_inner_expression_function->getArguments().getNodes();
            size_t nested_function_arguments_size = nested_function_arguments.size();

            const auto & nested_keys_names_constant_node = nested_function_arguments[0]->as<ConstantNode & >();
            const auto & nested_keys_names = nested_keys_names_constant_node.getValue().safeGet<Array &>();
            size_t nested_keys_names_size = nested_keys_names.size();

            if (nested_keys_names_size == nested_function_arguments_size - 1)
            {
                for (size_t i = 1; i < nested_function_arguments_size; ++i)
                {
                    if (!nested_function_arguments[i]->isEqual(*resolved_expression))
                        continue;

                    auto array_join_column = std::make_shared<ColumnNode>(array_join_column_expression_typed.getColumn(),
                        array_join_column_expression_typed.getColumnSource());

                    const auto & nested_key_name = nested_keys_names[i - 1].safeGet<String &>();
                    Identifier nested_identifier = Identifier(nested_key_name);
                    array_join_resolved_expression = wrapExpressionNodeInTupleElement(array_join_column, nested_identifier, scope.context);
                    break;
                }
            }
        }

        if (array_join_resolved_expression)
            break;

        if (array_join_column_inner_expression->isEqual(*resolved_expression))
        {
            array_join_resolved_expression = std::make_shared<ColumnNode>(array_join_column_expression_typed.getColumn(),
                array_join_column_expression_typed.getColumnSource());
            break;
        }

        /// When we select subcolumn of array joined column it also should be array joined
        array_join_resolved_expression = matchArrayJoinSubcolumns(array_join_column_inner_expression, array_join_column_expression_typed, resolved_expression, scope);
        if (array_join_resolved_expression)
            break;
    }
    return array_join_resolved_expression;
}

QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromArrayJoin(const IdentifierLookup & identifier_lookup,
    const QueryTreeNodePtr & table_expression_node,
    IdentifierResolveScope & scope)
{
    const auto & from_array_join_node = table_expression_node->as<const ArrayJoinNode &>();
    auto resolved_identifier = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, from_array_join_node.getTableExpression(), scope);

    if (scope.table_expressions_in_resolve_process.contains(table_expression_node.get()) || !identifier_lookup.isExpressionLookup())
        return resolved_identifier;

    const auto & array_join_column_expressions = from_array_join_node.getJoinExpressions();
    const auto & array_join_column_expressions_nodes = array_join_column_expressions.getNodes();

    /** Allow JOIN with USING with ARRAY JOIN.
      *
      * SELECT * FROM test_table_1 AS t1 ARRAY JOIN [1,2,3] AS id INNER JOIN test_table_2 AS t2 USING (id);
      * SELECT * FROM test_table_1 AS t1 ARRAY JOIN t1.id AS id INNER JOIN test_table_2 AS t2 USING (id);
      */
    for (const auto & array_join_column_expression : array_join_column_expressions_nodes)
    {
        auto & array_join_column_expression_typed = array_join_column_expression->as<ColumnNode &>();

        IdentifierView identifier_view(identifier_lookup.identifier);

        if (identifier_view.isCompound() && from_array_join_node.hasAlias() && identifier_view.front() == from_array_join_node.getAlias())
            identifier_view.popFirst();

        const auto & alias_or_name = array_join_column_expression_typed.hasAlias()
            ? array_join_column_expression_typed.getAlias()
            : array_join_column_expression_typed.getColumnName();

        if (identifier_view.front() == alias_or_name)
            identifier_view.popFirst();
        else if (identifier_view.getFullName() == alias_or_name)
            identifier_view.popFirst(identifier_view.getPartsSize()); /// Clear
        else
            continue;

        if (identifier_view.empty())
        {
            auto array_join_column = std::make_shared<ColumnNode>(array_join_column_expression_typed.getColumn(),
                array_join_column_expression_typed.getColumnSource());
            return array_join_column;
        }

        /// Resolve subcolumns. Example : SELECT x.y.z FROM tab ARRAY JOIN arr AS x
        auto compound_expr = tryResolveIdentifierFromCompoundExpression(
            identifier_lookup.identifier,
            identifier_lookup.identifier.getPartsSize() - identifier_view.getPartsSize() /*identifier_bind_size*/,
            array_join_column_expression,
            {} /* compound_expression_source */,
            scope,
            true /* can_be_not_found */);

        if (compound_expr)
            return compound_expr;
    }

    if (!resolved_identifier)
        return nullptr;

    auto array_join_resolved_expression = tryResolveExpressionFromArrayJoinExpressions(resolved_identifier, table_expression_node, scope);
    if (array_join_resolved_expression)
        resolved_identifier = std::move(array_join_resolved_expression);

    return resolved_identifier;
}

QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromJoinTreeNode(const IdentifierLookup & identifier_lookup,
    const QueryTreeNodePtr & join_tree_node,
    IdentifierResolveScope & scope)
{
    auto join_tree_node_type = join_tree_node->getNodeType();

    switch (join_tree_node_type)
    {
        case QueryTreeNodeType::JOIN:
            return tryResolveIdentifierFromJoin(identifier_lookup, join_tree_node, scope);
        case QueryTreeNodeType::ARRAY_JOIN:
            return tryResolveIdentifierFromArrayJoin(identifier_lookup, join_tree_node, scope);
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            /** Edge case scenario when subquery in FROM node try to resolve identifier from parent scopes, when FROM is not resolved.
              * SELECT subquery.b AS value FROM (SELECT value, 1 AS b) AS subquery;
              * TODO: This can be supported
              */
            if (scope.table_expressions_in_resolve_process.contains(join_tree_node.get()))
                return {};

            return tryResolveIdentifierFromTableExpression(identifier_lookup, join_tree_node, scope);
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Scope FROM section expected table, table function, query, union, join or array join. Actual {}. In scope {}",
                join_tree_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }
}

/** Resolve identifier from scope join tree.
  *
  * 1. If identifier is in function lookup context return nullptr.
  * 2. Try to resolve identifier from table columns.
  * 3. If there is no FROM section return nullptr.
  * 4. If identifier is in table lookup context, check if it has 1 or 2 parts, otherwise throw exception.
  * If identifier has 2 parts try to match it with database_name and table_name.
  * If identifier has 1 part try to match it with table_name, then try to match it with table alias.
  * 5. If identifier is in expression lookup context, we first need to bind identifier to some table column using identifier first part.
  * Start with identifier first part, if it match some column name in table try to get column with full identifier name.
  * TODO: Need to check if it is okay to throw exception if compound identifier first part bind to column but column is not valid.
  */
QueryTreeNodePtr IdentifierResolver::tryResolveIdentifierFromJoinTree(const IdentifierLookup & identifier_lookup,
    IdentifierResolveScope & scope)
{
    if (identifier_lookup.isFunctionLookup())
        return {};

    /// Try to resolve identifier from table columns
    if (auto resolved_identifier = tryResolveIdentifierFromTableColumns(identifier_lookup, scope))
        return resolved_identifier;

    if (scope.expression_join_tree_node)
        return tryResolveIdentifierFromJoinTreeNode(identifier_lookup, scope.expression_join_tree_node, scope);

    auto * query_scope_node = scope.scope_node->as<QueryNode>();
    if (!query_scope_node || !query_scope_node->getJoinTree())
        return {};

    const auto & join_tree_node = query_scope_node->getJoinTree();
    return tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_tree_node, scope);
}

}

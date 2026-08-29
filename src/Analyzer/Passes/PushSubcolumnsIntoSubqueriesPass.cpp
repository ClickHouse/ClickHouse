#include <Analyzer/Passes/PushSubcolumnsIntoSubqueriesPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/WriteHelpers.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>

#include <algorithm>
#include <boost/algorithm/string/predicate.hpp>
#include <cstdlib>
#include <map>
#include <optional>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_push_subcolumns_into_subqueries;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Position of an expression relative to the aggregation step of the query.
enum class ClauseKind
{
    /// JOIN TREE (including JOIN ON and ARRAY JOIN expressions), WHERE, PREWHERE.
    /// Expressions here are evaluated directly on the rows exported by the subquery.
    PreAggregation,
    /// Projection, GROUP BY, HAVING, WINDOW, QUALIFY, ORDER BY, LIMIT BY.
    /// With GROUP BY or aggregate functions present, expressions here can be evaluated
    /// only over aggregation keys and aggregate function results.
    PostAggregation,
    /// INTERPOLATE expressions are evaluated over the output columns of the query
    /// during ORDER BY ... WITH FILL, replacing them with a column reference is not valid.
    Interpolate,
};

/// How a matched occurrence is rewritten once the subcolumn is exported by the subquery.
/// Most carriers collapse to the projected subcolumn itself (with an optional cast), but some
/// need a richer outer expression built around it, mirroring what `FunctionToSubcolumnsPass`
/// does for columns read directly from a table.
enum class ReplacementKind
{
    /// The subcolumn itself, cast to the result type of the original function when they differ.
    Direct,
    /// `empty(x)` -> `equals(x.size0, 0)`.
    EqualsZero,
    /// `notEmpty(x)` -> `notEquals(x.size0, 0)`.
    NotEqualsZero,
    /// `isNotNull(x)` -> `not(x.null)`.
    Not,
    /// `mapContainsKey(x, key)` -> `has(x.keys, key)`.
    Has,
    /// `count(x)` -> `sum(not(x.null))`.
    SumNot,
};

/// A pending addition of the subcolumn projection to one leaf query of the pushdown target.
/// The projection node is null when an identical projection column already exists and is reused.
struct LeafApplication
{
    QueryNode * leaf = nullptr;
    QueryTreeNodePtr new_projection_node;
    /// The existing identical projection node when the subcolumn projection is reused.
    const IQueryTreeNode * reused_projection_node = nullptr;
    /// True when the projection expression of the exported column unwraps through a
    /// function-form carrier (see CandidateMatch::via_function_carrier).
    bool via_function_carrier = false;
};

struct PushdownGroup
{
    /// The subquery exporting the column, an element of the JOIN TREE of the query.
    QueryTreeNodePtr source;
    String column_name;
    String subcolumn_path;
    /// Type of the column argument of getSubcolumn.
    DataTypePtr column_type;
    /// Type of the projected subcolumn. The result type of the original function can differ
    /// from it (e.g. `variantElement` wraps the alternative in Nullable, and the carriers that
    /// need a richer outer expression return a boolean); it is recomputed per occurrence when
    /// the replacement is built.
    DataTypePtr subcolumn_type;
    /// `tupleElement` requires additional storage capability and ambiguity checks.
    bool requires_tuple_element_guards = false;
    /// True when at least one occurrence of the group is a function-form carrier rather than
    /// a plain `getSubcolumn`. Such a group must not be rewritten into a direct subcolumn read
    /// of a table with `FINAL`, mirroring the `FINAL` restriction of `FunctionToSubcolumnsPass`.
    bool has_function_carrier = false;
    ContextPtr context;
    /// False if at least one occurrence cannot be replaced. All occurrences of the same
    /// subcolumn are replaced together or not at all: replacing only some of them could
    /// desynchronize expressions that must stay equal, e.g. an aggregation key in the
    /// GROUP BY list and the same expression in the projection.
    bool viable = true;
    /// Number of `getSubcolumn` occurrences matched into this group.
    size_t occurrences = 0;

    /// Set when the group is validated: the subcolumn can actually be pushed into the target
    /// (validateGroup succeeded), and the collected per-leaf additions are ready to commit.
    bool applicable = false;
    std::vector<LeafApplication> applications;
    String new_column_name;

    /// Set when the group is applied.
    bool applied = false;
};

struct QueryProcessingState
{
    /// Table expressions of the JOIN TREE that are query nodes and are eligible
    /// as pushdown targets (in particular, not on a side of a JOIN that can be
    /// filled with default values for non-matched rows).
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> eligible_targets;

    /// Query nodes found on a side of a JOIN that can be filled with default values. An ordinary
    /// CTE referenced several times is a single shared query node, so the same node can also
    /// appear in an otherwise eligible position; such a node must stay ineligible everywhere.
    std::unordered_set<const IQueryTreeNode *> defaultable_targets;

    std::vector<PushdownGroup> groups;

    /// Number of references to each column of every query or union source, including the column
    /// arguments of matched `getSubcolumn` occurrences (each occurrence contributes one
    /// reference, compensated by `PushdownGroup::occurrences` when deciding to apply) and
    /// correlated columns of nested subqueries. Counted for all sources, not only the eligible
    /// targets: the counts of every query referencing a shared subquery are combined to decide
    /// which of its exported columns stay alive.
    std::unordered_map<const IQueryTreeNode *, std::unordered_map<String, size_t>> column_references;

    bool collect_candidates = false;
    bool query_has_aggregation = false;
    QueryTreeNodes group_by_keys;

    /// `getSubcolumn` projection expressions synthesized for groups with function-form carrier
    /// occurrences while processing the outer queries (shared across the whole pass run). When
    /// such an expression is pushed further down at the next level, the new group inherits the
    /// carrier origin, so the `FINAL` restriction is preserved through several levels of
    /// subqueries. Keyed by node identity: the synthesized node itself becomes the projection
    /// expression of the target and is matched again when the target is processed.
    std::unordered_set<const IQueryTreeNode *> * synthesized_carrier_reads = nullptr;

    PushdownGroup * findGroup(const IQueryTreeNode * source, const String & column_name, const String & subcolumn_path)
    {
        for (auto & group : groups)
            if (group.source.get() == source && group.column_name == column_name && group.subcolumn_path == subcolumn_path)
                return &group;
        return nullptr;
    }
};

struct CandidateMatch
{
    ColumnNode * column_node;
    QueryTreeNodePtr column_source;
    String subcolumn_path;
    DataTypePtr subcolumn_type;
    ReplacementKind replacement_kind = ReplacementKind::Direct;
    bool requires_tuple_element_guards = false;
    /// True when the occurrence is a function-form carrier rather than a plain `getSubcolumn`
    /// (a syntactic subcolumn access). `FunctionToSubcolumnsPass` deliberately refuses to rewrite
    /// such functions into subcolumn reads under `FINAL`, and this pass must not allow the
    /// subquery form of the same expression to tunnel around that restriction.
    bool via_function_carrier = false;
};

/// Match a function that can be expressed as reading a subcolumn where the column comes from a query or union node.
std::optional<CandidateMatch> matchCandidate(FunctionNode & function_node)
{
    if (!function_node.isResolved())
        return {};

    auto & function_arguments = function_node.getArguments().getNodes();
    if (function_arguments.empty())
        return {};

    auto * column_node = function_arguments[0]->as<ColumnNode>();
    if (!column_node)
        return {};

    /// Only query and union sources are rewritten. In particular, a materialized CTE that is
    /// referenced more than once stays a TableNode over its temporary table (single-use ones
    /// are inlined and covered by the query branch). The temporary table serves all references
    /// of the CTE, so pruning the parent column there would require proving that no reference
    /// needs the whole column, and adding the subcolumn without removing the parent column
    /// would only make the materialized table bigger. Such references are deliberately left as is.
    auto column_source = column_node->getColumnSourceOrNull();
    if (!column_source || !isQueryOrUnionNode(column_source))
        return {};

    String subcolumn_path;
    auto replacement_kind = ReplacementKind::Direct;
    /// `tupleElement` over a Tuple needs additional storage capability and ambiguity checks,
    /// while `tupleElement` over a QBit (which uses it for its bit-plane subcolumns) does not.
    bool requires_tuple_element_guards = false;
    /// Type of the subcolumn to project, when it is not the result type of the original function.
    DataTypePtr subcolumn_type_override;
    const auto & function_name = function_node.getFunctionName();

    /// The carriers below are rewritten into an expression over the subcolumn instead of the
    /// subcolumn itself, so the subcolumn type has to be taken from the type system rather than
    /// from the result type of the original function.
    auto declared_subcolumn_type = [&](std::string_view subcolumn_name, const DataTypePtr & expected_type) -> DataTypePtr
    {
        auto declared_type = column_node->getColumnType()->tryGetSubcolumnType(subcolumn_name);
        if (!declared_type || !declared_type->equals(*expected_type))
            return nullptr;
        return declared_type;
    };

    auto result_is_boolean = [&] { return function_node.getResultType()->equals(*std::make_shared<DataTypeUInt8>()); };

    if (function_name == "getSubcolumn")
    {
        if (function_arguments.size() != 2)
            return {};

        const auto * constant_node = function_arguments[1]->as<ConstantNode>();
        if (!constant_node)
            return {};

        auto constant_value = constant_node->getValue();
        if (constant_value.getType() != Field::Types::String)
            return {};

        subcolumn_path = constant_value.safeGet<String>();
    }
    else if (function_name == "mapKeys" && function_arguments.size() == 1 && column_node->getColumnType()->getTypeId() == TypeIndex::Map)
    {
        const auto & map_type = assert_cast<const DataTypeMap &>(*column_node->getColumnType());
        if (!function_node.getResultType()->equals(*std::make_shared<DataTypeArray>(map_type.getKeyType())))
            return {};

        subcolumn_path = "keys";
    }
    else if (function_name == "mapValues" && function_arguments.size() == 1 && column_node->getColumnType()->getTypeId() == TypeIndex::Map)
    {
        const auto & map_type = assert_cast<const DataTypeMap &>(*column_node->getColumnType());
        if (!function_node.getResultType()->equals(*std::make_shared<DataTypeArray>(map_type.getValueType())))
            return {};

        subcolumn_path = "values";
    }
    else if (
        function_name == "arrayElement" && function_arguments.size() == 2 && column_node->getColumnType()->getTypeId() == TypeIndex::Map)
    {
        const auto * constant_node = function_arguments[1]->as<ConstantNode>();
        if (!constant_node)
            return {};

        const auto & map_type = assert_cast<const DataTypeMap &>(*column_node->getColumnType());
        const auto & key_type = map_type.getKeyType();
        if (!function_node.getResultType()->equals(*map_type.getValueType()))
            return {};

        auto key_column = key_type->createColumn();
        if (!key_column->tryInsert(constant_node->getValue()))
        {
            /// A map with Enum keys can also be indexed by the name of the enum value,
            /// so convert the name to the numeric value of the enum.
            if (!isEnum(key_type) || constant_node->getValue().getType() != Field::Types::String)
                return {};

            Field enum_value = tryConvertFieldToType(constant_node->getValue(), *key_type);
            if (enum_value.isNull() || !key_column->tryInsert(enum_value))
                return {};
        }

        WriteBufferFromOwnString buffer;
        key_type->getDefaultSerialization()->serializeText(*key_column, 0, buffer, FormatSettings());
        subcolumn_path = String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + buffer.str();
    }
    else if (function_name == "isNull" && function_arguments.size() == 1 && column_node->getColumnType()->getTypeId() == TypeIndex::Nullable)
    {
        const auto & nullable_type = assert_cast<const DataTypeNullable &>(*column_node->getColumnType());
        if (nullable_type.getNestedType()->hasSubcolumn("null")
            || !function_node.getResultType()->equals(*std::make_shared<DataTypeUInt8>()))
            return {};

        subcolumn_path = "null";
    }
    else if (function_name == "length" && function_arguments.size() == 1)
    {
        const auto type_id = column_node->getColumnType()->getTypeId();
        if ((type_id != TypeIndex::String && type_id != TypeIndex::Array && type_id != TypeIndex::Map)
            || !function_node.getResultType()->equals(*std::make_shared<DataTypeUInt64>()))
            return {};

        subcolumn_path = type_id == TypeIndex::String ? "size" : "size0";
    }
    else if (function_name == "tupleElement" && function_arguments.size() == 2 && column_node->getColumnType()->getTypeId() == TypeIndex::Tuple)
    {
        const auto * constant_node = function_arguments[1]->as<ConstantNode>();
        if (!constant_node)
            return {};

        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*column_node->getColumnType());
        const auto & element_names = tuple_type.getElementNames();
        const auto & element_types = tuple_type.getElements();
        const auto & value = constant_node->getValue();
        std::optional<size_t> position;

        if (value.getType() == Field::Types::String)
            position = tuple_type.tryGetPositionByName(value.safeGet<String>());
        else if (value.getType() == Field::Types::UInt64)
        {
            auto index = value.safeGet<UInt64>();
            if (index != 0 && index <= element_types.size())
                position = index - 1;
        }
        else if (value.getType() == Field::Types::Int64)
        {
            auto index = value.safeGet<Int64>();
            if (index != 0 && std::abs(index) <= static_cast<Int64>(element_types.size()))
                position = index > 0 ? index - 1 : static_cast<Int64>(element_types.size()) + index;
        }

        if (!position || !function_node.getResultType()->equals(*element_types[*position]))
            return {};

        subcolumn_path = element_names[*position];
        requires_tuple_element_guards = true;
    }
    else if (function_name == "tupleElement" && function_arguments.size() == 2 && column_node->getColumnType()->getTypeId() == TypeIndex::QBit)
    {
        /// A QBit exposes its bit planes as subcolumns named by the one-based element index,
        /// and `tupleElement` is the syntax used to read them.
        const auto * constant_node = function_arguments[1]->as<ConstantNode>();
        if (!constant_node || constant_node->getValue().getType() != Field::Types::UInt64)
            return {};

        auto index = constant_node->getValue().safeGet<UInt64>();
        if (index == 0)
            return {};

        subcolumn_path = toString(index);
        subcolumn_type_override = column_node->getColumnType()->tryGetSubcolumnType(subcolumn_path);
        if (!subcolumn_type_override || !function_node.getResultType()->equals(*subcolumn_type_override))
            return {};
    }
    else if (function_name == "variantElement" && function_arguments.size() == 2 && column_node->getColumnType()->getTypeId() == TypeIndex::Variant)
    {
        const auto * constant_node = function_arguments[1]->as<ConstantNode>();
        if (!constant_node || constant_node->getValue().getType() != Field::Types::String)
            return {};

        const auto & variant_type = assert_cast<const DataTypeVariant &>(*column_node->getColumnType());
        const auto & variant_name = constant_node->getValue().safeGet<String>();
        auto discriminator = variant_type.tryGetVariantDiscriminator(variant_name);
        if (!discriminator)
            return {};

        subcolumn_path = variant_name;
    }
    else if ((function_name == "empty" || function_name == "notEmpty") && function_arguments.size() == 1)
    {
        const auto type_id = column_node->getColumnType()->getTypeId();
        if ((type_id != TypeIndex::String && type_id != TypeIndex::Array && type_id != TypeIndex::Map) || !result_is_boolean())
            return {};

        subcolumn_path = type_id == TypeIndex::String ? "size" : "size0";
        subcolumn_type_override = declared_subcolumn_type(subcolumn_path, std::make_shared<DataTypeUInt64>());
        if (!subcolumn_type_override)
            return {};

        replacement_kind = function_name == "empty" ? ReplacementKind::EqualsZero : ReplacementKind::NotEqualsZero;
    }
    else if (function_name == "isNotNull" && function_arguments.size() == 1 && column_node->getColumnType()->getTypeId() == TypeIndex::Nullable)
    {
        const auto & nullable_type = assert_cast<const DataTypeNullable &>(*column_node->getColumnType());
        if (nullable_type.getNestedType()->hasSubcolumn("null") || !result_is_boolean())
            return {};

        subcolumn_path = "null";
        subcolumn_type_override = declared_subcolumn_type(subcolumn_path, std::make_shared<DataTypeUInt8>());
        if (!subcolumn_type_override)
            return {};

        replacement_kind = ReplacementKind::Not;
    }
    else if (function_name == "mapContainsKey" && function_arguments.size() == 2 && column_node->getColumnType()->getTypeId() == TypeIndex::Map)
    {
        if (!result_is_boolean())
            return {};

        /// A Nullable key would make the result of `has` Nullable while `mapContainsKey`
        /// stays UInt8, so the rewritten expression would not be equivalent.
        const auto & key_argument_type = function_arguments[1]->getResultType();
        if (!key_argument_type || key_argument_type->isNullable() || key_argument_type->isLowCardinalityNullable())
            return {};

        const auto & map_type = assert_cast<const DataTypeMap &>(*column_node->getColumnType());
        subcolumn_path = "keys";
        subcolumn_type_override = declared_subcolumn_type(subcolumn_path, std::make_shared<DataTypeArray>(map_type.getKeyType()));
        if (!subcolumn_type_override)
            return {};

        replacement_kind = ReplacementKind::Has;
    }
    else if (
        function_name == "count" && function_arguments.size() == 1 && function_node.isAggregateFunction()
        && !function_node.isWindowFunction() && column_node->getColumnType()->getTypeId() == TypeIndex::Nullable)
    {
        const auto & nullable_type = assert_cast<const DataTypeNullable &>(*column_node->getColumnType());
        if (nullable_type.getNestedType()->hasSubcolumn("null")
            || !function_node.getResultType()->equals(*std::make_shared<DataTypeUInt64>()))
            return {};

        subcolumn_path = "null";
        subcolumn_type_override = declared_subcolumn_type(subcolumn_path, std::make_shared<DataTypeUInt8>());
        if (!subcolumn_type_override)
            return {};

        replacement_kind = ReplacementKind::SumNot;
    }
    else
        return {};

    if (subcolumn_path.empty())
        return {};

    auto subcolumn_type = subcolumn_type_override ? subcolumn_type_override : function_node.getResultType();
    if (function_name == "variantElement")
    {
        const auto & variant_type = assert_cast<const DataTypeVariant &>(*column_node->getColumnType());
        auto discriminator = variant_type.tryGetVariantDiscriminator(subcolumn_path);
        chassert(discriminator);
        subcolumn_type = variant_type.getVariant(*discriminator);
    }

    return CandidateMatch{
        column_node,
        std::move(column_source),
        std::move(subcolumn_path),
        std::move(subcolumn_type),
        replacement_kind,
        requires_tuple_element_guards,
        /*via_function_carrier=*/function_name != "getSubcolumn"};
}

bool tupleElementNameIsAmbiguousWhenFlattened(const DataTypeTuple & tuple, const String & element_name)
{
    std::string_view name = element_name;
    for (size_t dot = name.find('.'); dot != std::string_view::npos; dot = name.find('.', dot + 1))
    {
        auto head = name.substr(0, dot);
        auto tail = name.substr(dot + 1);
        if (!head.empty() && !tail.empty()
            && (tuple.tryGetPositionByName(head) || tuple.tryGetPositionByName(head, /*case_insensitive=*/true)))
            return true;
    }
    return false;
}

bool sourceHasColumnCaseInsensitive(const StorageSnapshotPtr & storage_snapshot, const String & column_name)
{
    for (const auto & column : storage_snapshot->getColumns(GetColumnsOptions::All))
        if (boost::iequals(column.name, column_name))
            return true;
    return false;
}

/// Collect query and union table expressions of the JOIN TREE that can accept
/// additional projection columns. A table expression under LEFT/RIGHT/FULL/PASTE
/// JOIN can have its columns replaced with default values for non-matched rows,
/// and `getSubcolumn` of a default value is not always equal to the default value
/// of the subcolumn type (e.g. the `null` subcolumn of the default NULL value is 1,
/// while the default value of its UInt8 column is 0), so such table expressions
/// are not eligible.
void collectEligibleTargets(const QueryTreeNodePtr & join_tree_node, bool can_be_filled_with_defaults, QueryProcessingState & state)
{
    if (!join_tree_node)
        return;

    if (auto * join_node = join_tree_node->as<JoinNode>())
    {
        auto kind = join_node->getKind();
        bool left_defaultable = can_be_filled_with_defaults || kind == JoinKind::Right || kind == JoinKind::Full || kind == JoinKind::Paste;
        bool right_defaultable = can_be_filled_with_defaults || kind == JoinKind::Left || kind == JoinKind::Full || kind == JoinKind::Paste;

        collectEligibleTargets(join_node->getLeftTableExpressionNode(), left_defaultable, state);
        collectEligibleTargets(join_node->getRightTableExpressionNode(), right_defaultable, state);
        return;
    }

    if (auto * cross_join_node = join_tree_node->as<CrossJoinNode>())
    {
        for (const auto & table_expression : cross_join_node->getTableExpressions())
            collectEligibleTargets(table_expression, can_be_filled_with_defaults, state);
        return;
    }

    if (auto * array_join_node = join_tree_node->as<ArrayJoinNode>())
    {
        collectEligibleTargets(array_join_node->getTableExpressionNode(), can_be_filled_with_defaults, state);
        return;
    }

    if (isQueryOrUnionNode(join_tree_node))
    {
        /// The same query or union node can occur several times in the JOIN TREE when it is a shared
        /// ordinary CTE. Eligibility is tracked per node, not per occurrence, so one occurrence
        /// in a defaultable position makes the node ineligible even for its other occurrences:
        /// the projection column added for an eligible occurrence would also be exported by the
        /// defaultable occurrence, where the JOIN fills it with default values of the subcolumn
        /// type for non-matched rows instead of `getSubcolumn` of the filled parent column.
        if (can_be_filled_with_defaults)
        {
            state.defaultable_targets.insert(join_tree_node.get());
            state.eligible_targets.erase(join_tree_node.get());
        }
        else if (!state.defaultable_targets.contains(join_tree_node.get()))
            state.eligible_targets.emplace(join_tree_node.get(), join_tree_node);
    }
}

/// A projection column can be added to the subquery without changing its result:
/// - DISTINCT deduplicates over all projection columns;
/// - with GROUP BY or aggregate functions an additional non-aggregated column is not valid;
/// - ORDER BY ... WITH FILL and INTERPOLATE fill the added column with default values
///   in the filled rows, which is not always equal to `getSubcolumn` of the filled
///   original column.
bool canAddProjectionColumns(const QueryNode & subquery)
{
    if (subquery.isDistinct() || subquery.hasGroupBy() || subquery.hasInterpolate())
        return false;

    if (hasAggregateFunctionNodes(subquery.getProjectionNode())
        || (subquery.hasHaving() && hasAggregateFunctionNodes(subquery.getHaving()))
        || (subquery.hasOrderBy() && hasAggregateFunctionNodes(subquery.getOrderByNode())))
        return false;

    if (subquery.hasOrderBy())
    {
        for (const auto & sort_node : subquery.getOrderBy().getNodes())
        {
            if (auto * sort = sort_node->as<SortNode>(); sort && sort->withFill())
                return false;
        }
    }

    return true;
}

/// Check that the target of the JOIN TREE can accept the pushed subcolumn projections.
/// A query target must allow adding projection columns and have the optimization enabled in its
/// own context: the subquery can carry its own settings (SETTINGS clause, view definition), and
/// disabling the setting there must protect that subquery from the rewrite.
/// A union target receives the pushed subcolumn into every branch, which keeps the union result
/// unchanged only for UNION ALL: the DISTINCT modes deduplicate (and INTERSECT/EXCEPT match rows)
/// over all projection columns, and a recursive CTE union has the fixed set of columns of its
/// recursive table.
bool canPushIntoTarget(const QueryTreeNodePtr & target)
{
    if (const auto * query_node = target->as<QueryNode>())
    {
        return query_node->getContext()->getSettingsRef()[Setting::optimize_push_subcolumns_into_subqueries]
            && canAddProjectionColumns(*query_node);
    }

    const auto & union_node = target->as<const UnionNode &>();

    if (union_node.getUnionMode() != SelectUnionMode::UNION_ALL || union_node.isRecursiveCTE() || union_node.hasRecursiveCTETable())
        return false;

    if (!union_node.getContext()->getSettingsRef()[Setting::optimize_push_subcolumns_into_subqueries])
        return false;

    const auto & branches = union_node.getQueries().getNodes();
    if (branches.empty())
        return false;

    return std::ranges::all_of(branches, [](const auto & branch) { return canPushIntoTarget(branch); });
}

ContextPtr getTargetContext(const QueryTreeNodePtr & target)
{
    if (const auto * query_node = target->as<QueryNode>())
        return query_node->getContext();

    return target->as<const UnionNode &>().getContext();
}

void collectCandidates(const QueryTreeNodePtr & node, ClauseKind clause_kind, bool inside_aggregate_function, QueryProcessingState & state)
{
    if (!node)
        return;

    if (isQueryOrUnionNode(node))
    {
        /// Nested subqueries are processed separately, but their correlated columns are uses
        /// of the columns of the enclosing queries. RemoveUnusedProjectionColumnsPass treats
        /// correlated columns as live uses of the outer query columns, so they are counted
        /// here as whole-column references: pushing a subcolumn of a column that a correlated
        /// subquery still needs would only add a projection column next to the surviving one.
        const auto * nested_query_node = node->as<QueryNode>();
        const auto & correlated_columns
            = nested_query_node ? nested_query_node->getCorrelatedColumns() : node->as<UnionNode &>().getCorrelatedColumns();

        for (const auto & correlated_column : correlated_columns.getNodes())
        {
            const auto * column_node = correlated_column->as<ColumnNode>();
            if (!column_node)
                continue;

            auto column_source = column_node->getColumnSourceOrNull();
            if (column_source && isQueryOrUnionNode(column_source))
                ++state.column_references[column_source.get()][column_node->getColumnName()];
        }

        return;
    }

    if (const auto * column_node = node->as<ColumnNode>())
    {
        auto column_source = column_node->getColumnSourceOrNull();
        if (column_source && isQueryOrUnionNode(column_source))
            ++state.column_references[column_source.get()][column_node->getColumnName()];
    }

    if (auto * function_node = node->as<FunctionNode>())
    {
        if (state.collect_candidates)
        {
            if (auto match = matchCandidate(*function_node))
            {
                auto target_it = state.eligible_targets.find(match->column_source.get());
                if (target_it != state.eligible_targets.end())
                {
                    const auto & column_name = match->column_node->getColumnName();
                    auto * group = state.findGroup(match->column_source.get(), column_name, match->subcolumn_path);
                    if (!group)
                    {
                        state.groups.push_back(PushdownGroup{
                            .source = target_it->second,
                            .column_name = column_name,
                            .subcolumn_path = match->subcolumn_path,
                            .column_type = match->column_node->getColumnType(),
                            .subcolumn_type = match->subcolumn_type,
                            .requires_tuple_element_guards = match->requires_tuple_element_guards,
                            .has_function_carrier = false,
                            .context = getTargetContext(target_it->second),
                            .viable = true,
                            .occurrences = 0,
                            .applicable = false,
                            .applications = {},
                            .new_column_name = {},
                            .applied = false});
                        group = &state.groups.back();
                    }

                    /// The column argument of the matched occurrence is visited below as a child
                    /// and counted in column_references; the occurrence counter compensates it.
                    ++group->occurrences;

                    /// A plain `getSubcolumn` synthesized for a carrier group at the previous
                    /// level carries the origin of that group.
                    group->has_function_carrier = group->has_function_carrier || match->via_function_carrier
                        || state.synthesized_carrier_reads->contains(function_node);

                    /// All occurrences must project the same subcolumn of the same column type.
                    /// Types can diverge e.g. when group_by_use_nulls wraps an occurrence used as a
                    /// GROUP BY key into Nullable. The result type of the original function is not
                    /// compared: different carriers of the same subcolumn (`length` and `empty` of
                    /// the same array) share the projection and rebuild their own replacement.
                    if (!group->column_type->equals(*match->column_node->getColumnType())
                        || !group->subcolumn_type->equals(*match->subcolumn_type)
                        || group->requires_tuple_element_guards != match->requires_tuple_element_guards)
                        group->viable = false;

                    /// The occurrence can be replaced with a column reference when it is evaluated
                    /// directly over the rows exported by the subquery: anywhere if the query has no
                    /// aggregation, otherwise before the aggregation step (WHERE, JOIN TREE, arguments
                    /// of aggregate functions) or when the whole expression is an aggregation key.
                    /// An aggregate carrier (`count`) is replaced by another aggregate function
                    /// over the same rows, so it stays valid exactly where the original was.
                    bool replaceable = clause_kind != ClauseKind::Interpolate
                        && (match->replacement_kind == ReplacementKind::SumNot
                            || !state.query_has_aggregation
                            || inside_aggregate_function
                            || clause_kind == ClauseKind::PreAggregation
                            || std::ranges::any_of(
                                state.group_by_keys,
                                [&](const auto & key) { return node->isEqual(*key, {.compare_aliases = false}); }));

                    if (!replaceable)
                        group->viable = false;
                }
            }
        }

        if (function_node->isAggregateFunction())
            inside_aggregate_function = true;
    }

    for (const auto & child : node->getChildren())
        collectCandidates(child, clause_kind, inside_aggregate_function, state);
}

/// Build the expression that reads the subcolumn inside the subquery, or nullptr if it cannot be built.
/// For a column read from a table, it is a direct reference to the subcolumn. For a column exported
/// by a deeper subquery, it is a `getSubcolumn` function that is pushed down further when that
/// subquery is processed.
/// Unwrap a chain of resolved subcolumn functions, composing their paths into `subcolumn_path`.
/// A subcolumn read of another subcolumn read is a read of a deeper subcolumn of the same
/// underlying column, so the paths compose (`a` + `b` -> `a.b`).
QueryTreeNodePtr unwrapSubcolumnFunctions(
    QueryTreeNodePtr node,
    String & subcolumn_path,
    const std::unordered_set<const IQueryTreeNode *> * synthesized_carrier_reads = nullptr,
    bool * via_function_carrier = nullptr)
{
    while (const auto * function_node = node->as<FunctionNode>())
    {
        auto match = matchCandidate(const_cast<FunctionNode &>(*function_node));
        /// Only carriers that are the subcolumn itself compose into a deeper path. A carrier
        /// rewritten into an expression over the subcolumn (`empty`, `isNotNull`, ...) is not
        /// a subcolumn read of its argument, so its path must not be prepended.
        if (!match || match->replacement_kind != ReplacementKind::Direct)
            return nullptr;

        if (via_function_carrier)
            *via_function_carrier = *via_function_carrier || match->via_function_carrier
                || (synthesized_carrier_reads && synthesized_carrier_reads->contains(function_node));

        auto path_prefix = std::move(match->subcolumn_path);
        if (path_prefix.empty())
            return nullptr;

        subcolumn_path = subcolumn_path.empty() ? path_prefix : path_prefix + "." + subcolumn_path;
        node = function_node->getArguments().getNodes()[0];
    }

    return node;
}

QueryTreeNodePtr buildSubcolumnProjectionNode(
    const PushdownGroup & group,
    const QueryTreeNodePtr & inner_node,
    const ContextPtr & context,
    const std::unordered_set<const IQueryTreeNode *> & synthesized_carrier_reads,
    bool & via_function_carrier)
{
    via_function_carrier = group.has_function_carrier;

    /// The projection expression can itself be a subcolumn read left as a `getSubcolumn` function,
    /// e.g. when the subquery exports `json.a AS x` over a deeper subquery.
    String subcolumn_path = group.subcolumn_path;
    QueryTreeNodePtr base_node = unwrapSubcolumnFunctions(inner_node, subcolumn_path, &synthesized_carrier_reads, &via_function_carrier);
    if (!base_node)
        return nullptr;

    auto * inner_column = base_node->as<ColumnNode>();
    if (!inner_column)
        return nullptr;

    /// An exported ALIAS column whose body is just another column of the same table (possibly
    /// chained) is semantically the underlying storage column, so the subcolumn can be read
    /// directly from it. Non-trivial expressions (function calls, casts, ARRAY JOIN and
    /// JOIN USING columns) are rejected by resolveTrivialAliasChain.
    if (inner_column->hasExpression())
    {
        inner_column = resolveTrivialAliasChain(inner_column);
        if (!inner_column)
            return nullptr;
    }

    auto inner_source = inner_column->getColumnSourceOrNull();
    if (!inner_source)
        return nullptr;

    auto * table_node = inner_source->as<TableNode>();
    auto * table_function_node = inner_source->as<TableFunctionNode>();

    if (table_node || table_function_node)
    {
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        /// For queries with FINAL, converting a function to a subcolumn may alter the special
        /// merging algorithms and produce a wrong result, so `FunctionToSubcolumnsPass` refuses
        /// such rewrites. A plain subcolumn access (`t.a`) over a table with FINAL is resolved
        /// into a direct subcolumn read by the analyzer itself, so pushing it down preserves the
        /// semantics of the equivalent direct query; a function-form carrier must not become a
        /// direct subcolumn read that the equivalent direct query would refuse.
        if (via_function_carrier)
        {
            bool has_final = table_node
                ? (table_node->hasTableExpressionModifiers() && table_node->getTableExpressionModifiers()->hasFinal())
                : (table_function_node->hasTableExpressionModifiers() && table_function_node->getTableExpressionModifiers()->hasFinal());
            if (has_final)
                return nullptr;
        }

        /// Some storages expose subcolumns syntactically but opt out of rewriting reads of a column
        /// into direct reads of its subcolumns (e.g. StorageFile, StorageURL, StorageDistributed).
        if (!storage_snapshot->storage.supportsOptimizationToSubcolumns()
            && !(group.requires_tuple_element_guards && storage_snapshot->storage.supportsOptimizationToTupleElementSubcolumns()))
            return nullptr;

        if (storage_snapshot->metadata->isVirtualColumn(inner_column->getColumnName()))
            return nullptr;

        auto subcolumn_full_name = inner_column->getColumnName() + "." + subcolumn_path;

        if (group.requires_tuple_element_guards)
        {
            /// An unnamed tuple names its elements "1", "2", ..., and a source that serves
            /// tuple elements by matching the flattened `<column>.<element>` against the real
            /// field names of a file cannot resolve a bare ordinal; a positional hint must
            /// keep reading the whole tuple (mirrors tupleElementNameIsOrdinalOnly of
            /// `FunctionToSubcolumnsPass`). A source serving subcolumns from its own metadata
            /// does have the ordinal subcolumn.
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(inner_column->getColumnType().get());
            if (!tuple_type
                || tupleElementNameIsAmbiguousWhenFlattened(*tuple_type, subcolumn_path)
                || sourceHasColumnCaseInsensitive(storage_snapshot, subcolumn_full_name)
                || (!tuple_type->hasExplicitNames() && !storage_snapshot->storage.supportsOptimizationToSubcolumns()))
                return nullptr;
        }

        /// An ordinary column with the same name would shadow the subcolumn.
        if (storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All), subcolumn_full_name))
            return nullptr;

        auto subcolumn = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), subcolumn_full_name);
        if (!subcolumn || !subcolumn->type->equals(*group.subcolumn_type))
            return nullptr;

        return std::make_shared<ColumnNode>(NameAndTypePair{subcolumn_full_name, subcolumn->type}, inner_source);
    }

    if (isQueryOrUnionNode(inner_source))
    {
        auto function_node = std::make_shared<FunctionNode>("getSubcolumn");

        auto constant_value = ConstantValue{subcolumn_path, std::make_shared<DataTypeString>()};

        ColumnsWithTypeAndName argument_columns;
        argument_columns.push_back({nullptr, inner_column->getColumnType(), {}});
        argument_columns.push_back({constant_value.getColumn(), constant_value.getType(), {}});

        auto function = FunctionFactory::instance().get("getSubcolumn", context);
        auto function_base = function->build(argument_columns);
        if (!function_base->getResultType()->equals(*group.subcolumn_type))
            return nullptr;

        auto & function_arguments = function_node->getArguments().getNodes();
        function_arguments.push_back(base_node->clone());
        function_arguments.push_back(std::make_shared<ConstantNode>(std::move(constant_value)));

        function_node->resolveAsFunction(std::move(function_base));
        return function_node;
    }

    return nullptr;
}

/// Check that an existing projection node is the same expression as a built subcolumn
/// projection node. Column equality compares only names and types, so the sources of the
/// columns (the same in both nodes by construction of buildSubcolumnProjectionNode) are
/// compared explicitly: a column of an unrelated table could have the same name and type.
bool isSameSubcolumnProjection(const QueryTreeNodePtr & existing_node, const QueryTreeNodePtr & built_node)
{
    if (!existing_node->isEqual(*built_node, {.compare_aliases = false}))
        return false;

    const auto * existing_column = existing_node->as<ColumnNode>();
    const auto * built_column = built_node->as<ColumnNode>();

    if (const auto * built_function = built_node->as<FunctionNode>())
    {
        /// isEqual above guarantees the same structure: getSubcolumn with a column argument.
        existing_column = existing_node->as<FunctionNode>()->getArguments().getNodes()[0]->as<ColumnNode>();
        built_column = built_function->getArguments().getNodes()[0]->as<ColumnNode>();
    }

    if (!existing_column || !built_column)
        return false;

    return existing_column->getColumnSourceOrNull() == built_column->getColumnSourceOrNull();
}

/// The exported columns of a pushdown target: the projection columns of a query,
/// the computed projection columns (names of the first branch, common types) of a union.
NamesAndTypes getExportedColumns(const QueryTreeNodePtr & source)
{
    if (const auto * query_node = source->as<QueryNode>())
        return query_node->getProjectionColumns();
    return source->as<const UnionNode &>().computeProjectionColumns();
}

/// Find the index of the exported column with the given name, when it is unambiguous.
std::optional<size_t> findUnambiguousColumnIndex(const NamesAndTypes & exported_columns, const String & column_name)
{
    std::optional<size_t> result;

    for (size_t i = 0; i < exported_columns.size(); ++i)
    {
        if (exported_columns[i].name == column_name)
        {
            if (result)
                return {};
            result = i;
        }
    }

    return result;
}

/// Validate that the subcolumn projection can be added to the leaf queries of the target at
/// the given projection column index, recursing into every branch of a union, without mutating
/// anything: a union must add the subcolumn to all of its branches or to none of them. When
/// `reuse_index` is set, an exported column with the name of the subcolumn already exists
/// (e.g. it was pushed into this shared target while processing another query referencing it),
/// and every leaf must hold the identical subcolumn projection at that index to reuse it.
bool collectLeafApplications(
    const PushdownGroup & group,
    const QueryTreeNodePtr & source,
    size_t column_index,
    std::optional<size_t> reuse_index,
    const String & new_column_name,
    const std::unordered_set<const IQueryTreeNode *> & synthesized_carrier_reads,
    std::vector<LeafApplication> & applications)
{
    if (auto * union_node = source->as<UnionNode>())
    {
        for (const auto & branch : union_node->getQueries().getNodes())
        {
            if (!collectLeafApplications(group, branch, column_index, reuse_index, new_column_name, synthesized_carrier_reads, applications))
                return false;
        }
        return true;
    }

    auto & subquery = source->as<QueryNode &>();
    const auto & projection_columns = subquery.getProjectionColumns();
    const auto & projection_nodes = subquery.getProjection().getNodes();

    if (column_index >= projection_columns.size() || column_index >= projection_nodes.size())
        return false;

    /// The types of all leaves must match the types at the reference site exactly: with
    /// diverging branch types the union exports the least supertype, and the subcolumn of the
    /// supertype is not guaranteed to be the supertype of the branch subcolumns.
    if (!projection_columns[column_index].type->equals(*group.column_type))
        return false;

    bool via_function_carrier = false;
    auto new_projection_node = buildSubcolumnProjectionNode(
        group, projection_nodes[column_index], subquery.getContext(), synthesized_carrier_reads, via_function_carrier);
    if (!new_projection_node || !new_projection_node->getResultType()->equals(*group.subcolumn_type))
        return false;

    if (reuse_index)
    {
        if (*reuse_index >= projection_columns.size() || *reuse_index >= projection_nodes.size()
            || !projection_columns[*reuse_index].type->equals(*group.subcolumn_type)
            || !isSameSubcolumnProjection(projection_nodes[*reuse_index], new_projection_node))
            return false;

        applications.push_back({&subquery, nullptr, projection_nodes[*reuse_index].get(), via_function_carrier});
        return true;
    }

    /// A projection column of the leaf with the name of the subcolumn would shadow the added one.
    for (const auto & projection_column : projection_columns)
    {
        if (projection_column.name == new_column_name)
            return false;
    }

    applications.push_back({&subquery, std::move(new_projection_node), nullptr, via_function_carrier});
    return true;
}

/// Validate that the subcolumn can be pushed into the target projection without mutating
/// anything, filling the group's new column name and pending per-leaf additions. The whole-column
/// guard in processQuery must know which sibling groups will actually be replaced before any
/// group is applied, so validation is a separate dry run: a group that is viable in the outer
/// query can still fail here (e.g. a shadowing storage column in the subquery, or a branch-local
/// collision in a UNION ALL leaf), and counting it as replaced would let a pushable sibling
/// through while the whole parent column stays alive.
bool validateGroup(PushdownGroup & group, const std::unordered_set<const IQueryTreeNode *> & synthesized_carrier_reads)
{
    auto exported_columns = getExportedColumns(group.source);

    /// The name must be unambiguous.
    auto column_index = findUnambiguousColumnIndex(exported_columns, group.column_name);
    if (!column_index)
        return false;

    /// The type of the column can differ from the type of the exported column,
    /// e.g. when join_use_nulls wraps columns of the outer JOIN into Nullable.
    if (!exported_columns[*column_index].type->equals(*group.column_type))
        return false;

    auto new_column_name = group.column_name + "." + group.subcolumn_path;

    /// An exported column with the name of the subcolumn may already exist. When it is the
    /// same subcolumn expression in every leaf, e.g. it was pushed into this shared target
    /// (an ordinary CTE referenced several times) while processing another query referencing
    /// it, then it is reused (validated per leaf below). Otherwise the reference to the new
    /// column would be ambiguous.
    std::optional<size_t> reuse_index;
    bool reuse_name_present = std::ranges::any_of(
        exported_columns, [&](const auto & exported_column) { return exported_column.name == new_column_name; });

    if (reuse_name_present)
    {
        reuse_index = findUnambiguousColumnIndex(exported_columns, new_column_name);
        if (!reuse_index || !exported_columns[*reuse_index].type->equals(*group.subcolumn_type))
            return false;
    }

    std::vector<LeafApplication> applications;
    if (!collectLeafApplications(group, group.source, *column_index, reuse_index, new_column_name, synthesized_carrier_reads, applications))
        return false;

    group.applications = std::move(applications);
    group.new_column_name = std::move(new_column_name);
    return true;
}

/// Add the subcolumn to the target projection (for a union, to the projection of every leaf
/// query of every branch, at the same position), as validated by validateGroup.
void commitGroup(PushdownGroup & group, std::unordered_set<const IQueryTreeNode *> & synthesized_carrier_reads)
{
    for (auto & application : group.applications)
    {
        /// A `getSubcolumn` projection pushed (or reused) for a group with a function-form
        /// carrier occurrence inherits the carrier origin: when it is pushed further down at
        /// the next level, the FINAL restriction must still apply. Direct table reads are
        /// ColumnNode-s and are never matched again, so only function nodes are recorded.
        bool carrier_origin = group.has_function_carrier || application.via_function_carrier;

        if (application.new_projection_node)
        {
            if (carrier_origin && application.new_projection_node->as<FunctionNode>())
                synthesized_carrier_reads.insert(application.new_projection_node.get());

            application.leaf->addProjectionColumn(
                std::move(application.new_projection_node), NameAndTypePair{group.new_column_name, group.subcolumn_type});
        }
        else if (carrier_origin && application.reused_projection_node && application.reused_projection_node->as<FunctionNode>())
            synthesized_carrier_reads.insert(application.reused_projection_node);
    }

    group.applied = true;
}

/// Identity of the underlying column of a trivial projection expression: the source
/// table expression and the column name in it. The name includes the subcolumn path when the
/// projection expression is a derived subcolumn read (`SELECT tup.a AS x`), so that such an
/// export is related to the exports of the columns it is a part of.
using CanonicalColumn = std::pair<const IQueryTreeNode *, String>;

/// Whether the column `ancestor_name` contains the column `name`: the same column, or a column
/// the other one is a subcolumn of. Reading `tup` reads everything that reading `tup.a` reads.
bool isSameOrAncestorColumn(const String & ancestor_name, const String & name)
{
    return name == ancestor_name
        || (name.size() > ancestor_name.size() && name.starts_with(ancestor_name) && name[ancestor_name.size()] == '.');
}

/// The underlying column an exported projection expression trivially reads, or nullopt when the
/// expression is not a column read (or is a non-trivial ALIAS).
std::optional<CanonicalColumn> resolveCanonicalColumn(const QueryTreeNodePtr & projection_node)
{
    String subcolumn_path;
    auto base_node = unwrapSubcolumnFunctions(projection_node, subcolumn_path);
    if (!base_node)
        return {};

    auto * column = base_node->as<ColumnNode>();
    if (column && column->hasExpression())
        column = resolveTrivialAliasChain(column);
    if (!column)
        return {};

    auto column_source = column->getColumnSourceOrNull();
    if (!column_source)
        return {};

    auto column_name = column->getColumnName();
    if (!subcolumn_path.empty())
        column_name += "." + subcolumn_path;

    return CanonicalColumn{column_source.get(), std::move(column_name)};
}

/// The leaf queries of a query-or-union target, in branch order: the leftmost leaf comes
/// first, and its projection column names are the exported names of the target
/// (UnionNode::computeProjectionColumns takes the names from the first branch transitively).
void collectLeafQueries(const IQueryTreeNode * source, std::vector<const QueryNode *> & leaves)
{
    if (const auto * union_node = source->as<UnionNode>())
    {
        for (const auto & branch : union_node->getQueries().getNodes())
            collectLeafQueries(branch.get(), leaves);
        return;
    }

    if (const auto * query_node = source->as<QueryNode>())
        leaves.push_back(query_node);
}

/// Map each exported column name of the target to the underlying column the corresponding
/// projection expression of the leaf trivially resolves to. The same physical column can be
/// exported under several names: `SELECT tup AS x, tup FROM t`, or a trivial ALIAS storage
/// column next to its base column. A derived subcolumn export (`SELECT tup.a AS x`) is mapped to
/// the underlying column together with its subcolumn path. Names whose projection expression is
/// not a column read (or is a non-trivial ALIAS) are not mapped. The exported names correspond to
/// the leaf's projection slots positionally (for a union, every branch exports under the same names).
std::unordered_map<String, CanonicalColumn> collectCanonicalExports(const QueryNode & subquery, const Names & exported_names)
{
    std::unordered_map<String, CanonicalColumn> result;

    const auto & projection_nodes = subquery.getProjection().getNodes();

    for (size_t i = 0; i < projection_nodes.size() && i < exported_names.size(); ++i)
    {
        if (auto canonical = resolveCanonicalColumn(projection_nodes[i]))
            result.emplace(exported_names[i], std::move(*canonical));
    }

    return result;
}

/// The canonical exports of every leaf query of the target, keyed by the exported names of
/// the target. The aliasing structure can differ between the branches of a union: the same
/// two exported names can be distinct columns in one branch and the same physical column in
/// another, so every branch is collected.
std::vector<std::unordered_map<String, CanonicalColumn>> collectCanonicalExportsPerLeaf(const IQueryTreeNode * source)
{
    std::vector<const QueryNode *> leaves;
    collectLeafQueries(source, leaves);
    if (leaves.empty())
        return {};

    Names exported_names;
    for (const auto & projection_column : leaves.front()->getProjectionColumns())
        exported_names.push_back(projection_column.name);

    std::vector<std::unordered_map<String, CanonicalColumn>> result;
    result.reserve(leaves.size());
    for (const auto * leaf : leaves)
        result.push_back(collectCanonicalExports(*leaf, exported_names));

    return result;
}

/// Build the expression that replaces a matched occurrence with a read of the subcolumn that the
/// target subquery now exports. Its result type is equal to the result type of the original
/// function by construction: `matchCandidate` accepts a carrier only when its result type is the
/// one the built expression produces.
QueryTreeNodePtr buildReplacementNode(const CandidateMatch & match, const PushdownGroup & group, const FunctionNode & function_node)
{
    QueryTreeNodePtr subcolumn_node = std::make_shared<ColumnNode>(
        NameAndTypePair{group.new_column_name, group.subcolumn_type},
        std::static_pointer_cast<ITableExpressionNode>(group.source));

    auto make_function = [&](const String & name, QueryTreeNodes arguments, bool is_operator)
    {
        auto result = std::make_shared<FunctionNode>(name);
        if (is_operator)
            result->markAsOperator();
        result->getArguments().getNodes() = std::move(arguments);
        resolveOrdinaryFunctionNodeByName(*result, name, group.context);
        return result;
    };

    switch (match.replacement_kind)
    {
        case ReplacementKind::Direct:
        {
            const auto & result_type = function_node.getResultType();
            if (!result_type->equals(*group.subcolumn_type))
                return buildCastFunction(subcolumn_node, result_type, group.context);
            return subcolumn_node;
        }
        case ReplacementKind::EqualsZero:
        case ReplacementKind::NotEqualsZero:
        {
            const auto * name = match.replacement_kind == ReplacementKind::EqualsZero ? "equals" : "notEquals";
            return make_function(name, {std::move(subcolumn_node), std::make_shared<ConstantNode>(static_cast<UInt64>(0))}, true);
        }
        case ReplacementKind::Not:
            return make_function("not", {std::move(subcolumn_node)}, true);
        case ReplacementKind::Has:
            return make_function("has", {std::move(subcolumn_node), function_node.getArguments().getNodes()[1]}, false);
        case ReplacementKind::SumNot:
        {
            auto negated = make_function("not", {std::move(subcolumn_node)}, true);

            auto result = std::make_shared<FunctionNode>("sum");
            result->getArguments().getNodes().push_back(std::move(negated));
            resolveAggregateFunctionNodeByName(*result, "sum");
            return result;
        }
    }
}

void replaceCandidates(QueryTreeNodePtr & node, QueryProcessingState & state)
{
    if (!node || isQueryOrUnionNode(node))
        return;

    if (auto * function_node = node->as<FunctionNode>())
    {
        if (auto match = matchCandidate(*function_node))
        {
            const auto * group = state.findGroup(match->column_source.get(), match->column_node->getColumnName(), match->subcolumn_path);
            if (group && group->applied)
            {
                auto replacement = buildReplacementNode(*match, *group, *function_node);
                if (!replacement->getResultType()->equals(*function_node->getResultType()))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Replacement of {} has type {}, expected {}",
                        function_node->getFunctionName(),
                        replacement->getResultType()->getName(),
                        function_node->getResultType()->getName());

                node = std::move(replacement);

                /// A carrier can keep arguments of the original function (the key of
                /// `mapContainsKey`), and they can contain occurrences of their own.
                for (auto & child : node->getChildren())
                    replaceCandidates(child, state);
                return;
            }
        }
    }

    for (auto & child : node->getChildren())
        replaceCandidates(child, state);
}

void processQuery(
    QueryNode & query_node,
    QueryProcessingState & state,
    const std::unordered_set<String> * pruned_exports)
{
    const auto & context = query_node.getContext();
    state.collect_candidates = context->getSettingsRef()[Setting::optimize_push_subcolumns_into_subqueries];

    if (state.collect_candidates)
    {
        collectEligibleTargets(query_node.getJoinTreeNode(), false /*can_be_filled_with_defaults*/, state);

        for (auto it = state.eligible_targets.begin(); it != state.eligible_targets.end();)
        {
            if (canPushIntoTarget(it->second))
                ++it;
            else
                it = state.eligible_targets.erase(it);
        }

        state.query_has_aggregation = query_node.hasGroupBy()
            || hasAggregateFunctionNodes(query_node.getProjectionNode())
            || (query_node.hasHaving() && hasAggregateFunctionNodes(query_node.getHaving()))
            || (query_node.hasOrderBy() && hasAggregateFunctionNodes(query_node.getOrderByNode()))
            || (query_node.hasQualify() && hasAggregateFunctionNodes(query_node.getQualify()));

        if (query_node.hasGroupBy())
        {
            for (const auto & key_node : query_node.getGroupBy().getNodes())
            {
                if (query_node.isGroupByWithGroupingSets())
                {
                    for (const auto & inner_key_node : key_node->as<ListNode &>().getNodes())
                        state.group_by_keys.push_back(inner_key_node);
                }
                else
                    state.group_by_keys.push_back(key_node);
            }
        }
    }

    /// Every clause of the query is traversed, even those that cannot reference columns of the
    /// JOIN TREE (e.g. LIMIT expressions must be constant): besides collecting candidates, the
    /// traversal counts column references, and clauses like LIMIT can contain scalar subqueries
    /// whose correlated columns are such references.
    /// Projection slots whose exported column became unused in the parent queries are skipped:
    /// they are removed by RemoveUnusedProjectionColumnsPass together with everything in them.
    auto for_each_clause = [&](auto && visit_clause)
    {
        visit_clause(query_node.getJoinTreeNode(), ClauseKind::PreAggregation);
        visit_clause(query_node.getPrewhere(), ClauseKind::PreAggregation);
        visit_clause(query_node.getWhere(), ClauseKind::PreAggregation);
        visit_clause(query_node.getWithNode(), ClauseKind::PostAggregation);

        const auto & projection_columns = query_node.getProjectionColumns();
        auto & projection_nodes = query_node.getProjection().getNodes();
        for (size_t i = 0; i < projection_nodes.size(); ++i)
        {
            if (pruned_exports && i < projection_columns.size() && pruned_exports->contains(projection_columns[i].name))
                continue;
            visit_clause(projection_nodes[i], ClauseKind::PostAggregation);
        }

        visit_clause(query_node.getGroupByNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getHaving(), ClauseKind::PostAggregation);
        visit_clause(query_node.getWindowNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getQualify(), ClauseKind::PostAggregation);
        visit_clause(query_node.getOrderByNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getInterpolate(), ClauseKind::Interpolate);
        visit_clause(query_node.getLimitByNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getLimitByLimit(), ClauseKind::PostAggregation);
        visit_clause(query_node.getLimitByOffset(), ClauseKind::PostAggregation);
        visit_clause(query_node.getLimit(), ClauseKind::PostAggregation);
        visit_clause(query_node.getOffset(), ClauseKind::PostAggregation);
    };

    for_each_clause([&](QueryTreeNodePtr & clause_node, ClauseKind clause_kind)
    {
        collectCandidates(clause_node, clause_kind, false /*inside_aggregate_function*/, state);
    });

    /// Dry-run validation: find the groups whose subcolumn can actually be pushed into the
    /// target, without mutating anything yet. The whole-column guard below must count only
    /// these groups as replaced.
    std::unordered_map<const IQueryTreeNode *, std::unordered_set<String>> claimed_new_names;
    for (auto & group : state.groups)
    {
        if (!group.viable || !validateGroup(group, *state.synthesized_carrier_reads))
            continue;

        /// Two distinct groups can produce the same new column name on the same target
        /// (e.g. column `t` with path `a.b` and column `t.a` with path `b`). Both were
        /// validated against the projection before any additions, so committing both would
        /// export duplicate names; only the first one adding a new projection column is kept.
        bool adds_new_column = std::ranges::any_of(
            group.applications, [](const auto & application) { return application.new_projection_node != nullptr; });
        if (adds_new_column && !claimed_new_names[group.source.get()].emplace(group.new_column_name).second)
            continue;

        group.applicable = true;
    }

    bool any_group_applied = false;
    std::unordered_map<const IQueryTreeNode *, std::vector<std::unordered_map<String, CanonicalColumn>>> canonical_exports_by_source;

    for (auto & group : state.groups)
    {
        if (!group.applicable)
            continue;

        /// If the whole column is still referenced outside of the replaced occurrences (either
        /// directly or by an occurrence of a non-viable or non-applicable group), the parent projection column
        /// stays, and the subquery would read both the whole column and the subcolumn from the
        /// table. Extracting the subcolumn from the already read column is cheaper, so the
        /// group is not applied. The reference counts are keyed by exported names, but the same
        /// physical column can be exported under several names, so the counts of every name
        /// resolving to the same underlying column as the group's column are combined: while any
        /// of them stays alive, the whole column is read from the table anyway.
        /// An export that is a derived subcolumn read (`SELECT tup.a AS x`) is contained in the
        /// exports of the columns it is a part of, so a live export of such a parent column
        /// (`SELECT tup.a AS x, tup FROM t` with `tup` referenced) keeps everything the pushed
        /// subcolumn reads alive as well and blocks the pushdown too. Exports of deeper
        /// subcolumns of the group's column do not: they read only a part of it.
        /// For a union target two names are combined when they resolve to the same underlying
        /// column in ANY leaf branch: the pushdown is applied to all branches or to none, so a
        /// single branch keeping the whole column alive through an alias-equivalent name is
        /// enough for that branch to read both the whole column and the subcolumn.
        auto [exports_it, exports_inserted] = canonical_exports_by_source.try_emplace(group.source.get());
        if (exports_inserted)
            exports_it->second = collectCanonicalExportsPerLeaf(group.source.get());
        const auto & canonical_exports_per_leaf = exports_it->second;

        auto contains_underlying_column = [&](const String & column_name)
        {
            if (column_name == group.column_name)
                return true;
            for (const auto & canonical_exports : canonical_exports_per_leaf)
            {
                auto group_it = canonical_exports.find(group.column_name);
                if (group_it == canonical_exports.end())
                    continue;
                auto other_it = canonical_exports.find(column_name);
                if (other_it == canonical_exports.end() || other_it->second.first != group_it->second.first)
                    continue;
                if (isSameOrAncestorColumn(other_it->second.second, group_it->second.second))
                    return true;
            }
            return false;
        };

        size_t references = 0;
        for (const auto & [column_name, count] : state.column_references[group.source.get()])
        {
            if (contains_underlying_column(column_name))
                references += count;
        }

        /// Only the occurrences of groups that are known to be applicable count as replaced:
        /// a viable but non-applicable sibling keeps its `getSubcolumn` occurrences, and they
        /// keep the whole parent column alive.
        size_t replaced_references = 0;
        for (const auto & other_group : state.groups)
        {
            if (other_group.applicable && other_group.source == group.source && contains_underlying_column(other_group.column_name))
                replaced_references += other_group.occurrences;
        }

        if (references > replaced_references)
            continue;

        commitGroup(group, *state.synthesized_carrier_reads);
        any_group_applied = true;
    }

    if (any_group_applied)
    {
        for_each_clause([&](QueryTreeNodePtr & clause_node, ClauseKind)
        {
            replaceCandidates(clause_node, state);
        });
    }
}

/// Exported column names of a target that are dead after the rewrite: an exported column with
/// some references replaced by pushed subcolumns and no references remaining is removed by the
/// subsequent RemoveUnusedProjectionColumnsPass. The same underlying column can be exported
/// under several names or paths (`SELECT tup.a AS x, tup FROM ...`); a never-referenced sibling
/// of a dead export is unused in the parents too and is removed together with it. Therefore,
/// when a dead export is a subcolumn of another export from the same source, the ancestor export
/// is dead too — keying only by the exported name or exact subcolumn path would keep sibling
/// slots, and the whole-column references inside them would block pushdown through the deeper
/// levels. The reverse is not true: the added subcolumn projection is the replacement for a dead
/// parent-column export and must remain live.
/// Every leaf branch of the target contributes its own relationships (they can differ between
/// the branches of a union); a name marked dead in one branch can expose another relationship
/// in a different branch, so the expansion runs to a fixpoint.
std::unordered_set<String> collectDeadExports(
    const std::unordered_map<String, size_t> & replaced_columns,
    const std::unordered_map<String, size_t> & alive_columns,
    const IQueryTreeNode * node)
{
    std::unordered_set<String> dead;

    auto alive_count = [&](const String & column_name)
    {
        auto it = alive_columns.find(column_name);
        return it == alive_columns.end() ? 0uz : it->second;
    };

    for (const auto & [column_name, replaced] : replaced_columns)
    {
        if (replaced > 0 && alive_count(column_name) == 0)
            dead.insert(column_name);
    }

    if (dead.empty())
        return dead;

    std::vector<std::unordered_map<String, CanonicalColumn>> canonical_exports_per_leaf;
    for (const auto & canonical_exports : collectCanonicalExportsPerLeaf(node))
        canonical_exports_per_leaf.push_back(canonical_exports);

    bool changed = true;
    while (changed)
    {
        changed = false;

        for (const auto & canonical_exports : canonical_exports_per_leaf)
        {
            for (const auto & [dead_name, dead_canonical] : canonical_exports)
            {
                if (!dead.contains(dead_name))
                    continue;

                for (const auto & [column_name, canonical] : canonical_exports)
                {
                    if (alive_count(column_name) == 0
                        && canonical.first == dead_canonical.first
                        && isSameOrAncestorColumn(canonical.second, dead_canonical.second))
                        changed |= dead.insert(column_name).second;
                }
            }
        }
    }

    return dead;
}

}

void PushSubcolumnsIntoSubqueriesPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr /*context*/)
{
    /// An ordinary (non-materialized) CTE referenced several times is a single query node
    /// shared by all the referencing queries, so the query graph is a DAG rather than a tree.
    /// Queries are processed in topological order: a shared subquery is processed only after
    /// every query referencing it, so that it sees all the subcolumns pushed into it (and
    /// pushes them further down), and the liveness of its exported columns is fully known.

    /// Discovery: record the directly nested query and union nodes of every node.
    QueryTreeNodes discovery_order = {query_tree_node};
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodes> nested_subqueries;
    std::unordered_map<const IQueryTreeNode *, size_t> unprocessed_parents;

    {
        std::unordered_set<const IQueryTreeNode *> discovered = {query_tree_node.get()};

        for (size_t i = 0; i < discovery_order.size(); ++i)
        {
            auto current = discovery_order[i];
            auto & current_nested_subqueries = nested_subqueries[current.get()];

            std::unordered_set<const IQueryTreeNode *> unique_nested_subqueries;
            QueryTreeNodes stack = current->getChildren();

            while (!stack.empty())
            {
                auto node = std::move(stack.back());
                stack.pop_back();

                if (!node)
                    continue;

                if (isQueryOrUnionNode(node))
                {
                    if (unique_nested_subqueries.emplace(node.get()).second)
                    {
                        ++unprocessed_parents[node.get()];
                        current_nested_subqueries.push_back(node);

                        if (discovered.emplace(node.get()).second)
                            discovery_order.push_back(node);
                    }
                    continue;
                }

                for (const auto & child : node->getChildren())
                    stack.push_back(child);
            }
        }
    }

    /// Number of references to each exported column of a query or union node that stay in the
    /// referencing queries after the rewrite, and the number of replaced references, combined
    /// over all the processed queries. An exported column with some references replaced and no
    /// references remaining is removed by the subsequent RemoveUnusedProjectionColumnsPass, so
    /// when the subquery itself is processed, references inside such dead projection slots must
    /// not count as uses of the whole column (otherwise pushdown through several levels of
    /// subqueries would stop at the first level).
    std::unordered_map<const IQueryTreeNode *, std::unordered_map<String, size_t>> alive_references;
    std::unordered_map<const IQueryTreeNode *, std::unordered_map<String, size_t>> replaced_references;
    std::unordered_set<const IQueryTreeNode *> processed;

    /// See QueryProcessingState::synthesized_carrier_reads; shared across the whole run so that
    /// the carrier origin survives pushdown through several levels of subqueries.
    std::unordered_set<const IQueryTreeNode *> synthesized_carrier_reads;

    auto process_node = [&](const QueryTreeNodePtr & node)
    {
        if (!processed.emplace(node.get()).second)
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
        {
            /// A union node has no clauses of its own: its exported columns map positionally
            /// onto the projection columns of every branch. An exported column of the union
            /// that became dead (fully replaced by pushed subcolumns) is propagated as a dead
            /// column of each branch, so that the branches, processed after the union, skip
            /// the corresponding projection slots and can push the subcolumns further down.
            const auto * union_node = node->as<UnionNode>();
            if (!union_node || union_node->hasRecursiveCTETable())
                return;

            auto replaced_it = replaced_references.find(node.get());
            if (replaced_it == replaced_references.end())
                return;

            auto exported_columns = union_node->computeProjectionColumns();

            for (const auto & column_name : collectDeadExports(replaced_it->second, alive_references[node.get()], node.get()))
            {
                auto column_index = findUnambiguousColumnIndex(exported_columns, column_name);
                if (!column_index)
                    continue;

                for (const auto & branch : union_node->getQueries().getNodes())
                {
                    auto branch_columns = getExportedColumns(branch);
                    if (*column_index < branch_columns.size())
                        ++replaced_references[branch.get()][branch_columns[*column_index].name];
                }
            }

            return;
        }

        std::unordered_set<String> pruned_exports;
        if (auto replaced_it = replaced_references.find(node.get()); replaced_it != replaced_references.end())
            pruned_exports = collectDeadExports(replaced_it->second, alive_references[node.get()], node.get());

        QueryProcessingState state;
        state.synthesized_carrier_reads = &synthesized_carrier_reads;
        processQuery(*query_node, state, pruned_exports.empty() ? nullptr : &pruned_exports);

        for (const auto & [source, columns] : state.column_references)
        {
            for (const auto & [column_name, references] : columns)
            {
                size_t replaced = 0;
                for (const auto & group : state.groups)
                {
                    if (group.applied && group.source.get() == source && group.column_name == column_name)
                        replaced += group.occurrences;
                }
                alive_references[source][column_name] += references - replaced;
                replaced_references[source][column_name] += replaced;
            }
        }
    };

    QueryTreeNodes ready = {query_tree_node};
    while (!ready.empty())
    {
        auto node = std::move(ready.back());
        ready.pop_back();

        process_node(node);

        for (const auto & subquery : nested_subqueries[node.get()])
        {
            if (--unprocessed_parents[subquery.get()] == 0)
                ready.push_back(subquery);
        }
    }

    /// Nodes whose number of unprocessed parents never reached zero are parts of reference
    /// cycles (e.g. recursive CTEs); they are processed in discovery order.
    for (const auto & node : discovery_order)
        process_node(node);
}

}

#include <cmath>
#include <functional>
#include <iterator>
#include <Access/ContextAccess.h>
#include <Access/EnabledRowPolicies.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/traverseQueryTree.h>
#include <Common/Logger.h>
#include <Common/NaNUtils.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Columns/ColumnString.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/addMissingDefaults.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Planner/CollectSets.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/narrowPipe.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnDefault.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageView.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool distributed_aggregation_memory_efficient;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsFloat max_streams_multiplier_for_merge_tables;
    extern const SettingsUInt64 merge_table_max_tables_to_look_for_schema_inference;
    extern const SettingsBool parallel_replicas_allow_merge_tables;
    extern const SettingsBool parallel_replicas_plan_based;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace FailPoints
{
    extern const char storage_merge_create_children_plans_pause[];
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int SAMPLING_NOT_SUPPORTED;
extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
extern const int DATABASE_ACCESS_DENIED;
extern const int STORAGE_REQUIRES_PARAMETER;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_TABLE;
extern const int PARAMETER_OUT_OF_BOUND;
extern const int UNSUPPORTED_METHOD;
extern const int INCOMPATIBLE_COLUMNS;
}

namespace
{

bool queryHasOrderBy(const SelectQueryInfo & query_info)
{
    if (query_info.query_tree)
    {
        if (const auto * query_node = query_info.query_tree->as<QueryNode>())
            return query_node->hasOrderBy();
    }

    if (query_info.query)
    {
        if (const auto * select = query_info.query->as<ASTSelectQuery>())
            return select->orderBy() != nullptr;
    }

    return false;
}

/// The storage a database enumerates is not always the storage a read must go through: a
/// `MaterializedPostgreSQL` database exposes the physical nested `ReplacingMergeTree` tables to
/// generic enumerators and hands out the `StorageMaterializedPostgreSQL` wrapper for reads. Reading
/// the nested table directly would bypass the wrapper's `_sign = 1` filter and forced `FINAL` and
/// return stale and deleted rows. See `IDatabase::getTableForRead`.
StoragePtr tableForRead(const DatabasePtr & database, const String & table_name, const StoragePtr & table, const ContextPtr & local_context)
{
    if (!database)
        return table;

    return database->getTableForRead(table_name, table, local_context);
}

/// The planner bounds-checks only `max_streams * max_streams_to_max_threads_ratio`, so the product
/// of the requested number of streams and the (clamped) `max_streams_multiplier_for_merge_tables`
/// can still exceed the range of `size_t`, and casting such a `Float64` to `size_t` is undefined
/// behavior.
size_t applyStreamsMultiplier(size_t requested_num_streams, Float64 num_streams_multiplier)
{
    Float64 num_streams = static_cast<Float64>(requested_num_streams) * num_streams_multiplier;
    if (!canConvertTo<size_t>(num_streams))
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Exceeded limit for the number of streams with `max_streams_multiplier_for_merge_tables`. "
            "Make sure that `max_streams * max_streams_multiplier_for_merge_tables` is in some reasonable boundaries, "
            "current value: {}",
            num_streams);
    return static_cast<size_t>(num_streams);
}

/// `ColumnsDescription` registers no subcolumns for an ALIAS column, so a name like `arr.size0`
/// never resolves through the subcolumn index even though the alias expression can produce it.
bool isSubcolumnOfAliasColumn(const ColumnsDescription & storage_columns, const String & name)
{
    for (auto [parent_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(name))
    {
        const auto * parent = storage_columns.tryGet(String(parent_name));
        if (!parent || parent->default_desc.kind != ColumnDefaultKind::Alias)
            continue;

        if (parent->type->tryGetSubcolumnType(subcolumn_name))
            return true;
    }

    return false;
}

}

StorageMerge::DatabaseNameOrRegexp::DatabaseNameOrRegexp(
    const String & source_database_name_or_regexp_,
    bool database_is_regexp_,
    std::optional<OptimizedRegularExpression> source_database_regexp_,
    std::optional<OptimizedRegularExpression> source_table_regexp_,
    std::optional<DBToTableSetMap> source_databases_and_tables_)
    : source_database_name_or_regexp(source_database_name_or_regexp_)
    , database_is_regexp(database_is_regexp_)
    , source_database_regexp(std::move(source_database_regexp_))
    , source_table_regexp(std::move(source_table_regexp_))
    , source_databases_and_tables(std::move(source_databases_and_tables_))
{
}

StorageMerge::StorageMerge(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    const String & source_database_name_or_regexp_,
    bool database_is_regexp_,
    const DBToTableSetMap & source_databases_and_tables_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , database_name_or_regexp(
        source_database_name_or_regexp_,
        database_is_regexp_,
        source_database_name_or_regexp_, {},
        source_databases_and_tables_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_.empty()
        ? getColumnsDescriptionFromSourceTables(context_)
        : columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

StorageMerge::StorageMerge(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    const String & source_database_name_or_regexp_,
    bool database_is_regexp_,
    const String & source_table_regexp_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , database_name_or_regexp(
        source_database_name_or_regexp_,
        database_is_regexp_,
        source_database_name_or_regexp_,
        source_table_regexp_, {})
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_.empty()
        ? getColumnsDescriptionFromSourceTables(context_)
        : columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

StorageMerge::DatabaseTablesIterators StorageMerge::getDatabaseIterators(ContextPtr context_) const
{
    return database_name_or_regexp.getDatabaseIterators(context_);
}

ColumnsDescription StorageMerge::getColumnsDescriptionFromSourceTables(
    const ContextPtr & query_context,
    const String & source_database_name_or_regexp,
    bool database_is_regexp,
    const String & source_table_regexp,
    size_t max_tables_to_look)
{
    DatabaseNameOrRegexp database_name_or_regexp(source_database_name_or_regexp, database_is_regexp, source_database_name_or_regexp, source_table_regexp, {});
    return getColumnsDescriptionFromSourceTablesImpl(query_context, database_name_or_regexp, max_tables_to_look, nullptr);
}

ColumnsDescription StorageMerge::getColumnsDescriptionFromSourceTables(const ContextPtr & query_context) const
{
    auto max_tables_to_look = query_context->getSettingsRef()[Setting::merge_table_max_tables_to_look_for_schema_inference];
    auto res = getColumnsDescriptionFromSourceTablesImpl(query_context, database_name_or_regexp, max_tables_to_look, this);
    if (res.empty())
        throw Exception{DB::ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "There are no tables satisfied provided regexp, you must specify table structure manually"};
    return res;
}

ColumnsDescription StorageMerge::getColumnsDescriptionFromSourceTablesImpl(
    const ContextPtr & query_context,
    const DatabaseNameOrRegexp & database_name_or_regexp,
    size_t max_tables_to_look,
    const IStorage * ignore_self)
{
    auto access = query_context->getAccess();
    size_t table_num = 0;
    ColumnsDescription res;

    traverseTablesUntilImpl(query_context, ignore_self, database_name_or_regexp, [&table_num, &access, &res, max_tables_to_look, &query_context](auto && t)
    {
        if (!t)
            return false;

        const auto storage_id = t->getStorageID();
        if (!access->isGranted(AccessType::SHOW_TABLES, storage_id.database_name, storage_id.table_name))
            return false;

        access->checkAccess(AccessType::SHOW_COLUMNS, storage_id.database_name, storage_id.table_name);
        auto table_metadata = t->getInMemoryMetadataPtr(query_context, false);
        auto structure = table_metadata->getColumns();
        String prev_column_name;
        for (const ColumnDescription & column : structure)
        {
            if (!res.has(column.name))
            {
                res.add(column, prev_column_name);
            }
            else if (column != res.get(column.name))
            {
                res.modify(column.name, [&column](ColumnDescription & what)
                {
                    what.type = getLeastSupertypeOrVariant(DataTypes{what.type, column.type});
                    if (what.default_desc != column.default_desc)
                        what.default_desc = {};
                });
            }
            prev_column_name = column.name;
        }

        ++table_num;
        return table_num >= max_tables_to_look;
    });

    return res;
}

template <typename F>
StoragePtr StorageMerge::traverseTablesUntil(F && predicate) const
{
    return traverseTablesUntilImpl(getContext(), this, database_name_or_regexp, std::forward<F>(predicate));
}

template <typename F>
StoragePtr StorageMerge::traverseTablesUntilImpl(const ContextPtr & query_context, const IStorage * ignore_self, const DatabaseNameOrRegexp & database_name_or_regexp, F && predicate)
{
    auto database_table_iterators = database_name_or_regexp.getDatabaseIterators(query_context);

    for (auto & iterator : database_table_iterators)
    {
        auto database = DatabaseCatalog::instance().tryGetDatabase(iterator->databaseName());

        while (iterator->isValid())
        {
            const auto & nested = iterator->table();
            if (nested.get() != ignore_self)
            {
                if (auto table = tableForRead(database, iterator->name(), nested, query_context); table && predicate(table))
                    return table;
            }

            iterator->next();
        }
    }

    return {};
}

template <typename F>
void StorageMerge::forEachTable(F && func) const
{
    traverseTablesUntil([&func](const auto & table)
    {
        func(table);
        /// Always continue to the next table.
        return false;
    });
}

bool StorageMerge::isRemote() const
{
    auto first_remote_table = traverseTablesUntil([](const StoragePtr & table) { return table && table->isRemote(); });
    return first_remote_table != nullptr;
}

bool StorageMerge::hasChildTable(std::function<bool(const StoragePtr &)> predicate) const
{
    return traverseTablesUntil([&predicate](const StoragePtr & table)
    {
        return table && predicate(table);
    }) != nullptr;
}

bool StorageMerge::supportsPrewhere() const
{
    return traverseTablesUntil([](const auto & table) { return !table->supportsPrewhere(); }) == nullptr;
}

bool StorageMerge::supportsOptimizationToSubcolumns() const
{
    return traverseTablesUntil([](const auto & table) { return !table->supportsOptimizationToSubcolumns(); }) == nullptr;
}

bool StorageMerge::supportsOptimizationToTupleElementSubcolumns() const
{
    return traverseTablesUntil([](const auto & table) { return !table->supportsOptimizationToTupleElementSubcolumns(); }) == nullptr;
}

bool StorageMerge::canMoveConditionsToPrewhere() const
{
    /// NOTE: This check and the above check are used during query analysis as condition for applying
    /// "move to PREWHERE" optimization. However, it contains a logical race:
    /// If new table that matches regexp for current storage and doesn't support PREWHERE
    /// will appear after this check and before calling "read" method, the optimized query may fail.
    /// Since it's quite rare case, we just ignore this possibility.
    /// TODO: Store tables inside StorageSnapshot
    ///
    /// NOTE: Type can be different, and in this case, PREWHERE cannot be
    /// applied for those columns, but there a separate method to return
    /// supported columns for PREWHERE - supportedPrewhereColumns().
    return traverseTablesUntil([](const auto & table) { return !table->canMoveConditionsToPrewhere(); }) == nullptr;
}

std::optional<NameSet> StorageMerge::supportedPrewhereColumns() const
{
    bool supports_prewhere = true;

    const auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    const auto & columns = metadata_snapshot->getColumns();

    NameSet supported_columns;

    std::unordered_map<std::string, std::pair<const IDataType *, ColumnDefaultKind>> column_info;
    for (const auto & name_type : columns.getAll())
    {
        const auto & column_default = columns.getDefault(name_type.name).value_or(ColumnDefault{});
        column_info.emplace(name_type.name, std::make_pair(
            name_type.type.get(),
            column_default.kind));
        supported_columns.emplace(name_type.name);
    }

    forEachTable([&](const StoragePtr & table)
    {
        const auto table_metadata_ptr = table->getInMemoryMetadataPtr(getContext(), false);
        if (!table_metadata_ptr)
            supports_prewhere = false;
        if (!supports_prewhere)
            return;

        const auto & table_columns = table_metadata_ptr->getColumns();
        for (const auto & column : table_columns.getAll())
        {
            const auto & column_default = table_columns.getDefault(column.name).value_or(ColumnDefault{});
            const auto & [root_type, src_default_kind] = column_info[column.name];
            if ((root_type && !root_type->equals(*column.type)) ||
                !columnDefaultKindHasSameType(src_default_kind, column_default.kind))
            {
                supported_columns.erase(column.name);
            }
        }

        /// A column the child does not declare at all fails the same way: it is stripped from the
        /// child's read list and filled with defaults only after the read, so a filter pushed into
        /// that read has no input for it.
        std::erase_if(supported_columns, [&](const auto & name) { return !table_columns.has(name); });

        /// The loop above compares the root type against the child's *declared* columns. When the
        /// child aggregates other tables itself (a nested `Merge`, a `MaterializedView`, ...), its
        /// declared type can match while a leaf's differs. PREWHERE would then be built against the
        /// root type and re-derived against the leaf's, so `ActionsDAG` sees a return type that
        /// disagrees with the node it stored and throws `Unexpected return type from ...`.
        /// Intersect with what the child itself allows, so the constraint holds transitively.
        /// `supportsPrewhere` above is already transitive - it recurses through virtual dispatch.
        if (const auto nested_supported_columns = table->supportedPrewhereColumns())
            std::erase_if(supported_columns, [&](const auto & name) { return !nested_supported_columns->contains(name); });
    });

    return supported_columns;
}

namespace
{

/// Does converting a column from `from` to `to` keep the order AND map distinct values to distinct
/// ones? The `Array` branch composes this elementwise, so a collapsing pair would reorder arrays.
/// Unrecognised pairs are refused: a false "safe" gives wrong results, a false "unsafe" a pushdown.
bool conversionPreservesOrder(const IDataType & from, const IDataType & to)
{
    if (from.equals(to))
        return true;

    const WhichDataType which_from(from);
    const WhichDataType which_to(to);

    /// An `Enum` is `static_cast` to the target's field type, so the order survives only when that
    /// mapping is the identity: the target must agree on the values AND be wide enough not to
    /// truncate, which `contains` does not check. An unmatched `to` falls through to the unwrapping.
    if (const auto * from_enum = dynamic_cast<const IDataTypeEnum *>(&from))
    {
        if (const auto * to_enum = dynamic_cast<const IDataTypeEnum *>(&to))
        {
            if (from.getSizeOfValueInMemory() <= to.getSizeOfValueInMemory() && to_enum->contains(*from_enum))
                return true;
        }
        else if (which_to.isInt() && from.getSizeOfValueInMemory() <= to.getSizeOfValueInMemory())
            return true;
    }

    /// Widening an integer keeps the order when the signedness is preserved or the target is
    /// signed, mirroring `ToNumberMonotonicity`'s expansion branch. An equal width can flip the
    /// sign bit and a narrowing wraps, so both stay refused. `isInteger` covers the wide types as
    /// well: `getLeastSupertype` derives `Int128`/`UInt128`/`Int256`/`UInt256` for an ordinary
    /// column-list-less `Merge` over mixed integer widths, and those casts are just as injective.
    if (which_from.isInteger() && which_to.isInteger()
        && from.getSizeOfValueInMemory() < to.getSizeOfValueInMemory()
        && (from.isValueRepresentedByUnsignedInteger() == to.isValueRepresentedByUnsignedInteger()
            || !to.isValueRepresentedByUnsignedInteger()))
        return true;

    /// `ColumnLowCardinality::compareAt` compares through the dictionary, so a `LowCardinality`
    /// column orders exactly like its nested type. The wrapper is therefore stripped from either
    /// side; it never nests, so the stripped side is not `LowCardinality` again.
    const auto * from_lc = typeid_cast<const DataTypeLowCardinality *>(&from);
    const auto * to_lc = typeid_cast<const DataTypeLowCardinality *>(&to);
    if (from_lc || to_lc)
        return conversionPreservesOrder(
            from_lc ? *from_lc->getDictionaryType() : from, to_lc ? *to_lc->getDictionaryType() : to);

    /// Keeping or adding nullability moves no value: no NULL appears and every non-NULL keeps its
    /// place, so only the nested pair matters. Removing it falls through, because a nullable value
    /// then has to become a concrete one and NULL placement changes.
    if (const auto * to_nullable = typeid_cast<const DataTypeNullable *>(&to))
    {
        const auto * from_nullable = typeid_cast<const DataTypeNullable *>(&from);
        return conversionPreservesOrder(from_nullable ? *from_nullable->getNestedType() : from, *to_nullable->getNestedType());
    }

    /// `ColumnArray::compareAt` compares elementwise then by length, so a strictly monotonic element
    /// conversion orders arrays the same way. Both sides must be `Array`: wrapping or unwrapping one
    /// changes what is compared. `Tuple` and `Map` need their own analysis and stay refused.
    const auto * from_array = typeid_cast<const DataTypeArray *>(&from);
    const auto * to_array = typeid_cast<const DataTypeArray *>(&to);
    if (from_array && to_array)
        return conversionPreservesOrder(*from_array->getNestedType(), *to_array->getNestedType());

    return false;
}

}

bool StorageMerge::supportedPrewhereColumnsIncludeSubcolumns() const
{
    /// The filter is re-derived against every child, so a subcolumn rides its origin column
    /// only if all of them resolve it.
    bool include_subcolumns = true;
    forEachTable([&](const StoragePtr & table)
    {
        include_subcolumns = include_subcolumns && table->supportedPrewhereColumnsIncludeSubcolumns();
    });
    return include_subcolumns;
}

QueryProcessingStage::Enum StorageMerge::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    /// In case of JOIN or ARRAY JOIN the first stage (which includes JOIN/ARRAY JOIN)
    /// should be done on the initiator always.
    ///
    /// Since in case of JOIN query on shards will receive query without JOIN (and their columns).
    /// (see removeJoin())
    ///
    /// ARRAY JOIN also requires FetchColumns because `buildQueryPlanForArrayJoinNode` expects
    /// the child plan to be at FetchColumns stage. If we return a later stage here,
    /// the ARRAY JOIN processing is skipped entirely in `buildJoinTreeQueryPlan`
    /// (see the early return when stage != FetchColumns), leading to missing chunk info
    /// in MergingAggregatedTransform.
    ///
    /// And for this we need to return FetchColumns.
    if (const auto * select = query_info.query->as<ASTSelectQuery>(); select && (hasJoin(*select) || hasArrayJoin(*select)))
        return QueryProcessingStage::FetchColumns;

    auto stage_in_source_tables = QueryProcessingStage::FetchColumns;

    DatabaseTablesIterators database_table_iterators = database_name_or_regexp.getDatabaseIterators(local_context);

    size_t selected_table_size = 0;
    bool any_child_conversion_breaks_order = false;
    /// These are the types `convertAndFilterSourceStream` casts every child stream to, because the
    /// same snapshot builds the common header (see `read`). Aliases cross that boundary as well, so
    /// they are compared too; `Ephemeral` is not, since it is never read from a source table.
    const GetColumnsOptions order_relevant_columns(GetColumnsOptions::AllPhysicalAndAliases);
    const auto & declared_columns = storage_snapshot->metadata->getColumns();

    for (const auto & iterator : database_table_iterators)
    {
        auto database = DatabaseCatalog::instance().tryGetDatabase(iterator->databaseName());

        while (iterator->isValid())
        {
            const auto & nested = iterator->table();
            auto table = nested.get() == this ? nested : tableForRead(database, iterator->name(), nested, local_context);
            if (table && table.get() != this)
            {
                ++selected_table_size;
                const auto table_metadata = table->getInMemoryMetadataPtr(local_context, false);
                stage_in_source_tables = std::max(
                    stage_in_source_tables,
                    table->getQueryProcessingStage(local_context, to_stage,
                        table->getStorageSnapshot(table_metadata, local_context), query_info));

                for (const auto & child_column : table_metadata->getColumns().get(order_relevant_columns))
                {
                    auto declared_column = declared_columns.tryGetColumn(order_relevant_columns, child_column.name);
                    if (declared_column && !conversionPreservesOrder(*child_column.type, *declared_column->type))
                        any_child_conversion_breaks_order = true;
                }
            }

            iterator->next();
        }
    }

    auto stage = selected_table_size == 1 ? stage_in_source_tables : std::min(stage_in_source_tables, QueryProcessingStage::WithMergeableState);

    /// Caller asked for WithMergeableState but a child reported a higher stage
    /// (e.g. Distributed with `distributed_group_by_no_merge=1` reports Complete).
    /// Cap to WithMergeableState so we don't emit finalized values where the caller
    /// expects AggregateFunction states - otherwise `convertAndFilterSourceStream`
    /// throws CANNOT_CONVERT_TYPE. The multi-table branch above already caps at
    /// WithMergeableState for the same reason; this extends it to the single-table branch.
    ///
    /// Only when the caller asked for exactly WithMergeableState: for FetchColumns the
    /// caller wants raw columns, the child's higher stage (Complete from a single-shard
    /// Distributed) is fine, and raising it to WithMergeableState routes the child onto a
    /// path that keeps the analyzer-qualified `__table1.name` header (THERE_IS_NO_COLUMN
    /// under serialize_query_plan).
    if (to_stage == QueryProcessingStage::WithMergeableState && stage > to_stage)
        stage = QueryProcessingStage::WithMergeableState;

    /// Gated on the effective stage, not on `to_stage`: a single-node `Distributed` child returns
    /// `Complete` even for a `FetchColumns` request, and that is deliberately kept above.
    if (stage > QueryProcessingStage::FetchColumns && any_child_conversion_breaks_order)
        return QueryProcessingStage::FetchColumns;

    return stage;
}

VirtualColumnsDescription StorageMerge::createVirtuals()
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    return desc;
}

StorageMetadataHandle StorageMerge::getInMemoryMetadataPtr(ContextPtr query_context, bool bypass_metadata_cache) const
{
    auto base_metadata = IStorage::getInMemoryMetadataPtr(query_context, bypass_metadata_cache);
    if (!query_context)
        return base_metadata;

    auto virtuals = createVirtuals();
    try
    {
        const auto & access = query_context->getAccess();
        if (auto first_table = traverseTablesUntil([access](auto && table)
        {
            if (!table)
                return false;

            auto id = table->getStorageID();
            return access->isGranted(AccessType::SHOW_TABLES, id.database_name, id.table_name);
        }))
        {
            const auto source_table_metadata = first_table->getInMemoryMetadataPtr(query_context, bypass_metadata_cache);
            for (const auto & column : source_table_metadata->virtuals)
            {
                if (virtuals.has(column.name))
                    continue;

                virtuals.add(column);
            }
        }
    }
    catch (const Exception & e)
    {
        /// The source database may have been dropped (`UNKNOWN_DATABASE`), or it may be the internal
        /// database of temporary tables, which `getDatabaseIterator` refuses to enumerate
        /// (`DATABASE_ACCESS_DENIED`). Neither should prevent resolving the table's own metadata:
        /// the virtual columns of the source tables are a best-effort enrichment, and an actual read
        /// still throws in `getDatabaseIterators`. In particular, loading a stored table definition
        /// (`ATTACH`, backup `RESTORE`, replicated-database replay) validates the storage through here.
        if (e.code() != ErrorCodes::UNKNOWN_DATABASE && e.code() != ErrorCodes::DATABASE_ACCESS_DENIED)
            throw;
    }

    return std::make_shared<StorageInMemoryMetadata>(base_metadata->withVirtuals(std::move(virtuals)));
}

void StorageMerge::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    size_t num_streams)
{
    /// What will be result structure depending on query processed stage in source tables?
    auto common_header = getHeaderForProcessingStage(column_names, storage_snapshot, query_info, local_context, processed_stage);

    if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer]
        && processed_stage != QueryProcessingStage::FetchColumns)
    {
        auto block = *common_header;
        /// Remove constants.
        /// For StorageDistributed some functions like `hostName` that are constants only for local queries.
        for (auto & column : block)
            column.column = column.column->convertToFullColumnIfConst();
        common_header = std::make_shared<const Block>(std::move(block));
    }

    auto step = std::make_unique<ReadFromMerge>(
        column_names,
        query_info,
        storage_snapshot,
        local_context,
        common_header,
        max_block_size,
        num_streams,
        shared_from_this(),
        processed_stage);

    query_plan.addStep(std::move(step));
}

ReadFromMerge::ReadFromMerge(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    SharedHeader common_header_,
    size_t max_block_size,
    size_t num_streams,
    StoragePtr storage,
    QueryProcessingStage::Enum processed_stage)
    : SourceStepWithFilter(common_header_, column_names_, query_info_, storage_snapshot_, context_)
    , required_max_block_size(max_block_size)
    , requested_num_streams(num_streams)
    , common_header(common_header_)
    , all_column_names(column_names_)
    , storage_merge(std::move(storage))
    , merge_storage_snapshot(storage_snapshot)
    , common_processed_stage(processed_stage)
{
}

/// True if the query has subquery sets (`IN (SELECT ...)`). A child plan is built and optimized
/// while the *outer* plan is already being executed (`ReadFromMerge` materializes its children
/// lazily), so by this point `addStepsToBuildSets` has already moved the source plan out of every
/// `FutureSetFromSubquery`. A child fragment referencing such a consumed set then fails to
/// serialize with the logical error `Cannot serialize FutureSetFromSubquery with no query plan`.
static bool queryHasSubquerySets(const SelectQueryInfo & query_info)
{
    if (query_info.planner_context && query_info.planner_context->getPreparedSets().hasSubqueries())
        return true;
    if (query_info.prepared_sets && query_info.prepared_sets->hasSubqueries())
        return true;
    return false;
}

/// Optimization settings for a child plan of a `Merge` table.
///
/// Parallel replicas must stay disabled here. The outer plan has decided its own
/// parallel-replicas strategy, and distributing the child read from here ships a fragment that
/// (a) silently loses the filters pushed down into it, and (b) may reference a subquery set
/// consumed by the outer plan (see `queryHasSubquerySets`).
///
/// `make_distributed_plan` stays enabled — distributing the child plans is supported (see
/// 04367_distributed_plan_merge_scatter_multishard; the second, materializing run of the
/// transforms in `ReadFromMerge::buildPipeline` is fenced by `planContainsLogicalExchange`) —
/// unless the query has subquery sets, whose plans a child fragment cannot carry anymore.
static QueryPlanOptimizationSettings getChildPlanOptimizationSettings(const ContextPtr & context, const SelectQueryInfo & query_info)
{
    QueryPlanOptimizationSettings optimization_settings(context);
    optimization_settings.enable_parallel_replicas = false;
    if (queryHasSubquerySets(query_info))
        optimization_settings.make_distributed_plan = false;
    return optimization_settings;
}

void ReadFromMerge::addFilter(FilterDAGInfo filter)
{
    output_header = std::make_shared<const Block>(FilterTransform::transformHeader(
            *output_header,
            &filter.actions,
            filter.column_name,
            filter.do_remove_column));

    if (child_plans)
    {
        /// Propagate new filter to all child plans if they are already present
        for (auto & child : *child_plans)
        {
            if (!child.plan.isInitialized())
                continue;

            auto filter_step = std::make_unique<FilterStep>(
                child.plan.getCurrentHeader(),
                filter.actions.clone(),
                filter.column_name,
                filter.do_remove_column);

            child.plan.addStep(std::move(filter_step));

            /// Push down this newly added filter if possible
            child.plan.optimize(getChildPlanOptimizationSettings(context, query_info));
        }
    }

    pushed_down_filters.push_back(std::move(filter));
}

void ReadFromMerge::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    filterTablesAndCreateChildrenPlans();

    if (selected_tables.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(output_header)));
        return;
    }

    QueryPlanResourceHolder resources;
    VectorWithMemoryTracking<std::unique_ptr<QueryPipelineBuilder>> pipelines;

    auto table_it = selected_tables.begin();
    auto modified_context = Context::createCopy(context);
    for (size_t i = 0; i < selected_tables.size(); ++i, ++table_it)
    {
        auto & child_plan = child_plans->at(i);
        const auto & table = *table_it;
        auto source_pipeline = buildPipeline(child_plan, common_processed_stage);

        if (source_pipeline && source_pipeline->initialized())
        {
            resources.storage_holders.push_back(std::get<1>(table));
            resources.table_locks.push_back(std::get<2>(table));

            pipelines.emplace_back(std::move(source_pipeline));
        }
    }

    if (pipelines.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(output_header)));
        return;
    }

    pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines));

    // It's possible to have many tables read from merge, resize(num_streams) might open too many files at the same time.
    // Using narrowPipe instead. But in case of reading in order of primary key, we cannot do it,
    // because narrowPipe doesn't preserve order. Also, if we are doing a memory efficient distributed agggregation, bucket
    // order must be preserved.
    //
    // Order must be preserved as well when the children were read at a stage where the query's `ORDER BY` has already
    // run remotely: every child then sorts on its own (a `Distributed` child sorts on the shards), so the step on top
    // of `ReadFromMerge` is `Sorting (Merge sorted streams ... for ORDER BY)`, which requires each input stream to be
    // sorted. Narrowing would feed it unsorted streams, silently producing a wrongly ordered - and, together with
    // `LIMIT`, incomplete - result.
    //
    // That happens at any stage above `WithMergeableState` (the remote side did the full `ORDER BY`), and at
    // `WithMergeableState` only for queries without aggregation and window functions - the same conditions under
    // which the remote part of a distributed query performs the preliminary sort (and the planner merges sorted
    // streams instead of doing a full sort on the initiator). For example, a window function query over `Distributed`
    // is processed only up to `WithMergeableState` with no remote sort, so narrowing remains allowed.
    const bool children_produce_sorted_streams = queryHasOrderBy(query_info)
        && (common_processed_stage > QueryProcessingStage::WithMergeableState
            || (common_processed_stage > QueryProcessingStage::FetchColumns && !query_info.need_aggregate
                && !query_info.has_window));

    // Memory efficient distributed aggregation delivers two-level blocks bucket by bucket, and that bucket order must be
    // preserved. It can only happen when the query aggregates: without aggregation there are no buckets at all, so the
    // setting alone - it is enabled by default - must not keep every shard's stream alive. Otherwise the very fan-out
    // this optimization guards against would come back for all the other queries stopping at `WithMergeableState`, such
    // as the window function queries above.
    const bool memory_efficient_aggregation = query_info.need_aggregate
        && context->getSettingsRef()[Setting::distributed_aggregation_memory_efficient]
        && common_processed_stage == QueryProcessingStage::Enum::WithMergeableState;

    const bool should_not_narrow = query_info.input_order_info
        || children_produce_sorted_streams
        || memory_efficient_aggregation;
    if (!should_not_narrow)
    {
        size_t tables_count = selected_tables.size();
        Float64 num_streams_multiplier = std::min(
            static_cast<Float64>(tables_count),
            std::floor(std::max(1.0, static_cast<Float64>(context->getSettingsRef()[Setting::max_streams_multiplier_for_merge_tables]))));
        size_t num_streams = applyStreamsMultiplier(requested_num_streams, num_streams_multiplier);

        pipeline.narrow(num_streams);
    }

    pipeline.addResources(resources);
}

void ReadFromMerge::filterTablesAndCreateChildrenPlans()
{
    if (child_plans)
        return;

    selected_tables = getSelectedTables(context);
    child_plans = createChildrenPlans(query_info);

    /// A `'break'`-mode deadline stops that loop early, so drop the tables left unplanned to keep
    /// `selected_tables` aligned 1:1 with `child_plans` for every reader.
    if (child_plans->size() < selected_tables.size())
        selected_tables.resize(child_plans->size());
}

/// Every materialized CTE reachable from `node`. Pointer identity is what matters:
/// all references to one CTE - including the ones a child plan resolves by name to
/// the CTE's temporary `StorageMemory` - share the same `MaterializedCTE` object.
static MaterializedCTESet collectMaterializedCTEsFromQueryTree(const QueryTreeNodePtr & node)
{
    MaterializedCTESet result;
    if (!node)
        return result;

    traverseQueryTree(node, Everything{},
        [&](const QueryTreeNodePtr & current_node)
        {
            if (const auto * table_node = current_node->as<TableNode>())
            {
                if (auto cte = table_node->getMaterializedCTE())
                    result.insert(std::move(cte));
            }
        });

    return result;
}

std::vector<ReadFromMerge::ChildPlan> ReadFromMerge::createChildrenPlans(SelectQueryInfo & query_info_) const
{
    if (selected_tables.empty())
        return {};

    std::vector<ChildPlan> res;

    /// Materialized CTEs the outer query references. Each child plan below is optimized
    /// on its own, and `resolveMaterializingCTEs` claims a CTE globally
    /// (`MaterializedCTE::is_materialization_planned`): the first child plan to be
    /// optimized would move the CTE's plan into *its* tree and leave every other
    /// `DelayedMaterializingCTEsStep` for that CTE - in the sibling children and in the
    /// outer plan - degenerate. The writer would then sit in one child's pipeline while
    /// the readers sit in another, with no `DelayedPortsProcessor` between them, and
    /// `ReadFromMemoryStorageStep` would (rightly) report a missing gate.
    ///
    /// So the children must not claim these: strip their steps and let the outer plan,
    /// whose `MaterializingCTEsStep` sits above the whole merge, own the materialization
    /// and gate every child. This mirrors what `DelayedCreatingSetsStep::makePlansForSets`
    /// does with pre-built IN-subquery plans. A CTE defined *inside* one child (a `View`
    /// with its own `WITH ... AS MATERIALIZED`) is not in this set, so that child keeps
    /// owning it - it is the only reader.
    const auto outer_materialized_ctes = collectMaterializedCTEsFromQueryTree(query_info.query_tree);

    size_t tables_count = selected_tables.size();
    Float64 num_streams_multiplier = std::min(
        static_cast<Float64>(tables_count),
        std::max(1.0, static_cast<double>(context->getSettingsRef()[Setting::max_streams_multiplier_for_merge_tables])));
    size_t num_streams = applyStreamsMultiplier(requested_num_streams, num_streams_multiplier);

    /// A trivial LIMIT bounds the rows that all child reads can produce. `GenerateRandom`
    /// creates blocks of `required_max_block_size` rows and limits its sources accordingly.
    /// Apply the same bound here, so the aggregate guard does not reject a safe limited read
    /// before the child storage gets the opportunity to apply its reduction.
    if (query_info_.trivial_limit)
    {
        const size_t streams_for_limit = static_cast<size_t>(query_info_.trivial_limit / required_max_block_size)
            + (query_info_.trivial_limit % required_max_block_size != 0);
        num_streams = std::min(num_streams, streams_for_limit);
    }

    /// Check the aggregate source count before building child plans: `Merge` fan-out can keep every
    /// child below the limit while exceeding it in total. Storages which know a tighter bound expose
    /// it through `IStorage::getMaxReadStreams`; proxies forward that capability to their nested storage.
    /// When there are fewer requested streams than tables, every table still gets one stream.
    /// Otherwise, the current distributor gives every table `num_streams / tables_count` streams
    /// and discards the remainder.
    static constexpr size_t max_streams_for_merge_read = 65536;
    const size_t streams_per_table = tables_count >= num_streams ? 1 : num_streams / tables_count;
    size_t total_streams = 0;
    for (const auto & table : selected_tables)
    {
        const size_t child_streams = std::get<1>(table)->getMaxReadStreams(streams_per_table, context);
        if (child_streams > max_streams_for_merge_read - total_streams)
            throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                "Too many streams for a `Merge` table read (the maximum is {}). "
                "Lower `max_streams_to_max_threads_ratio`, `max_threads`, or `max_streams_multiplier_for_merge_tables`",
                max_streams_for_merge_read);
        total_streams += child_streams;
    }

    size_t remaining_streams = num_streams;

    if (order_info)
    {
        query_info_.input_order_info = order_info;
    }
    else if (query_info.order_optimizer)
    {
        InputOrderInfoPtr input_sorting_info;
        for (auto it = selected_tables.begin(); it != selected_tables.end(); ++it)
        {
            auto storage_ptr = std::get<1>(*it);
            auto storage_metadata_snapshot = storage_ptr->getInMemoryMetadataPtr(context, false);
            auto current_info = query_info.order_optimizer->getInputOrder(storage_metadata_snapshot, context);
            if (it == selected_tables.begin())
                input_sorting_info = current_info;
            else if (!current_info || (input_sorting_info && *current_info != *input_sorting_info))
                input_sorting_info.reset();

            if (!input_sorting_info)
                break;
        }

        query_info_.input_order_info = input_sorting_info;
    }

    auto logger = getLogger("StorageMerge");

    /// A `FINAL` read is never distributed, so leave its children exactly as before.
    const auto & settings = context->getSettingsRef();
    const bool keep_parallel_replicas_for_children = settings[Setting::parallel_replicas_plan_based]
        && settings[Setting::parallel_replicas_allow_merge_tables] && !InterpreterSelectQuery::isQueryWithFinal(query_info);

    /** Cache getModifiedQueryInfo results per column structure.
      * For tables with identical columns, getModifiedQueryInfo produces functionally identical results
      * (same cloned query tree, same aliases, same column names). The only differences are the table
      * reference and storage pointer, which are handled separately by createPlanForTable.
      * This avoids O(N * query_tree_size) cloning for N tables with the same structure.
      */
    struct CachedModifiedQueryInfo
    {
        SelectQueryInfo query_info;
        Names column_names_as_aliases;
        bool is_smallest_column_requested = false;
        Aliases aliases;
    };
    std::unordered_map<String, CachedModifiedQueryInfo> query_info_cache;

    QueryStatusPtr query_status = context->getProcessListElementSafe();

    /// Settings will be modified when planning children tables.
    for (const auto & table : selected_tables)
    {
        /// Building a plan (including query analysis) for every child table can take a long time when
        /// the Merge table matches many tables, so honor `KILL QUERY` and `max_execution_time` between tables.
        /// `checkTimeLimit` throws for `KILL QUERY` and `timeout_overflow_mode = 'throw'`; for `'break'` it
        /// returns false instead, and the caller then truncates `selected_tables` to the plans built here.
        if (query_status && !query_status->checkTimeLimit())
            break;

        FailPointInjection::pauseFailPoint(FailPoints::storage_merge_create_children_plans_pause);

        const auto & storage = std::get<1>(table);

        LOG_TRACE(logger, "Building plan for child table {}", storage->getStorageID().getNameForLogs());

        try
        {
            auto modified_context = Context::createCopy(context);
            /// See `getChildPlanOptimizationSettings`: a child plan must never use parallel
            /// replicas. The setting is cleared in the context as well, because the
            /// parallel-replicas conversion re-checks `canUseParallelReplicasOnInitiator` against
            /// the context captured by the reading step, not only the optimization settings, and
            /// nested interpreters (e.g. for a `View` child) derive their own settings from this
            /// context. `make_distributed_plan` is cleared under the same condition as in
            /// `getChildPlanOptimizationSettings`.
            ///
            /// The exception is a plain `MergeTree` child of a `Merge` read which is going to be expanded
            /// for the plan-based parallel replicas (see `expandForParallelReplicas`): its read becomes an
            /// ordinary read of the outer plan, which is distributed there, and that conversion needs the
            /// setting in the context this read captures. Such a child is read directly, without a nested
            /// interpreter, and its own plan is still never distributed - `getChildPlanOptimizationSettings`
            /// disables the transformation for it regardless of the context.
            if (!keep_parallel_replicas_for_children || !storage->isMergeTree())
                modified_context->setSetting("enable_parallel_replicas", Field(0));
            if (queryHasSubquerySets(query_info))
                modified_context->setSetting("make_distributed_plan", Field(0));

            size_t current_need_streams = tables_count >= num_streams ? 1 : (num_streams / tables_count);
            size_t current_streams = std::min(current_need_streams, remaining_streams);
            remaining_streams -= current_streams;
            current_streams = std::max(1uz, current_streams);

            /// Storages with a tighter source bound may otherwise recreate the raw stream request
            /// with their optional output resize, so preserve the reported bound here.
            if (storage->getMaxReadStreams(current_streams, context) < current_streams)
                modified_context->setSetting("parallelize_output_from_storages", Field(0));

            bool sampling_requested = query_info.query->as<ASTSelectQuery>()->sampleSize() != nullptr;
            if (query_info.table_expression_modifiers)
                sampling_requested = query_info.table_expression_modifiers->hasSampleSizeRatio();

            /// If sampling requested, then check that table supports it.
            if (sampling_requested && !storage->supportsSampling())
                throw Exception(ErrorCodes::SAMPLING_NOT_SUPPORTED, "Illegal SAMPLE: table {} doesn't support sampling", storage->getStorageID().getNameForLogs());

            Aliases aliases;
            RowPolicyDataOpt row_policy_data_opt;
            auto storage_metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);

            if (storage_metadata_snapshot->getColumns().empty())
            {
                /// An `Alias` reports its target's metadata, so the empty column list belongs to the target.
                const auto * alias = storage->as<StorageAlias>();
                const StoragePtr alias_target = alias ? alias->tryGetTargetTable() : nullptr;
                const IStorage * columns_owner = alias ? alias_target.get() : storage.get();

                /// (Assuming that view has empty list of columns if it's parameterized.)
                const auto * view = columns_owner ? columns_owner->as<StorageView>() : nullptr;
                if (view && view->isParameterizedView())
                    throw Exception(ErrorCodes::STORAGE_REQUIRES_PARAMETER, "Parameterized view can't be queried through a Merge table.");

                if (alias && !alias_target)
                    throw Exception(
                        ErrorCodes::UNKNOWN_TABLE,
                        "Table {} matched by the regexp of {} is an `Alias` whose target table is missing",
                        storage->getStorageID().getNameForLogs(),
                        storage_merge->getStorageID().getNameForLogs());

                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Table {} matched by the regexp of {} has no columns to read",
                    storage->getStorageID().getNameForLogs(),
                    storage_merge->getStorageID().getNameForLogs());
            }

            /// `StorageMerge::getQueryProcessingStage` refuses a delegated stage when a child's type
            /// converts to the declared one without preserving the order, but it decides that from
            /// its own child enumeration and metadata snapshots. This loop reads a later, frozen
            /// set, so a concurrent `ALTER` of a child, or a table that starts matching the regexp
            /// in between, can present a child the refusal never saw. `common_processed_stage` is
            /// already baked into the plan above and cannot be lowered here, and
            /// `convertAndFilterSourceStream` would put the order-breaking cast above the child's
            /// own sort or aggregation, so this fails the query instead of returning wrong rows.
            if (common_processed_stage > QueryProcessingStage::FetchColumns)
            {
                const auto & declared_columns = merge_storage_snapshot->metadata->getColumns();
                const GetColumnsOptions order_relevant_columns(GetColumnsOptions::AllPhysicalAndAliases);
                for (const auto & child_column : storage_metadata_snapshot->getColumns().get(order_relevant_columns))
                {
                    auto declared_column = declared_columns.tryGetColumn(order_relevant_columns, child_column.name);
                    if (declared_column && !conversionPreservesOrder(*child_column.type, *declared_column->type))
                        throw Exception(
                            ErrorCodes::INCOMPATIBLE_COLUMNS,
                            "Column {} of table {} has type {}, which does not preserve the order when converted to "
                            "the type {} declared by {}. The query processing stage was chosen before this type was "
                            "visible, most likely because the table was altered, or started matching the regexp, "
                            "while the query was being planned. Retry the query",
                            backQuoteIfNeed(child_column.name),
                            storage->getStorageID().getNameForLogs(),
                            child_column.type->getName(),
                            declared_column->type->getName(),
                            storage_merge->getStorageID().getNameForLogs());
                }
            }

            auto nested_storage_snapshot = storage->getStorageSnapshot(storage_metadata_snapshot, modified_context);

            Names column_names_as_aliases;
            Names real_column_names = all_column_names;

            /// If there are no real columns requested from this table, we will read the smallest column.
            /// We should remember it to not include this column in the result.
            bool is_smallest_column_requested = false;

            const auto & database_name = std::get<0>(table);
            const auto & table_name = std::get<3>(table);
            auto row_policy_filter_ptr = modified_context->getRowPolicyFilter(
                database_name,
                table_name,
                RowPolicyFilterType::SELECT_FILTER);
            /// `Merge` reads matched tables directly, so include the target policy when a matched table is an `Alias`.
            if (const auto * alias = storage->as<StorageAlias>())
            {
                const auto target_storage_id = alias->getTargetTable()->getStorageID();
                auto target_row_policy_filter = modified_context->getRowPolicyFilter(
                    target_storage_id.getDatabaseName(),
                    target_storage_id.getTableName(),
                    RowPolicyFilterType::SELECT_FILTER);
                row_policy_filter_ptr = combineRowPolicyFilters(
                    std::move(row_policy_filter_ptr), std::move(target_row_policy_filter));
            }

            if (row_policy_filter_ptr && !row_policy_filter_ptr->isAlwaysTrue())
            {
                row_policy_data_opt = RowPolicyData(row_policy_filter_ptr, storage, modified_context);
                row_policy_data_opt->extendNames(real_column_names);
            }

            SelectQueryInfo modified_query_info;

            /// Try to reuse cached modified_query_info for tables with the same column structure.
            /// Skip caching for:
            ///  - the non-analyzer path: getModifiedQueryInfo rewrites _table and _database
            ///    directly into the cloned AST, so sharing it across tables is incorrect;
            ///  - tables with row policies (they extend real_column_names differently);
            ///  - Merge/Distributed/View storages (they interpret table_expression for
            ///    query routing and nested plan building, so sharing a representative's
            ///    table_expression would route reads to the wrong table);
            ///  - when processed_stage > FetchColumns, because createPlanForTable will
            ///    either convert query_tree->toAST() (analyzer path, referencing the wrong
            ///    table) or call replaceDatabaseAndTable on the shared AST (non-analyzer
            ///    path, corrupting the cache).
            /// The cache key includes the database name because getModifiedQueryInfo injects
            /// a _database constant into the query tree (analyzer path), so tables in
            /// different databases must not share cached entries.
            bool can_cache = query_info.table_expression
                && !row_policy_data_opt
                && common_processed_stage == QueryProcessingStage::FetchColumns
                && !std::dynamic_pointer_cast<StorageMerge>(storage)
                && !std::dynamic_pointer_cast<StorageDistributed>(storage)
                && !storage->isView();
            auto structure_key = can_cache
                ? (std::get<0>(table) + "\n" + storage_metadata_snapshot->getColumns().toString(false))
                : String{};
            auto cache_it = can_cache ? query_info_cache.find(structure_key) : query_info_cache.end();

            if (cache_it != query_info_cache.end())
            {
                /// Reuse cached query info. The shallow copy shares the query_tree
                /// (which references the representative table), but that is fine: all tables
                /// in the group have identical column structure, so filter/prewhere/key
                /// conditions built from the shared query tree apply equally to every table.
                auto & cached = cache_it->second;
                modified_query_info = cached.query_info;
                column_names_as_aliases = cached.column_names_as_aliases;
                is_smallest_column_requested = cached.is_smallest_column_requested;
                aliases = cached.aliases;

                /// Deep-clone the AST `query` because `createPlanForTable` may mutate it
                /// in-place via `modified_select.setFinal()` (e.g. when the underlying storage's
                /// `needRewriteQueryWithFinal` returns true). Without this clone, one table
                /// would flip `FINAL` on the shared AST for every subsequent table in this
                /// cache bucket, making semantics depend on table iteration order.
                if (modified_query_info.query)
                    modified_query_info.query = modified_query_info.query->clone();

                /// Rebind table_expression to the current table so that downstream code
                /// (e.g. storage->read, getQueryProcessingStage) sees the correct table identity,
                /// even though the shared query_tree internally still references the representative table.
                const auto & storage_lock = std::get<2>(table);
                auto replacement_table_expression = std::make_shared<TableNode>(storage, storage_lock, nested_storage_snapshot);
                replacement_table_expression->setAlias(modified_query_info.table_expression->getAlias());
                if (query_info.table_expression_modifiers)
                    replacement_table_expression->setTableExpressionModifiers(*query_info.table_expression_modifiers);
                modified_query_info.table_expression = replacement_table_expression;
                if (modified_query_info.planner_context)
                {
                    /// Create a fresh PlannerContext for this table (just like getModifiedQueryInfo does)
                    /// to avoid accumulating table expression data in the shared cached context.
                    modified_query_info.planner_context = std::make_shared<PlannerContext>(modified_context, modified_query_info.planner_context);
                    modified_query_info.planner_context->getOrCreateTableExpressionData(replacement_table_expression);
                }
            }
            else
            {
                modified_query_info
                    = getModifiedQueryInfo(modified_context, table, nested_storage_snapshot, real_column_names, column_names_as_aliases, is_smallest_column_requested, aliases);

                if (can_cache)
                {
                    /// Store a deep clone of the AST in the cache so that subsequent in-place
                    /// mutation by `createPlanForTable` (e.g. `modified_select.setFinal`) on
                    /// the first table does not leak into the cached baseline. Otherwise, later
                    /// cache hits would clone an already-mutated AST and `FINAL` propagation
                    /// would depend on the first table's `needRewriteQueryWithFinal` result.
                    SelectQueryInfo cached_query_info = modified_query_info;
                    if (cached_query_info.query)
                        cached_query_info.query = cached_query_info.query->clone();
                    query_info_cache[structure_key] = {std::move(cached_query_info), column_names_as_aliases, is_smallest_column_requested, aliases};
                }
            }

            /// Filter DAGs can be modified by optimizations, so each child must own its own copy:
            /// otherwise optimizing one child's plan invalidates the sibling headers that were
            /// derived from the shared object.
            if (modified_query_info.prewhere_info)
                modified_query_info.prewhere_info = std::make_shared<PrewhereInfo>(modified_query_info.prewhere_info->clone());
            if (modified_query_info.row_level_filter)
            {
                auto row_level_filter_copy = std::make_shared<FilterDAGInfo>();
                row_level_filter_copy->actions = modified_query_info.row_level_filter->actions.clone();
                row_level_filter_copy->column_name = modified_query_info.row_level_filter->column_name;
                row_level_filter_copy->do_remove_column = modified_query_info.row_level_filter->do_remove_column;
                modified_query_info.row_level_filter = std::move(row_level_filter_copy);
            }

            if (!context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                auto storage_columns = storage_metadata_snapshot->getColumns();
                auto syntax_result = TreeRewriter(context).analyzeSelect(
                    modified_query_info.query, TreeRewriterResult({}, storage, nested_storage_snapshot));

                bool with_aliases = common_processed_stage == QueryProcessingStage::FetchColumns && !storage_columns.getAliases().empty();
                if (with_aliases)
                {
                    ASTPtr required_columns_expr_list = make_intrusive<ASTExpressionList>();
                    ASTPtr column_expr;

                    auto sample_block = merge_storage_snapshot->metadata->getSampleBlock();

                    for (const auto & column : real_column_names)
                    {
                        const auto column_default = storage_columns.getDefault(column);
                        bool is_alias = column_default && column_default->kind == ColumnDefaultKind::Alias;

                        if (is_alias)
                        {
                            column_expr = column_default->expression->clone();
                            replaceAliasColumnsInQuery(column_expr, storage_metadata_snapshot->getColumns(),
                                                    syntax_result->array_join_result_to_source, context);

                            const auto & column_description = storage_columns.get(column);
                            column_expr = addTypeConversionToAST(std::move(column_expr), column_description.type->getName(),
                                                                storage_metadata_snapshot->getColumns().getAll(), context);
                            column_expr = setAlias(column_expr, column);

                            /// use storage type for transient columns that are not represented in result
                            ///  e.g. for columns that needed to evaluate row policy
                            auto type = sample_block.has(column) ? sample_block.getByName(column).type : column_description.type;

                            aliases.push_back({ .name = column, .type = type, .expression = column_expr->clone() });
                        }
                        else
                            column_expr = make_intrusive<ASTIdentifier>(column);

                        required_columns_expr_list->children.emplace_back(std::move(column_expr));
                    }

                    syntax_result = TreeRewriter(context).analyze(
                        required_columns_expr_list, storage_columns.getAllPhysical(), storage, storage->getStorageSnapshot(storage_metadata_snapshot, context));

                    auto alias_actions = ExpressionAnalyzer(required_columns_expr_list, syntax_result, context).getActionsDAG(true);

                    column_names_as_aliases = alias_actions.getRequiredColumns().getNames();
                    if (column_names_as_aliases.empty())
                    {
                        column_names_as_aliases.push_back(ExpressionActions::getSmallestColumn(storage_metadata_snapshot->getColumns().getAllPhysical()).name);
                        is_smallest_column_requested = true;
                    }
                }
            }

            Names column_names_to_read = column_names_as_aliases.empty() ? std::move(real_column_names) : std::move(column_names_as_aliases);

            std::erase_if(column_names_to_read, [existing_columns = nested_storage_snapshot->getAllColumnsDescription()](const auto & column_name){ return !existing_columns.has(column_name) && !existing_columns.hasSubcolumn(GetColumnsOptions::All, column_name); });

            auto child = createPlanForTable(
                nested_storage_snapshot,
                modified_query_info,
                common_processed_stage,
                required_max_block_size,
                table,
                column_names_to_read,
                is_smallest_column_requested,
                row_policy_data_opt,
                modified_context,
                current_streams);

            child.plan.addInterpreterContext(modified_context);

            if (child.plan.isInitialized())
            {
                /// Source tables could have different but convertible types, like numeric types of different width.
                /// We must return streams with structure equals to structure of Merge table.
                convertAndFilterSourceStream(*common_header, modified_query_info, nested_storage_snapshot, aliases, row_policy_data_opt, context, child, is_smallest_column_requested);

                for (const auto & filter_info : pushed_down_filters)
                {
                    auto filter_step = std::make_unique<FilterStep>(
                        child.plan.getCurrentHeader(),
                        filter_info.actions.clone(),
                        filter_info.column_name,
                        filter_info.do_remove_column);

                    child.plan.addStep(std::move(filter_step));
                }

                removeDelayedMaterializingCTEsStepFor(child.plan, outer_materialized_ctes);

                child.plan.optimize(getChildPlanOptimizationSettings(modified_context, query_info));
            }

            res.emplace_back(std::move(child));
        }
        catch (Exception & e)
        {
            e.addMessage("Child table: " + storage->getStorageID().getNameForLogs());
            throw;
        }
    }

    return res;
}

namespace
{

class ApplyAliasColumnExpressionsVisitor : public InDepthQueryTreeVisitor<ApplyAliasColumnExpressionsVisitor>
{
public:
    explicit ApplyAliasColumnExpressionsVisitor(TableExpressionNodePtr replacement_table_expression_)
        : replacement_table_expression(replacement_table_expression_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto * column = node->as<ColumnNode>())
        {
            if (column->hasExpression())
            {
                QueryTreeNodePtr column_expression = column->getExpressionOrThrow();
                column_expression->setAlias(column->getColumnName());
                node = std::move(column_expression);
            }
            else
            {
                /// Do not replace column source for lambda arguments.
                /// Lambda argument columns reference the lambda arguments node as their source,
                /// and replacing it with the table expression would cause toAST()
                /// to qualify them with the table alias (e.g. `__table1.x` instead of `x`),
                /// which is invalid for lambda argument identifiers.
                auto column_source = column->getColumnSourceOrNull();
                if (column_source && column_source->getNodeType() == QueryTreeNodeType::LAMBDA_ARGS)
                    return;

                column->setColumnSource(replacement_table_expression);
            }
        }
    }
private:
    TableExpressionNodePtr replacement_table_expression;
};

QueryTreeNodePtr replaceTableExpressionAndRemoveJoin(
    QueryTreeNodePtr query,
    TableExpressionNodePtr original_table_expression,
    TableExpressionNodePtr replacement_table_expression,
    const ContextPtr & context,
    const Names & required_column_names)
{
    auto * query_node = query->as<QueryNode>();
    auto join_tree_type = query_node->getJoinTreeNode()->getNodeType();
    auto modified_query = query_node->cloneAndReplace(original_table_expression, replacement_table_expression);

    // For the case when join tree is just a table or a table function we don't need to do anything more.
    if (join_tree_type == QueryTreeNodeType::TABLE || join_tree_type == QueryTreeNodeType::TABLE_FUNCTION)
        return modified_query;

    // JOIN needs to be removed because StorageMerge should produce not joined data.
    // GROUP BY should be removed as well.

    auto * modified_query_node = modified_query->as<QueryNode>();

    // Remove the JOIN statement. As a result query will have a form like: SELECT * FROM <table> ...
    modified_query = modified_query->cloneAndReplace(modified_query_node->getJoinTreeNodeTyped(), replacement_table_expression);
    modified_query_node = modified_query->as<QueryNode>();

    query_node = modified_query->as<QueryNode>();

    // For backward compatibility we need to leave all filters related to this table.
    // It may lead to some incorrect result.
    if (query_node->hasPrewhere())
        removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getPrewhere(), replacement_table_expression, context);
    if (query_node->hasWhere())
        removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getWhere(), replacement_table_expression, context);

    query_node->getGroupBy().getNodes().clear();
    query_node->getHaving() = {};
    query_node->getWindow().getNodes().clear();
    query_node->getQualify() = {};
    query_node->getOrderBy().getNodes().clear();
    query_node->getInterpolate() = {};
    if (query_node->hasLimitByLimit())
        query_node->getLimitByLimit() = {};
    if (query_node->hasLimitByOffset())
        query_node->getLimitByOffset() = {};
    query_node->getLimitBy().getNodes().clear();

    auto & projection = modified_query_node->getProjection().getNodes();
    projection.clear();
    NamesAndTypes projection_columns;

    // Select only required columns from the table, because projection list may contain:
    // 1. aggregate functions
    // 2. expressions referencing other tables of JOIN
    //
    // All the identifiers are resolved by a single `QueryAnalysisPass` run. Running the pass once per
    // identifier would rebuild `AnalysisTableExpressionData` for the whole `Merge` table every time,
    // which is quadratic in the number of columns. As this function is called once per source table,
    // the total cost becomes cubic, and a query joining `merge` over many wide tables (for example,
    // `merge('system', '')`) spends minutes in query planning.
    if (!required_column_names.empty())
    {
        auto identifiers_list = std::make_shared<ListNode>();
        identifiers_list->getNodes().reserve(required_column_names.size());
        for (const auto & column_name : required_column_names)
            identifiers_list->getNodes().push_back(std::make_shared<IdentifierNode>(Identifier{column_name}));

        QueryTreeNodePtr resolved_identifiers = std::move(identifiers_list);

        QueryAnalysisPass query_analysis_pass(original_table_expression);
        query_analysis_pass.run(resolved_identifiers, context);

        auto & resolved_nodes = resolved_identifiers->as<ListNode &>().getNodes();
        if (resolved_nodes.size() != required_column_names.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected {} resolved columns, got {}",
                required_column_names.size(),
                resolved_nodes.size());

        projection.reserve(required_column_names.size());
        projection_columns.reserve(required_column_names.size());

        for (size_t i = 0; i < required_column_names.size(); ++i)
        {
            auto & fake_node = resolved_nodes[i];

            auto * resolved_column = fake_node->as<ColumnNode>();
            if (!resolved_column)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Required column '{}' is not resolved", required_column_names[i]);
            auto fake_column = resolved_column->getColumn();

            // Identifier is resolved to ColumnNode, but we need to get rid of ALIAS columns
            // and also fix references to source expression (now column is referencing original table expression).
            ApplyAliasColumnExpressionsVisitor visitor(replacement_table_expression);
            visitor.visit(fake_node);

            projection.push_back(fake_node);
            projection_columns.push_back(fake_column);
        }
    }

    query_node->resolveProjectionColumns(std::move(projection_columns));

    return modified_query;
}

}

SelectQueryInfo ReadFromMerge::getModifiedQueryInfo(const ContextMutablePtr & modified_context,
    const StorageWithLockAndName & storage_with_lock_and_name,
    const StorageSnapshotPtr & storage_snapshot_,
    Names required_column_names,
    Names & column_names_as_aliases,
    bool & is_smallest_column_requested,
    Aliases & aliases) const
{
    const auto & [database_name, storage, storage_lock, table_name] = storage_with_lock_and_name;
    const StorageID current_storage_id = storage->getStorageID();

    SelectQueryInfo modified_query_info = query_info;

    modified_query_info.initial_storage_snapshot = merge_storage_snapshot;

    if (modified_query_info.planner_context)
        modified_query_info.planner_context = std::make_shared<PlannerContext>(modified_context, modified_query_info.planner_context);

    if (modified_query_info.table_expression)
    {
        auto replacement_table_expression = std::make_shared<TableNode>(storage, storage_lock, storage_snapshot_);
        replacement_table_expression->setAlias(modified_query_info.table_expression->getAlias());
        if (query_info.table_expression_modifiers)
            replacement_table_expression->setTableExpressionModifiers(*query_info.table_expression_modifiers);

        modified_query_info.query_tree = replaceTableExpressionAndRemoveJoin(modified_query_info.query_tree, modified_query_info.table_expression, replacement_table_expression, modified_context, required_column_names);
        modified_query_info.table_expression = replacement_table_expression;
        modified_query_info.planner_context->getOrCreateTableExpressionData(replacement_table_expression);

        auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(storage_snapshot_->storage.supportsSubcolumns()).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All);

        /// Replace references to columns that don't exist in this child table with default values.
        /// This happens when merge() is used over tables with different schemas and the processing
        /// stage is above FetchColumns (e.g., for distributed/remote tables where the full query
        /// is sent to the child for processing).
        auto storage_columns = storage_snapshot_->metadata->getColumns();

        std::unordered_map<std::string, QueryTreeNodePtr> column_name_to_node;
        for (const auto & column_name : required_column_names)
        {
            if (column_name_to_node.contains(column_name))
                continue;

            if (storage_snapshot_->tryGetColumn(get_column_options, column_name))
                continue;

            /// The child can produce this value, so it must not be replaced by a default.
            if (isSubcolumnOfAliasColumn(storage_columns, column_name))
                continue;

            auto merge_column = merge_storage_snapshot->tryGetColumn(
                GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All)
                    .withSubcolumns(merge_storage_snapshot->storage.supportsSubcolumns()),
                column_name);
            if (!merge_column)
                continue;

            column_name_to_node.emplace(column_name,
                std::make_shared<ConstantNode>(merge_column->type->getDefault(), merge_column->type));
        }

        bool with_aliases = /* common_processed_stage == QueryProcessingStage::FetchColumns && */ !storage_columns.getAliases().empty();
        if (with_aliases)
        {
            auto filter_actions_dag = std::make_shared<ActionsDAG>();
            for (const auto & column : required_column_names)
            {
                /// Try to resolve column, including subcolumns (e.g. JSON sub-paths like json.x).
                auto resolved_pair = storage_snapshot_->tryGetColumn(get_column_options, column);

                const auto column_default = storage_columns.getDefault(column);
                bool is_alias = column_default && column_default->kind == ColumnDefaultKind::Alias;

                /// Such a name resolves through neither lookup above, and the analyzer turns it into
                /// `getSubcolumn` over the alias expression, so the alias branch handles it.
                bool is_subcolumn_of_alias = !resolved_pair && !is_alias && isSubcolumnOfAliasColumn(storage_columns, column);

                /// Skip columns that don't exist in this table. It may happen when we use merge over tables with different schemas.
                if (!resolved_pair && !is_subcolumn_of_alias)
                    continue;

                QueryTreeNodePtr column_node;

                // Replace all references to ALIAS columns in the query by expressions.
                if (is_alias || is_subcolumn_of_alias)
                {
                    QueryTreeNodePtr fake_node = std::make_shared<IdentifierNode>(Identifier{column});

                    QueryAnalysisPass query_analysis_pass(modified_query_info.table_expression);
                    query_analysis_pass.run(fake_node, modified_context);

                    /// An ALIAS column resolves to a ColumnNode carrying its expression, a subcolumn
                    /// of one to a FunctionNode that owns no expression of its own.
                    auto * resolved_column = fake_node->as<ColumnNode>();
                    if (is_subcolumn_of_alias ? !fake_node->as<FunctionNode>() : (!resolved_column || !resolved_column->getExpression()))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Alias column {} is not resolved", column);

                    auto column_type = fake_node->getResultType();

                    column_node = fake_node;
                    ApplyAliasColumnExpressionsVisitor visitor(replacement_table_expression);
                    visitor.visit(column_node);

                    column_name_to_node.emplace(column, column_node);
                    aliases.push_back({ .name = column, .type = column_type, .expression = column_node->toAST() });
                }
                else
                {
                    column_node = std::make_shared<ColumnNode>(*resolved_pair, modified_query_info.table_expression);
                }

                /// The set registry of the freshly derived planner context is empty, and
                /// `PlannerActionsVisitor` resolves `IN` through it.
                collectSets(column_node, *modified_query_info.planner_context);

                ColumnNodePtrWithHashSet empty_correlated_columns_set;
                PlannerActionsVisitor actions_visitor(modified_query_info.planner_context, empty_correlated_columns_set, false /*use_column_identifier_as_action_node_name*/);
                actions_visitor.visit(*filter_actions_dag, column_node);
            }
            column_names_as_aliases = filter_actions_dag->getRequiredColumnsNames();
            if (column_names_as_aliases.empty())
            {
                column_names_as_aliases.push_back(ExpressionActions::getSmallestColumn(storage_snapshot_->metadata->getColumns().getAllPhysical()).name);
                is_smallest_column_requested = true;
            }
        }

        if (!column_name_to_node.empty())
        {
            replaceColumns(modified_query_info.query_tree,
                replacement_table_expression,
                column_name_to_node);
        }

        modified_query_info.query = queryNodeToSelectQuery(modified_query_info.query_tree);
    }
    else
    {
        modified_query_info.query = query_info.query->clone();

        /// Original query could contain JOIN but we need only the first joined table and its columns.
        auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();
        TreeRewriterResult new_analyzer_res = *modified_query_info.syntax_analyzer_result;
        removeJoin(modified_select, new_analyzer_res, modified_context);
        modified_query_info.syntax_analyzer_result = std::make_shared<TreeRewriterResult>(std::move(new_analyzer_res));
    }

    return modified_query_info;
}

static bool recursivelyApplyToReadingSteps(QueryPlan::Node * node, const std::function<bool(ReadFromMergeTree &)> & func)
{
    bool ok = true;
    for (auto * child : node->children)
        ok &= recursivelyApplyToReadingSteps(child, func);

    // This code is mainly meant to be used to call `requestReadingInOrder` on child steps.
    // In this case it is ok if one child will read in order and other will not (though I don't know when it is possible),
    // the only important part is to acknowledge this at the parent and don't rely on any particular ordering of input data.
    if (!ok)
        return false;

    if (auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        ok &= func(*read_from_merge_tree);

    return ok;
}

QueryPipelineBuilderPtr ReadFromMerge::buildPipeline(
    ChildPlan & child,
    QueryProcessingStage::Enum processed_stage) const
{
    if (!child.plan.isInitialized())
        return nullptr;

    /// `buildQueryPipeline` honors `make_distributed_plan` even with `optimize_plan = false`:
    /// this is the run that materializes the logical exchanges inserted into the child plan when
    /// it was optimized at creation. See `getChildPlanOptimizationSettings` for why a child plan
    /// referencing a subquery set must not be distributed.
    auto optimization_settings = getChildPlanOptimizationSettings(context, query_info);
    /// All optimizations will be done at plans creation
    optimization_settings.optimize_plan = false;
    auto builder = child.plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings(context));

    if (!builder->initialized())
        return builder;

    if (processed_stage > child.stage
        || (context->getSettingsRef()[Setting::allow_experimental_analyzer] && processed_stage != QueryProcessingStage::FetchColumns))
    {
        /** Materialization is needed, since from distributed storage the constants come materialized.
          * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
          * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
          */
        builder->addSimpleTransform([](const SharedHeader & stream_header) { return std::make_shared<MaterializingTransform>(stream_header); });
    }

    return builder;
}

ReadFromMerge::ChildPlan ReadFromMerge::createPlanForTable(
    const StorageSnapshotPtr & storage_snapshot_,
    SelectQueryInfo & modified_query_info,
    QueryProcessingStage::Enum processed_stage,
    UInt64 max_block_size,
    const StorageWithLockAndName & storage_with_lock,
    const Names & real_column_names_read_from_the_source_table,
    bool & is_smallest_column_requested,
    const RowPolicyDataOpt & row_policy_data_opt,
    ContextMutablePtr modified_context,
    size_t streams_num) const
{
    const auto & [database_name, storage, _, table_name] = storage_with_lock;
    auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();

    if (!InterpreterSelectQuery::isQueryWithFinal(modified_query_info) && storage->needRewriteQueryWithFinal(real_column_names_read_from_the_source_table))
    {
        /// NOTE: It may not work correctly in some cases, because query was analyzed without final.
        /// However, it's needed for Materialized...SQL and it's unlikely that someone will use it with Merge tables.
        modified_select.setFinal();

        if (modified_query_info.query_tree)
        {
            if (!modified_query_info.table_expression_modifiers)
                modified_query_info.table_expression_modifiers.emplace();
            modified_query_info.table_expression_modifiers->setHasFinal(true);
        }
    }

    bool use_analyzer = modified_context->getSettingsRef()[Setting::allow_experimental_analyzer];

    auto storage_stage = storage->getQueryProcessingStage(modified_context,
        processed_stage,
        storage_snapshot_,
        modified_query_info);

    QueryPlan plan;

    bool must_return_interpreter_select_query_plan
        = use_analyzer && processed_stage > QueryProcessingStage::FetchColumns && dynamic_cast<StorageMerge *>(storage.get());
    if (processed_stage <= storage_stage && !must_return_interpreter_select_query_plan)
    {
        /// If there are only virtual columns in query, we must request at least one other column.
        Names real_column_names = real_column_names_read_from_the_source_table;
        if (real_column_names.empty())
        {
            real_column_names.push_back(ExpressionActions::getSmallestColumn(storage_snapshot_->metadata->getColumns().getAllPhysical()).name);
            is_smallest_column_requested = true;
        }

        storage->read(plan,
            real_column_names,
            storage_snapshot_,
            modified_query_info,
            modified_context,
            processed_stage,
            max_block_size,
            streams_num);

        if (!plan.isInitialized())
            return {};

        if (row_policy_data_opt)
        {
            if (auto * source_step_with_filter = dynamic_cast<SourceStepWithFilter *>((plan.getRootNode()->step.get())))
                row_policy_data_opt->addStorageFilter(source_step_with_filter);
        }
    }
    else
    {
        /// Maximum permissible parallelism is streams_num
        modified_context->setSetting("max_threads", streams_num);
        modified_context->setSetting("max_streams_to_max_threads_ratio", 1);

        /// The child plan is united into this pipeline in the same process, where nothing
        /// unmarshalls its blocks, so `BlocksMarshallingStep` must not be added to it.
        auto child_select_query_options = SelectQueryOptions(processed_stage);
        child_select_query_options.is_local_plan_for_distributed_query = true;

        if (use_analyzer)
        {
            /// Converting query to AST because types might be different in the source table.
            /// Need to resolve types again.
            auto ast = modified_query_info.query_tree->toAST();
            InterpreterSelectQueryAnalyzer interpreter(ast,
                modified_context,
                child_select_query_options);

            auto & planner = interpreter.getPlanner();
            planner.buildQueryPlanIfNeeded();
            plan = std::move(planner).extractQueryPlan();
        }
        else
        {
            modified_select.replaceDatabaseAndTable(database_name, table_name);
            /// TODO: Find a way to support projections for StorageMerge
            InterpreterSelectQuery interpreter{modified_query_info.query,
                modified_context,
                child_select_query_options};

            interpreter.buildQueryPlan(plan);
        }
    }

    return ChildPlan{std::move(plan), storage_stage};
}

ReadFromMerge::RowPolicyData::RowPolicyData(RowPolicyFilterPtr row_policy_filter_ptr,
    std::shared_ptr<DB::IStorage> storage,
    ContextPtr local_context)
{
    const auto storage_metadata = storage->getInMemoryMetadataPtr(local_context, false);
    storage_metadata_snapshot = storage_metadata;
    auto storage_columns = storage_metadata_snapshot->getColumns();
    auto needed_columns = storage_columns.getAll();

    /// `RowPolicyFilter::expression` is the parsed policy condition owned by `RowPolicyCache`. That AST is
    /// shared: every query of every user reading this table gets the same nodes, and a policy defined on a
    /// whole database is shared by all its tables. `TreeRewriter` and `ExpressionAnalyzer` rewrite the AST
    /// they are given in place - they normalize identifiers, substitute the results of scalar subqueries for
    /// the subqueries themselves, and record `ASTLiteral::unique_column_name` - so they must be handed a
    /// private copy. Analyzing the shared AST is both a data race against concurrent readers of the same
    /// policy and a correctness bug: a scalar subquery such as `USING x <= (SELECT max(v) FROM limits)` gets
    /// replaced by its value in the cache and is then frozen for the rest of the server's lifetime.
    /// `generateFilterActions` in `InterpreterSelectQuery` clones for the same reason.
    ASTPtr expr = row_policy_filter_ptr->expression->clone();

    auto syntax_result = TreeRewriter(local_context).analyze(expr, needed_columns);
    auto expression_analyzer = ExpressionAnalyzer{expr, syntax_result, local_context};

    actions_dag = expression_analyzer.getActionsDAG(false /* add_aliases */, false /* project_result */);

    /// The filter column is dropped from the stream after filtering, so it must be a dedicated
    /// column that does not coincide with a data column. Wrap the policy predicate in a
    /// uniquely-named alias and make the post-filter outputs exactly the source columns plus that
    /// alias. Dropping the alias then leaves the data columns intact, and no synthetic predicate
    /// output (e.g. greater(a, 1) for "USING a > 1") leaks downstream. See commit message.
    const auto & filter_node = actions_dag.findInOutputs(expr->getColumnName());

    /// The alias name must be unique against the current DAG outputs and against the child table's
    /// real columns: a source table may legitimately have a column named __row_policy_filter, and
    /// on SELECT * it flows into the same block as the alias, so a clash makes Block::insert throw.
    NameSet reserved_names;
    for (const auto & column : needed_columns)
        reserved_names.insert(column.name);

    filter_column_name = "__row_policy_filter";
    for (size_t i = 0; actions_dag.tryFindInOutputs(filter_column_name) != nullptr || reserved_names.contains(filter_column_name); ++i)
        filter_column_name = "__row_policy_filter_" + std::to_string(i);

    const auto & alias_node = actions_dag.addAlias(filter_node, filter_column_name);

    /// Keep only the source (input) columns and the alias as outputs. This drops the raw predicate
    /// output regardless of query_plan_enable_optimizations, so a single table does not leak it and
    /// a Merge over children with different policies keeps matching headers in Pipe::unitePipes.
    ActionsDAG::NodeRawConstPtrs new_outputs;
    for (const auto * output : actions_dag.getOutputs())
        if (output->type == ActionsDAG::ActionType::INPUT)
            new_outputs.push_back(output);
    new_outputs.push_back(&alias_node);
    actions_dag.getOutputs() = std::move(new_outputs);

    filter_actions = std::make_shared<ExpressionActions>(actions_dag.clone(), ExpressionActionsSettings(local_context, CompileExpressions::yes));
}

void ReadFromMerge::RowPolicyData::extendNames(Names & names) const
{
    boost::container::flat_set<std::string_view> names_set(names.begin(), names.end());
    NameSet added_names;

    for (const auto & req_column : filter_actions->getRequiredColumns())
    {
        if (!names_set.contains(req_column))
        {
            added_names.emplace(req_column);
        }
    }

    if (!added_names.empty())
    {
        std::copy(added_names.begin(), added_names.end(), std::back_inserter(names));
    }
}

void ReadFromMerge::RowPolicyData::addStorageFilter(SourceStepWithFilter * step) const
{
    step->addFilter(actions_dag.clone(), filter_column_name);
}

void ReadFromMerge::RowPolicyData::addFilterTransform(QueryPlan & plan) const
{
    auto filter_step = std::make_unique<FilterStep>(plan.getCurrentHeader(), actions_dag.clone(), filter_column_name, true /* remove filter column */);
    plan.addStep(std::move(filter_step));
}

StorageMerge::StorageListWithLocks ReadFromMerge::getSelectedTables(
    ContextPtr query_context) const
{
    const Settings & settings = query_context->getSettingsRef();
    StorageListWithLocks res;
    DatabaseTablesIterators database_table_iterators = assert_cast<StorageMerge &>(*storage_merge).getDatabaseIterators(query_context);

    std::function<bool(const String&,const String&)> table_filter;
    if (filter_actions_dag && merge_storage_snapshot->metadata->isVirtualColumn("_database") && merge_storage_snapshot->metadata->isVirtualColumn("_table"))
    {
        auto lc_string_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        Block sample_block = {
            ColumnWithTypeAndName(lc_string_type, "_database"),
            ColumnWithTypeAndName(lc_string_type, "_table")
        };
        // Extract predicate part, that could be evaluated only with _database and _table columns
        auto table_filter_dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &sample_block, query_context);
        if (table_filter_dag)
        {
            auto filter_expression = VirtualColumnUtils::buildFilterExpression(std::move(*table_filter_dag), query_context);
            auto filter_column_name = filter_expression->getActionsDAG().getOutputs().at(0)->result_name;
            table_filter = [filter=std::move(filter_expression), column_name=std::move(filter_column_name), lc_string_type] (const auto& database_name, const auto& table_name)
            {
                MutableColumnPtr database_column = lc_string_type->createColumn();
                MutableColumnPtr table_column = lc_string_type->createColumn();
                database_column->insert(database_name);
                table_column->insert(table_name);
                Block block{
                    ColumnWithTypeAndName(std::move(database_column), lc_string_type, "_database"),
                    ColumnWithTypeAndName(std::move(table_column), lc_string_type, "_table")
                };
                filter->execute(block);
                // Valid only when block has exactly one row.
                return block.getByName(column_name).column->getBool(0);
            };
        }
    }

    auto access = query_context->getAccess();
    for (const auto & iterator : database_table_iterators)
    {
        auto database = DatabaseCatalog::instance().tryGetDatabase(iterator->databaseName());
        auto granted_show_on_all_tables = access->isGranted(AccessType::SHOW_TABLES, iterator->databaseName());
        auto granted_select_on_all_tables = access->isGranted(AccessType::SELECT, iterator->databaseName());
        while (iterator->isValid())
        {
            StoragePtr storage = tableForRead(database, iterator->name(), iterator->table(), query_context);
            if (!storage)
            {
                /// `next` must be called on every path, otherwise the loop never terminates.
                iterator->next();
                continue;
            }

            /// The `_table` and `_database` values of the rows are stamped by the table that
            /// actually produces the rows. If the child table reads from other tables, its rows
            /// carry those tables' names, not the child's own name, so pruning the child by its
            /// name could incorrectly discard the rows the predicate selects. Such children are
            /// always read, and the predicate is applied to the rows.
            if (storage.get() != storage_merge.get())
                if (!table_filter || storage->readsFromOtherTables() || table_filter(iterator->databaseName(), iterator->name()))
                    if (granted_show_on_all_tables || access->isGranted(AccessType::SHOW_TABLES, iterator->databaseName(), iterator->name()))
                    {
                        if  (!granted_select_on_all_tables)
                        {
                            const auto columns_to_check = VirtualColumnUtils::filterVirtualColumns(all_column_names, storage_snapshot->metadata, VirtualsKind::All, VirtualsMaterializationPlace::All);
                            access->checkAccess(AccessType::SELECT, iterator->databaseName(), iterator->name(), columns_to_check);
                        }

                        auto table_lock = storage->lockForShare(query_context->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);
                        res.emplace_back(iterator->databaseName(), storage, std::move(table_lock), iterator->name());
                    }
            iterator->next();
        }
    }

    return res;
}

DatabaseTablesIteratorPtr StorageMerge::DatabaseNameOrRegexp::getDatabaseIterator(const String & database_name, ContextPtr local_context) const
{
    /// The internal database of temporary tables holds the temporary tables of all sessions and all users,
    /// and it is not covered by access control, so direct access to it is denied, see `DatabaseCatalog::tryGetDatabaseAndTable`.
    if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED, "Direct access to `{}` database is not allowed", DatabaseCatalog::TEMPORARY_DATABASE);

    auto database = DatabaseCatalog::instance().getDatabase(database_name);

    auto table_name_match = [this, database_name](const String & table_name_) -> bool
    {
        if (source_databases_and_tables)
        {
            if (auto it = source_databases_and_tables->find(database_name); it != source_databases_and_tables->end())
                return it->second.contains(table_name_);
            return false;
        }
        return source_table_regexp->match(table_name_);
    };

    return database->getTablesIterator(local_context, table_name_match);
}

StorageMerge::DatabaseTablesIterators StorageMerge::DatabaseNameOrRegexp::getDatabaseIterators(ContextPtr local_context) const
{
    try
    {
        checkStackSize();
    }
    catch (Exception & e)
    {
        e.addMessage("while getting table iterator of Merge table. Maybe caused by two Merge tables that will endlessly try to read each other's data");
        throw;
    }

    DatabaseTablesIterators database_table_iterators;

    if (!database_is_regexp)
    {
        /// database_name argument is not a regexp
        database_table_iterators.emplace_back(getDatabaseIterator(source_database_name_or_regexp, local_context));
    }
    else
    {
        /// database_name argument is a regexp
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true, .with_remote_databases = true});

        for (const auto & db : databases)
        {
            /// A regexp is not an explicit request for the internal database of temporary tables, so it is skipped silently.
            if (db.first == DatabaseCatalog::TEMPORARY_DATABASE)
                continue;

            if (source_database_regexp->match(db.first))
                database_table_iterators.emplace_back(getDatabaseIterator(db.first, local_context));
        }
    }

    return database_table_iterators;
}


void StorageMerge::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    std::optional<NameDependencies> name_deps{};
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());

        if (command.type == AlterCommand::Type::DROP_COLUMN && !command.clear)
        {
            if (!name_deps)
                name_deps = getDependentViewsByColumn(local_context);
            const auto & deps_mv = name_deps.value()[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP column {} which is referenced by materialized view {}",
                    backQuoteIfNeed(command.column_name), toString(deps_mv));
            }
        }
    }
}

void StorageMerge::alter(
    const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    auto table_id = getStorageID();

    auto metadata_snapshot = getInMemoryMetadataPtr(local_context, false);
    StorageInMemoryMetadata storage_metadata = *metadata_snapshot;
    params.apply(storage_metadata, local_context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, storage_metadata, /*validate_new_create_query=*/true);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

void ReadFromMerge::convertAndFilterSourceStream(
    const Block & header,
    SelectQueryInfo & modified_query_info,
    const StorageSnapshotPtr & snapshot,
    const Aliases & aliases,
    const RowPolicyDataOpt & row_policy_data_opt,
    ContextPtr local_context,
    ChildPlan & child,
    bool is_smallest_column_requested)
{
    auto before_block_header = child.plan.getCurrentHeader();

    auto pipe_columns = before_block_header->getNamesAndTypesList();

    if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        for (const auto & alias : aliases)
        {
            ActionsDAG actions_dag(pipe_columns);

            QueryTreeNodePtr query_tree = buildQueryTree(alias.expression, local_context);
            query_tree->setAlias(alias.name);

            QueryAnalysisPass query_analysis_pass(modified_query_info.table_expression);
            query_analysis_pass.run(query_tree, local_context);

            /// On the query info cache path nothing registered this expression's sets.
            collectSets(query_tree, *modified_query_info.planner_context);

            ColumnNodePtrWithHashSet empty_correlated_columns_set;
            PlannerActionsVisitor actions_visitor(modified_query_info.planner_context, empty_correlated_columns_set, false /*use_column_identifier_as_action_node_name*/);
            const auto & [nodes, _] = actions_visitor.visit(actions_dag, query_tree);

            if (nodes.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected to have 1 output but got {}", nodes.size());

            actions_dag.addOrReplaceInOutputs(actions_dag.addAlias(*nodes.front(), alias.name));
            auto expression_step = std::make_unique<ExpressionStep>(child.plan.getCurrentHeader(), std::move(actions_dag));
            child.plan.addStep(std::move(expression_step));
        }
    }
    else
    {
        for (const auto & alias : aliases)
        {
            pipe_columns.emplace_back(NameAndTypePair(alias.name, alias.type));
            ASTPtr expr = alias.expression;
            auto syntax_result = TreeRewriter(local_context).analyze(expr, pipe_columns);
            auto expression_analyzer = ExpressionAnalyzer{alias.expression, syntax_result, local_context};

            auto dag = std::make_shared<ActionsDAG>(pipe_columns);
            auto actions_dag = expression_analyzer.getActionsDAG(true, false);
            auto expression_step = std::make_unique<ExpressionStep>(child.plan.getCurrentHeader(), std::move(actions_dag));
            child.plan.addStep(std::move(expression_step));
        }
    }

    /// This is the filter for the individual source table, that's why filtering has to be done before all structure adaptations.
    if (row_policy_data_opt)
        row_policy_data_opt->addFilterTransform(child.plan);

    /** Output headers may differ from what StorageMerge expects in some cases.
      * When the child table engine produces a query plan for the stage after FetchColumns,
      * execution names in the output header may be different.
      * The same happens with StorageDistributed, even in the case of FetchColumns.
      */

    /** Convert types of columns according to the resulting Merge table.
      * And convert column names to the expected ones.
       */
    ColumnsWithTypeAndName current_step_columns = child.plan.getCurrentHeader()->getColumnsWithTypeAndName();
    ColumnsWithTypeAndName converted_columns;
    size_t size = current_step_columns.size();
    converted_columns.reserve(current_step_columns.size());
    auto materializeIfSourceIsNotConst = [](const ColumnWithTypeAndName & expected, const ColumnWithTypeAndName & source)
    {
        if (expected.column && isColumnConst(*expected.column) && (!source.column || !isColumnConst(*source.column)))
        {
            ColumnWithTypeAndName materialized = expected;
            materialized.column = expected.column->convertToFullColumnIfConst();
            return materialized;
        }
        return expected;
    };

    String smallest_column_name = ExpressionActions::getSmallestColumn(snapshot->metadata->getColumns().getAllPhysical()).name;
    for (size_t i = 0; i < size; ++i)
    {
        const auto & source_elem = current_step_columns[i];
        if (header.has(source_elem.name))
        {
            converted_columns.push_back(materializeIfSourceIsNotConst(header.getByName(source_elem.name), source_elem));
        }
        else if (is_smallest_column_requested && smallest_column_name == source_elem.name)
        {
            /// This column is unneeded in the result.
            converted_columns.push_back(source_elem);
        }
        else if (header.columns() == current_step_columns.size())
        {
            /// Virtual columns and columns read from Distributed tables (having different name but matched by position).
            converted_columns.push_back(materializeIfSourceIsNotConst(header.getByPosition(i), source_elem));
        }
        else
        {
            /// Matching by name, but some columns are unneeded.
            converted_columns.push_back(source_elem);
        }
    }

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        current_step_columns,
        converted_columns,
        ActionsDAG::MatchColumnsMode::Position,
        local_context);

    auto expression_step = std::make_unique<ExpressionStep>(child.plan.getCurrentHeader(), std::move(convert_actions_dag));
    child.plan.addStep(std::move(expression_step));

    /// Add missing columns for the resulting Merge table.
    {
        bool inner_share_nested_offsets = true;
        if (const auto * merge_tree = dynamic_cast<const MergeTreeData *>(&snapshot->storage))
            inner_share_nested_offsets = (*merge_tree->getSettings())[MergeTreeSetting::share_nested_offsets];

        auto adding_missing_defaults_dag = addMissingDefaults(
            *child.plan.getCurrentHeader(),
            header.getNamesAndTypesList(),
            snapshot->getAllColumnsDescription(),
            local_context,
            false,
            inner_share_nested_offsets);

        auto adding_missing_defaults_step = std::make_unique<ExpressionStep>(child.plan.getCurrentHeader(), std::move(adding_missing_defaults_dag));
        child.plan.addStep(std::move(adding_missing_defaults_step));
    }
}

const ReadFromMerge::StorageListWithLocks & ReadFromMerge::getSelectedTables()
{
    filterTablesAndCreateChildrenPlans();
    return selected_tables;
}

bool ReadFromMerge::requestReadingInOrder(InputOrderInfoPtr order_info_, size_t query_limit)
{
    filterTablesAndCreateChildrenPlans();

    /// Disable read-in-order optimization for reverse order with final.
    /// Otherwise, it can lead to incorrect final behavior because the implementation may rely on the reading in direct order).
    if (order_info_->direction != 1 && InterpreterSelectQuery::isQueryWithFinal(query_info))
        return false;

    auto request_read_in_order = [order_info_, query_limit](ReadFromMergeTree & read_from_merge_tree)
    {
        return read_from_merge_tree.requestReadingInOrder(
            order_info_->used_prefix_of_sorting_key_size, order_info_->direction, order_info_->limit, query_limit);
    };

    bool ok = true;
    for (const auto & child_plan : *child_plans)
        if (child_plan.plan.isInitialized())
            ok &= recursivelyApplyToReadingSteps(child_plan.plan.getRootNode(), request_read_in_order);

    if (!ok)
        return false;

    order_info = order_info_;
    query_info.input_order_info = order_info;
    return true;
}

void ReadFromMerge::applyFilters(ActionDAGNodes added_filter_nodes)
{
    for (const auto & filter_info : pushed_down_filters)
        added_filter_nodes.nodes.push_back(&filter_info.actions.findInOutputs(filter_info.column_name));

    SourceStepWithFilter::applyFilters(added_filter_nodes);

    filterTablesAndCreateChildrenPlans();
}

QueryPlanRawPtrs ReadFromMerge::getChildPlans()
{
    filterTablesAndCreateChildrenPlans();

    QueryPlanRawPtrs plans;
    for (auto & child_plan : *child_plans)
        if (child_plan.plan.isInitialized())
            plans.push_back(&child_plan.plan);

    return plans;
}

std::vector<QueryPlan *> ReadFromMerge::getAllChildPlans()
{
    filterTablesAndCreateChildrenPlans();

    std::vector<QueryPlan *> plans;
    plans.reserve(child_plans->size());
    for (auto & child_plan : *child_plans)
        plans.push_back(child_plan.plan.isInitialized() ? &child_plan.plan : nullptr);

    return plans;
}

const std::vector<StorageID> & ReadFromMerge::getExpandableReads(
    const std::function<bool(const ReadFromMergeTree &)> & can_ship_read)
{
    /// The parallel-replicas plan transformation only understands `ReadFromMergeTree` reads and unions of
    /// them. This step is opaque to it: the per-table subplans are built lazily and their pipelines - not
    /// their plans - are united in `initializePipeline`, so the underlying reads are invisible while the
    /// plan is transformed. `expandForParallelReplicas` unites the very same subplans at plan level instead,
    /// turning the `Merge` into exactly the shape the transformation already distributes: a union of
    /// `MergeTree` reads. This tells the caller whether that is possible, and which tables the union would
    /// read, without touching the plan - so that the decision to distribute can be taken before anything is
    /// rewritten.
    if (expandable_reads)
        return *expandable_reads;

    filterTablesAndCreateChildrenPlans();

    if (selected_tables.empty() || child_plans->empty())
        return expandable_reads.emplace();

    /// Every child must be a `MergeTree` table read by a plain read step, and none of them may be `FINAL`.
    /// A child read through an interpreter (a `View`, a nested `Merge`) or a table of another engine has no
    /// marks to coordinate, and a `FINAL` read is incompatible with parallel reading; either way the child
    /// would be read in full by every replica and its rows duplicated. One such child disables the expansion
    /// for the whole `Merge`: keeping the plan-level union for the remaining children would split the
    /// `Merge` between two different reading mechanisms.
    ///
    /// The engine is checked on the table and not only on the shape of its plan, because the plan of a
    /// `View` over a single `MergeTree` table has the same shape. Such a child was planned with parallel
    /// replicas cleared from its context (see `createChildrenPlans`), so distributing its read would be
    /// rejected later anyway, leaving an expanded `Merge` that is read by a single replica after all.
    ///
    /// The last word on whether a read can be distributed belongs to the caller, whose `can_ship_read` says
    /// no for a table which is not replicated while `parallel_replicas_for_non_replicated_merge_tree` is off,
    /// and for the target of a refreshable materialized view.
    std::vector<StorageID> storage_ids;
    storage_ids.reserve(child_plans->size());

    /// `filterTablesAndCreateChildrenPlans` keeps the two aligned one to one, truncating the tables to the
    /// plans it managed to build; walk them together, and expand nothing should they ever disagree.
    chassert(selected_tables.size() == child_plans->size());

    auto table_it = selected_tables.begin();
    for (const auto & child : *child_plans)
    {
        if (table_it == selected_tables.end())
            return expandable_reads.emplace();

        const auto & storage = std::get<1>(*table_it);
        ++table_it;

        if (!storage->isMergeTree() || !child.plan.isInitialized())
            return expandable_reads.emplace();

        /// Descend the steps the child plan puts on top of the read - the converting expressions and the
        /// row policy filter of `convertAndFilterSourceStream`. Anything else means the child is not read
        /// by a plain read, whatever its leaf turns out to be.
        const auto * node = child.plan.getRootNode();
        while (node && node->children.size() == 1
               && (typeid_cast<const ExpressionStep *>(node->step.get()) || typeid_cast<const FilterStep *>(node->step.get())))
            node = node->children.front();

        const auto * reading = node ? typeid_cast<const ReadFromMergeTree *>(node->step.get()) : nullptr;
        if (!reading || reading->isQueryWithFinal() || !can_ship_read(*reading))
            return expandable_reads.emplace();

        storage_ids.push_back(reading->getMergeTreeData().getStorageID());
    }

    return expandable_reads.emplace(std::move(storage_ids));
}

QueryPlan ReadFromMerge::expandForParallelReplicas()
{
    /// Precondition: `getExpandableReads` returned a value, so the child plans exist and every one of them
    /// is a plain `MergeTree` read this union may distribute.
    chassert(child_plans && !child_plans->empty());

    SharedHeaders input_headers;
    std::vector<std::unique_ptr<QueryPlan>> plans;
    input_headers.reserve(child_plans->size());
    plans.reserve(child_plans->size());
    for (auto & child : *child_plans)
    {
        input_headers.push_back(child.plan.getCurrentHeader());
        plans.push_back(std::make_unique<QueryPlan>(std::move(child.plan)));
    }

    /// Narrowing is allowed, as it is for the `UNION ALL` this union stands for. `initializePipeline` does
    /// the same thing by hand (`pipeline.narrow`) because it unites pipelines, where the step's own machinery
    /// is out of reach; here the union step caps the number of simultaneously reading children itself, by
    /// `max_streams_for_union_step` and `max_streams_for_union_step_to_max_threads_ratio`. Of the three cases
    /// in which `initializePipeline` skips narrowing, two cannot happen for an expanded `Merge` - every child
    /// is a plain `MergeTree` read, so no child produces sorted streams or partial aggregation states - and
    /// reading in order is handled generically: `optimizeReadInOrder` and `applyOrder` call `disableNarrowing`
    /// on a union whose streams have to stay individually sorted.
    QueryPlan union_plan;
    union_plan.unitePlans(
        std::make_unique<UnionStep>(std::move(input_headers), /*max_threads_=*/ 0, /*allow_narrowing_=*/ true),
        std::move(plans));

    /// This step is destroyed once it is replaced by the union, so the tables it holds must be kept alive by
    /// the plan instead - the same holders `initializePipeline` attaches to the pipeline.
    QueryPlanResourceHolder resources;
    for (const auto & table : selected_tables)
    {
        resources.storage_holders.push_back(std::get<1>(table));
        resources.table_locks.push_back(std::get<2>(table));
    }
    union_plan.addResources(std::move(resources));

    return union_plan;
}

IStorage::ColumnSizeByName StorageMerge::getColumnSizes() const
{
    ColumnSizeByName column_sizes;

    forEachTable([&](const auto & table)
    {
        for (const auto & [name, size] : table->getColumnSizes())
            column_sizes[name].add(size);
    });

    return column_sizes;
}

IStorage::ColumnSizeByName StorageMerge::getColumnSizes(const Names & columns, bool calculate_subcolumn_sizes) const
{
    ColumnSizeByName column_sizes;

    forEachTable([&](const auto & table)
    {
        for (const auto & [name, size] : table->getColumnSizes(columns, calculate_subcolumn_sizes))
            column_sizes[name].add(size);
    });

    return column_sizes;
}

std::optional<IStorage::ColumnSizeByName> StorageMerge::tryGetColumnSizes() const
{
    try
    {
        return getColumnSizes();
    }
    catch (const Exception & e)
    {
        /// The column sizes are a best-effort introspection (`system.columns`). The source database
        /// may have been dropped (`UNKNOWN_DATABASE`), or it may be the internal database of temporary
        /// tables, which `getDatabaseIterator` refuses to enumerate (`DATABASE_ACCESS_DENIED`) - such a
        /// table can no longer be created, but a pre-existing definition still loads (`ATTACH`, backup
        /// `RESTORE`, replicated-database replay) and must not break `system.columns`.
        if (e.code() == ErrorCodes::UNKNOWN_DATABASE || e.code() == ErrorCodes::DATABASE_ACCESS_DENIED)
            return std::nullopt;
        throw;
    }
}


std::tuple<bool /* is_regexp */, ASTPtr> StorageMerge::evaluateDatabaseName(const ASTPtr & node, ContextPtr context_)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == "REGEXP")
    {
        if (func->arguments->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "REGEXP in Merge ENGINE takes only one argument");

        auto * literal = func->arguments->children[0]->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::Which::String || literal->value.safeGet<String>().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument for REGEXP in Merge ENGINE should be a non empty String Literal");

        return {true, func->arguments->children[0]};
    }

    auto ast = evaluateConstantExpressionForDatabaseName(node, context_);
    return {false, ast};
}

bool StorageMerge::supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr ctx) const
{
    /// Here we actually need storage snapshot of all nested tables.
    /// But to avoid complexity pass nullptr to make more lightweight check in MergeTreeData.
    return traverseTablesUntil([&](const auto & table) { return !table->supportsTrivialCountOptimization(nullptr, ctx); }) == nullptr;
}

std::optional<UInt64> StorageMerge::totalRows(ContextPtr query_context) const
{
    return totalRowsOrBytes([&](const auto & table) { return table->totalRows(query_context); });
}

std::optional<UInt64> StorageMerge::totalBytes(ContextPtr query_context) const
{
    return totalRowsOrBytes([&](const auto & table) { return table->totalBytes(query_context); });
}

template <typename F>
std::optional<UInt64> StorageMerge::totalRowsOrBytes(F && func) const
{
    UInt64 total_rows_or_bytes = 0;
    auto first_table = traverseTablesUntil([&](const auto & table)
    {
        if (auto rows_or_bytes = func(table))
        {
            total_rows_or_bytes += *rows_or_bytes;
            return false;
        }
        return true;
    });

    return first_table ? std::nullopt : std::make_optional(total_rows_or_bytes);
}

void registerStorageMerge(StorageFactory & factory);
void registerStorageMerge(StorageFactory & factory)
{
    factory.registerStorage("Merge", [](const StorageFactory::Arguments & args)
    {
        /** In query, the name of database is specified as table engine argument which contains source tables,
          *  as well as regex for source-table names.
          */

        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Merge requires exactly 2 parameters - name "
                            "of source database and regexp for table names.");

        auto [is_regexp, database_ast] = StorageMerge::evaluateDatabaseName(engine_args[0], args.getLocalContext());

        if (!is_regexp)
            engine_args[0] = database_ast;

        String source_database_name_or_regexp = checkAndGetLiteralArgument<String>(database_ast, "database_name");

        /// With an explicit column list, `CREATE` (or a full-definition `ATTACH`, which is CREATE-like user input)
        /// does not need schema inference and would not read the source tables, so the unusable table definition
        /// would be stored; deny it right away, the same way as reading does, see `DatabaseNameOrRegexp::getDatabaseIterator`.
        /// Only fresh user-supplied definitions are denied. Loads of previously stored metadata (server startup,
        /// short-syntax `ATTACH`) and replays of definitions that already exist elsewhere (`SECONDARY_CREATE`:
        /// replicated-database DDL replay, backup `RESTORE`) stay loadable, so a table created before this check
        /// existed can still be restored or materialized on a new replica. Reading from such a table is denied
        /// anyway, so unlike `StorageDistributed` there is nothing a restoring user could reach through it.
        bool fresh_user_definition = args.mode == LoadingStrictnessLevel::CREATE
            || (args.mode == LoadingStrictnessLevel::ATTACH && !args.query.attach_short_syntax);
        if (!is_regexp && source_database_name_or_regexp == DatabaseCatalog::TEMPORARY_DATABASE
            && fresh_user_definition)
            throw Exception(
                ErrorCodes::DATABASE_ACCESS_DENIED, "Direct access to `{}` database is not allowed", DatabaseCatalog::TEMPORARY_DATABASE);

        engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.getLocalContext());
        String table_name_regexp = checkAndGetLiteralArgument<String>(engine_args[1], "table_name_regexp");

        return std::make_shared<StorageMerge>(
            args.table_id, args.columns, args.comment, source_database_name_or_regexp, is_regexp, table_name_regexp, args.getLocalContext());
    },
    {
        .supports_schema_inference = true
    },
    Documentation{
        .description = R"DOCS_MD(
The `Merge` engine (not to be confused with `MergeTree`) does not store data itself, but allows reading from any number of other tables simultaneously.

Reading is automatically parallelized. Writing to a table is not supported. When reading, the indexes of tables that are actually being read are used, if they exist.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

## Engine parameters {#engine-parameters}

### `db_name` {#db_name}

`db_name` — Possible values:
    - database name,
    - constant expression that returns a string with a database name, for example, `currentDatabase()`,
    - `REGEXP(expression)`, where `expression` is a regular expression to match the DB names.

### `tables_regexp` {#tables_regexp}

`tables_regexp` — A regular expression to match the table names in the specified DB or DBs.

Regular expressions — [re2](https://github.com/google/re2) (supports a subset of PCRE), case-sensitive.
See the notes about escaping symbols in regular expressions in the "match" section.

## Usage {#usage}

When selecting tables to read, the `Merge` table itself is not selected, even if it matches the regex. This is to avoid loops.
It is possible to create two `Merge` tables that will endlessly try to read each others' data, but this is not a good idea.

The typical way to use the `Merge` engine is for working with a large number of `TinyLog` tables as if with a single table.

## Examples {#examples}

**Example 1**

Consider two databases `ABC_corporate_site` and `ABC_store`. The `all_visitors` table will contain IDs from the tables `visitors` in both databases.

```sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**Example 2**

Let's say you have an old table `WatchLog_old` and decided to change partitioning without moving data to a new table `WatchLog_new`, and you need to see data from both tables.

```sql
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
ORDER BY (date, UserId, EventType);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
PARTITION BY date
ORDER BY (UserId, EventType)
SETTINGS index_granularity=8192;

INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog AS WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

```text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## Virtual columns {#virtual-columns}

- `_table` — The name of the table from which data was read. Type: [String](/reference/data-types/string).

    If you filter on `_table`, (for example `WHERE _table='xyz'`) only tables which satisfy the filter condition are read. A table that itself reads from other tables (`Distributed`, `Merge`, `Buffer`, `Alias`) returns rows carrying the name of the table that actually produced them, so such tables are always read and the filter is applied to their rows.

- `_database` — Contains the name of the database from which data was read. Type: [String](/reference/data-types/string).

**See Also**

- [Virtual columns](/reference/engines/table-engines/index#table_engines-virtual_columns)
- [merge](/reference/functions/table-functions/merge) table function
)DOCS_MD",
        .syntax = "ENGINE = Merge(db_name, tables_regexp)",
        .related = {"Distributed"}});
}

}

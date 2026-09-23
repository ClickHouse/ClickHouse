#include <Storages/ProjectionsDescription.h>
#include <DataTypes/DataTypeString.h>

#include <base/sort.h>
#include <Access/AccessControl.h>
#include <Columns/ColumnConst.h>
#include <Common/iota.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_PROJECTION;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_PROJECTION_IN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{

extern const SettingsBool enable_positional_arguments_for_projections;

}

namespace MergeTreeSetting
{

extern const MergeTreeSettingsUInt64 index_granularity_bytes;
extern const MergeTreeSettingsBool add_minmax_index_for_numeric_columns;
extern const MergeTreeSettingsBool add_minmax_index_for_string_columns;
extern const MergeTreeSettingsBool add_minmax_index_for_temporal_columns;
extern const MergeTreeSettingsBool add_minmax_index_for_block_number_column;
extern const MergeTreeSettingsBool add_minmax_index_for_block_offset_column;

}

bool ProjectionDescription::isPrimaryKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node) const
{
    const String column_name = node->getColumnName();

    for (const auto & key_name : metadata->getPrimaryKeyColumns())
        if (column_name == key_name)
            return true;

    if (const auto * func = node->as<ASTFunction>())
        if (func->arguments->children.size() == 1)
            return isPrimaryKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front());

    return false;
}


ProjectionDescription ProjectionDescription::clone() const
{
    ProjectionDescription other;
    if (definition_ast)
        other.definition_ast = definition_ast->clone();
    if (query_ast)
        other.query_ast = query_ast->clone();

    other.name = name;
    other.type = type;
    other.required_columns = required_columns;
    other.sample_block = sample_block;
    other.sample_block_for_keys = sample_block_for_keys;
    other.metadata = metadata;
    other.key_size = key_size;
    other.primary_key_max_column_name = primary_key_max_column_name;
    other.partition_value_indices = partition_value_indices;
    other.with_parent_part_offset = with_parent_part_offset;
    other.with_block_number = with_block_number;
    other.with_block_offset = with_block_offset;
    other.index = index;
    other.settings_changes = settings_changes;
    other.has_index_granularity_overrides = has_index_granularity_overrides;
    if (where_clause_ast)
        other.where_clause_ast = where_clause_ast->clone();

    return other;
}

ProjectionsDescription ProjectionsDescription::clone() const
{
    ProjectionsDescription other;
    for (const auto & projection : projections)
        other.add(projection.clone());
    for (const auto & definition_ast : unavailable)
        other.addUnavailable(definition_ast->clone());

    return other;
}

bool ProjectionDescription::operator==(const ProjectionDescription & other) const
{
    return name == other.name
        && definition_ast->formatIgnoringRedundantParentheses() == other.definition_ast->formatIgnoringRedundantParentheses();
}

namespace
{

/// Fake storage used to build projection metadata
class StorageProjectionSource final : public IStorage
{
public:
    explicit StorageProjectionSource(const ColumnsDescription & columns_description, const KeyDescription * partition_key)
        : IStorage({"_", "_"})
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_description);
        storage_metadata.setVirtuals(MergeTreeData::createVirtuals(partition_key));
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override { return "ProjectionSource"; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsColumnsWithDynamicStructure() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo &,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processing_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override
    {
        return Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names))));
    }
};

/// Provides source data for the projection pipeline
class ProjectionDataSource final : public ISource
{
public:
    explicit ProjectionDataSource(SharedHeader block)
        : ISource(std::make_shared<const Block>(block->cloneEmpty()))
        , chunk(block->getColumns(), block->rows())
    {
    }

    String getName() const override { return "ProjectionDataSource"; }

    /// Avoid tracking read progress for projection calculation pipeline.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

protected:
    Chunk generate() override { return std::move(chunk); }

private:
    Chunk chunk;
};

/// Collects processed data from the projection pipeline into a single chunk,
/// Enforces that projections cannot increase the number of rows beyond the original input.
class ProjectionDataSink final : public ISink
{
public:
    ProjectionDataSink(SharedHeader header, size_t max_rows_allowed_)
        : ISink(std::move(header))
        , max_rows_allowed(max_rows_allowed_)
    {
    }

    String getName() const override { return "ProjectionDataSink"; }

    /// Accumulated data can be empty if DELETE query deleted all the rows from block
    bool isAccumulatedSomething() const { return accumulated_chunk && accumulated_chunk.getNumRows() > 0; }

    Columns detachAccumulatedColumns() { return accumulated_chunk.detachColumns(); }

protected:
    void consume(Chunk chunk) override
    {
        num_rows += chunk.getNumRows();
        if (num_rows > max_rows_allowed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection cannot increase the number of rows in a block. It's a bug");

        if (!accumulated_chunk)
        {
            accumulated_chunk = std::move(chunk);
            return;
        }

        auto mutable_columns = accumulated_chunk.mutateColumns();
        for (size_t i = 0, size = mutable_columns.size(); i < size; ++i)
        {
            const auto source_column = chunk.getColumns()[i];
            mutable_columns[i]->insertRangeFrom(*source_column, 0, source_column->size());
        }

        accumulated_chunk.setColumns(std::move(mutable_columns), num_rows);
    }

private:
    Chunk accumulated_chunk;
    size_t num_rows = 0;
    const size_t max_rows_allowed;
};

}

ProjectionDescription ProjectionDescription::getProjectionFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    const KeyDescription * partition_key,
    const ContextPtr & query_context,
    LoadingStrictnessLevel mode,
    bool attach_short_syntax)
{
    const auto * projection_definition = definition_ast->as<ASTProjectionDeclaration>();

    if (!projection_definition)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot create projection from non ASTProjectionDeclaration AST");

    if (projection_definition->name.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Projection must have name in definition.");

    /// The name is used unescaped as a directory name (`getDirectoryName`) inside a part directory,
    /// so a '/' in it would address files outside of the part and outside of the data directory.
    if (projection_definition->name.contains('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Projection name ({}) cannot contain '/'", projection_definition->name);

    ProjectionDescription result;
    result.definition_ast = projection_definition->clone();
    result.name = projection_definition->name;

    if (projection_definition->index)
    {
        chassert(projection_definition->type);
        result.index = ProjectionIndexFactory::instance().get(*projection_definition);
    }

    /// Compute effective MergeTree settings for the projection (defaults possibly contributed by
    /// the projection index, with user-supplied WITH SETTINGS overrides applied on top). This must
    /// happen before fillProjectionDescription[ByQuery] because the latter reconstructs settings
    /// from result.settings_changes to drive implicit-minmax skip-index creation.
    auto merge_tree_settings = result.index ? result.index->getDefaultSettings() : std::make_shared<MergeTreeSettings>();
    if (projection_definition->with_settings)
        merge_tree_settings->applyChanges(projection_definition->with_settings->changes, query_context, isLoadingFromExistingMetadata(mode));
    result.settings_changes = merge_tree_settings->changes();

    /// Track whether the effective settings include index_granularity or index_granularity_bytes overrides
    /// (from either the projection index's getDefaultSettings() or the user's explicit SETTINGS clause),
    /// so that checkProperties can reject projections with granularity overrides on non-adaptive tables.
    for (const auto & change : result.settings_changes)
    {
        if (change.name == "index_granularity" || change.name == "index_granularity_bytes")
        {
            result.has_index_granularity_overrides = true;
            break;
        }
    }

    if (result.index)
    {
        result.index->fillProjectionDescription(result, projection_definition->index, columns, partition_key, query_context, *merge_tree_settings);
    }
    else
    {
        fillProjectionDescriptionByQuery(result, projection_definition->query->as<ASTProjectionSelectQuery &>(), columns, partition_key, query_context, *merge_tree_settings);
    }

    /// `WITH SETTINGS` is part of the table definition, so it is checked whenever that is
    if (isFreshTableDefinition(mode, attach_short_syntax))
    {
        static const std::unordered_set<std::string_view> ALLOWED_PROJECTION_SETTINGS = {
            "index_granularity",
            "index_granularity_bytes",
            "add_minmax_index_for_numeric_columns",
            "add_minmax_index_for_string_columns",
            "add_minmax_index_for_temporal_columns",
            "add_minmax_index_for_block_number_column",
            "add_minmax_index_for_block_offset_column",
            "min_compress_block_size",
            "max_compress_block_size",
            "min_bytes_for_wide_part",
            "min_level_for_wide_part",
            "min_rows_for_wide_part",
            "ratio_of_defaults_for_sparse_serialization",
            "write_marks_for_substreams_in_compact_parts",
            "serialization_info_version",
            "nullable_serialization_version",
            "string_serialization_version",
            "replace_long_file_name_to_hash",
            "map_serialization_version",
            "map_serialization_version_for_zero_level_parts",
            "propagate_types_serialization_versions_to_nested_types",
        };

        for (const auto & change : result.settings_changes)
        {
            if (!ALLOWED_PROJECTION_SETTINGS.contains(change.name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} is not allowed for projections", change.name);
        }

        /// What `WITH SETTINGS` changes from the defaults this projection would otherwise have.
        auto default_settings = result.index ? result.index->getDefaultSettings() : std::make_shared<MergeTreeSettings>();
        query_context->checkMergeTreeSettingsConstraints(*default_settings, merge_tree_settings->changesFrom(*default_settings));

        query_context->getGlobalContext()->initializeBackgroundExecutorsIfNeeded();
        merge_tree_settings->sanityCheck(
            query_context->getMergeMutateExecutor()->getMaxTasksCount(),
            query_context->wasBackgroundPoolAutoLowered());
    }

    /// Ensure index_granularity_bytes is non-zero to prevent the projection from falling back
    /// to fixed granularity. Enforced unconditionally (both CREATE and ATTACH paths).
    if ((*merge_tree_settings)[MergeTreeSetting::index_granularity_bytes] == 0)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "projection index_granularity_bytes cannot be 0, which leads to fixed granularity");
    }

    return result;
}

void ProjectionDescription::fillProjectionDescriptionByQuery(
    ProjectionDescription & result,
    const ASTProjectionSelectQuery & query,
    const ColumnsDescription & columns,
    const KeyDescription * partition_key,
    const ContextPtr & query_context,
    const MergeTreeSettings & projection_settings)
{
    auto projection_order_by = query.orderBy();
    result.query_ast = query.cloneToASTSelect();

    /// Store the WHERE clause AST if present (Issue #74234).
    /// This will be used by the optimizer to check if a query's WHERE implies
    /// this projection's WHERE, and during materialization to filter rows.
    auto projection_where = query.where();
    if (projection_where)
        result.where_clause_ast = projection_where->clone();

    /// Prevent normal projection from storing parent part offset if the parent table defines `_parent_part_offset` or
    /// `_part_offset` as physical columns, which would cause a conflict. Parent table cannot defines `_part_index` as
    /// physical column either because it's used to build part offset mapping during merge.
    bool can_hold_parent_part_offset = !(columns.has("_part_index") || columns.has("_part_offset") || columns.has("_parent_part_offset"));

    StoragePtr storage = std::make_shared<StorageProjectionSource>(columns, partition_key);

    bool positional_arguments_for_projections = query_context->getSettingsRef()[Setting::enable_positional_arguments_for_projections];

    /// Force INITIAL_QUERY so that the Analyzer's replaceNodesWithPositionalArguments
    /// works correctly even in DatabaseReplicated mode (where query_kind == SECONDARY_QUERY).
    auto mut_context = Context::createCopy(query_context);
    mut_context->setSetting("enable_positional_arguments", positional_arguments_for_projections);
    /// Projection required-columns must always expand ALIAS columns, regardless of session settings.
    mut_context->setSetting("optimize_respect_aliases", true);
    mut_context->setQueryKindInitial();

    bool is_aggregate = false;
    {
        /// Use all column names and types but as Ordinary columns for the Analyzer. This avoids
        /// QueryAnalyzer::initializeTableExpressionData eagerly resolving ALIAS column expressions
        /// (which may fail when session settings like allow_nonconst_timezone_arguments are unavailable,
        /// e.g. during ATTACH TABLE), while still allowing the projection query to reference any column
        /// including table-level ALIAS columns by name.
        StoragePtr analyzer_storage = std::make_shared<StorageProjectionSource>(ColumnsDescription(columns.getAll()), partition_key);

        auto query_tree = buildQueryTree(result.query_ast, mut_context);
        auto & query_node = query_tree->as<QueryNode &>();
        query_node.getJoinTreeNode() = std::make_shared<TableNode>(analyzer_storage, mut_context);

        QueryTreePassManager query_tree_pass_manager(mut_context);
        addQueryTreePasses(query_tree_pass_manager, /*only_analyze=*/true);
        query_tree_pass_manager.runOnlyResolve(query_tree);

        is_aggregate = query_node.hasGroupBy() || hasAggregateFunctionNodes(query_tree);

        /// Expand aliases in projection ORDER BY using the Analyzer-resolved query tree.
        /// cloneToASTSelect() appends the ORDER BY expression as the last SELECT child,
        /// so the last resolved projection column has aliases fully expanded.
        if (projection_order_by)
        {
            auto & projection_nodes = query_node.getProjection().getNodes();
            ConvertToASTOptions ast_options;
            ast_options.fully_qualified_identifiers = false;
            projection_order_by = projection_nodes.back()->toAST(ast_options);
            projection_order_by->setAlias({});
        }
    }

    InterpreterSelectQuery select(
        result.query_ast,
        mut_context,
        storage,
        {},
        /// Here we ignore ast optimizations because otherwise aggregation keys may be removed from result header as constants.
        SelectQueryOptions{(is_aggregate || result.where_clause_ast) ? QueryProcessingStage::WithMergeableState : QueryProcessingStage::FetchColumns}
            .modify()
            .ignoreAlias()
            .ignoreASTOptimizations()
            .ignoreSettingConstraints());

    result.required_columns = select.getRequiredColumns();
    result.sample_block = *select.getSampleBlock();

    StorageInMemoryMetadata metadata;
    metadata.partition_key = KeyDescription::buildEmptyKey();

    const auto & query_select = result.query_ast->as<const ASTSelectQuery &>();
    if (is_aggregate)
    {
        /// Aggregate projections cannot hold parent part offset.
        can_hold_parent_part_offset = false;

        if (projection_order_by)
            throw Exception(ErrorCodes::ILLEGAL_PROJECTION, "When aggregation is used in projection, ORDER BY cannot be specified");

        /// Aggregate projections with WHERE are now supported: the optimizer's predicate
        /// implication check (doesQueryFilterImplyProjectionWhere) ensures a filtered
        /// aggregate projection is only selected when the query's filter covers it.

        result.type = ProjectionDescription::Type::Aggregate;
        if (const auto & group_expression_list = query_select.groupBy())
        {
            ASTPtr order_expression;
            if (group_expression_list->children.size() == 1)
            {
                result.key_size = 1;
                order_expression = make_intrusive<ASTIdentifier>(group_expression_list->children.front()->getColumnName());
            }
            else
            {
                auto function_node = make_intrusive<ASTFunction>();
                function_node->name = "tuple";
                function_node->arguments = group_expression_list->clone();
                result.key_size = function_node->arguments->children.size();
                for (auto & child : function_node->arguments->children)
                    child = make_intrusive<ASTIdentifier>(child->getColumnName());
                function_node->children.push_back(function_node->arguments);
                order_expression = function_node;
            }
            auto columns_with_state = ColumnsDescription(result.sample_block.getNamesAndTypesList());
            metadata.sorting_key = KeyDescription::getKeyFromAST(order_expression, columns_with_state, {}, query_context);
            metadata.primary_key = KeyDescription::getKeyFromAST(order_expression, columns_with_state, {}, query_context);
            metadata.primary_key.definition_ast = nullptr;
        }
        else
        {
            metadata.sorting_key = KeyDescription::buildEmptyKey();
            metadata.primary_key = KeyDescription::buildEmptyKey();
        }
        for (const auto & key : select.getQueryAnalyzer()->aggregationKeys())
            result.sample_block_for_keys.insert({nullptr, key.type, key.name});
    }
    else
    {
        result.type = ProjectionDescription::Type::Normal;

        auto metadata_snapshot = storage->getInMemoryMetadataPtr(query_context, false);
        const auto virtuals = metadata_snapshot->virtuals;
        metadata.sorting_key = KeyDescription::getKeyFromAST(projection_order_by, columns, virtuals, query_context);
        metadata.primary_key = KeyDescription::getKeyFromAST(projection_order_by, columns, virtuals, query_context);
        metadata.primary_key.definition_ast = nullptr;
    }

    /// Rename parent _part_offset to _parent_part_offset column
    if (can_hold_parent_part_offset && result.sample_block.has("_part_offset"))
    {
        auto new_column = result.sample_block.getByName("_part_offset");
        new_column.name = "_parent_part_offset";
        result.sample_block.erase("_part_offset");
        result.sample_block.insert(std::move(new_column));
        result.with_parent_part_offset = true;
        std::erase_if(result.required_columns, [](const String & s) { return s.contains("_part_offset"); });
    }

    /// Track whether projection stores _block_number/_block_offset from the parent table.
    result.with_block_number = result.sample_block.has(BlockNumberColumn::name);
    result.with_block_offset = result.sample_block.has(BlockOffsetColumn::name);

    ColumnsDescription metadata_columns;
    for (const auto & column_with_type_name : result.sample_block)
    {
        if (column_with_type_name.column && isColumnConst(*column_with_type_name.column))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Projections cannot contain constant columns: {}", column_with_type_name.name);

        /// Subcolumns can be used in projection only when the original column is used.
        if (columns.hasSubcolumn(GetColumnsOptions::All, column_with_type_name.name))
        {
            auto subcolumn = columns.getColumnOrSubcolumn(GetColumnsOptions::All, column_with_type_name.name);
            if (!result.sample_block.has(subcolumn.getNameInStorage()))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Projections cannot contain individual subcolumns: {}", column_with_type_name.name);
            /// Also remove this subcolumn from the required columns as we have the original column.
            std::erase_if(result.required_columns, [&](const String & column_name){ return column_name == column_with_type_name.name; });
        }
        else
        {
            ColumnDescription column_description(column_with_type_name.name, column_with_type_name.type);
            /// Carry over the parent column's DEFAULT so a column missing from a projection part written
            /// before the column was added reads the table default, not the column type's default.
            if (columns.has(column_with_type_name.name) && columns.get(column_with_type_name.name).default_desc.expression)
                column_description.default_desc = columns.get(column_with_type_name.name).default_desc;
            metadata_columns.add(std::move(column_description));
        }
    }

    metadata.setColumns(std::move(metadata_columns));
    metadata.setVirtuals(MergeTreeData::createVirtuals(partition_key));

    /// Initialize implicit-minmax skip indices from the effective projection-level MergeTree settings
    /// (defaults from the projection index plus any user-supplied WITH SETTINGS overrides).
    metadata.add_minmax_index_for_numeric_columns = projection_settings[MergeTreeSetting::add_minmax_index_for_numeric_columns];
    metadata.add_minmax_index_for_string_columns = projection_settings[MergeTreeSetting::add_minmax_index_for_string_columns];
    metadata.add_minmax_index_for_temporal_columns = projection_settings[MergeTreeSetting::add_minmax_index_for_temporal_columns];
    metadata.add_minmax_index_for_block_number_column = projection_settings[MergeTreeSetting::add_minmax_index_for_block_number_column];
    metadata.add_minmax_index_for_block_offset_column = projection_settings[MergeTreeSetting::add_minmax_index_for_block_offset_column];
    metadata.addImplicitIndicesForVirtualColumns(query_context);
    for (const auto & column : metadata.columns)
        metadata.addImplicitIndicesForColumn(column, query_context);

    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
}

ProjectionDescription ProjectionDescription::getMinMaxCountProjection(
    const ColumnsDescription & columns,
    const ASTPtr & partition_columns,
    const Names & minmax_columns,
    const KeyDescription & primary_key,
    const KeyDescription * partition_key,
    const ContextPtr & query_context)
{
    ProjectionDescription result;

    auto select_query = make_intrusive<ASTProjectionSelectQuery>();
    ASTPtr select_expression_list = make_intrusive<ASTExpressionList>();
    /// The i-th min/max pair below is answered from slot i of the part min-max index, whose own order is
    /// derived the same way: from the partition key column names, never from the table column order.
    Names sorted_minmax_columns = minmax_columns;
    ::sort(sorted_minmax_columns.begin(), sorted_minmax_columns.end());
    for (const auto & column : sorted_minmax_columns)
    {
        select_expression_list->children.push_back(makeASTFunction("min", make_intrusive<ASTIdentifier>(column)));
        select_expression_list->children.push_back(makeASTFunction("max", make_intrusive<ASTIdentifier>(column)));
    }

    auto primary_key_asts = primary_key.expression_list_ast->children;
    if (!primary_key_asts.empty())
    {
        if (!primary_key.reverse_flags.empty() && primary_key.reverse_flags[0])
        {
            select_expression_list->children.push_back(makeASTFunction("max", primary_key_asts.front()->clone()));
            select_expression_list->children.push_back(makeASTFunction("min", primary_key_asts.front()->clone()));
        }
        else
        {
            select_expression_list->children.push_back(makeASTFunction("min", primary_key_asts.front()->clone()));
            select_expression_list->children.push_back(makeASTFunction("max", primary_key_asts.front()->clone()));
        }
    }
    select_expression_list->children.push_back(makeASTFunction("count"));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::SELECT, std::move(select_expression_list));

    if (partition_columns && !partition_columns->children.empty())
    {
        auto partition_columns_copy = partition_columns->clone();
        for (const auto & partition_column : partition_columns_copy->children)
            KeyDescription::moduloToModuloLegacyRecursive(partition_column);
        select_query->setExpression(ASTProjectionSelectQuery::Expression::GROUP_BY, std::move(partition_columns_copy));
    }

    result.definition_ast = select_query;
    result.name = MINMAX_COUNT_PROJECTION_NAME;
    result.query_ast = select_query->cloneToASTSelect();

    StoragePtr storage = std::make_shared<StorageProjectionSource>(columns, partition_key);
    InterpreterSelectQuery select(
        result.query_ast,
        query_context,
        storage,
        {},
        /// Here we ignore ast optimizations because otherwise aggregation keys may be removed from result header as constants.
        SelectQueryOptions{QueryProcessingStage::WithMergeableState}
            .modify()
            .ignoreAlias()
            .ignoreASTOptimizations()
            .ignoreSettingConstraints());
    result.required_columns = select.getRequiredColumns();
    result.sample_block = *select.getSampleBlock();

    std::set<size_t> constant_positions;
    for (size_t i = 0; i < result.sample_block.columns(); ++i)
    {
        if (typeid_cast<const ColumnConst *>(result.sample_block.getByPosition(i).column.get()))
            constant_positions.insert(i);
    }
    result.sample_block.erase(constant_positions);

    const auto & analysis_result = select.getAnalysisResult();
    if (analysis_result.need_aggregate)
    {
        for (const auto & key : select.getQueryAnalyzer()->aggregationKeys())
        {
            if (result.sample_block.has(key.name))
            {
                result.sample_block_for_keys.insert({nullptr, key.type, key.name});
                result.partition_value_indices.push_back(result.sample_block.getPositionByName(key.name));
            }
        }
    }

    /// If we have primary key and it's not in minmax_columns, it will be used as one additional minmax columns.
    if (!primary_key_asts.empty()
        && result.sample_block.columns()
            == 2 * (minmax_columns.size() + 1) /* minmax columns */ + 1 /* count() */
                + result.partition_value_indices.size() /* partition_columns */)
    {
        /// partition_expr1, partition_expr2, ..., min(p1), max(p1), min(p2), max(p2), ..., min(k1), max(k1), count()
        ///                                                                                              ^
        ///                                                                                           size - 2
        result.primary_key_max_column_name = *(result.sample_block.getNames().cend() - 2);
    }
    result.type = ProjectionDescription::Type::Aggregate;
    StorageInMemoryMetadata metadata;
    metadata.setColumns(ColumnsDescription(result.sample_block.getNamesAndTypesList()));
    metadata.setVirtuals(MergeTreeData::createVirtuals(partition_key));
    metadata.partition_key = KeyDescription::buildEmptyKey();
    metadata.sorting_key = KeyDescription::buildEmptyKey();
    metadata.primary_key = KeyDescription::buildEmptyKey();
    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
    return result;
}

bool ProjectionDescription::isStaleForPartColumns(
    const NamesAndTypesList & part_columns,
    const SerializationInfoByName & part_serialization_infos,
    const NamesAndTypesList & projection_part_columns,
    const SerializationInfoByName & projection_part_serialization_infos,
    const ColumnsDescription & table_columns) const
{
    /// Every physical name asked about is compared against a type some part on disk recorded for it:
    /// the parent part's own list first, then the type the projection part recorded for the copy it
    /// froze. A name NEITHER side records has no type to compare against, and then exactly one case
    /// is not staleness: a column this projection OUTPUTS, because the projection part holds no value
    /// of it either, so both read paths synthesise it from the declaration as it stands today
    /// (04412_projection_added_column_default). Any other unrecorded name is one the projection stored
    /// something derived from under a declaration nothing on disk records, so it cannot be shown to
    /// match: refuse (the rule our #112484 chose for skip indices).
    /// A non-physical name (an ALIAS, a virtual) is never compared, only followed: no part records
    /// one, so its absence is not evidence.
    Names columns_to_check;
    /// Names whose DEFAULT expression still has to be followed.
    Names to_walk;
    NameSet seen;

    /// Did the projection part record a type for @column_name? Then it holds a value FROZEN under
    /// whatever the declaration was when it was written, so every column that value was computed from
    /// still matters. When it recorded nothing either, both read paths synthesise the column from
    /// today's declaration, and there is nothing frozen for its inputs to have been computed under.
    auto projection_recorded = [&](const String & column_name)
    {
        if (projection_part_columns.tryGetByName(column_name))
            return true;
        const auto * missing = projection_part_serialization_infos.getMissingColumnInfo(column_name);
        return missing && !missing->type_name.empty();
    };

    /// A dependency the projection did not read verbatim: compare it, and follow its default when the
    /// parent part does not record it while the projection part does. Mirrors
    /// injectRequiredColumnsRecursively(): an ALIAS never resolves as physical, yet a DEFAULT may read
    /// one (01084_defaults_on_aliases) and the parent reader expands it through
    /// ColumnsDescription::getDefault() all the same.
    auto add_dependency = [&](const String & dependency_name)
    {
        auto dependency = table_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, dependency_name);
        const String & key = dependency ? dependency->getNameInStorage() : dependency_name;
        if (!seen.emplace(key).second)
            return;
        if (!dependency)
        {
            to_walk.push_back(key);
            return;
        }
        columns_to_check.push_back(key);
        if (!part_columns.tryGetByName(key) && projection_recorded(key))
            to_walk.push_back(key);
    };

    /// A projection index fills its own description, so which of the structures it stores were derived
    /// from which column is not narrowed down here either.
    bool narrowed = type == Type::Normal && !index;

    if (narrowed)
    {
        for (const auto & output : sample_block)
        {
            auto column = table_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, output.name);
            if (!column)
            {
                /// An ALIAS or virtual output, or a stored expression: it may consume a column the
                /// projection also passes through, so ask about everything.
                narrowed = false;
                break;
            }

            /// The part records the column, so the projection part holds a copy written under the same
            /// recorded type and both reads convert it identically. Nothing to ask about.
            if (part_columns.tryGetByName(column->getNameInStorage()))
                continue;

            /// The part does NOT record it, so a parent read synthesises it now from the column's
            /// DEFAULT over the inputs as they read TODAY, while the projection part holds what the
            /// same DEFAULT produced when the projection was written. Compare it, and when the
            /// projection part did record it, follow that DEFAULT too: one of its inputs may be a
            /// column the part does not record either.
            if (seen.emplace(column->getNameInStorage()).second)
            {
                columns_to_check.push_back(column->getNameInStorage());
                if (projection_recorded(column->getNameInStorage()))
                    to_walk.push_back(column->getNameInStorage());
            }
        }
    }

    if (narrowed)
    {
        /// A Normal projection's key is built over the PARENT columns (getProjectionFromAST), so its
        /// required-column names are comparable with @table_columns. No ORDER BY yields an empty set,
        /// which is correct: such a projection stores every column verbatim and derives nothing.
        for (const auto & key_column : metadata->getColumnsRequiredForPrimaryKey())
            add_dependency(key_column);

        /// A filtered projection stores only the rows its WHERE kept, so its row set was derived from
        /// the columns that WHERE reads (05076 states the same rule for the JSON case it refuses).
        if (where_clause_ast)
        {
            IdentifierNameSet filter_identifiers;
            where_clause_ast->collectIdentifierNames(filter_identifiers);
            for (const auto & identifier : filter_identifiers)
                add_dependency(identifier);
        }
    }
    else
    {
        /// Anything the loop above collected before it gave up is in `required_columns` too.
        columns_to_check = required_columns;

        /// A required column the parent part does not record is synthesised by a parent read from its
        /// DEFAULT as declared TODAY, while a projection part that recorded it holds what the same
        /// DEFAULT produced when it was written. Follow that expression so the columns the frozen
        /// value was computed FROM are compared too; the narrowed branch does this for its outputs.
        for (const auto & required_column : required_columns)
        {
            auto column = table_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, required_column);
            if (!column)
                continue;
            const auto & storage_name = column->getNameInStorage();
            if (part_columns.tryGetByName(storage_name) || !projection_recorded(storage_name))
                continue;
            if (seen.emplace(storage_name).second)
                to_walk.push_back(storage_name);
        }
    }

    /// Follow the default expressions, because one of them may itself read a column the part does not
    /// record either; the reader's own injection is recursive for the same reason.
    size_t walked = 0;
    /// `add_dependency` appends to `to_walk`, so the bound is re-read on every iteration.
    while (walked < to_walk.size())
    {
        const auto column_default = table_columns.getDefault(to_walk[walked++]);
        if (!column_default)
            continue;

        IdentifierNameSet identifiers;
        column_default->expression->collectIdentifierNames(identifiers);
        for (const auto & identifier : identifiers)
            add_dependency(identifier);
    }

    auto is_stale = [&](const Names & names)
    {
        for (const auto & required_column : names)
        {
            /// `required_columns` keeps a subcolumn unless the projection also OUTPUTS it, so
            /// `toInt64(t.a)` leaves `t.a` here; the types below belong to the column it is stored in.
            auto column = table_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, required_column);
            if (!column)
                continue;
            const auto & storage_name = column->getNameInStorage();

            DataTypePtr part_type;
            /// The part's OWN list: IMergeTreeDataPart::tryGetColumn() answers from a storage-wide cache
            /// keyed on IDataType::equals(), which erases the attributes getName() below asks about.
            if (auto own = part_columns.tryGetByName(storage_name))
            {
                part_type = own->type;
            }
            else if (const auto * missing = part_serialization_infos.getMissingColumnInfo(storage_name);
                     missing && !missing->type_name.empty())
            {
                /// The part holds no values of the column: every one was the default of this recorded
                /// type, which is also the type a read reconstructs it from.
                part_type = DataTypeFactory::instance().tryGet(missing->type_name);
            }

            if (!part_type)
            {
                /// The parent part records no type, so a parent read SYNTHESISES the column from its
                /// DEFAULT as declared today, while what the projection part holds for it was FROZEN
                /// when the projection was written, from the DEFAULT as declared THEN. The type
                /// recorded on the projection side is what that frozen value - and the sort key and
                /// the row set computed from it - was computed under.
                if (auto stored = projection_part_columns.tryGetByName(storage_name))
                    part_type = stored->type;
                else if (const auto * missing = projection_part_serialization_infos.getMissingColumnInfo(storage_name);
                         missing && !missing->type_name.empty())
                    part_type = DataTypeFactory::instance().tryGet(missing->type_name);
            }

            if (!part_type)
            {
                /// Nothing on disk records the column. When this projection OUTPUTS it, its own part
                /// holds no value of it either, so both read paths synthesise it from the DEFAULT as
                /// declared today and there is nothing frozen to disagree with. Otherwise the
                /// projection stored a key, an order or a row set derived from a declaration no part
                /// recorded, which cannot be shown to match.
                if (sample_block.has(storage_name))
                    continue;
                return true;
            }

            const auto & table_type = table_columns.get(storage_name).type;
            /// equals() answers whether the on-disk representation is the same; getName() also carries
            /// the attributes it drops (a time zone, a custom name over a plain type), which change what
            /// an expression computes from unchanged bytes. MergeTreeIndices.cpp's hasSameMeaning() is
            /// the same test for skip indices and carries the full rationale.
            if (!part_type->equals(*table_type) || part_type->getName() != table_type->getName())
                return true;
        }
        return false;
    };

    return is_stale(columns_to_check);
}

Block ProjectionDescription::calculate(
    const Block & block, UInt64 starting_offset, ContextPtr context, const IColumnPermutation * perm_ptr) const
{
    if (index)
    {
        if (block.rows() > index->getMaxRows())
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Cannot calculate projection index with {} rows, which exceeds the limit ({}) "
                "for projection index '{}' (Type: {}).",
                block.rows(),
                index->getMaxRows(),
                name,
                index->getName());
        }
        return index->calculate(*this, block, starting_offset, context, perm_ptr);
    }

    return calculateByQuery(block, starting_offset, context, perm_ptr);
}

Block ProjectionDescription::calculateByQuery(
    const Block & block, UInt64 starting_offset, ContextPtr context, const IColumnPermutation * perm_ptr) const
{
    /// Nothing to project from an empty block. This can happen when TTL deletes all rows during merge.
    /// Aggregate projections with constant GROUP BY keys (e.g., GROUP BY 0.674) would produce 1 row
    /// from 0 input rows, violating the ProjectionDataSink row count invariant.
    if (block.rows() == 0)
        return sample_block.cloneEmpty();

    auto mut_context = Context::createCopy(context);
    /// We ignore aggregate_functions_null_for_empty cause it changes aggregate function types.
    /// Now, projections do not support in on SELECT, and (with this change) should ignore on INSERT as well.
    mut_context->setSetting("aggregate_functions_null_for_empty", Field(0));
    mut_context->setSetting("transform_null_in", Field(0));
    const bool positional_arguments_for_projections = context->getSettingsRef()[Setting::enable_positional_arguments_for_projections];

    /// Disable positional arguments. Positional references are unsafe/unsupported in this context (e.g., within
    /// internal queries like those used for Projection definitions), as they rely on a fixed column order and alias
    /// resolution that is neither guaranteed nor sensible here.
    ///
    /// Setting `enable_positional_arguments_for_projections` may enable positional arguments for projections.
    /// It is needed for compatibility with existing projections that use positional arguments to allow successful cluster upgrade.
    mut_context->setSetting("enable_positional_arguments", positional_arguments_for_projections);

    ASTPtr query_ast_copy = nullptr;

    /// Only keep required columns
    Block source_block;
    for (const auto & column : required_columns)
        source_block.insert(block.getByName(column));

    /// Respect the _row_exists column.
    if (block.has(RowExistsColumn::name))
    {
        query_ast_copy = query_ast->clone();
        auto * select_row_exists = query_ast_copy->as<ASTSelectQuery>();
        if (!select_row_exists)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get ASTSelectQuery when adding _row_exists = 1. It's a bug");

        auto row_exists_condition = makeASTOperator("equals", make_intrusive<ASTIdentifier>(RowExistsColumn::name), make_intrusive<ASTLiteral>(1));

        /// If the projection already has a WHERE clause (e.g., from a filtered projection),
        /// we must AND the _row_exists condition with the existing WHERE rather than replacing it.
        /// Otherwise the projection's own filter would be lost during mutations.
        auto existing_where = select_row_exists->where();
        if (existing_where)
        {
            auto combined_where = makeASTFunction("and", std::move(existing_where), std::move(row_exists_condition));
            select_row_exists->setExpression(ASTSelectQuery::Expression::WHERE, std::move(combined_where));
        }
        else
        {
            select_row_exists->setExpression(ASTSelectQuery::Expression::WHERE, std::move(row_exists_condition));
        }

        source_block.insert(block.getByName(RowExistsColumn::name));
    }

    /// Create "_part_offset" column when needed for projection with parent part offsets
    if (with_parent_part_offset)
    {
        chassert(sample_block.has("_parent_part_offset"));
        chassert(!source_block.has("_part_offset"));
        auto uint64 = std::make_shared<DataTypeUInt64>();
        auto column = uint64->createColumn();
        auto & offset = assert_cast<ColumnUInt64 &>(*column).getData();
        offset.resize_exact(block.rows());
        if (perm_ptr)
        {
            /// Insertion path
            chassert(starting_offset == 0);
            for (size_t i = 0; i < block.rows(); ++i)
                offset[(*perm_ptr)[i]] = i;
        }
        else
        {
            /// Rebuilding path
            iota(offset.data(), offset.size(), starting_offset);
        }

        source_block.insert({std::move(column), std::move(uint64), "_part_offset"});
    }

    auto builder = InterpreterSelectQuery(
                       query_ast_copy ? query_ast_copy : query_ast,
                       mut_context,
                       Pipe(std::make_shared<ProjectionDataSource>(std::make_shared<const Block>(std::move(source_block)))),
                       SelectQueryOptions{
                           type == ProjectionDescription::Type::Normal && !where_clause_ast ? QueryProcessingStage::FetchColumns
                                                                       : QueryProcessingStage::WithMergeableState}
                           .ignoreASTOptimizations()
                           .ignoreSettingConstraints())
                       .buildQueryPipeline();
    builder.resize(1);

    // Generate aggregated blocks with rows less or equal than the original block.
    // There should be only one output block after this transformation.
    auto sink = std::make_shared<ProjectionDataSink>(builder.getSharedHeader(), block.rows());
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    pipeline.complete(sink);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    /// Always return the proper header, even if nothing was accumulated, in case the caller needs to use it
    Block projection_block = sink->isAccumulatedSomething() ? sink->getPort().getHeader().cloneWithColumns(sink->detachAccumulatedColumns())
                                                            : sink->getPort().getHeader().cloneEmpty();
    /// Rename parent _part_offset to _parent_part_offset column
    if (with_parent_part_offset)
    {
        chassert(projection_block.has("_part_offset"));
        chassert(!projection_block.has("_parent_part_offset"));

        auto new_column = projection_block.getByName("_part_offset");
        new_column.name = "_parent_part_offset";
        projection_block.erase("_part_offset");
        projection_block.insert(std::move(new_column));
    }

    return projection_block;
}


String ProjectionsDescription::toString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & projection : projections)
        list.children.push_back(projection.definition_ast);

    return list.formatIgnoringRedundantParentheses();
}

ProjectionsDescription ProjectionsDescription::parse(
    const String & str,
    const ColumnsDescription & columns,
    const KeyDescription * parent_partition_key,
    const ContextPtr & query_context)
{
    ProjectionsDescription result;
    if (str.empty())
        return result;

    ParserProjectionDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (const auto & projection_ast : list->children)
    {
        auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, columns, parent_partition_key, query_context);
        result.add(std::move(projection));
    }

    return result;
}

bool ProjectionsDescription::has(const String & projection_name) const
{
    return map.contains(projection_name);
}

const ProjectionDescription & ProjectionsDescription::get(const String & projection_name) const
{
    auto it = map.find(projection_name);
    if (it == map.end())
    {
        throw Exception(
            ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE,
            "There is no projection {} in table{}",
            projection_name,
            getHintsMessage(projection_name));
    }

    return *(it->second);
}

void ProjectionsDescription::add(ProjectionDescription && projection, const String & after_projection, bool first, bool if_not_exists)
{
    if (has(projection.name))
    {
        if (if_not_exists)
            return;
        throw Exception(
            ErrorCodes::ILLEGAL_PROJECTION, "Cannot add projection {}: projection with this name already exists", projection.name);
    }

    for (const auto & definition_ast : unavailable)
    {
        if (definition_ast->as<const ASTProjectionDeclaration &>().name != projection.name)
            continue;
        if (if_not_exists)
            return;
        throw Exception(
            ErrorCodes::ILLEGAL_PROJECTION,
            "Cannot add projection {}: a projection with this name is declared but could not be analyzed when the table "
            "was loaded. Drop it first, or remove the cause recorded in the server log and restart the server",
            projection.name);
    }

    auto insert_it = projections.cend();

    if (first)
        insert_it = projections.cbegin();
    else if (!after_projection.empty())
    {
        auto it = std::find_if(
            projections.cbegin(),
            projections.cend(),
            [&after_projection](const auto & projection_) { return projection_.name == after_projection; });
        if (it != projections.cend())
            ++it;
        insert_it = it;
    }

    auto it = projections.insert(insert_it, std::move(projection));
    map[it->name] = it;
}

void ProjectionsDescription::remove(const String & projection_name, bool if_exists)
{
    auto it = map.find(projection_name);
    if (it == map.end())
    {
        for (auto unavailable_it = unavailable.begin(); unavailable_it != unavailable.end(); ++unavailable_it)
        {
            if ((*unavailable_it)->as<const ASTProjectionDeclaration &>().name != projection_name)
                continue;
            unavailable.erase(unavailable_it);
            return;
        }

        if (if_exists)
            return;

        throw Exception(
            ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE,
            "There is no projection {} in table{}",
            projection_name,
            getHintsMessage(projection_name));
    }

    projections.erase(it->second);
    map.erase(it);
}

void ProjectionsDescription::addUnavailable(ASTPtr definition_ast)
{
    unavailable.push_back(std::move(definition_ast));
}

Names ProjectionsDescription::getUnavailableNames() const
{
    Names names;
    names.reserve(unavailable.size());
    for (const auto & definition_ast : unavailable)
        names.push_back(definition_ast->as<const ASTProjectionDeclaration &>().name);
    return names;
}

void ProjectionsDescription::replace(ProjectionDescription && projection)
{
    auto it = map.find(projection.name);
    if (it == map.end())
        throw Exception(
            ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE,
            "There is no projection {} in table{}",
            projection.name,
            getHintsMessage(projection.name));

    *it->second = std::move(projection);
}

VectorWithMemoryTracking<String> ProjectionsDescription::getAllRegisteredNames() const
{
    VectorWithMemoryTracking<String> names;
    names.reserve(map.size());
    for (const auto & pair : map)
        names.push_back(pair.first);
    return names;
}

ExpressionActionsPtr
ProjectionsDescription::getSingleExpressionForProjections(const ColumnsDescription & columns, ContextPtr query_context) const
{
    ASTPtr combined_expr_list = make_intrusive<ASTExpressionList>();
    for (const auto & projection : projections)
        for (const auto & projection_expr : projection.query_ast->children)
            combined_expr_list->children.push_back(projection_expr->clone());

    auto syntax_result = TreeRewriter(query_context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, query_context).getActions(false);
}

}

#include <Interpreters/InterpreterHypotheticalObjectQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HypotheticalObjectStore.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTHypotheticalObjectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Core/Settings.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Disks/IDisk.h>

#include <fmt/ranges.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace Setting
{
    extern const SettingsBool allow_suspicious_indices;
}

namespace
{

/// Mirrors `ALTER TABLE ... ADD PROJECTION`: the descriptor is built and validated the same way,
/// so EXPLAIN WHATIF can never report a benefit for a projection the user could not materialize
BlockIO createHypotheticalProjection(
    const ASTHypotheticalObjectQuery & query,
    const MergeTreeData & merge_tree,
    const StorageMetadataPtr & metadata,
    const StorageID & table_id,
    HypotheticalObjectStore & store,
    const ContextPtr & context)
{
    const auto & projection_ast = query.projection_decl->as<ASTProjectionDeclaration &>();

    /// `IF NOT EXISTS` must short-circuit before building the descriptor, matching
    /// `ALTER TABLE ... ADD PROJECTION IF NOT EXISTS`
    if (query.if_not_exists)
    {
        for (const auto & existing : store.getProjectionsForTable(table_id))
            if (existing.name == projection_ast.name)
                return {};
        if (metadata->projections.has(projection_ast.name))
            return {};
    }

    /// `LoadingStrictnessLevel::CREATE` is what a real `ADD PROJECTION` passes, so an invalid
    /// definition is rejected here rather than silently accepted and skipped later
    auto projection_desc = ProjectionDescription::getProjectionFromAST(
        query.projection_decl, metadata->getColumns(), &metadata->partition_key, context, LoadingStrictnessLevel::CREATE);

    /// run the engine's own ADD PROJECTION validation rather than copying its checks, so a
    /// definition that could not be materialized is rejected here too
    checkHypotheticalProjectionIsAddable(merge_tree, metadata, query.projection_decl, query.if_not_exists, context);

    store.addProjection(table_id, projection_desc, query.if_not_exists);
    return {};
}

/// CREATE HYPOTHETICAL INDEX, mirroring `ALTER TABLE ... ADD INDEX`
BlockIO createHypotheticalIndex(
    const ASTHypotheticalObjectQuery & query,
    const MergeTreeData & merge_tree,
    const StorageMetadataPtr & metadata,
    const StorageID & table_id,
    HypotheticalObjectStore & store,
    const ContextPtr & context)
{
    const auto & index_ast = query.index_decl->as<ASTIndexDeclaration &>();

    /// `IF NOT EXISTS` must short-circuit before building/validating the descriptor,
    /// matching `ALTER TABLE ... ADD INDEX IF NOT EXISTS`. The name is taken if a
    /// hypothetical index already uses it or a real secondary index does
    if (query.if_not_exists)
    {
        for (const auto & existing : store.getForTable(table_id))
            if (existing.name == index_ast.name)
                return {};
        if (metadata->getSecondaryIndices().has(index_ast.name))
            return {};
    }

    auto index_desc = IndexDescription::getIndexFromAST(
        query.index_decl,
        metadata->getColumns(),
        /* is_implicitly_created = */ false,
        /* escape_filenames = */ true,
        context);

    /// Empirical estimation reads the index's columns, so require column-level
    /// SELECT — otherwise a user with table-level access could infer a restricted
    /// column's distribution from the reported skip ratio.
    if (index_desc.expression)
        context->checkAccess(AccessType::SELECT, table_id, index_desc.expression->getRequiredColumns());

    /// validate() must run before get(): index creators assume their arguments were already
    /// validated and read them unguarded (e.g. set/bloom_filter index.arguments->children[0]),
    /// so calling get() on an unvalidated user AST can dereference absent arguments.
    MergeTreeIndexFactory::instance().validate(index_desc, /* attach = */ false, *merge_tree.getSettings());

    /// fail closed, a newly registered index type is rejected until someone checks it
    static constexpr std::string_view supported_types[]
        {"bloom_filter", "minmax", "ngrambf_v1", "set", "sparse_grams", "tokenbf_v1"};
    if (!std::ranges::contains(supported_types, index_desc.type))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes of type '{}' are not supported. Supported types: {}",
            index_desc.type,
            fmt::join(supported_types, ", "));

    /// some argument checks live in the creator, not the validator (tokenizer bounds for
    /// sparse_grams), so construct once here instead of failing later inside EXPLAIN WHATIF
    MergeTreeIndexFactory::instance().get(metadata, index_desc, *merge_tree.getSettings());

    /// Mirror real ADD INDEX: the `auto_minmax_index_` prefix is reserved when
    /// implicit minmax indexes are enabled.
    const bool using_auto_minmax_index =
           metadata->add_minmax_index_for_numeric_columns
        || metadata->add_minmax_index_for_string_columns
        || metadata->add_minmax_index_for_temporal_columns
        || metadata->add_minmax_index_for_block_number_column
        || metadata->add_minmax_index_for_block_offset_column;
    if (using_auto_minmax_index && index_desc.name.starts_with(IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot add hypothetical index {} because it uses a reserved index name",
            index_desc.name);

    for (const auto & existing : metadata->getSecondaryIndices())
    {
        if (existing.name == index_desc.name)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Hypothetical index '{}' conflicts with an existing secondary index on {}.{}",
                index_desc.name,
                table_id.getDatabaseName(),
                table_id.getTableName());
    }

    if (!context->getSettingsRef()[Setting::allow_suspicious_indices])
    {
        ASTPtr index_expression = index_ast.getExpression();
        if (const auto * index_function = index_expression ? index_expression->as<ASTFunction>() : nullptr)
            checkSuspiciousIndices(index_function);
    }

    store.add(table_id, index_desc, query.if_not_exists);
    return {};
}

}

/// covers the merging-mode, UNIQUE KEY and projection-property checks (commit_order, block
/// number/offset columns) by driving the engine's own ADD PROJECTION validation
void checkHypotheticalProjectionIsAddable(
    const MergeTreeData & merge_tree,
    const StorageMetadataPtr & metadata,
    const ASTPtr & projection_decl,
    bool if_not_exists,
    const ContextPtr & context)
{
    auto command_ast = make_intrusive<ASTAlterCommand>();
    command_ast->type = ASTAlterCommand::ADD_PROJECTION;
    command_ast->set(command_ast->projection_decl, projection_decl->clone());
    command_ast->if_not_exists = if_not_exists;

    auto command = AlterCommand::parse(command_ast.get());
    if (!command)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD PROJECTION command was not parsed back into an alter command");

    AlterCommands commands;
    commands.push_back(std::move(*command));
    /// the eligibility check applies the commands, which requires prepare first
    commands.prepare(*metadata, (*merge_tree.getSettings())[MergeTreeSetting::share_nested_offsets]);
    merge_tree.checkAlterEligibility(commands, context);
}

BlockIO InterpreterHypotheticalObjectQuery::execute()
{
    const auto & query = query_ptr->as<ASTHypotheticalObjectQuery &>();
    auto context = getContext();

    const bool is_projection = query.object_kind == ASTHypotheticalObjectQuery::Projection;
    const char * object_kind_name = is_projection ? "projections" : "indexes";

    if (query.kind == ASTHypotheticalObjectQuery::DropAll)
    {
        if (is_projection)
            context->getHypotheticalObjectStore().clearProjections();
        else
            context->getHypotheticalObjectStore().clear();
        return {};
    }

    /// same privilege as the real ADD PROJECTION, before the table is resolved so nothing about it leaks;
    /// dropping needs it too, otherwise the drop alone would answer what the caller may not ask
    if (is_projection)
        context->checkAccess(
            AccessType::ALTER_ADD_PROJECTION, context->resolveDatabase(query.getDatabase()), query.getTable());

    auto table_id = context->resolveStorageID(StorageID(query.getDatabase(), query.getTable()));
    auto table = DatabaseCatalog::instance().getTable(table_id, context);

    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(table.get());
    if (!merge_tree)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical {} are only supported for MergeTree family tables, got {}",
            object_kind_name,
            table->getName());

    /// The store keys entries by UUID; without one (Ordinary databases) a stored
    /// object would never match later lookups, so reject it up front
    if (table_id.uuid == UUIDHelpers::Nil)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical {} require a table with a UUID (Atomic database); {}.{} has none",
            object_kind_name,
            table_id.getDatabaseName(),
            table_id.getTableName());

    auto & store = context->getHypotheticalObjectStore();

    if (query.kind == ASTHypotheticalObjectQuery::Drop)
    {
        auto object_name = query.object_name->as<ASTIdentifier &>().name();
        if (is_projection)
            store.removeProjection(table_id, object_name, query.if_exists);
        else
            store.remove(table_id, object_name, query.if_exists);
        return {};
    }

    auto metadata = table->getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ false);

    /// Old-syntax MergeTree rejects `ALTER TABLE ... ADD INDEX` / `ADD PROJECTION`, so reject it here too.
    if (!merge_tree->is_custom_partitioned)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hypothetical {} are not supported for tables with the old MergeTree syntax",
            object_kind_name);

    /// Mirror `checkAlterIsPossible`: adding one is rejected on immutable (no-hard-link) disks,
    /// so accepting it here would report a benefit the user could never materialize
    for (const auto & disk : merge_tree->getDisks())
        if (!disk->supportsHardLinks())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Hypothetical {} are not supported on immutable disk '{}'",
                object_kind_name,
                disk->getName());

    if (is_projection)
        return createHypotheticalProjection(query, *merge_tree, metadata, table_id, store, context);
    return createHypotheticalIndex(query, *merge_tree, metadata, table_id, store, context);
}

void registerInterpreterHypotheticalObjectQuery(InterpreterFactory & factory);

void registerInterpreterHypotheticalObjectQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterHypotheticalObjectQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterHypotheticalObjectQuery", create_fn);
}

}

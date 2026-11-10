#include <Interpreters/WhatIf/InterpreterHypotheticalObjectQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/WhatIf/HypotheticalObjectStore.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/WhatIf/ASTHypotheticalObjectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageProxy.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <fmt/ranges.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace
{

ASTPtr expandUserDefinedSQLFunctions(const ASTPtr & declaration, const ContextPtr & context)
{
    ASTPtr expanded = declaration->clone();
    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(expanded, context);
    return expanded;
}

/// the real `ALTER TABLE ... ADD INDEX` or `ADD PROJECTION` validation, so `CREATE` cannot accept what `ALTER` rejects
void checkHypotheticalObjectIsAddable(
    const MergeTreeData & merge_tree,
    const StorageMetadataPtr & metadata,
    const ASTPtr & declaration,
    const ContextPtr & context)
{
    auto command_ast = make_intrusive<ASTAlterCommand>();
    if (declaration->as<ASTIndexDeclaration>())
    {
        command_ast->type = ASTAlterCommand::ADD_INDEX;
        command_ast->set(command_ast->index_decl, declaration->clone());
    }
    else
    {
        command_ast->type = ASTAlterCommand::ADD_PROJECTION;
        command_ast->set(command_ast->projection_decl, declaration->clone());
    }

    auto command = AlterCommand::parse(command_ast.get());
    if (!command)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD command was not parsed back into an alter command");

    AlterCommands commands;
    commands.push_back(std::move(*command));
    /// the eligibility check applies the commands, which requires prepare first
    commands.prepare(*metadata, (*merge_tree.getSettings())[MergeTreeSetting::share_nested_offsets]);
    merge_tree.checkAlterEligibility(commands, context);
}

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
            if (existing.name() == projection_ast.name)
                return {};
        if (metadata->projections.has(projection_ast.name))
            return {};
    }

    /// SQL UDFs are expanded first, as for `ALTER TABLE ... ADD PROJECTION`, so the binding sees the columns they read
    ASTPtr expanded_projection_decl = expandUserDefinedSQLFunctions(query.projection_decl, context);

    /// `LoadingStrictnessLevel::CREATE` is what a real `ADD PROJECTION` passes, so an invalid
    /// definition is rejected here rather than silently accepted and skipped later
    auto projection_desc = ProjectionDescription::getProjectionFromAST(
        expanded_projection_decl, metadata->getColumns(), &metadata->partition_key, context, LoadingStrictnessLevel::CREATE);

    checkHypotheticalObjectIsAddable(merge_tree, metadata, expanded_projection_decl, context);

    store.addProjection(table_id, projection_desc, HypotheticalBinding::of(projection_desc, *metadata), query.if_not_exists);
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
            if (existing.name() == index_ast.name)
                return {};
        if (metadata->getSecondaryIndices().has(index_ast.name))
            return {};
    }

    /// SQL UDFs are expanded first, as for `ALTER TABLE ... ADD INDEX`, so the binding sees the columns they read
    ASTPtr expanded_index_decl = expandUserDefinedSQLFunctions(query.index_decl, context);

    auto index_desc = IndexDescription::getIndexFromAST(
        expanded_index_decl,
        metadata->getColumns(),
        /* is_implicitly_created = */ false,
        /* escape_filenames = */ true,
        context);

    /// Empirical estimation reads the index's columns, so require column-level
    /// SELECT — otherwise a user with table-level access could infer a restricted
    /// column's distribution from the reported skip ratio.
    if (index_desc.expression)
        context->checkAccess(
            AccessType::SELECT,
            table_id,
            metadata->getColumns().getColumnNamesForSelectAccessCheck(index_desc.expression->getRequiredColumns(), context, table_id));

    /// The alias checks also read ALIAS column definitions that constant folding removed from `index_desc.expression`.
    const auto & columns = metadata->getColumns();
    NameSet traversed_columns;
    Names referenced_columns;
    auto collect_columns = [&](this auto && self, const ASTPtr & ast) -> void
    {
        RequiredSourceColumnsVisitor::Data data;
        RequiredSourceColumnsVisitor(data).visit(ast);
        for (const auto & name : data.requiredColumns())
        {
            /// A name that is neither a column nor a subcolumn is an alias in the expression.
            const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
            if (!column)
                continue;
            /// Keep the requested (sub)column name: `getColumnNamesForSelectAccessCheck` below maps it to its storage
            /// column unless a grant names the subcolumn itself.
            referenced_columns.push_back(name);
            const String storage_name = column->getNameInStorage();
            if (traversed_columns.insert(storage_name).second && columns.hasAlias(storage_name))
                if (auto column_default = columns.getDefault(storage_name))
                    self(column_default->expression);
        }
    };
    collect_columns(expanded_index_decl->as<ASTIndexDeclaration &>().getExpression());
    context->checkAccess(
        AccessType::SELECT, table_id, columns.getColumnNamesForSelectAccessCheck(referenced_columns, context, table_id));

    /// validates the arguments, which get() below reads unguarded
    checkHypotheticalObjectIsAddable(merge_tree, metadata, expanded_index_decl, context);

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

    store.add(table_id, index_desc, HypotheticalBinding::of(index_desc, *metadata), query.if_not_exists);
    return {};
}

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
    /// Indexes need `SELECT` on their columns, which is checked once the index is built. Before that, the table lookup
    /// and the checks of its engine would reveal the existence and the engine of a table the user cannot see, both
    /// when creating and when dropping, so require the table to be visible first (any grant on it implies this).
    else
    {
        context->checkAccess(
            AccessType::SHOW_TABLES, context->resolveDatabase(query.getDatabase()), query.getTable());
        /// the `ADD INDEX` checks validate the whole table, so their errors can name any column
        if (query.kind == ASTHypotheticalObjectQuery::Create)
            context->checkAccess(AccessType::SHOW_COLUMNS, context->resolveDatabase(query.getDatabase()), query.getTable());
    }

    auto table_id = context->resolveStorageID(StorageID(query.getDatabase(), query.getTable()));
    auto table = DatabaseCatalog::instance().getTable(table_id, context);
    auto & store = context->getHypotheticalObjectStore();

    /// Dropping only forgets a session entry, so it must not load a lazily loaded table.
    if (query.kind == ASTHypotheticalObjectQuery::Drop)
    {
        auto object_name = query.object_name->as<ASTIdentifier &>().name();
        if (is_projection)
            store.removeProjection(table_id, object_name, query.if_exists);
        else
            store.remove(table_id, object_name, query.if_exists);
        return {};
    }

    table = resolveStorageProxyLoading(table);
    const auto * merge_tree = castStorage<MergeTreeData>(table, DeferredTable::Load).get();
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

    auto metadata = table->getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ false);

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

#include <Interpreters/ProjectionMetadataValidation.h>

#include <Access/ContextAccess.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DDLTask.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/IStorage.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <unordered_set>

#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_projection_column_list_in_replicated_metadata;
    extern const SettingsUInt64 distributed_ddl_entry_format_version;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

ProjectionDefinitionSource getProjectionDefinitionSource(
    LoadingStrictnessLevel mode, bool attach_short_syntax, bool is_restore_from_backup)
{
    if (is_restore_from_backup)
        return ProjectionDefinitionSource::Backup;
    return isFreshTableDefinition(mode, attach_short_syntax)
        ? ProjectionDefinitionSource::NewQuery
        : ProjectionDefinitionSource::PreviouslyAccepted;
}

bool isSecondaryProjectionMetadataReplay(const ContextPtr & context)
{
    if (const auto metadata_txn = context->getZooKeeperMetadataTransaction();
        metadata_txn && !metadata_txn->isInitialQuery())
        return true;
#if CLICKHOUSE_CLOUD
    if (context->getClientInfo().is_shared_catalog_internal && !SharedDatabaseCatalog::isInitialQuery(context))
        return true;
#endif
    return false;
}

bool isInitialProjectionMetadataQuery(const ContextPtr & context)
{
    return !context->isRecoveryFromStoredMetadata()
        && !context->isDDLOrOnClusterInternal()
        && !context->getClientInfo().is_replicated_database_internal
        && !isSecondaryProjectionMetadataReplay(context);
}

bool shouldValidateProjectionCodecsOnCreate(
    const ContextPtr & context,
    LoadingStrictnessLevel mode,
    bool attach_short_syntax,
    bool is_restore_from_backup)
{
    if (getProjectionDefinitionSource(mode, attach_short_syntax, is_restore_from_backup)
            == ProjectionDefinitionSource::PreviouslyAccepted
        || context->isRecoveryFromStoredMetadata()
        || context->getClientInfo().is_replicated_database_internal
        || isSecondaryProjectionMetadataReplay(context))
        return false;

    /// Format 3+ is validated before enqueue; format 2 carries the initiating settings to its worker.
    if (!is_restore_from_backup && context->isDDLOrOnClusterInternal()
        && context->getSettingsRef()[Setting::distributed_ddl_entry_format_version].value
            >= DDLLogEntry::NORMALIZE_CREATE_ON_INITIATOR_VERSION)
        return false;

    return true;
}

bool shouldValidateProjectionCodecsOnAlter(const ContextPtr & context)
{
    /// Distributed format 2 workers validate with the initiating settings. `Replicated` database
    /// and Shared Catalog replay must not apply their own session's codec policy a second time.
    return !isSecondaryProjectionMetadataReplay(context);
}

namespace
{

bool isReplicated(const ASTStorage & storage)
{
    if (!storage.engine)
        return false;
    const auto & storage_name = storage.engine->name;
    return storage_name.starts_with("Replicated") || storage_name.starts_with("Shared");
}

}

bool isProjectionStorageReplicated(const ASTCreateQuery & create)
{
    if (create.storage && isReplicated(*create.storage))
        return true;

    if (create.targets)
    {
        for (const auto & inner_table_engine : create.targets->getInnerEngines())
        {
            if (isReplicated(*inner_table_engine))
                return true;
        }
    }

    return false;
}

void validateProjectionMetadataAdmission(
    const ASTCreateQuery & create,
    const ContextPtr & context,
    const std::shared_ptr<IDatabase> & database,
    ProjectionDefinitionSource source,
    bool copies_source_projections,
    const ProjectionsDescription * copied_projections)
{
    /// Workers replay the DDL entry with their own settings when the old entry format is used.
    /// The initiator must check the syntax before enqueueing it, not the worker during replay.
    if (source == ProjectionDefinitionSource::PreviouslyAccepted || !isInitialProjectionMetadataQuery(context))
        return;

    const bool reject_column_list = !context->getSettingsRef()[Setting::allow_projection_column_list_in_replicated_metadata]
        && (isProjectionStorageReplicated(create) || !create.cluster.empty()
            || (database && (database->getEngineName() == "Replicated" || database->getEngineName() == "Shared")));
    const bool reject_old_format_codec = !create.cluster.empty()
        && context->getSettingsRef()[Setting::distributed_ddl_entry_format_version].value == DDLLogEntry::OLDEST_VERSION
        && !copies_source_projections;
    /// An old-format worker reads the source table itself. Its unavailable projections cannot
    /// be copied into a distributed `CREATE`, even though no codec setting needs to be forwarded.
    const bool reject_unavailable_copy = copies_source_projections && !create.cluster.empty() && !copied_projections;
    if (!reject_column_list && !reject_old_format_codec && !reject_unavailable_copy)
        return;

    bool has_projection_column_list = false;
    bool has_projection_column_codec = false;
    bool has_unavailable_source_projection = false;
    if (create.columns_list && create.columns_list->projections)
    {
        for (const auto & projection_ast : create.columns_list->projections->children)
        {
            if (const auto * declaration = projection_ast ? projection_ast->as<ASTProjectionDeclaration>() : nullptr;
                declaration && declaration->columns)
            {
                has_projection_column_list = true;
                has_projection_column_codec |= hasDeclaredProjectionColumnCodec(*declaration);
            }
        }
    }
    else if (!create.columns_list && !create.as_table.empty() && !create.isView() && !create.is_dictionary
        && (!create.storage || !create.storage->engine || endsWith(create.storage->engine->name, "MergeTree")))
    {
        /// Old `ON CLUSTER` formats expand `AS source_table` on each worker. If the initiator
        /// cannot inspect a source that may supply projections, reject the copy unless the
        /// initiator explicitly opted in to projection column lists.
        bool source_projection_safety_known = false;
        const String source_database = context->resolveDatabase(create.as_database);
        if (context->getAccess()->isGranted(AccessType::SHOW_COLUMNS, source_database, create.as_table))
        {
            const auto source_table = DatabaseCatalog::instance().tryGetTable({source_database, create.as_table}, context);
            const auto * alias = source_table ? source_table->as<StorageAlias>() : nullptr;
            if (source_table && (!alias || alias->isTargetTableGranted(context, AccessType::SHOW_COLUMNS, {})))
            {
                if ((create.storage && create.storage->engine) || endsWith(source_table->getName(), "MergeTree"))
                {
                    /// Without an explicit engine, the destination inherits the source's engine.
                    /// Only a `MergeTree` destination copies projections.
                    const auto source_metadata = source_table->getInMemoryMetadataPtr(context, false);
                    has_unavailable_source_projection = source_metadata->getProjections().hasUnavailable();
                    auto inspect_projection = [&](const ASTPtr & definition)
                    {
                        if (const auto * declaration = definition ? definition->as<const ASTProjectionDeclaration>() : nullptr;
                            declaration && declaration->columns)
                        {
                            has_projection_column_list = true;
                            has_projection_column_codec |= hasDeclaredProjectionColumnCodec(*declaration);
                        }
                    };

                    for (const auto & projection : source_metadata->getProjections())
                        inspect_projection(projection.definition_ast);
                    /// `ProjectionsDescription::clone` copies unavailable declarations too.
                    for (const auto & definition : source_metadata->getProjections().getUnavailableDefinitions())
                        inspect_projection(definition);
                    source_projection_safety_known = true;
                }
                else if (!alias)
                    source_projection_safety_known = true;
            }
        }
        if ((reject_column_list || reject_unavailable_copy) && !source_projection_safety_known)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Cannot verify projection metadata of the source table for ON CLUSTER AS. "
                "Make the source visible to the initiator with SHOW COLUMNS access before copying it");
    }
    /// `CREATE AS` has already normalized its query at this call site. Unavailable source declarations
    /// remain in the copied properties, but have not yet been appended to the query for persistence.
    if (copied_projections)
    {
        for (const auto & definition : copied_projections->getUnavailableDefinitions())
        {
            if (const auto * declaration = definition->as<const ASTProjectionDeclaration>(); declaration && declaration->columns)
            {
                has_projection_column_list = true;
                has_projection_column_codec |= hasDeclaredProjectionColumnCodec(*declaration);
            }
        }
    }

    if (reject_column_list && has_projection_column_list)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Projection column lists in replicated metadata require setting "
            "allow_projection_column_list_in_replicated_metadata = 1. "
            "Upgrade every replica before enabling it");

    if (reject_unavailable_copy && has_unavailable_source_projection)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot copy unavailable projection declarations into replicated or distributed CREATE metadata. "
            "Restore projection analysis on the source table or drop the unavailable declaration before copying it");

    if (reject_old_format_codec && has_projection_column_codec)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Projection column CODEC declarations in ON CLUSTER DDL require "
            "distributed_ddl_entry_format_version >= 2, because version 1 does not carry codec validation settings");
}

void validateProjectionMetadataAdmission(
    const ASTAlterQuery & alter, const StoragePtr & table, const std::shared_ptr<IDatabase> & database, const ContextPtr & context)
{
    if (isInitialProjectionMetadataQuery(context) && table)
    {
        const auto metadata = table->getInMemoryMetadataPtr(context, false);
        std::unordered_set<String> unavailable_names;
        for (const auto & name : metadata->projections.getUnavailableNames())
            unavailable_names.insert(name);
        for (const auto & child : alter.command_list->children)
        {
            const auto & command = child->as<const ASTAlterCommand &>();
            if (command.type == ASTAlterCommand::DROP_PROJECTION && command.projection
                && !command.partition && !command.clear_projection)
            {
                unavailable_names.erase(command.projection->as<const ASTIdentifier &>().name());
                continue;
            }
            if (command.type != ASTAlterCommand::MODIFY_PROJECTION || !command.projection_decl)
                continue;
            const auto & declaration = command.projection_decl->as<const ASTProjectionDeclaration &>();
            if (unavailable_names.contains(declaration.name))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot modify unavailable projection {}: restore its analysis or drop it before changing its settings",
                    backQuote(declaration.name));
        }
    }

    if (context->getSettingsRef()[Setting::allow_projection_column_list_in_replicated_metadata]
        || !isInitialProjectionMetadataQuery(context))
        return;

    /// Reject before an `ALTER` with new syntax enters a replicated or distributed DDL log.
    if (alter.cluster.empty() && !(table && table->supportsReplication())
        && !(database && (database->getEngineName() == "Replicated" || database->getEngineName() == "Shared")))
        return;

    for (const auto & child : alter.command_list->children)
    {
        const auto & command = child->as<const ASTAlterCommand &>();
        /// Even an `ADD IF NOT EXISTS` that would be a no-op must be screened: the SQL text
        /// itself is persisted in a replicated/distributed DDL log and older servers cannot parse it.
        if (command.type == ASTAlterCommand::ADD_PROJECTION || command.type == ASTAlterCommand::MODIFY_PROJECTION)
        {
            const auto & declaration = command.projection_decl->as<const ASTProjectionDeclaration &>();
            if (declaration.columns)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "Projection column lists in replicated metadata require setting "
                    "allow_projection_column_list_in_replicated_metadata = 1. "
                    "Upgrade every replica before enabling it");
        }
    }
}

void validateProjectionCodecOldDistributedDDLAdmission(
    const ASTAlterQuery & alter, const ContextPtr & context)
{
    /// Format 1 stores no query settings. Codec validation on the worker would therefore use its
    /// own defaults, even when the initiator explicitly allowed a suspicious or gated codec.
    if (context->getSettingsRef()[Setting::distributed_ddl_entry_format_version].value != DDLLogEntry::OLDEST_VERSION)
        return;

    for (const auto & child : alter.command_list->children)
    {
        const auto & command = child->as<const ASTAlterCommand &>();
        /// `MODIFY PROJECTION` can only change `WITH SETTINGS`. Its codec declaration restates
        /// already accepted metadata and is not validated with the worker's settings.
        if (command.type == ASTAlterCommand::ADD_PROJECTION && command.projection_decl
            && hasDeclaredProjectionColumnCodec(command.projection_decl->as<const ASTProjectionDeclaration &>()))
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Projection column CODEC declarations in ON CLUSTER DDL require "
                "distributed_ddl_entry_format_version >= 2, because version 1 does not carry codec validation settings");
    }
}

}

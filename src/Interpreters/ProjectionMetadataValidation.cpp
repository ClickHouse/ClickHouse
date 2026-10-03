#include <Interpreters/ProjectionMetadataValidation.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/IStorage.h>
#include <Storages/ProjectionsDescription.h>
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
    const ProjectionsDescription * copied_projections,
    ProjectionMetadataPublication publication)
{
    /// Old `ON CLUSTER` entries expand `AS source_table` on each worker. Check the copied
    /// projections there too, since the source may not exist on the initiator.
    const bool validate_distributed_source_copy = source == ProjectionDefinitionSource::NewQuery
        && copies_source_projections && copied_projections && context->isDDLOrOnClusterInternal()
        && !context->getClientInfo().is_replicated_database_internal && !isSecondaryProjectionMetadataReplay(context);
    if ((source == ProjectionDefinitionSource::PreviouslyAccepted && publication == ProjectionMetadataPublication::No)
        || (!isInitialProjectionMetadataQuery(context) && !validate_distributed_source_copy))
        return;

    const bool is_distributed = !create.cluster.empty() || validate_distributed_source_copy;
    const bool reject_column_list = !context->getSettingsRef()[Setting::allow_projection_column_list_in_replicated_metadata]
        && (publication == ProjectionMetadataPublication::ReplicatedStorage
            || isProjectionStorageReplicated(create) || is_distributed
            || (database && (database->getEngineName() == "Replicated" || database->getEngineName() == "Shared")));
    const bool reject_old_format_codec = is_distributed
        && context->getSettingsRef()[Setting::distributed_ddl_entry_format_version].value == DDLLogEntry::OLDEST_VERSION
        && !copies_source_projections;
    /// For old `ON CLUSTER` entries, only workers know which source metadata is copied.
    const bool reject_unavailable_copy = validate_distributed_source_copy;
    if (!reject_column_list && !reject_old_format_codec && !reject_unavailable_copy)
        return;

    bool has_projection_column_list = false;
    bool has_projection_column_codec = false;
    const bool has_unavailable_source_projection = copied_projections && copied_projections->hasUnavailable();
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
    if (isInitialProjectionMetadataQuery(context))
    {
        /// `MODIFY PROJECTION` needs an existing definition to prove that only settings change.
        /// Without a local table, an ON CLUSTER initiator cannot perform that check before
        /// publishing the DDL; some workers could apply it while others reject it.
        if (!table && !alter.cluster.empty())
            for (const auto & child : alter.command_list->children)
                if (child->as<const ASTAlterCommand &>().type == ASTAlterCommand::MODIFY_PROJECTION)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Table {}.{} does not exist on this host. ALTER TABLE ... ON CLUSTER ... MODIFY PROJECTION "
                        "must be initiated from a host that has the table",
                        backQuoteIfNeed(alter.getDatabase()), backQuoteIfNeed(alter.getTable()));

        if (table)
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

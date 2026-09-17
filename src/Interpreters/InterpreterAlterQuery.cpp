#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/UDTAccessTarget.h>
#include <Access/UDTUsageAccess.h>
#include <Analyzer/UDT/SelectedOutputTypeBindings.h>
#include <Backups/BackupsWorker.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Databases/UDT/AuthorityStorageOperationGate.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/replaceLegacyToTime.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryMetadataCache.h>
#include <Interpreters/UDT/StoredObjectTypeBindingPreparation.h>
#include <Interpreters/UDT/StoredObjectTypeSupport.h>
#include <Interpreters/UDT/UDTExecutionBoundary.h>
#include <Interpreters/UDTScalarAliasColumnBinder.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExecuteCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageKeeperMap.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <limits>
#include <unordered_map>
#include <vector>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <base/scope_guard.h>

#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsAlterUpdateMode alter_update_mode;
    extern const SettingsBool enable_lightweight_update;
    extern const SettingsBool validate_mutation_query;
    extern const SettingsTimezone session_timezone;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsBool use_legacy_to_time;
    extern const SettingsBool allow_experimental_user_defined_types;
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ServerSetting
{
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_IS_PERMANENTLY_READ_ONLY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int QUERY_IS_PROHIBITED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNKNOWN_TYPE;
    extern const int ABORTED;
}

namespace
{

void normalizeLegacyToTimeInAlterMetadataDefinitions(ASTAlterQuery & alter)
{
    for (const auto & child : alter.command_list->children)
    {
        auto * command = child->as<ASTAlterCommand>();

        /// Every slot that reaches table metadata, so that a reload re-derives the same spelling the
        /// statement resolved. Mutation expressions (`predicate`, `update_assignments`, the
        /// `IN PARTITION` value in `partition`) need it too: they are persisted in mutation entries
        /// and resolved by the background executor and by replicas with the server default settings,
        /// not with the settings of this session.
        for (IAST * payload : {command->col_decl,
                               command->order_by,
                               command->sample_by,
                               command->index_decl,
                               command->constraint_decl,
                               command->projection_decl,
                               command->ttl,
                               command->select,
                               command->predicate,
                               command->update_assignments,
                               command->partition})
        {
            if (payload)
                replaceLegacyToTime(*payload);
        }
    }
}

using CommandSegment = std::variant<AlterCommands, MutationCommands, PartitionCommands, ExecuteCommands>;
using CommandSegments = std::vector<CommandSegment>;
using PreparedUDTAlterColumns
    = std::unordered_map<const ASTAlterCommand *, UDT::PersistedTypeReferences>;

struct SegmentsHolder
{
    CommandSegments segments;

    template <class SegmentType>
    SegmentType & take()
    {
        if (segments.empty() || !std::holds_alternative<SegmentType>(segments.back()))
            segments.emplace_back(SegmentType{});

        return std::get<SegmentType>(segments.back());
    }
};

template <class CommandsType>
bool hasCommands(const CommandSegments & segments)
{
    return std::ranges::any_of(segments, [](const auto & segment) { return std::holds_alternative<CommandsType>(segment); });
}

CommandSegments parseAlterCommandSegments(
    const ASTAlterQuery & alter,
    const StoragePtr & table,
    const ContextPtr & context,
    const PreparedUDTAlterColumns & udt_columns)
{
    SegmentsHolder segments_holder;
    const auto & settings = context->getSettingsRef();

    for (const auto & child : alter.command_list->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        if (command_ast->type == ASTAlterCommand::EXECUTE_COMMAND)
        {
            segments_holder.take<ExecuteCommands>().push_back(command_ast);
        }
        else if (auto alter_command = AlterCommand::parse(command_ast))
        {
            if (const auto it = udt_columns.find(command_ast); it != udt_columns.end())
                alter_command->udt_column_references = it->second;
            segments_holder.take<AlterCommands>().push_back(std::move(alter_command.value()));
        }
        else if (auto partition_command = PartitionCommand::parse(command_ast))
        {
            segments_holder.take<PartitionCommands>().push_back(std::move(partition_command.value()));
        }
        else if (auto mutation_command = MutationCommand::parse(
                     *command_ast,
                     /* parse_alter_commands = */ false,
                     /* with_pure_metadata_commands = */ false,
                     settings[Setting::max_parser_depth],
                     settings[Setting::max_parser_backtracks]))
        {
            if (mutation_command->type == MutationCommand::UPDATE || mutation_command->type == MutationCommand::DELETE)
            {
                /// TODO: add a check for result query size.
                auto rewritten_command_ast = replaceNonDeterministicToScalars(*command_ast, context);
                if (rewritten_command_ast)
                {
                    auto * new_alter_command = rewritten_command_ast->as<ASTAlterCommand>();
                    mutation_command = MutationCommand::parse(
                        *new_alter_command,
                        /* parse_alter_commands = */ false,
                        /* with_pure_metadata_commands = */ false,
                        settings[Setting::max_parser_depth],
                        settings[Setting::max_parser_backtracks]);
                    if (!mutation_command)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Alter command '{}' is rewritten to invalid command '{}'",
                            command_ast->formatForErrorMessage(), rewritten_command_ast->formatForErrorMessage());
                }
            }

            /// When session_timezone is set, string literals compared to DateTime columns
            /// must be wrapped with explicit timezone to avoid misinterpretation in the
            /// background mutation thread which lacks the session context.
            const auto & session_tz = settings[Setting::session_timezone].value;
            if (!session_tz.empty())
            {
                auto source_alter = mutation_command->ast();
                auto metadata_snapshot = table->getInMemoryMetadataPtr(context, true);
                auto tz_rewritten_ast = rewriteDateTimeLiteralsWithTimezone(
                    *source_alter, metadata_snapshot->columns, session_tz);
                if (tz_rewritten_ast)
                {
                    auto * tz_alter_command = tz_rewritten_ast->as<ASTAlterCommand>();
                    mutation_command = MutationCommand::parse(
                        *tz_alter_command,
                        /* parse_alter_commands = */ false,
                        /* with_pure_metadata_commands = */ false,
                        settings[Setting::max_parser_depth],
                        settings[Setting::max_parser_backtracks]);
                    if (!mutation_command)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Alter command '{}' is rewritten to invalid command '{}'",
                            source_alter->formatForErrorMessage(), tz_rewritten_ast->formatForErrorMessage());
                }
            }

            segments_holder.take<MutationCommands>().push_back(std::move(mutation_command.value()));
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong parameter type in ALTER query");
    }

    return std::move(segments_holder.segments);
}

[[noreturn]] void rethrowUDTAlterBinderError(
    const UDT::ScalarAliasColumnBinderError & error)
{
    using Code = UDT::ScalarAliasColumnBinderError::Code;
    const int exception_code = [&]
    {
        switch (error.code)
        {
            case Code::UnknownDefinition: return ErrorCodes::UNKNOWN_TYPE;
            case Code::InvalidInput:
            case Code::CrossDatabaseReference: return ErrorCodes::BAD_ARGUMENTS;
            case Code::UnsupportedColumnShape:
            case Code::ParameterizedDefinition: return ErrorCodes::NOT_IMPLEMENTED;
            case Code::AuthorityMismatch:
            case Code::QueryChanged:
            case Code::NormalizedSchemaMismatch:
            case Code::InvalidState: return ErrorCodes::LOGICAL_ERROR;
        }
        return ErrorCodes::LOGICAL_ERROR;
    }();
    throw Exception(exception_code, "{}", error.what());
}

PreparedUDTAlterColumns prepareUDTAlterColumns(
    ASTAlterQuery & alter,
    const StorageID & table_id,
    DatabaseAtomic & database,
    const ContextPtr & context)
{
    PreparedUDTAlterColumns result;
    const UDT::SchemaObjectID table_object{
        .kind = UDT::SchemaObjectKind::Table,
        .database_uuid = database.getUUID(),
        .object_uuid = table_id.uuid,
    };
    std::vector<ASTAlterCommand *> commands;
    std::vector<ASTColumnDeclaration *> declarations;
    for (const auto & child : alter.command_list->children)
    {
        auto * command = child->as<ASTAlterCommand>();
        if ((command->type != ASTAlterCommand::ADD_COLUMN && command->type != ASTAlterCommand::MODIFY_COLUMN)
            || !command->col_decl)
            continue;
        auto & declaration = command->col_decl->as<ASTColumnDeclaration &>();
        if (!UDT::hasReferencesInAlterColumn(declaration))
            continue;
        commands.push_back(command);
        declarations.push_back(&declaration);
    }

    try
    {
        auto prepared = UDT::prepareScalarAliasAlterColumns(
            declarations, table_id.database_name, context, database.getUDTAuthorityAdapter());
        if (!prepared)
            return result;
        if (table_id.uuid == UUIDHelpers::Nil || database.getUUID() == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT ALTER table identity is incomplete");
        prepared->applyPhysicalTypeASTs();
        auto fragments = std::move(*prepared).finishIndividualColumns(table_object, 1);
        if (fragments.size() != commands.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT ALTER binder produced an unaligned fragment batch");
        for (size_t index = 0; index < fragments.size(); ++index)
        {
            if (!fragments[index])
                continue;
            if (fragments[index]->uses.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT ALTER binder produced an empty logical fragment");
            result.emplace(commands[index], std::move(*fragments[index]));
        }
    }
    catch (const UDT::ScalarAliasColumnBinderError & error)
    {
        rethrowUDTAlterBinderError(error);
    }
    return result;
}

[[noreturn]] void rethrowStoredObjectAlterBindingError(const UDT::StoredObjectTypeBindingPreparationError & error)
{
    using Code = UDT::StoredObjectTypeBindingPreparationError::Code;
    const int exception_code = [&]
    {
        switch (error.code)
        {
            case Code::InvalidDeclaration:
            case Code::InvalidObject:
            case Code::CrossDatabaseReference:
            case Code::LimitExceeded: return ErrorCodes::BAD_ARGUMENTS;
            case Code::SourceSidecarMismatch:
            case Code::QueryChanged:
            case Code::NormalizedSchemaMismatch: return ErrorCodes::ABORTED;
            case Code::InvalidDecision:
            case Code::MissingLogicalBinding:
            case Code::InvalidState: return ErrorCodes::LOGICAL_ERROR;
        }
        return ErrorCodes::LOGICAL_ERROR;
    }();
    throw Exception(exception_code, "{}", error.what());
}

void prepareMappedStoredObjectModifyQuery(
    AlterCommands & commands,
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context,
    bool require_boundary_handoff_target)
{
    if (!storage || !metadata || !context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped stored-object ALTER preparation has an incomplete input");
    metadata->validateBoundUDTReferences();
    const auto & bound = metadata->getBoundUDTReferences();
    if (!bound || bound->getObject().kind != UDT::SchemaObjectKind::View)
    {
        if (require_boundary_handoff_target)
        {
            throw Exception(
                ErrorCodes::ABORTED, "The mapped MaterializedView authorized by the DDL boundary changed before MODIFY QUERY preparation");
        }
        return;
    }

    AlterCommand * modify_query = nullptr;
    for (auto & command : commands)
    {
        if (command.ignore || command.type != AlterCommand::MODIFY_QUERY)
            continue;
        if (modify_query)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mapped MaterializedView ALTER supports one MODIFY QUERY command at a time");
        modify_query = &command;
    }
    if (!modify_query)
    {
        if (require_boundary_handoff_target)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped MaterializedView boundary handoff lost its MODIFY QUERY command");
        return;
    }

    const auto * materialized_view = storage->as<StorageMaterializedView>();
    if (!materialized_view || storage->getName() != "MaterializedView" || materialized_view->isRefreshable() || !modify_query->select)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mapped MODIFY QUERY is supported only for a non-refreshable MaterializedView");
    }
    if (!context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Mapped MaterializedView MODIFY QUERY requires the experimental analyzer for exact selected-output provenance");
    }
    const auto & before_expectation = metadata->getBoundUDTExpectation();
    if (!before_expectation || before_expectation->object != bound->getObject()
        || before_expectation->object_schema_revision != bound->getObjectSchemaRevision()
        || before_expectation->object_schema_revision == std::numeric_limits<UInt64>::max())
    {
        throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView metadata has no exact successor revision");
    }

    const auto table_id = storage->getStorageID();
    auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    auto * atomic = typeid_cast<DatabaseAtomic *>(database.get());
    if (!atomic || typeid_cast<DatabaseReplicated *>(database.get()) || !atomic->hasActiveUDTAuthority()
        || table_id.uuid == UUIDHelpers::Nil || bound->getObject().database_uuid != atomic->getUUID()
        || bound->getObject().object_uuid != table_id.uuid)
    {
        throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView identity or Atomic authority changed before MODIFY QUERY");
    }

    auto collector = std::make_shared<UDT::SelectedOutputTypeBindingCollector>();
    auto analysis_context = Context::createCopy(context);
    analysis_context->setCurrentDatabase(table_id.database_name);
    analysis_context->setUDTSelectedOutputTypeBindingCollector(collector);
    auto select_options = SelectQueryOptions{}.analyze().createView().checkSubqueryTableAccess();
    auto analyzed_header = InterpreterSelectQueryAnalyzer::getSampleBlock(modify_query->select->clone(), analysis_context, select_options);
    auto collection = collector->take();
    if (!collection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped MaterializedView analyzer did not publish selected-output provenance");

    const auto physical_outputs = analyzed_header->getNamesAndTypesList();
    if (materialized_view->hasInnerTable())
    {
        const auto inner_table = materialized_view->getTargetTable();
        const auto inner_metadata = inner_table ? inner_table->getInMemoryMetadataPtr(context, false) : nullptr;
        if (!inner_table || !inner_metadata || inner_metadata->getColumns().getAllPhysical() != physical_outputs)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Mapped inner-table MaterializedView MODIFY QUERY requires an output schema exactly equal to its physical inner table");
        }
    }
    UDT::SelectedOutputTypeBindings selected_outputs;
    if (collection->kind == UDT::SelectedOutputTypeBindingCollectionKind::NoLogicalSourceFastPath)
    {
        selected_outputs.reserve(physical_outputs.size());
        for (const auto & output : physical_outputs)
        {
            selected_outputs.push_back({
                .output_name = output.name,
                .physical_type = output.type,
                .explicit_logical_tree = {},
                .explicit_type_child_prefix = {},
                .prebound_references = {},
                .prebound_runtime_owner_key = {},
                .prebound_type_child_prefix = {},
            });
        }
    }
    else if (collection->kind == UDT::SelectedOutputTypeBindingCollectionKind::CompleteBindings)
    {
        selected_outputs = std::move(collection->bindings);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped MaterializedView analyzer published an unknown provenance result");

    if (selected_outputs.size() != physical_outputs.size())
        throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView selected-output count changed during analysis");
    auto physical = physical_outputs.begin();
    for (const auto & selected : selected_outputs)
    {
        if (physical == physical_outputs.end() || !physical->type || !selected.isValid() || selected.output_name != physical->name
            || !selected.physical_type->equals(*physical->type) || selected.physical_type->getName() != physical->type->getName())
        {
            throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView selected-output proof differs from its physical header");
        }
        ++physical;
    }

    try
    {
        auto handoff = UDT::prepareStoredObjectSelectedOutputAlterBindings(
            modify_query->select,
            UDT::StoredObjectKind::MaterializedView,
            bound->getObject(),
            before_expectation->object_schema_revision + 1,
            table_id.database_name,
            context,
            atomic->getUDTAuthorityAdapter(),
            selected_outputs);
        if (handoff.getObjectKind() != UDT::StoredObjectKind::MaterializedView
            || handoff.getSourceMode() != UDT::StoredObjectSourceMode::AsSelect || handoff.getObject() != bound->getObject()
            || !handoff.usesSelectedOutputClassification())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped MaterializedView ALTER handoff changed its closed preparation route");
        }
        handoff.applyPhysicalTypeASTs();
        auto prepared = std::move(handoff).releaseViewBindings();
        const bool mapped = static_cast<bool>(prepared.persisted_references);
        if (prepared.physical_outputs != physical_outputs || mapped != static_cast<bool>(prepared.bound_physical_schema)
            || mapped != static_cast<bool>(prepared.sidecar_expectation) || mapped != !prepared.dependency_edges.empty())
        {
            throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView MODIFY QUERY produced an incomplete exact binding package");
        }
        if (mapped
            && (prepared.persisted_references->object != bound->getObject()
                || prepared.persisted_references->object_schema_revision != before_expectation->object_schema_revision + 1
                || prepared.persisted_references->physical_schema_fingerprint != prepared.physical_schema_fingerprint
                || prepared.bound_physical_schema->physical_schema_fingerprint != prepared.physical_schema_fingerprint
                || prepared.sidecar_expectation->physical_schema_fingerprint != prepared.physical_schema_fingerprint))
        {
            throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView MODIFY QUERY sidecar identity changed during preparation");
        }
        if (mapped && !context->getSettingsRef()[Setting::allow_experimental_user_defined_types])
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Mapped MaterializedView MODIFY QUERY cannot retain logical user-defined type outputs while "
                "allow_experimental_user_defined_types is disabled");
        }

        if (prepared.persisted_references)
        {
            std::vector<UDT::AccessTarget> access_targets;
            access_targets.reserve(prepared.persisted_references->descriptors.size());
            for (const auto & descriptor : prepared.persisted_references->descriptors)
            {
                const auto & identity = descriptor.getDefinitionIdentity();
                if (identity.database_uuid != bound->getObject().database_uuid || identity.type_uuid == UUIDHelpers::Nil
                    || !identity.revision)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped MaterializedView binding contains an invalid descriptor identity");
                }
                access_targets.push_back({
                    .database_uuid = identity.database_uuid,
                    .type_uuid = identity.type_uuid,
                });
            }
            UDT::checkUsageAccess(context, access_targets);
        }

        modify_query->udt_stored_object_rebind_prepared = true;
        modify_query->udt_stored_object_physical_outputs = std::move(prepared.physical_outputs);
        modify_query->udt_stored_object_references = std::move(prepared.persisted_references);
    }
    catch (const UDT::StoredObjectTypeBindingPreparationError & error)
    {
        rethrowStoredObjectAlterBindingError(error);
    }
    catch (const UDT::ScalarAliasColumnBinderError & error)
    {
        rethrowUDTAlterBinderError(error);
    }
}

bool hasUDTAlterColumns(const ASTAlterQuery & alter)
{
    for (const auto & child : alter.command_list->children)
    {
        const auto * command = child->as<ASTAlterCommand>();
        if ((command->type == ASTAlterCommand::ADD_COLUMN || command->type == ASTAlterCommand::MODIFY_COLUMN)
            && command->col_decl
            && UDT::hasReferencesInAlterColumn(
                command->col_decl->as<ASTColumnDeclaration &>()))
        {
            return true;
        }
    }
    return false;
}

DatabaseAtomic & validateUDTAlterSurface(
    IDatabase & database,
    const StoragePtr & table)
{
    auto * atomic = typeid_cast<DatabaseAtomic *>(&database);
    if (!atomic || typeid_cast<DatabaseReplicated *>(&database))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "UDT table ALTER requires a local Atomic database");
    }
    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "UDT ALTER table does not exist");
    const String engine_name = table->getName();
    const bool supported_engine = (engine_name == "Memory" && !table->storesDataOnDisk())
        || (table->isMergeTree() && table->storesDataOnDisk() && !table->isSharedStorage()
            && !engine_name.starts_with("Replicated") && !engine_name.starts_with("Shared"));
    if (!supported_engine)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "UDT table ALTER supports only Memory and non-replicated, non-shared MergeTree-family tables");
    }
    if (!atomic->hasActiveUDTAuthority())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown user-defined type in ALTER TABLE column declaration");
    return *atomic;
}

void validateSegmentsCombination(CommandSegments & segments)
{
    size_t partition_commands_count = 0;
    size_t partition_commands_segments_count = 0;
    size_t execute_commands_count = 0;
    for (auto & segment : segments)
    {
        if (auto * partition_commands = std::get_if<PartitionCommands>(&segment))
        {
            partition_commands_count += partition_commands->size();
            partition_commands_segments_count += 1;
        }
        else if (auto * execute_commands = std::get_if<ExecuteCommands>(&segment))
        {
            execute_commands_count += execute_commands->size();
        }
    }

    if (partition_commands_count != 0 && execute_commands_count != 0)
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Partition and Execute commands can not be used together");

    if (partition_commands_count != 0)
        if (partition_commands_segments_count != 1)
            throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Partition commands must be sequential in alter query");

    if (execute_commands_count > 0)
        if (execute_commands_count != 1)
            throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Execute commands should not be combined");
}

void validateMutationsAllowed(const CommandSegments & segments, const DatabasePtr & database, const ContextPtr & context)
{
    if (!context->getServerSettings()[ServerSetting::disable_insertion_and_mutation])
        return;

    if (database->getDatabaseName() == DatabaseCatalog::SYSTEM_DATABASE)
        return;

    for (const auto & segment : segments)
    {
        if (const auto * mutation_commands = std::get_if<MutationCommands>(&segment))
            if (mutation_commands->hasNonEmptyMutationCommands())
                throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Mutations are prohibited");

        if (std::holds_alternative<PartitionCommands>(segment))
            throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Mutations are prohibited");
    }
}

void validateReplicatedDatabaseSegments(const CommandSegments & segments, const DatabasePtr & database)
{
    if (!typeid_cast<DatabaseReplicated *>(database.get()))
        return;

    if (segments.size() != 1)
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED,
            "For Replicated databases it's not allowed to execute ALTERs of different types in single query");

    for (const auto & segment : segments)
        if (const auto * alter_commands = std::get_if<AlterCommands>(&segment))
            if (alter_commands->hasNonReplicatedAlterCommand() && !alter_commands->areNonReplicatedAlterCommands())
                throw Exception(ErrorCodes::QUERY_IS_PROHIBITED,
                    "For Replicated databases it's not allowed "
                    "to execute ALTERs of different types (replicated and non replicated) in single query");
}

std::optional<BlockIO> tryRewriteToLightweightUpdate(CommandSegments & segments, const StoragePtr & table, const ContextPtr & context, const ASTPtr & query_ptr)
{
    bool has_update_commands = false;
    for (const auto & segment : segments)
        if (const auto * mutation_commands = std::get_if<MutationCommands>(&segment))
            has_update_commands |= mutation_commands->hasAnyUpdateCommand();

    if (!has_update_commands)
        return std::nullopt;

    const auto & settings = context->getSettingsRef();
    const auto alter_update_mode = settings[Setting::alter_update_mode];
    if (alter_update_mode == AlterUpdateMode::HEAVY)
        return std::nullopt;

    const auto throw_if_needed = [&](const auto & reason) -> std::optional<BlockIO>
    {
        if (alter_update_mode == AlterUpdateMode::LIGHTWEIGHT_FORCE)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Setting alter_update_mode='{}' but cannot execute query '{}' as a lightweight update. {}",
                alter_update_mode.toString(), query_ptr->formatForErrorMessage(), reason);

        LOG_INFO(getLogger("InterpreterAlterQuery"), "Will not execute '{}' as a lightweight update. {}", query_ptr->formatForErrorMessage(), reason);
        return std::nullopt;
    };

    if (hasCommands<AlterCommands>(segments) || hasCommands<PartitionCommands>(segments) || hasCommands<ExecuteCommands>(segments))
        return throw_if_needed("Not only update commands were passed to alter");

    chassert(segments.size() == 1);
    const MutationCommands & mutation_commands = std::get<MutationCommands>(segments.at(0));

    if (!settings[Setting::enable_lightweight_update])
        return throw_if_needed("Lightweight updates are not allowed. Set 'enable_lightweight_update = 1' to allow them");

    if (!mutation_commands.hasOnlyUpdateCommands())
        return throw_if_needed("Query has non UPDATE commands");

    if (auto supports = table->supportsLightweightUpdate(); !supports)
        return throw_if_needed(supports.error().text);

    LOG_DEBUG(getLogger("InterpreterAlterQuery"), "Will execute query '{}' as a lightweight update", query_ptr->formatForErrorMessage());

    const auto metadata = table->getInMemoryMetadataPtr(context, true);
    UDT::assertAuthorityStorageNewOperationAllowed(*table, metadata, UDT::AuthorityQuarantineOperationKind::Mutation);
    BlockIO res;
    res.pipeline = table->updateLightweight(mutation_commands, context);
    res.pipeline.addStorageHolder(table);
    return res;
}

BlockIO runCommandSegments(
    CommandSegments & segments, const StoragePtr & table, const ContextPtr & context, bool require_mapped_modify_query_boundary_handoff)
{
    BlockIO res;
    const auto & settings = context->getSettingsRef();

    for (auto & segment : segments)
    {
        if (auto * alter_commands = std::get_if<AlterCommands>(&segment))
        {
            auto alter_lock = table->lockForAlter(settings[Setting::lock_acquire_timeout]);
            /// Drop the query-scoped metadata cache, which may hold a snapshot pinned before this
            /// lock. The reads below (validate/prepare/checkAlterIsPossible and the storage's alter)
            /// then all repopulate from the metadata committed as of holding the lock.
            if (auto metadata_cache = context->getQueryMetadataCache())
            {
                auto [cache, cache_lock] = metadata_cache->getStorageMetadataCache();
                cache->clear();
            }
            auto metadata_snapshot = table->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
            UDT::assertAuthorityStorageNewOperationAllowed(*table, metadata_snapshot, UDT::AuthorityQuarantineOperationKind::DDL);
            alter_commands->validate(table, context);

            bool share_nested = true;
            if (auto * merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
                share_nested = (*merge_tree->getSettings())[MergeTreeSetting::share_nested_offsets];

            alter_commands->prepare(*metadata_snapshot, share_nested);
            prepareMappedStoredObjectModifyQuery(
                *alter_commands, table, metadata_snapshot, context, require_mapped_modify_query_boundary_handoff);
            table->checkAlterIsPossible(*alter_commands, context);
            table->alter(*alter_commands, context, alter_lock);
        }
        else if (auto * mutation_commands = std::get_if<MutationCommands>(&segment))
        {
            if (mutation_commands->hasNonEmptyMutationCommands())
            {
                auto metadata_snapshot = table->getInMemoryMetadataPtr(context, true);
                UDT::assertAuthorityStorageNewOperationAllowed(*table, metadata_snapshot, UDT::AuthorityQuarantineOperationKind::Mutation);
                table->checkMutationIsPossible(*mutation_commands, settings);
                /// Replicated-storage non-determinism check must always run, even when
                /// `validate_mutation_query=0` — bypassing it would let nondeterministic mutations
                /// diverge replicas.  The heavier query-shape validation that constructs a full
                /// `MutationsInterpreter` is gated by the setting, since invalid mutations may
                /// reference not-yet-existing objects when the user opts out of validation.
                MutationsInterpreter::validateNonDeterministicMutationsForStorage(table, *mutation_commands, context);
                if (settings[Setting::validate_mutation_query])
                {
                    MutationsInterpreter::Settings mutation_settings(false);
                    MutationsInterpreter(table, metadata_snapshot, *mutation_commands, context, mutation_settings).validate();
                }
                table->mutate(*mutation_commands, context);
            }
        }
        else if (auto * partition_commands = std::get_if<PartitionCommands>(&segment))
        {
            auto metadata_snapshot = table->getInMemoryMetadataPtr(context, true);
            const bool attaches_parts = std::ranges::any_of(
                *partition_commands,
                [](const PartitionCommand & command)
                {
                    return command.type == PartitionCommand::ATTACH_PARTITION || command.type == PartitionCommand::FETCH_PARTITION
                        || command.type == PartitionCommand::REPLACE_PARTITION;
                });
            UDT::assertAuthorityStorageNewOperationAllowed(
                *table,
                metadata_snapshot,
                attaches_parts ? UDT::AuthorityQuarantineOperationKind::Attach : UDT::AuthorityQuarantineOperationKind::DDL);
            table->checkAlterPartitionIsPossible(*partition_commands, metadata_snapshot, settings, context);
            auto partition_commands_pipe = table->alterPartition(metadata_snapshot, *partition_commands, context);
            if (!partition_commands_pipe.empty())
                res.pipeline = QueryPipeline(std::move(partition_commands_pipe));
        }
        else if (auto * execute_commands = std::get_if<ExecuteCommands>(&segment))
        {
            const auto metadata_snapshot = table->getInMemoryMetadataPtr(context, true);
            UDT::assertAuthorityStorageNewOperationAllowed(*table, metadata_snapshot, UDT::AuthorityQuarantineOperationKind::DDL);
            for (const auto * execute_command : *execute_commands)
            {
                ASTPtr args_ast = execute_command->execute_args ? execute_command->execute_args->ptr() : nullptr;
                auto execute_pipe = table->executeCommand(execute_command->execute_command_name, args_ast, context);
                if (!execute_pipe.empty())
                    res.pipeline = QueryPipeline(std::move(execute_pipe));
            }
        }
    }

    return res;
}

}

InterpreterAlterQuery::InterpreterAlterQuery(
    const ASTPtr & query_ptr_,
    ContextMutablePtr context_,
    std::shared_ptr<UDT::UDTStoredObjectDDLSelectBoundaryHandoff> udt_stored_object_ddl_select_boundary_handoff_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
    , udt_stored_object_ddl_select_boundary_handoff(std::move(udt_stored_object_ddl_select_boundary_handoff_))
{
}

BlockIO InterpreterAlterQuery::execute()
{
    auto udf_scope_context = getContext();
    const bool previous_reject_stored_udt_syntax = udf_scope_context->shouldRejectStoredUDTSyntaxInSQLUDFBodies();
    const bool previous_udf_substitution_frozen = udf_scope_context->isStoredObjectSQLUDFSubstitutionFrozen();
    SCOPE_EXIT({
        udf_scope_context->setRejectStoredUDTSyntaxInSQLUDFBodies(previous_reject_stored_udt_syntax);
        udf_scope_context->setStoredObjectSQLUDFSubstitutionFrozen(previous_udf_substitution_frozen);
    });

    if (udt_stored_object_ddl_select_boundary_handoff)
    {
        auto * alter = query_ptr ? query_ptr->as<ASTAlterQuery>() : nullptr;
        if (!alter || !alter->command_list || alter->command_list->children.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stored-object DDL boundary handoff has no exact ALTER MODIFY QUERY owner");
        auto * command = alter->command_list->children.front()->as<ASTAlterCommand>();
        if (!command || command->type != ASTAlterCommand::MODIFY_QUERY || !command->select)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stored-object DDL boundary handoff lost its ALTER MODIFY QUERY SELECT child");
        udt_stored_object_ddl_select_boundary_handoff->consumeForAlter(*alter, *command->select);
        udt_stored_object_ddl_select_boundary_handoff.reset();
        udt_stored_object_ddl_select_boundary_consumed = true;
    }

    getContext()->setRejectStoredUDTSyntaxInSQLUDFBodies();
    FunctionNameNormalizer::visit(query_ptr.get());
    auto & alter = query_ptr->as<ASTAlterQuery &>();

    if (alter.alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
    {
        return executeToDatabase(alter);
    }
    if (alter.alter_object == ASTAlterQuery::AlterObjectType::TABLE)
    {
        const auto command_list_owners = std::count_if(
            query_ptr->children.begin(),
            query_ptr->children.end(),
            [&](const ASTPtr & child) { return child.get() == alter.command_list; });
        if (!alter.command_list || command_list_owners != 1
            || std::any_of(
                alter.command_list->children.begin(),
                alter.command_list->children.end(),
                [](const ASTPtr & child) { return !child || !child->as<ASTAlterCommand>(); }))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ALTER TABLE has a malformed command AST");
        }

        /// Audit the complete retained definition graph before any command
        /// child is interpreted or distributed. The exact replacement repeats
        /// the body check against the cloned definition image.
        if (!UserDefinedSQLFunctionFactory::instance().empty())
        {
            UserDefinedSQLFunctionVisitor::assertNoStoredUDTSyntaxInFunctionBodiesToReplace(query_ptr, getContext());
            UserDefinedSQLFunctionVisitor::visit(query_ptr, getContext(), /*reject_stored_udt_syntax_in_function_bodies=*/true);
        }
        getContext()->setStoredObjectSQLUDFSubstitutionFrozen();
        return executeToTable(alter);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown alter object type");
}

BlockIO InterpreterAlterQuery::executeToTable(const ASTAlterQuery & alter)
{
    ASTSelectWithUnionQuery * modify_query = nullptr;

    for (auto & child : alter.command_list->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        if (command_ast->sql_security)
            InterpreterCreateQuery::processSQLSecurityOption(getContext(), command_ast->sql_security->as<ASTSQLSecurity &>());
        else if (command_ast->type == ASTAlterCommand::MODIFY_QUERY)
            modify_query = command_ast->select->as<ASTSelectWithUnionQuery>();
    }

    BlockIO res;
    const auto & settings = getContext()->getSettingsRef();

    /// This predicate must run before every distributed-DDL fast path. Older
    /// DDL envelopes cannot carry the operation-bound UDT snapshot/USAGE
    /// result and must never enqueue reference-bearing ALTER commands.
    const bool has_udt_columns = hasUDTAlterColumns(alter);
    if (has_udt_columns)
    {
        if (!settings[Setting::allow_experimental_user_defined_types])
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "User-defined type table columns are disabled; enable allow_experimental_user_defined_types to use them");
        }
        if (!alter.cluster.empty())
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "User-defined type table columns do not support ALTER TABLE ON CLUSTER");
        }
    }

    if (getContext()->getSettingsRef()[Setting::use_legacy_to_time])
        normalizeLegacyToTimeInAlterMetadataDefinitions(query_ptr->as<ASTAlterQuery &>());

    auto table_id = getContext()->tryResolveStorageID(alter);
    StoragePtr table;
    bool alters_mapped_udt_object = false;

    if (table_id)
    {
        query_ptr->as<ASTAlterQuery &>().setDatabase(table_id.database_name);
        table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    }

    if (table)
    {
        const auto metadata = table->getInMemoryMetadataPtr(getContext(), true);
        metadata->validateBoundUDTReferences();
        alters_mapped_udt_object = static_cast<bool>(metadata->getBoundUDTReferences());
        UDT::assertAuthorityStorageNewOperationAllowed(*table, metadata, UDT::AuthorityQuarantineOperationKind::DDL);
    }

    if (alters_mapped_udt_object && !alter.cluster.empty())
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Mapped stored-object ALTER does not support ON CLUSTER without an operation-bound sidecar protocol");
    }

    if (!alter.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        if (table && table->as<StorageKeeperMap>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations with ON CLUSTER are not allowed for KeeperMap tables");

        /// Substitute the database of the altered table into table functions that use the current database
        /// implicitly, e.g. `merge('tables_regexp')` in a mutation, so that they read the same tables
        /// as in the non-clustered case. It has to be done before `executeDDLQueryOnCluster`,
        /// which replaces `currentDatabase()` with the database of the session.
        /// The table identifiers are not qualified here: they are qualified with the database
        /// of the altered table when the query is interpreted on each host.
        if (table_id)
        {
            AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
            visitor.substituteDatabaseInTableFunctions(*alter.command_list);
        }

        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess(table);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(getRequiredAccess(table));

    if (!table_id)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", backQuoteIfNeed(alter.getDatabase()));

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    UDT::assertAuthorityOwnedInnerStorageOperationAllowed(table, "ALTER");
    if (udt_stored_object_ddl_select_boundary_consumed)
    {
        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);

        const auto metadata = table->getInMemoryMetadataPtr(getContext(), true);
        metadata->validateBoundUDTReferences();
        const auto & bound = metadata->getBoundUDTReferences();
        const auto * materialized_view = table->as<StorageMaterializedView>();
        auto * atomic = typeid_cast<DatabaseAtomic *>(database.get());
        const auto & storage_id = table->getStorageID();
        if (!bound || bound->getObject().kind != UDT::SchemaObjectKind::View || !materialized_view || table->getName() != "MaterializedView"
            || materialized_view->isRefreshable() || !atomic || typeid_cast<DatabaseReplicated *>(database.get())
            || !atomic->hasActiveUDTAuthority() || storage_id.uuid == UUIDHelpers::Nil
            || bound->getObject().database_uuid != atomic->getUUID() || bound->getObject().object_uuid != storage_id.uuid)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "A UDT-bearing ALTER MODIFY QUERY is supported only for an exactly mapped non-refreshable "
                "MaterializedView in a local Atomic authority");
        }
    }
    if (has_udt_columns && database->shouldReplicateQuery(getContext(), query_ptr))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Replicated database ALTER is not supported for user-defined type table columns");
    }
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name, database.get());
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), {}, std::move(guard));
    }

#if CLICKHOUSE_CLOUD
    if (has_udt_columns && SharedDatabaseCatalog::shouldReplicateQuery(getContext(), query_ptr))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Shared database ALTER is not supported for user-defined type table columns");
    }
    if (SharedDatabaseCatalog::shouldReplicateQuery(getContext(), query_ptr))
    {
        return SharedDatabaseCatalog::instance().tryExecuteDDLQuery(query_ptr, getContext());
    }
#endif

    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);

    DatabaseAtomic * udt_database = nullptr;
    if (has_udt_columns)
        udt_database = &validateUDTAlterSurface(*database, table);

    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY, "Table is read-only");

#if CLICKHOUSE_CLOUD
    if (alter.isUnlockSnapshot())
    {
        ContextPtr context = getContext();
        auto & backups_worker = context->getBackupsWorker();
        backups_worker.unlockSnapshot(query_ptr, context);
        return res;
    }
#endif

    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);

    if (modify_query)
    {
        // Expand CTE before filling default database
        ApplyWithSubqueryVisitor::visit(*modify_query);
    }

    /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    PreparedUDTAlterColumns udt_columns;
    if (udt_database)
    {
        udt_columns = prepareUDTAlterColumns(
            query_ptr->as<ASTAlterQuery &>(), table_id, *udt_database, getContext());
    }
    auto segments = parseAlterCommandSegments(alter, table, getContext(), udt_columns);
    validateSegmentsCombination(segments);
    validateMutationsAllowed(segments, database, getContext());
    validateReplicatedDatabaseSegments(segments, database);

    if (auto lightweight_result = tryRewriteToLightweightUpdate(segments, table, getContext(), query_ptr))
    {
        /// The patch part is committed while the pipeline runs, so the share lock must outlive this
        /// function: otherwise a concurrent DROP can clear the data parts index under the sink.
        QueryPlanResourceHolder update_resources;
        update_resources.table_locks.emplace_back(std::move(table_lock));
        lightweight_result->pipeline.addResources(std::move(update_resources));
        return std::move(lightweight_result.value());
    }

    return runCommandSegments(segments, table, getContext(), udt_stored_object_ddl_select_boundary_consumed);
}

BlockIO InterpreterAlterQuery::executeToDatabase(const ASTAlterQuery & alter)
{
    BlockIO res;
    /// ALTER DATABASE has no table and no UPDATE commands, so the `_row_exists` marker check never applies.
    getContext()->checkAccess(getRequiredAccess(nullptr));
    AlterCommands alter_commands;

    for (const auto & child : alter.command_list->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        if (auto alter_command = AlterCommand::parse(command_ast))
            alter_commands.emplace_back(std::move(*alter_command));
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong parameter type in ALTER DATABASE query");
    }

    if (!alter.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess(nullptr);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    auto ddl_guard = (!alter.no_ddl_lock ? DatabaseCatalog::instance().getDDLGuard(alter.getDatabase(), "", nullptr) : nullptr);
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(alter.getDatabase());

#if CLICKHOUSE_CLOUD
    bool managed_by_shared_catalog = SharedDatabaseCatalog::initialized() && SharedDatabaseCatalog::isDatabaseEngineSupported(database->getEngineName());
    if (managed_by_shared_catalog && !getContext()->getClientInfo().is_shared_catalog_internal)
    {
        ddl_guard.reset();
        return SharedDatabaseCatalog::instance().tryExecuteDDLQuery(query_ptr, getContext());
    }
#endif

    if (!alter_commands.empty())
    {
        /// Only ALTER SETTING and ALTER COMMENT is supported.
        for (const auto & command : alter_commands)
        {
            if (command.type != AlterCommand::MODIFY_DATABASE_SETTING && command.type != AlterCommand::MODIFY_DATABASE_COMMENT)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported alter type for database engines");
        }

        for (const auto & command : alter_commands)
        {
            if (command.ignore)
                continue;

            switch (command.type)
            {
                case AlterCommand::MODIFY_DATABASE_SETTING:
                    database->applySettingsChanges(command.settings_changes, getContext());
                    break;
                case AlterCommand::MODIFY_DATABASE_COMMENT:
                    database->alterDatabaseComment(command, getContext());
                    break;
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported alter command");
            }
        }
    }

    return res;
}

bool InterpreterAlterQuery::isRowExistsLightweightDeleteMarker(const StoragePtr & storage, const ContextPtr & context_)
{
    /// `_row_exists` is the hidden lightweight-delete marker only on storages that register it as a
    /// virtual column (the MergeTree family). Testing merely for the absence of a physical `_row_exists`
    /// column is too broad: on e.g. a `Memory` table `_row_exists` is not the marker, yet has no
    /// physical column either, so a user could `ADD COLUMN _row_exists, UPDATE _row_exists = 0` and edit
    /// a real physical column with only `ALTER DELETE`. `isVirtualColumn` is true only when `_row_exists`
    /// is a registered virtual and not shadowed by a real column, which precisely identifies the marker.
    /// A null storage (non-local ON CLUSTER target) fails closed -> treated as a regular column.
    if (!storage)
        return false;
    const auto metadata_snapshot = storage->getInMemoryMetadataPtr(context_, false);
    return metadata_snapshot->isVirtualColumn(RowExistsColumn::name);
}

AccessRightsElements InterpreterAlterQuery::getRequiredAccess(const StoragePtr & storage) const
{
    AccessRightsElements required_access;
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    const bool row_exists_is_marker = isRowExistsLightweightDeleteMarker(storage, getContext());
    for (const auto & child : alter.command_list->children)
        required_access.append_range(
            getRequiredAccessForCommand(child->as<ASTAlterCommand&>(), alter.getDatabase(), alter.getTable(), row_exists_is_marker));

    return required_access;
}

AccessRightsElements InterpreterAlterQuery::getRequiredAccessForCommand(
    const ASTAlterCommand & command, const String & database, const String & table, bool row_exists_is_lightweight_marker)
{
    AccessRightsElements required_access;

    auto column_name = [&]() -> String { return getIdentifierName(command.column); };
    auto column_name_from_col_decl = [&]() -> std::string_view { return command.col_decl->as<ASTColumnDeclaration &>().name; };

    switch (command.type)
    {
        case ASTAlterCommand::UPDATE:
        {
            /// Setting the `_row_exists` lightweight-delete marker to 0 is a delete, not an update:
            /// `DELETE FROM` rewrites to `ALTER ... UPDATE _row_exists = 0`. Govern that exact form by
            /// ALTER DELETE so `DELETE FROM` needs only the documented ALTER DELETE privilege. Any other
            /// assignment - including `_row_exists = <expr>` that resurrects/edits the deletion mask -
            /// stays a real update requiring ALTER UPDATE. The shortcut applies only when `_row_exists`
            /// is the hidden virtual marker (not an ordinary physical column on some other engine).
            std::vector<std::string_view> updated_columns;
            bool deletes_via_row_exists = false;
            for (const ASTPtr & assignment_ast : command.update_assignments->children)
            {
                const auto & assignment = assignment_ast->as<const ASTAssignment &>();
                if (row_exists_is_lightweight_marker && isLightweightDeleteAssignment(assignment))
                    deletes_via_row_exists = true;
                else
                    updated_columns.emplace_back(assignment.column_name);
            }
            if (!updated_columns.empty())
                required_access.emplace_back(AccessType::ALTER_UPDATE, database, table, updated_columns);
            if (deletes_via_row_exists)
                required_access.emplace_back(AccessType::ALTER_DELETE, database, table);
            break;
        }
        case ASTAlterCommand::ADD_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_COLUMN, database, table, column_name_from_col_decl());
            /// A column-declaration STATISTICS adds statistics like the dedicated ADD STATISTICS command does,
            /// so it must not bypass the corresponding access right.
            if (command.col_decl->as<ASTColumnDeclaration &>().getStatisticsDesc())
                required_access.emplace_back(AccessType::ALTER_ADD_STATISTICS, database, table);
            break;
        }
        case ASTAlterCommand::DROP_COLUMN:
        {
            if (command.clear_column)
                required_access.emplace_back(AccessType::ALTER_CLEAR_COLUMN, database, table, column_name());
            else
                required_access.emplace_back(AccessType::ALTER_DROP_COLUMN, database, table, column_name());
            break;
        }
        case ASTAlterCommand::MODIFY_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_COLUMN, database, table, column_name_from_col_decl());
            /// A column-declaration STATISTICS replaces the explicit statistics of the column like the dedicated
            /// MODIFY STATISTICS command does, so it must not bypass the corresponding access right.
            if (command.col_decl->as<ASTColumnDeclaration &>().getStatisticsDesc())
                required_access.emplace_back(AccessType::ALTER_MODIFY_STATISTICS, database, table);
            break;
        }
        case ASTAlterCommand::COMMENT_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_COMMENT_COLUMN, database, table, column_name());
            break;
        }
        case ASTAlterCommand::MATERIALIZE_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_COLUMN, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_ORDER_BY:
        {
            required_access.emplace_back(AccessType::ALTER_ORDER_BY, database, table);
            break;
        }
        case ASTAlterCommand::REMOVE_SAMPLE_BY:
        case ASTAlterCommand::MODIFY_SAMPLE_BY:
        {
            required_access.emplace_back(AccessType::ALTER_SAMPLE_BY, database, table);
            break;
        }
        case ASTAlterCommand::ADD_STATISTICS:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_STATISTICS, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_STATISTICS:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_STATISTICS, database, table);
            break;
        }
        case ASTAlterCommand::DROP_STATISTICS:
        {
            required_access.emplace_back(AccessType::ALTER_DROP_STATISTICS, database, table);
            break;
        }
        case ASTAlterCommand::MATERIALIZE_STATISTICS:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_STATISTICS, database, table);
            break;
        }
        case ASTAlterCommand::UNLOCK_SNAPSHOT:
        {
            required_access.emplace_back(AccessType::ALTER_UNLOCK_SNAPSHOT, database, table);
            break;
        }
        case ASTAlterCommand::ADD_INDEX:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_INDEX, database, table);
            break;
        }
        case ASTAlterCommand::DROP_INDEX:
        {
            if (command.clear_index)
                required_access.emplace_back(AccessType::ALTER_CLEAR_INDEX, database, table);
            else
                required_access.emplace_back(AccessType::ALTER_DROP_INDEX, database, table);
            break;
        }
        case ASTAlterCommand::MATERIALIZE_INDEX:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_INDEX, database, table);
            break;
        }
        case ASTAlterCommand::ADD_CONSTRAINT:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_CONSTRAINT, database, table);
            break;
        }
        case ASTAlterCommand::DROP_CONSTRAINT:
        {
            required_access.emplace_back(AccessType::ALTER_DROP_CONSTRAINT, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_CONSTRAINT:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_CONSTRAINT, database, table);
            break;
        }
        case ASTAlterCommand::ADD_PROJECTION:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_PROJECTION, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_PROJECTION:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_PROJECTION, database, table);
            break;
        }
        case ASTAlterCommand::DROP_PROJECTION:
        {
            if (command.clear_projection)
                required_access.emplace_back(AccessType::ALTER_CLEAR_PROJECTION, database, table);
            else
                required_access.emplace_back(AccessType::ALTER_DROP_PROJECTION, database, table);
            break;
        }
        case ASTAlterCommand::MATERIALIZE_PROJECTION:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_PROJECTION, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_TTL:
        case ASTAlterCommand::REMOVE_TTL:
        {
            required_access.emplace_back(AccessType::ALTER_TTL, database, table);
            break;
        }
        case ASTAlterCommand::MATERIALIZE_TTL:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_TTL, database, table);
            break;
        }
        case ASTAlterCommand::REWRITE_PARTS:
        {
            required_access.emplace_back(AccessType::ALTER_REWRITE_PARTS, database, table);
            break;
        }
        case ASTAlterCommand::RESET_SETTING: [[fallthrough]];
        case ASTAlterCommand::MODIFY_SETTING:
        {
            required_access.emplace_back(AccessType::ALTER_SETTINGS, database, table);
            break;
        }
        case ASTAlterCommand::ATTACH_PARTITION:
        {
            required_access.emplace_back(AccessType::INSERT, database, table);
            break;
        }
        case ASTAlterCommand::DELETE:
        case ASTAlterCommand::APPLY_DELETED_MASK:
        case ASTAlterCommand::DROP_PARTITION:
        case ASTAlterCommand::DROP_DETACHED_PARTITION:
        case ASTAlterCommand::FORGET_PARTITION:
        {
            required_access.emplace_back(AccessType::ALTER_DELETE, database, table);
            break;
        }
        case ASTAlterCommand::MOVE_PARTITION:
        {
            switch (command.move_destination_type)
            {
                case DataDestinationType::DISK: [[fallthrough]];
                case DataDestinationType::VOLUME:
                    required_access.emplace_back(AccessType::ALTER_MOVE_PARTITION, database, table);
                    break;
                case DataDestinationType::TABLE:
                    required_access.emplace_back(AccessType::ALTER_MOVE_PARTITION, database, table);
                    required_access.emplace_back(AccessType::INSERT, command.to_database, command.to_table);
                    break;
                case DataDestinationType::SHARD:
                    required_access.emplace_back(AccessType::ALTER_MOVE_PARTITION, database, table);
                    required_access.emplace_back(AccessType::MOVE_PARTITION_BETWEEN_SHARDS);
                    break;
                case DataDestinationType::DELETE:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected destination type for command.");
            }
            break;
        }
        case ASTAlterCommand::REPLACE_PARTITION:
        {
            required_access.emplace_back(AccessType::SELECT, command.from_database, command.from_table);
            required_access.emplace_back(AccessType::ALTER_DELETE | AccessType::INSERT, database, table);
            break;
        }
        case ASTAlterCommand::FETCH_PARTITION:
        {
            required_access.emplace_back(AccessType::ALTER_FETCH_PARTITION, database, table);
            break;
        }
        case ASTAlterCommand::FREEZE_PARTITION:
        case ASTAlterCommand::FREEZE_ALL:
        case ASTAlterCommand::UNFREEZE_PARTITION:
        case ASTAlterCommand::UNFREEZE_ALL:
        {
            required_access.emplace_back(AccessType::ALTER_FREEZE_PARTITION, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_QUERY:
        {
            required_access.emplace_back(AccessType::ALTER_VIEW_MODIFY_QUERY, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_REFRESH:
        {
            required_access.emplace_back(AccessType::ALTER_VIEW_MODIFY_REFRESH, database, table);
            break;
        }
        case ASTAlterCommand::RENAME_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_RENAME_COLUMN, database, table, column_name());
            break;
        }
        case ASTAlterCommand::MODIFY_DATABASE_SETTING:
        {
            required_access.emplace_back(AccessType::ALTER_DATABASE_SETTINGS, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_DATABASE_COMMENT:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_DATABASE_COMMENT, database, table);
            break;
        }
        case ASTAlterCommand::NO_TYPE:
            break;
        case ASTAlterCommand::MODIFY_COMMENT:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_COMMENT, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_SQL_SECURITY:
        {
            required_access.emplace_back(AccessType::ALTER_VIEW_MODIFY_SQL_SECURITY, database, table);
            break;
        }
        case ASTAlterCommand::APPLY_PATCHES:
        {
            required_access.emplace_back(AccessType::ALTER_UPDATE, database, table);
            break;
        }
        case ASTAlterCommand::EXECUTE_COMMAND:
        {
            required_access.emplace_back(AccessType::ALTER_EXECUTE, database, table);
            break;
        }
    }

    return required_access;
}

void InterpreterAlterQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr query_context) const
{
    const auto & alter = ast->as<const ASTAlterQuery &>();

    if (alter.command_list != nullptr && alter.alter_object != ASTAlterQuery::AlterObjectType::DATABASE)
    {
        auto main_database = alter.getDatabase();
        auto main_table = alter.getTable();

        if (main_database.empty())
            main_database = query_context->getCurrentDatabase();

        String prefix = backQuoteIfNeed(main_database) + "." + backQuoteIfNeed(main_table) + ".";

        for (const auto & child : alter.command_list->children)
        {
            const auto * command = child->as<ASTAlterCommand>();

            if (command->column)
                elem.query_columns.insert(prefix + command->column->getColumnName());

            if (command->rename_to)
                elem.query_columns.insert(prefix + command->rename_to->getColumnName());

            // ADD COLUMN
            if (command->col_decl)
            {
                elem.query_columns.insert(prefix + command->col_decl->as<ASTColumnDeclaration &>().name);
            }

            if (!command->from_table.empty())
            {
                String database = command->from_database.empty() ? getContext()->getCurrentDatabase() : command->from_database;
                elem.query_databases.insert(database);
                elem.query_tables.insert(database + "." + command->from_table);
            }

            if (!command->to_table.empty())
            {
                String database = command->to_database.empty() ? getContext()->getCurrentDatabase() : command->to_database;
                elem.query_databases.insert(database);
                elem.query_tables.insert(database + "." + command->to_table);
            }
        }
    }
}

void registerInterpreterAlterQuery(InterpreterFactory & factory);
void registerInterpreterAlterQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    { return std::make_unique<InterpreterAlterQuery>(args.query, args.context, args.udt_stored_object_ddl_select_boundary_handoff); };
    factory.registerInterpreter("InterpreterAlterQuery", create_fn);
}

}

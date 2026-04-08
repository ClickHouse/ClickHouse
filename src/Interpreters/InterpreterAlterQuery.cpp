#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Access/Common/AccessRightsElement.h>
#include <Backups/BackupsWorker.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/ExecuteCommands.h>
#include <Storages/StorageKeeperMap.h>
#include <Storages/IStorage.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

namespace DB
{

namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsAlterUpdateMode alter_update_mode;
    extern const SettingsBool enable_lightweight_update;
    extern const SettingsTimezone session_timezone;
}

namespace ServerSetting
{
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int QUERY_IS_PROHIBITED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

using CommandSegment = std::variant<AlterCommands, MutationCommands, PartitionCommands, ExecuteCommands>;
using CommandSegments = std::vector<CommandSegment>;

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

CommandSegments parseAlterCommandSegments(const ASTAlterQuery & alter, const StoragePtr & table, const ContextPtr & context)
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
            segments_holder.take<AlterCommands>().push_back(std::move(alter_command.value()));
        }
        else if (auto partition_command = PartitionCommand::parse(command_ast))
        {
            segments_holder.take<PartitionCommands>().push_back(std::move(partition_command.value()));
        }
        else if (auto mutation_command = MutationCommand::parse(*command_ast))
        {
            if (mutation_command->type == MutationCommand::UPDATE || mutation_command->type == MutationCommand::DELETE)
            {
                /// TODO: add a check for result query size.
                auto rewritten_command_ast = replaceNonDeterministicToScalars(*command_ast, context);
                if (rewritten_command_ast)
                {
                    auto * new_alter_command = rewritten_command_ast->as<ASTAlterCommand>();
                    mutation_command = MutationCommand::parse(*new_alter_command);
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
                const auto & source_ast = *mutation_command->ast->as<ASTAlterCommand>();
                auto tz_rewritten_ast = rewriteDateTimeLiteralsWithTimezone(
                    source_ast, table->getInMemoryMetadata().columns, session_tz);
                if (tz_rewritten_ast)
                {
                    auto * tz_alter_command = tz_rewritten_ast->as<ASTAlterCommand>();
                    mutation_command = MutationCommand::parse(*tz_alter_command);
                    if (!mutation_command)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Alter command '{}' is rewritten to invalid command '{}'",
                            source_ast.formatForErrorMessage(), tz_rewritten_ast->formatForErrorMessage());
                }
            }

            segments_holder.take<MutationCommands>().push_back(std::move(mutation_command.value()));
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong parameter type in ALTER query");
    }

    return std::move(segments_holder.segments);
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

    BlockIO res;
    res.pipeline = table->updateLightweight(mutation_commands, context);
    res.pipeline.addStorageHolder(table);
    return res;
}

BlockIO runCommandSegments(CommandSegments & segments, const StoragePtr & table, const ContextPtr & context)
{
    BlockIO res;
    const auto & settings = context->getSettingsRef();

    for (auto & segment : segments)
    {
        if (auto * alter_commands = std::get_if<AlterCommands>(&segment))
        {
            auto alter_lock = table->lockForAlter(settings[Setting::lock_acquire_timeout]);
            auto metadata_snapshot = table->getInMemoryMetadataPtr(/*bypass_metadata_cache=*/true);
            alter_commands->validate(table, context);
            alter_commands->prepare(*metadata_snapshot);
            table->checkAlterIsPossible(*alter_commands, context);
            table->alter(*alter_commands, context, alter_lock);
        }
        else if (auto * mutation_commands = std::get_if<MutationCommands>(&segment))
        {
            if (mutation_commands->hasNonEmptyMutationCommands())
            {
                auto metadata_snapshot = table->getInMemoryMetadataPtr(/*bypass_metadata_cache=*/true);
                table->checkMutationIsPossible(*mutation_commands, settings);
                MutationsInterpreter::Settings mutation_settings(false);
                MutationsInterpreter(table, metadata_snapshot, *mutation_commands, context, mutation_settings).validate();
                table->mutate(*mutation_commands, context);
            }
        }
        else if (auto * partition_commands = std::get_if<PartitionCommands>(&segment))
        {
            auto metadata_snapshot = table->getInMemoryMetadataPtr(/*bypass_metadata_cache=*/true);
            table->checkAlterPartitionIsPossible(*partition_commands, metadata_snapshot, settings, context);
            auto partition_commands_pipe = table->alterPartition(metadata_snapshot, *partition_commands, context);
            if (!partition_commands_pipe.empty())
                res.pipeline = QueryPipeline(std::move(partition_commands_pipe));
        }
        else if (auto * execute_commands = std::get_if<ExecuteCommands>(&segment))
        {
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

InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

BlockIO InterpreterAlterQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    if (alter.alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
    {
        return executeToDatabase(alter);
    }
    if (alter.alter_object == ASTAlterQuery::AlterObjectType::TABLE)
    {
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

    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query_ptr, getContext());

    auto table_id = getContext()->tryResolveStorageID(alter);
    StoragePtr table;

    if (table_id)
    {
        query_ptr->as<ASTAlterQuery &>().setDatabase(table_id.database_name);
        table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    }

    if (!alter.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        if (table && table->as<StorageKeeperMap>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations with ON CLUSTER are not allowed for KeeperMap tables");

        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(getRequiredAccess());

    if (!table_id)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", backQuoteIfNeed(alter.getDatabase()));

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name, database.get());
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), {}, std::move(guard));
    }

#if CLICKHOUSE_CLOUD
    if (SharedDatabaseCatalog::shouldReplicateQuery(getContext(), query_ptr))
    {
        return SharedDatabaseCatalog::instance().tryExecuteDDLQuery(query_ptr, getContext());
    }
#endif

    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);

    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

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
        ApplyWithSubqueryVisitor(getContext()).visit(*modify_query);
    }

    /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    auto segments = parseAlterCommandSegments(alter, table, getContext());
    validateSegmentsCombination(segments);
    validateMutationsAllowed(segments, database, getContext());
    validateReplicatedDatabaseSegments(segments, database);

    if (auto lightweight_result = tryRewriteToLightweightUpdate(segments, table, getContext(), query_ptr))
        return std::move(lightweight_result.value());

    return runCommandSegments(segments, table, getContext());
}

BlockIO InterpreterAlterQuery::executeToDatabase(const ASTAlterQuery & alter)
{
    BlockIO res;
    getContext()->checkAccess(getRequiredAccess());
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(alter.getDatabase());
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
        params.access_to_check = getRequiredAccess();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

#if CLICKHOUSE_CLOUD
    bool managed_by_shared_catalog = SharedDatabaseCatalog::initialized() && SharedDatabaseCatalog::isDatabaseEngineSupported(database->getEngineName());
    if (managed_by_shared_catalog && !getContext()->getClientInfo().is_shared_catalog_internal)
    {
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

AccessRightsElements InterpreterAlterQuery::getRequiredAccess() const
{
    AccessRightsElements required_access;
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    for (const auto & child : alter.command_list->children)
        required_access.append_range(getRequiredAccessForCommand(child->as<ASTAlterCommand&>(), alter.getDatabase(), alter.getTable()));

    return required_access;
}

AccessRightsElements InterpreterAlterQuery::getRequiredAccessForCommand(const ASTAlterCommand & command, const String & database, const String & table)
{
    AccessRightsElements required_access;

    auto column_name = [&]() -> String { return getIdentifierName(command.column); };
    auto column_name_from_col_decl = [&]() -> std::string_view { return command.col_decl->as<ASTColumnDeclaration &>().name; };
    auto column_names_from_update_assignments = [&]() -> std::vector<std::string_view>
    {
        std::vector<std::string_view> column_names;
        for (const ASTPtr & assignment_ast : command.update_assignments->children)
            column_names.emplace_back(assignment_ast->as<const ASTAssignment &>().column_name);
        return column_names;
    };

    switch (command.type)
    {
        case ASTAlterCommand::UPDATE:
        {
            required_access.emplace_back(AccessType::ALTER_UPDATE, database, table, column_names_from_update_assignments());
            break;
        }
        case ASTAlterCommand::ADD_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_COLUMN, database, table, column_name_from_col_decl());
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
        case ASTAlterCommand::ADD_PROJECTION:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_PROJECTION, database, table);
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

void registerInterpreterAlterQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterAlterQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterAlterQuery", create_fn);
}

}

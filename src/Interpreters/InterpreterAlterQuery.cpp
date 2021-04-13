#include <Interpreters/InterpreterAlterQuery.h>

#include <Access/AccessRightsElement.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/LiveView/LiveViewCommands.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Common/typeid_cast.h>

#include <boost/range/algorithm_ext/push_back.hpp>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
}


InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_)
{
}

BlockIO InterpreterAlterQuery::execute()
{
    BlockIO res;
    const auto & alter = query_ptr->as<ASTAlterQuery &>();


    if (!alter.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccess());

    getContext()->checkAccess(getRequiredAccess());
    auto table_id = getContext()->resolveStorageID(alter, Context::ResolveOrdinary);
    query_ptr->as<ASTAlterQuery &>().database = table_id.database_name;

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (typeid_cast<DatabaseReplicated *>(database.get())
        && getContext()->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return typeid_cast<DatabaseReplicated *>(database.get())->tryEnqueueReplicatedDDL(query_ptr, getContext());
    }

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto alter_lock = table->lockForAlter(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    /// Add default database to table identifiers that we can encounter in e.g. default expressions,
    /// mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(table_id.getDatabaseName());
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    MutationCommands mutation_commands;
    LiveViewCommands live_view_commands;
    for (const auto & child : alter.command_list->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        if (auto alter_command = AlterCommand::parse(command_ast))
            alter_commands.emplace_back(std::move(*alter_command));
        else if (auto partition_command = PartitionCommand::parse(command_ast))
        {
            partition_commands.emplace_back(std::move(*partition_command));
        }
        else if (auto mut_command = MutationCommand::parse(command_ast))
        {
            if (mut_command->type == MutationCommand::MATERIALIZE_TTL && !metadata_snapshot->hasAnyTTL())
                throw Exception("Cannot MATERIALIZE TTL as there is no TTL set for table "
                    + table->getStorageID().getNameForLogs(), ErrorCodes::INCORRECT_QUERY);

            mutation_commands.emplace_back(std::move(*mut_command));
        }
        else if (auto live_view_command = LiveViewCommand::parse(command_ast))
            live_view_commands.emplace_back(std::move(*live_view_command));
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }

    if (typeid_cast<DatabaseReplicated *>(database.get()))
    {
        int command_types_count = !mutation_commands.empty() + !partition_commands.empty() + !live_view_commands.empty() + !alter_commands.empty();
        if (1 < command_types_count)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "For Replicated databases it's not allowed "
                                                         "to execute ALTERs of different types in single query");
    }

    if (!mutation_commands.empty())
    {
        table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());
        MutationsInterpreter(table, metadata_snapshot, mutation_commands, getContext(), false).validate();
        table->mutate(mutation_commands, getContext());
    }

    if (!partition_commands.empty())
    {
        table->checkAlterPartitionIsPossible(partition_commands, metadata_snapshot, getContext()->getSettingsRef());
        auto partition_commands_pipe = table->alterPartition(metadata_snapshot, partition_commands, getContext());
        if (!partition_commands_pipe.empty())
            res.pipeline.init(std::move(partition_commands_pipe));
    }

    if (!live_view_commands.empty())
    {
        live_view_commands.validate(*table);
        for (const LiveViewCommand & command : live_view_commands)
        {
            auto live_view = std::dynamic_pointer_cast<StorageLiveView>(table);
            switch (command.type)
            {
                case LiveViewCommand::REFRESH:
                    live_view->refresh();
                    break;
            }
        }
    }

    if (!alter_commands.empty())
    {
        StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
        alter_commands.validate(metadata, getContext());
        alter_commands.prepare(metadata);
        table->checkAlterIsPossible(alter_commands, getContext());
        table->alter(alter_commands, getContext(), alter_lock);
    }

    return res;
}


AccessRightsElements InterpreterAlterQuery::getRequiredAccess() const
{
    AccessRightsElements required_access;
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    for (const auto & child : alter.command_list->children)
        boost::range::push_back(required_access, getRequiredAccessForCommand(child->as<ASTAlterCommand&>(), alter.database, alter.table));
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
        case ASTAlterCommand::DELETE:
        {
            required_access.emplace_back(AccessType::ALTER_DELETE, database, table);
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
        case ASTAlterCommand::MODIFY_ORDER_BY:
        {
            required_access.emplace_back(AccessType::ALTER_ORDER_BY, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_SAMPLE_BY:
        {
            required_access.emplace_back(AccessType::ALTER_SAMPLE_BY, database, table);
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
        case ASTAlterCommand::MODIFY_TTL:
        {
            required_access.emplace_back(AccessType::ALTER_TTL, database, table);
            break;
        }
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
        case ASTAlterCommand::DROP_PARTITION: [[fallthrough]];
        case ASTAlterCommand::DROP_DETACHED_PARTITION:
        {
            required_access.emplace_back(AccessType::ALTER_DELETE, database, table);
            break;
        }
        case ASTAlterCommand::MOVE_PARTITION:
        {
            if ((command.move_destination_type == DataDestinationType::DISK)
                || (command.move_destination_type == DataDestinationType::VOLUME))
            {
                required_access.emplace_back(AccessType::ALTER_MOVE_PARTITION, database, table);
            }
            else if (command.move_destination_type == DataDestinationType::TABLE)
            {
                required_access.emplace_back(AccessType::SELECT | AccessType::ALTER_DELETE, database, table);
                required_access.emplace_back(AccessType::INSERT, command.to_database, command.to_table);
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
        case ASTAlterCommand::FREEZE_PARTITION: [[fallthrough]];
        case ASTAlterCommand::FREEZE_ALL: [[fallthrough]];
        case ASTAlterCommand::UNFREEZE_PARTITION: [[fallthrough]];
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
        case ASTAlterCommand::LIVE_VIEW_REFRESH:
        {
            required_access.emplace_back(AccessType::ALTER_VIEW_REFRESH, database, table);
            break;
        }
        case ASTAlterCommand::RENAME_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_RENAME_COLUMN, database, table, column_name());
            break;
        }
        case ASTAlterCommand::NO_TYPE: break;
    }

    return required_access;
}

void InterpreterAlterQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const
{
    const auto & alter = ast->as<const ASTAlterQuery &>();

    elem.query_kind = "Alter";
    if (alter.command_list != nullptr)
    {
        // Alter queries already have their target table inserted into `elem`.
        if (elem.query_tables.size() != 1)
            throw Exception("Alter query should have target table recorded already", ErrorCodes::LOGICAL_ERROR);

        String prefix = *elem.query_tables.begin() + ".";
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

}

#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/LiveView/LiveViewCommands.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Common/typeid_cast.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
}


InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterAlterQuery::execute()
{
    const auto & alter = query_ptr->as<ASTAlterQuery &>();

    if (!alter.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {alter.database});

    const String & table_name = alter.table;
    String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
    StoragePtr table = context.getTable(database_name, table_name);

    /// Add default database to table identifiers that we can encounter in e.g. default expressions,
    /// mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(database_name);
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    MutationCommands mutation_commands;
    LiveViewCommands live_view_commands;
    for (ASTAlterCommand * command_ast : alter.command_list->commands)
    {
        if (auto alter_command = AlterCommand::parse(command_ast))
            alter_commands.emplace_back(std::move(*alter_command));
        else if (auto partition_command = PartitionCommand::parse(command_ast))
        {
            if (partition_command->type == PartitionCommand::DROP_DETACHED_PARTITION
                && !context.getSettingsRef().allow_drop_detached)
                throw DB::Exception("Cannot execute query: DROP DETACHED PART is disabled "
                                    "(see allow_drop_detached setting)", ErrorCodes::SUPPORT_IS_DISABLED);
            partition_commands.emplace_back(std::move(*partition_command));
        }
        else if (auto mut_command = MutationCommand::parse(command_ast))
            mutation_commands.emplace_back(std::move(*mut_command));
        else if (auto live_view_command = LiveViewCommand::parse(command_ast))
            live_view_commands.emplace_back(std::move(*live_view_command));
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!mutation_commands.empty())
    {
        auto table_lock_holder = table->lockStructureForShare(false /* because mutation is executed asyncronously */, context.getCurrentQueryId());
        MutationsInterpreter(table, mutation_commands, context, false).validate(table_lock_holder);
        table->mutate(mutation_commands, context);
    }

    if (!partition_commands.empty())
    {
        partition_commands.validate(*table);
        table->alterPartition(query_ptr, partition_commands, context);
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
                    live_view->refresh(context);
                    break;
            }
        }
    }

    if (!alter_commands.empty())
    {
        auto table_lock_holder = table->lockAlterIntention(context.getCurrentQueryId());
        alter_commands.validate(*table, context);
        table->alter(alter_commands, context, table_lock_holder);
    }

    return {};
}

}

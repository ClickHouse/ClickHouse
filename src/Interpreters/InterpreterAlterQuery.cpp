#include <Interpreters/InterpreterAlterQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageKeeperMap.h>
#include <Common/typeid_cast.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <boost/range/algorithm_ext/push_back.hpp>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}


InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterAlterQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    if (alter.alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
    {
        return executeToDatabase(alter);
    }
    else if (alter.alter_object == ASTAlterQuery::AlterObjectType::TABLE
            || alter.alter_object == ASTAlterQuery::AlterObjectType::LIVE_VIEW)
    {
        return executeToTable(alter);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown alter object type");
}

BlockIO InterpreterAlterQuery::executeToTable(const ASTAlterQuery & alter)
{
    BlockIO res;

    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query_ptr);

    auto table_id = getContext()->resolveStorageID(alter, Context::ResolveOrdinary);
    query_ptr->as<ASTAlterQuery &>().setDatabase(table_id.database_name);
    StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());

    if (!alter.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        if (table && table->as<StorageKeeperMap>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations with ON CLUSTER are not allowed for KeeperMap tables");

        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(getRequiredAccess());

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext());
    }

    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);

    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");
    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    MutationCommands mutation_commands;
    for (const auto & child : alter.command_list->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        if (auto alter_command = AlterCommand::parse(command_ast))
        {
            alter_commands.emplace_back(std::move(*alter_command));
        }
        else if (auto partition_command = PartitionCommand::parse(command_ast))
        {
            partition_command->trigger_view = alter.need_trigger_view;
            partition_commands.emplace_back(std::move(*partition_command));
        }
        else if (auto mut_command = MutationCommand::parse(command_ast))
        {
            if (mut_command->type == MutationCommand::MATERIALIZE_TTL && !metadata_snapshot->hasAnyTTL())
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot MATERIALIZE TTL as there is no TTL set for table {}",
                    table->getStorageID().getNameForLogs());

            mutation_commands.emplace_back(std::move(*mut_command));
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong parameter type in ALTER query");
    }

    if (typeid_cast<DatabaseReplicated *>(database.get()))
    {
        int command_types_count = !mutation_commands.empty() + !partition_commands.empty() + !alter_commands.empty();
        bool mixed_settings_amd_metadata_alter = alter_commands.hasSettingsAlterCommand() && !alter_commands.isSettingsAlter();
        if (1 < command_types_count || mixed_settings_amd_metadata_alter)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "For Replicated databases it's not allowed "
                                                         "to execute ALTERs of different types in single query");
    }

    if (mutation_commands.hasNonEmptyMutationCommands())
    {
        table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());
        MutationsInterpreter::Settings settings(false);
        MutationsInterpreter(table, metadata_snapshot, mutation_commands, getContext(), settings).validate();
        table->mutate(mutation_commands, getContext());
    }

    if (!partition_commands.empty())
    {
        table->checkAlterPartitionIsPossible(partition_commands, metadata_snapshot, getContext()->getSettingsRef());
        auto partition_commands_pipe = table->alterPartition(metadata_snapshot, partition_commands, getContext());
        if (!partition_commands_pipe.empty())
            res.pipeline = QueryPipeline(std::move(partition_commands_pipe));

        // check if need to trigger view
        for (const PartitionCommand & command : partition_commands){
            if (!command.trigger_view) {
                continue;
            }
            triggerViewInsert(command, table_id);
        }
    }

    if (!alter_commands.empty())
    {
        auto alter_lock = table->lockForAlter(getContext()->getSettingsRef().lock_acquire_timeout);
        StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
        alter_commands.validate(table, getContext());
        alter_commands.prepare(metadata);
        table->checkAlterIsPossible(alter_commands, getContext());
        table->alter(alter_commands, getContext(), alter_lock);
    }

    return res;
}

/// insert into db.tb_view select * from db.tb_tml_local where xx and _partition_id = partition
void InterpreterAlterQuery::triggerViewInsert(const PartitionCommand & command, const StorageID & table_id) {
    /// get partition_id
    auto partition = command.partition;
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);
    MergeTreeData* merge_tree_data = dynamic_cast<MergeTreeData *>(table.get());
    String partition_id = merge_tree_data->getPartitionIDFromQuery(partition, context);
    LOG_TRACE(&Poco::Logger::get("InterpreterAlterQuery"), "trigger partition_id: {}", partition_id );

    /// get materialize view by table
    Dependencies dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty()) {
        return;
    }

    /// insert into select => InterpreterInsertQuery
    for (const auto & database_table : dependencies) {
        auto dependent_table = DatabaseCatalog::instance().getTable(database_table, context);
        ASTPtr query;
        BlockOutputStreamPtr out;
        /// get materialized view
        if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(dependent_table.get()))
        {
            std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
            auto distribute_table_id = materialized_view->getTargetTableId();
            insert->table_id = distribute_table_id;

            /// Specify the corresponding column in the INSERT statement
            insert->columns = std::make_shared<ASTExpressionList>();
            StorageInMemoryMetadata storage_metadata = materialized_view->getInMemoryMetadata();
            auto all_columns = storage_metadata.columns;
            for (const auto & column : all_columns)
            {
                String name = column.name;
                insert->columns->children.push_back(std::make_shared<ASTIdentifier>(name));
            }

            /// get select query from materialized view
            query = materialized_view->getInMemoryMetadataPtr()->getSelectQuery().inner_query->clone();
            auto select_query = query->as<ASTSelectQuery>();

            /// make where condition
            auto func = std::make_shared<ASTFunction>();
            auto p1 = std::make_shared<ASTIdentifier>("_partition_id");
            auto p2 = std::make_shared<ASTLiteral>(partition_id);
            func = makeASTFunction("equals", p1, p2);

            // if have where xxxx
            if (select_query->where()) {
                // and _partition_id = xxx
                select_query->setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", select_query->where()->clone(), func));
            } else {
                // where _partition_id = xxx
                select_query->setExpression(ASTSelectQuery::Expression::WHERE, func);
            }
            LOG_TRACE(&Poco::Logger::get("InterpreterAlterQuery"), "Initial trigger select sql: {}", DB::serializeAST(*select_query, true) );

            /// convert type of ASTSelectQuery to type of ASTSelectWithUnionQuery
            const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
            select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
            auto new_select_query = std::dynamic_pointer_cast<ASTSelectQuery>(select_query->clone());
            select_with_union_query->list_of_selects->children.push_back(new_select_query);

            /// set insert of select
            insert->select = select_with_union_query;
            LOG_TRACE(&Poco::Logger::get("InterpreterAlterQuery"), "Initial trigger insert into select sql: {}", DB::serializeAST(*insert, true) );

            /// get pointer
            ASTPtr insert_query_ptr(insert.release());

            /// InterpreterInsertQuery::execute()
            std::unique_ptr<Context> insert_context = std::make_unique<Context>(context);
            Settings new_settings = insert_context->getSettings();
            new_settings.max_insert_threads = std::max<UInt64>(1, new_settings.max_insert_threads);
            insert_context->setSettings(new_settings);
            LOG_TRACE(&Poco::Logger::get("InterpreterAlterQuery"), "insert_context: {}", insert_context->getSettingsRef().toString());

            InterpreterInsertQuery interpreter(insert_query_ptr, *insert_context);
            BlockIO io = interpreter.execute();
            auto executor = io.pipeline.execute();
            executor->execute(insert_context->getSettingsRef().max_insert_threads);
        }
    }
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

    if (!alter_commands.empty())
    {
        /// Only ALTER SETTING is supported.
        for (const auto & command : alter_commands)
        {
            if (command.type != AlterCommand::MODIFY_DATABASE_SETTING)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported alter type for database engines");
        }

        for (const auto & command : alter_commands)
        {
            if (!command.ignore)
            {
                if (command.type == AlterCommand::MODIFY_DATABASE_SETTING)
                    database->applySettingsChanges(command.settings_changes, getContext());
                else
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
        boost::range::push_back(required_access, getRequiredAccessForCommand(child->as<ASTAlterCommand&>(), alter.getDatabase(), alter.getTable()));
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
        case ASTAlterCommand::DROP_PARTITION:
        case ASTAlterCommand::DROP_DETACHED_PARTITION:
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
                    required_access.emplace_back(AccessType::SELECT | AccessType::ALTER_DELETE, database, table);
                    required_access.emplace_back(AccessType::INSERT, command.to_database, command.to_table);
                    break;
                case DataDestinationType::SHARD:
                    required_access.emplace_back(AccessType::SELECT | AccessType::ALTER_DELETE, database, table);
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
        case ASTAlterCommand::MODIFY_DATABASE_SETTING:
        {
            required_access.emplace_back(AccessType::ALTER_DATABASE_SETTINGS, database, table);
            break;
        }
        case ASTAlterCommand::NO_TYPE: break;
        case ASTAlterCommand::MODIFY_COMMENT:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_COMMENT, database, table);
            break;
        }
    }

    return required_access;
}

void InterpreterAlterQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const
{
    const auto & alter = ast->as<const ASTAlterQuery &>();

    if (alter.command_list != nullptr && alter.alter_object != ASTAlterQuery::AlterObjectType::DATABASE)
    {
        // Alter queries already have their target table inserted into `elem`.
        if (elem.query_tables.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Alter query should have target table recorded already");

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

#include <Databases/AlterDatabaseCommands.h>

#include <map>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/IDatabase.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/formatAST.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_ALTER_DATABASE_QUERY;
    extern const int TABLE_OVERRIDE_ALREADY_EXISTS;
    extern const int TABLE_OVERRIDE_NOT_FOUND;
}

void DB::AlterDatabaseSettingCommand::prepare(const DatabasePtr &, ContextPtr, AlterDatabaseApplyData & apply_data)
{
    for (const auto & change : settings_changes)
    {
        // Allow duplicate settings as long as the values are the same.
        if (!apply_data.merged_settings_changes.insertSetting(change.name, change.value))
        {
            if (Field * found = apply_data.merged_settings_changes.tryGet(change.name); found && *found != change.value)
            {
                throw Exception(
                    ErrorCodes::INVALID_ALTER_DATABASE_QUERY,
                    "ALTER DATABASE MODIFY SETTING: multiple conflicting values for setting '{}'",
                    change.name);
            }
        }
    }
    for (const auto & reset : settings_resets)
    {
        // Do not allow mixing MODIFY and RESET for the same setting
        Field found_value;
        if (!apply_data.merged_settings_changes.tryGet(reset, found_value))
            apply_data.merged_settings_changes.push_back({reset, Field()});
        else if (found_value.getType() != Field::Types::Null)
            throw Exception(
                ErrorCodes::INVALID_ALTER_DATABASE_QUERY,
                "ALTER DATABASE RESET SETTING: can not mix MODIFY and RESET for setting '{}'",
                reset);
    }
}

void AlterDatabaseSettingCommand::apply(DatabasePtr &, ContextPtr, AlterDatabaseApplyData &)
{
    /// Nothing to do here, settings are batch-applied in AlterDatabaseSettings::applyAll()
}

AlterDatabaseTableOverrideCommand::AlterDatabaseTableOverrideCommand(ASTAlterCommand::Type type_, const ASTPtr & override_ast_)
    : override_ast(override_ast_), type(type_), table_override(override_ast->as<const ASTTableOverride>())
{
}

namespace
{
    template <typename T>
    std::vector<const T *> toConstElements(std::vector<T *> input)
    {
        std::vector<const T *> output(input.begin(), input.end());
        return output;
    }

    ASTTableOverride * tryGetExistingOverride(const ASTCreateQuery * create, const String & override_name)
    {
        if (!create || !create->table_overrides)
            return nullptr;
        if (auto table_override = create->table_overrides->tryGetTableOverride(override_name))
            return table_override->as<ASTTableOverride>();
        return nullptr;
    }

    ContextMutablePtr createInternalContext(ContextPtr context)
    {
        ContextMutablePtr internal_context = Context::createCopy(context);
        internal_context->setInternalQuery(true);
        return internal_context;
    }

    String whichOverrideCommand(ASTAlterCommand::Type type)
    {
        switch (type)
        {
            case ASTAlterCommand::ADD_TABLE_OVERRIDE:
                return "ADD";
            case ASTAlterCommand::MODIFY_TABLE_OVERRIDE:
                return "MODIFY";
            case ASTAlterCommand::DROP_TABLE_OVERRIDE:
                return "DROP";
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not a table override ASTAlterCommand::Type: {} - this is a bug!", magic_enum::enum_name(type));
        }
    }
}

void AlterDatabaseTableOverrideCommand::prepare(const DatabasePtr & database, ContextPtr context, AlterDatabaseApplyData & apply_data)
{
    if (apply_data.overrides_already_seen_in_query.contains(table_override->table_name))
    {
        throw Exception(
            ErrorCodes::INVALID_ALTER_DATABASE_QUERY,
            "Multiple ALTER DATABASE {{ADD|MODIFY|DROP}} TABLE OVERRIDE commands referring to override `{}`",
            table_override->table_name);
    }
    apply_data.overrides_already_seen_in_query.insert(table_override->table_name);
    ASTTableOverride * existing_override = tryGetExistingOverride(apply_data.getCreateQuery(), table_override->table_name);
    if ((type == ASTAlterCommand::DROP_TABLE_OVERRIDE || type == ASTAlterCommand::MODIFY_TABLE_OVERRIDE) && !existing_override)
    {
        throw Exception(
            ErrorCodes::TABLE_OVERRIDE_NOT_FOUND,
            "ALTER DATABASE {} TABLE OVERRIDE refers to unknown override `{}`",
            whichOverrideCommand(type),
            table_override->table_name);
    }
    else if (type == ASTAlterCommand::ADD_TABLE_OVERRIDE && existing_override)
    {
        throw Exception(
            ErrorCodes::TABLE_OVERRIDE_ALREADY_EXISTS,
            "ALTER DATABASE {} TABLE OVERRIDE refers to existing override `{}`",
            whichOverrideCommand(type),
            table_override->table_name);
    }

    if (database->isTableExist(table_override->table_name, context))
    {
        if (table_override->modifiesStorage() != (existing_override && existing_override->modifiesStorage()))
        {
            /// Only the old or new override modifies storage
            apply_data.need_to_recreate_database = true;
            if (table_override->modifiesStorage())
                apply_data.need_to_recreate_database_reason = "existing override does not modify storage, new override does";
            else
                apply_data.need_to_recreate_database_reason = "existing override modifies storage, new override does not";

        }
        else if (table_override->modifiesStorage())
        {
            /// Here, we know existing_override->storage exists after the previous if. If storage parameters
            /// are different, the database needs to be recreated.
            chassert(table_override->storage);
            if (serializeAST(*table_override->storage) != serializeAST(*existing_override->storage))
            {
                apply_data.need_to_recreate_database = true;
                apply_data.need_to_recreate_database_reason = "new override modifies storage";
            }
        }
    }
    /// TODO: check that no overrides touch internal columns like _version and _sign
    ///       (but account for that these names could be _version2 etc.)
    /// TODO: check that no overrides touch internal indices like _version
    if (auto alter_table_query = tryGetAlterTableQuery(database, apply_data, context))
    {
        apply_data.reconciliation_queries.push_back(alter_table_query);
    }

}

void AlterDatabaseTableOverrideCommand::apply(DatabasePtr &, ContextPtr, AlterDatabaseApplyData & apply_data)
{
    ASTCreateQuery * create = apply_data.getCreateQuery();
    if (type == ASTAlterCommand::ADD_TABLE_OVERRIDE || type == ASTAlterCommand::MODIFY_TABLE_OVERRIDE)
    {
        if (!create->table_overrides)
        {
            chassert(type == ASTAlterCommand::ADD_TABLE_OVERRIDE);
            create->set(create->table_overrides, std::make_shared<ASTTableOverrideList>());
        }
        create->table_overrides->setTableOverride(table_override->table_name, table_override->clone());
    }
    else if (type == ASTAlterCommand::DROP_TABLE_OVERRIDE)
    {
        create->table_overrides->removeTableOverride(table_override->table_name);
    }
}

ASTPtr AlterDatabaseTableOverrideCommand::tryGetAlterTableQuery(const DatabasePtr & database, AlterDatabaseApplyData & apply_data, ContextPtr context) const
{
    const auto table = database->tryGetTable(table_override->table_name, context);
    /// If the database is about to be recreated, don't bother with running ALTER TABLE queries
    if (!table || apply_data.need_to_recreate_database)
        return nullptr;

    const auto table_metadata = table->getInMemoryMetadata();
    auto alter_table_query = std::make_shared<ASTAlterQuery>();
    alter_table_query->setDatabase(database->getDatabaseName());
    alter_table_query->setTable(table_override->table_name);
    alter_table_query->alter_object = ASTAlterQuery::AlterObjectType::TABLE;
    auto command_list = std::make_shared<ASTExpressionList>();
    auto add_alter_command = [&] (ASTAlterCommand::Type type_, std::function<void(std::shared_ptr<ASTAlterCommand> &)> apply_func)
    {
        auto command = std::make_shared<ASTAlterCommand>();
        command->type = type_;
        apply_func(command);
        command_list->children.push_back(command);
    };

    /// Add or modify existing columns or indices:
    if ((type == ASTAlterCommand::ADD_TABLE_OVERRIDE || type == ASTAlterCommand::MODIFY_TABLE_OVERRIDE) && table_override->columns)
    {
        if (table_override->columns->columns)
        {
            for (const auto & column_child : table_override->columns->columns->children)
            {
                const auto * override_column = column_child->as<ASTColumnDeclaration>();
                add_alter_command(table_metadata.columns.has(override_column->name) ? ASTAlterCommand::MODIFY_COLUMN : ASTAlterCommand::ADD_COLUMN, [&](const auto & c)
                {
                    c->col_decl = column_child;
                    c->children.push_back(column_child);
                });
            }
        }
        if (table_override->columns->indices)
        {
            for (const auto & index_child : table_override->columns->indices->children)
            {
                const auto * override_index = index_child->as<ASTIndexDeclaration>();
                if (table_metadata.secondary_indices.has(override_index->name))
                {
                    /// ClickHouse does not have "ALTER TABLE MODIFY INDEX", so drop/add instead
                    add_alter_command(ASTAlterCommand::DROP_INDEX, [&](std::shared_ptr<ASTAlterCommand> & c)
                    {
                        c->index = std::make_shared<ASTIdentifier>(override_index->name);
                        c->children.push_back(c->index);
                    });
                }
                add_alter_command(ASTAlterCommand::ADD_INDEX, [&](std::shared_ptr<ASTAlterCommand> & c)
                {
                    c->index_decl = index_child;
                    c->children.push_back(index_child);
                });
            }
        }
    }

    /// Drop columns or indices that were removed from the override:
    const auto * existing_table_override = tryGetExistingOverride(apply_data.getCreateQuery(), table_override->table_name);
    if ((type == ASTAlterCommand::DROP_TABLE_OVERRIDE || type == ASTAlterCommand::MODIFY_TABLE_OVERRIDE)
        && existing_table_override && existing_table_override->columns)
    {
        if (existing_table_override->columns->columns)
        {
            for (const auto & column_child : existing_table_override->columns->columns->children)
            {
                const auto & col_name = column_child->as<ASTColumnDeclaration>()->name;
                if (type == ASTAlterCommand::DROP_TABLE_OVERRIDE ||
                    (existing_table_override->modifiesColumn(col_name) && !table_override->modifiesColumn(col_name)))
                {
                    add_alter_command(ASTAlterCommand::DROP_COLUMN, [&](std::shared_ptr<ASTAlterCommand> & c)
                    {
                        c->if_exists = true;
                        c->column = std::make_shared<ASTIdentifier>(col_name);
                        c->children.push_back(c->column);
                    });
                }
            }
        }
        if (existing_table_override->columns && existing_table_override->columns->indices)
        {
            for (const auto & index_child : existing_table_override->columns->indices->children)
            {
                const auto & index_name = index_child->as<ASTIndexDeclaration>()->name;
                if (type == ASTAlterCommand::DROP_TABLE_OVERRIDE ||
                    (existing_table_override->modifiesIndex(index_name) && !table_override->modifiesIndex(index_name)))
                {
                    add_alter_command(ASTAlterCommand::DROP_INDEX, [&](std::shared_ptr<ASTAlterCommand> & c)
                    {
                        c->if_exists = true;
                        c->index = std::make_shared<ASTIdentifier>(index_name);
                        c->children.push_back(c->index);
                    });
                }
            }
        }
    }
    alter_table_query->set(alter_table_query->command_list, command_list);
    return alter_table_query->children.empty() ? nullptr : alter_table_query;
}

std::vector<const AlterDatabaseSettingCommand *> AlterDatabaseCommands::getSettingCommands() const
{
    return toConstElements(setting_commands);
}

std::vector<const AlterDatabaseTableOverrideCommand *> AlterDatabaseCommands::getTableOverrideCommands() const
{
    return toConstElements(table_override_commands);
}

void AlterDatabaseCommands::applyAll(DatabasePtr & database, ContextPtr context)
{
    if (!apply_data.merged_settings_changes.empty())
    {
        database->applySettingsChanges(apply_data.merged_settings_changes, context);
    }

    if (!table_override_commands.empty())
    {
        auto internal_context = createInternalContext(context);
        auto * logger = &Poco::Logger::get("AlterDatabaseTableOverride");
        for (const auto & command : table_override_commands)
        {
            command->apply(database, context, apply_data);
        }
        if (apply_data.need_to_recreate_database)
        {
            LOG_INFO(logger, "Dropping and re-creating database `{}`", database->getDatabaseName());
            auto drop_query = std::make_shared<ASTDropQuery>();
            drop_query->kind = ASTDropQuery::Drop;
            drop_query->setDatabase(database->getDatabaseName());
            InterpreterDropQuery(drop_query, internal_context).execute();
            InterpreterCreateQuery(apply_data.getCreateAST(), internal_context).execute();
        }
        else
        {
            database->persistCreateDatabaseQuery(apply_data.getCreateAST(), []{});
            /// FIXME: persist pending reconciliation queries in persistCreateDatabaseQuery's commit callback
            ///        (currently causes a deadlock)
            for (const auto & query : apply_data.reconciliation_queries)
            {
                LOG_DEBUG(logger, "Executing reconciliation query: {}", serializeAST(*query.get()));
                InterpreterAlterQuery(query, internal_context).execute();
            }
        }
    }
}

void AlterDatabaseCommands::addCommand(const ASTAlterCommand & command_ast)
{
    if (command_ast.type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        if (auto * query = command_ast.database_settings_changes->as<ASTSetQuery>())
        {
            AlterDatabaseCommandPtr modify_command = std::make_shared<AlterDatabaseSettingCommand>(query->changes);
            push_back(modify_command);
            setting_commands.push_back(modify_command->as<AlterDatabaseSettingCommand>());
            return;
        }
    }
    else if (command_ast.type == ASTAlterCommand::RESET_DATABASE_SETTING)
    {
        Names reset_settings;
        for (const ASTPtr & identifier_ast : command_ast.database_settings_resets->children)
        {
            const auto & identifier = identifier_ast->as<ASTIdentifier &>();
            reset_settings.push_back(identifier.name());
        }
        AlterDatabaseCommandPtr reset_command = std::make_shared<AlterDatabaseSettingCommand>(reset_settings);
        push_back(reset_command);
        setting_commands.push_back(reset_command->as<AlterDatabaseSettingCommand>());
        return;
    }
    else if (
        command_ast.type == ASTAlterCommand::ADD_TABLE_OVERRIDE ||
        command_ast.type == ASTAlterCommand::DROP_TABLE_OVERRIDE ||
        command_ast.type == ASTAlterCommand::MODIFY_TABLE_OVERRIDE)
    {
        if (auto * override = command_ast.table_override->as<ASTTableOverride>())
        {
            AlterDatabaseCommandPtr override_command = std::make_shared<AlterDatabaseTableOverrideCommand>(command_ast.type, override->clone());
            push_back(override_command);
            table_override_commands.push_back(override_command->as<AlterDatabaseTableOverrideCommand>());
            return;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unrecognized ALTER DATABASE command: {}", magic_enum::enum_name(command_ast.type));
}

void AlterDatabaseCommands::prepareAll(const DatabasePtr & database, ContextPtr query_context)
{
    // auto prepare_context = createInternalContext(query_context);

    const ASTCreateQuery * create = apply_data.setCreateQuery(database->getCreateDatabaseQuery());
    if (!create)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not load create query for database: {}", database->getDatabaseName());
    }

    for (auto & setting_command : *this)
        setting_command->prepare(database, query_context, apply_data);

    if (apply_data.need_to_recreate_database && !query_context->getSettingsRef().allow_alter_database_to_drop_and_recreate_if_needed)
    {
        throw Exception(
            ErrorCodes::INVALID_ALTER_DATABASE_QUERY,
            "This ALTER DATABASE query requires dropping and recreating the database ({}) - "
            "set 'allow_alter_database_to_drop_and_recreate_if_needed=1' if you really want to do this!",
            apply_data.need_to_recreate_database_reason);
    }
    database->checkAlterIsPossible(*this, query_context);
}

}

#pragma once

#include <algorithm>
#include <vector>

#include <Core/Field.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>
#include <base/types.h>

namespace DB
{

class AlterDatabaseApplyData;
class ASTCreateQuery;
class ASTStorage;
class ASTTableOverride;

class AlterDatabaseCommand : public TypePromotion<AlterDatabaseCommand>
{
public:
    virtual void prepare(const DatabasePtr & database, ContextPtr context, AlterDatabaseApplyData & apply_data) = 0;
    virtual void apply(DatabasePtr & database, ContextPtr context, AlterDatabaseApplyData & apply_data) = 0;
    virtual ~AlterDatabaseCommand() = default;
};

using AlterDatabaseCommandPtr = std::shared_ptr<AlterDatabaseCommand>;

class AlterDatabaseTableOverrideCommand : public AlterDatabaseCommand
{
public:
    AlterDatabaseTableOverrideCommand(ASTAlterCommand::Type type_, const ASTPtr & override_ast_);
    void prepare(const DatabasePtr & database, ContextPtr context, AlterDatabaseApplyData & apply_data) override;
    void apply(DatabasePtr & database, ContextPtr context, AlterDatabaseApplyData & apply_data) override;

private:
    ASTPtr override_ast;

    /// Returns an ALTER TABLE query to reconcile materialized tables with new overrides, or nullptr if no changes are needed.
    ASTPtr tryGetAlterTableQuery(const DatabasePtr & database, AlterDatabaseApplyData & apply_data, ContextPtr context) const;

public:
    const ASTAlterCommand::Type type;
    const ASTTableOverride * table_override;
};

class AlterDatabaseSettingCommand : public AlterDatabaseCommand
{
public:
    AlterDatabaseSettingCommand(const SettingsChanges & modified_settings) : settings_changes(modified_settings), settings_resets() { }
    AlterDatabaseSettingCommand(const Names & reset_settings) : settings_changes(), settings_resets(reset_settings) { }
    void prepare(const DatabasePtr & database, ContextPtr context, AlterDatabaseApplyData & apply_data) override;
    void apply(DatabasePtr & database, ContextPtr context, AlterDatabaseApplyData & apply_data) override;

    const SettingsChanges settings_changes;
    const Names settings_resets;
};

class AlterDatabaseApplyData
{
public:
    bool need_to_recreate_database = false;
    String need_to_recreate_database_reason;
    SettingsChanges merged_settings_changes;
    NameSet overrides_already_seen_in_query;
    ASTs reconciliation_queries;

    ASTCreateQuery * setCreateQuery(const ASTPtr & create_ast_)
    {
        create_ast = create_ast_->clone();
        create_query = create_ast->as<ASTCreateQuery>();
        return create_query;
    }
    ASTCreateQuery * getCreateQuery() { return create_query; }
    const ASTCreateQuery * getConstCreateQuery() const { return create_query; }
    ASTPtr getCreateAST() { return create_ast; }

private:
    ASTPtr create_ast;
    ASTCreateQuery * create_query;
};

class AlterDatabaseCommands : private std::vector<AlterDatabaseCommandPtr>
{
public:
    void addCommand(const ASTAlterCommand & command);
    void prepareAll(const DatabasePtr & database, ContextPtr context);
    void applyAll(DatabasePtr & database, ContextPtr context);
    std::vector<const AlterDatabaseSettingCommand *> getSettingCommands() const;
    std::vector<const AlterDatabaseTableOverrideCommand *> getTableOverrideCommands() const;

private:
    AlterDatabaseApplyData apply_data;

    std::vector<AlterDatabaseSettingCommand *> setting_commands;
    std::vector<AlterDatabaseTableOverrideCommand *> table_override_commands;
};

}

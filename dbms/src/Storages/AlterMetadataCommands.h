#pragma once
#include <Core/Types.h>
#include <optional>
#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>


namespace DB
{
class AlterMetadataCommand
{
    enum Type
    {
        COMMENT_COLUMN,
        MODIFY_ORDER_BY,
        ADD_INDEX,
        ADD_CONSTRAINT,
        DROP_CONSTRAINT,
        MODIFY_TTL,
        MODIFY_SETTING,
    };

    Type type;

    String column_name;

    std::optional<String> comment;

    /// For DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN
    bool if_exists = false;

    /// For MODIFY_ORDER_BY
    ASTPtr order_by;

    /// For ADD INDEX
    ASTPtr index_decl;
    String after_index_name;

    /// For ADD/DROP INDEX
    String index_name;

    // For ADD CONSTRAINT
    ASTPtr constraint_decl;

    // For ADD/DROP CONSTRAINT
    String constraint_name;

    /// For MODIFY TTL
    ASTPtr ttl;

    /// indicates that this command should not be applied, for example in case of if_exists=true and column doesn't exist.
    bool ignore = false;

    /// For MODIFY SETTING
    SettingsChanges settings_changes;
};

class AlterMetadataCommands : public std::vector<AlterMetadataCommand>
{
public:
    void apply(
        IndicesDescription & indices_description,
        ConstraintsDescription & constraints_description,
        ASTPtr & order_by_ast,
        ASTPtr & primary_key_ast,
        ASTPtr & ttl_table_ast,
        SettingsChanges & changes) const;

    /// Apply alter commands only for settings. Exception will be thrown if any other part of table structure will be modified.
    void applyForSettings(SettingsChanges & changes) const;

    void validate(const IStorage & table, const Context & context);
    bool isModifyingData() const;
    bool isSettingsAlter() const;
};
}

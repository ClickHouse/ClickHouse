#pragma once

#include <optional>
#include <Core/NamesAndTypes.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ConstraintsDescription.h>

#include <Common/SettingsChanges.h>


namespace DB
{

class ASTAlterCommand;

/// Operation from the ALTER query (except for manipulation with PART/PARTITION).
/// Adding Nested columns is not expanded to add individual columns.
struct AlterCommand
{
    enum Type
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        COMMENT_COLUMN,
        MODIFY_ORDER_BY,
        ADD_INDEX,
        DROP_INDEX,
        ADD_CONSTRAINT,
        DROP_CONSTRAINT,
        MODIFY_TTL,
        UKNOWN_TYPE,
        MODIFY_SETTING,
    };

    Type type = UKNOWN_TYPE;

    String column_name;

    /// For DROP COLUMN ... FROM PARTITION
    String partition_name;

    /// For ADD and MODIFY, a new column type.
    DataTypePtr data_type;

    ColumnDefaultKind default_kind{};
    ASTPtr default_expression{};
    std::optional<String> comment;

    /// For ADD - after which column to add a new one. If an empty string, add to the end. To add to the beginning now it is impossible.
    String after_column;

    /// For DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN
    bool if_exists = false;

    /// For ADD_COLUMN
    bool if_not_exists = false;

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

    /// For ADD and MODIFY
    CompressionCodecPtr codec;

    /// For MODIFY SETTING
    SettingsChanges settings_changes;

    AlterCommand() = default;
    AlterCommand(const Type type_, const String & column_name_, const DataTypePtr & data_type_,
                 const ColumnDefaultKind default_kind_, const ASTPtr & default_expression_,
                 const String & after_column_, const String & comment_,
                 const bool if_exists_, const bool if_not_exists_)
        : type{type_}, column_name{column_name_}, data_type{data_type_}, default_kind{default_kind_},
        default_expression{default_expression_}, comment(comment_), after_column{after_column_},
        if_exists(if_exists_), if_not_exists(if_not_exists_)
    {}

    static std::optional<AlterCommand> parse(const ASTAlterCommand * command);

    void apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
        ConstraintsDescription & constraints_description, ASTPtr & order_by_ast,
        ASTPtr & primary_key_ast, ASTPtr & ttl_table_ast, SettingsChanges & changes) const;

    /// Checks that alter query changes data. For MergeTree:
    ///    * column files (data and marks)
    ///    * each part meta (columns.txt)
    /// in each part on disk (it's not lightweight alter).
    bool isModifyingData() const;

    /// checks that only settings changed by alter
    bool isSettingsAlter() const;
};

class Context;

class AlterCommands : public std::vector<AlterCommand>
{
public:
    /// Used for primitive table engines, where only columns metadata can be changed
    void applyForColumnsOnly(ColumnsDescription & columns_description) const;
    void apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
        ConstraintsDescription & constraints_description, ASTPtr & order_by_ast, ASTPtr & primary_key_ast,
        ASTPtr & ttl_table_ast, SettingsChanges & changes) const;

    /// Apply alter commands only for settings. Exception will be thrown if any other part of table structure will be modified.
    void applyForSettingsOnly(SettingsChanges & changes) const;

    void validate(const IStorage & table, const Context & context);
    bool isModifyingData() const;
    bool isSettingsAlter() const;
};

}

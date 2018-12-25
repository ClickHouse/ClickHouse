#pragma once

#include <optional>
#include <Core/NamesAndTypes.h>
#include <Storages/ColumnsDescription.h>
#include <optional>



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
        UKNOWN_TYPE,
    };

    Type type = UKNOWN_TYPE;

    String column_name;

    /// For DROP COLUMN ... FROM PARTITION
    String partition_name;

    /// For ADD and MODIFY, a new column type.
    DataTypePtr data_type;

    ColumnDefaultKind default_kind{};
    ASTPtr default_expression{};
    String comment;

    /// For ADD - after which column to add a new one. If an empty string, add to the end. To add to the beginning now it is impossible.
    String after_column;

    /// For DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN
    bool if_exists;

    /// For ADD_COLUMN
    bool if_not_exists;

    /// For MODIFY_ORDER_BY
    ASTPtr order_by;

    /// indicates that this command should not be applied, for example in case of if_exists=true and column doesn't exist.
    bool ignore = false;

    AlterCommand() = default;
    AlterCommand(const Type type, const String & column_name, const DataTypePtr & data_type,
                 const ColumnDefaultKind default_kind, const ASTPtr & default_expression,
                 const String & after_column = String{}, const String & comment = "",
                 const bool if_exists = false, const bool if_not_exists = false) // TODO: разобраться здесь с параметром по умолчанию
        : type{type}, column_name{column_name}, data_type{data_type}, default_kind{default_kind},
        default_expression{default_expression}, comment(comment), after_column{after_column},
        if_exists(if_exists), if_not_exists(if_not_exists)
    {}

    static std::optional<AlterCommand> parse(const ASTAlterCommand * command);

    void apply(ColumnsDescription & columns_description, ASTPtr & order_by_ast, ASTPtr & primary_key_ast) const;
    /// Checks that not only metadata touched by that command
    bool is_mutable() const;
};

class IStorage;
class Context;

class AlterCommands : public std::vector<AlterCommand>
{
public:
    void apply(ColumnsDescription & columns_description, ASTPtr & order_by_ast, ASTPtr & primary_key_ast) const;

    /// For storages that don't support MODIFY_ORDER_BY.
    void apply(ColumnsDescription & columns_description) const;

    void validate(const IStorage & table, const Context & context);
    bool is_mutable() const;
};

}

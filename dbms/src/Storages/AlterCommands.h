#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

/// Operation from the ALTER query (except for manipulation with PART/PARTITION). Adding Nested columns is not expanded to add individual columns.
struct AlterCommand
{
    enum Type
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        MODIFY_PRIMARY_KEY,
    };

    Type type;

    String column_name;

    /// For DROP COLUMN ... FROM PARTITION
    String partition_name;

    /// For ADD and MODIFY, a new column type.
    DataTypePtr data_type;

    ColumnDefaultKind default_kind{};
    ASTPtr default_expression{};

    /// For ADD - after which column to add a new one. If an empty string, add to the end. To add to the beginning now it is impossible.
    String after_column;

    /// For MODIFY_PRIMARY_KEY
    ASTPtr primary_key;

    /// the names are the same if they match the whole name or name_without_dot matches the part of the name up to the dot
    static bool namesEqual(const String & name_without_dot, const DB::NameAndTypePair & name_type)
    {
        String name_with_dot = name_without_dot + ".";
        return (name_with_dot == name_type.name.substr(0, name_without_dot.length() + 1) || name_without_dot == name_type.name);
    }

    void apply(ColumnsDescription & columns_description) const;

    AlterCommand() = default;
    AlterCommand(const Type type, const String & column_name, const DataTypePtr & data_type,
                 const ColumnDefaultKind default_kind, const ASTPtr & default_expression,
                 const String & after_column = String{})
        : type{type}, column_name{column_name}, data_type{data_type}, default_kind{default_kind},
        default_expression{default_expression}, after_column{after_column}
    {}
};

class IStorage;
class Context;

class AlterCommands : public std::vector<AlterCommand>
{
public:
    void apply(ColumnsDescription & columns_description) const;

    void validate(IStorage * table, const Context & context);
};

}

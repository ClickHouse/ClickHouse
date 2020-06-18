#pragma once

#include <optional>

#include <Parsers/ASTIdentifier.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct IdentifierSemanticImpl
{
    bool special = false;       /// for now it's 'not a column': tables, subselects and some special stuff like FORMAT
    bool can_be_alias = true;   /// if it's a cropped name it could not be an alias
    bool covered = false;       /// real (compound) name is hidden by an alias (short name)
    std::optional<size_t> membership; /// table position in join
};

/// Static calss to manipulate IdentifierSemanticImpl via ASTIdentifier
struct IdentifierSemantic
{
    enum class ColumnMatch
    {
        NoMatch,
        ColumnName,       /// column qualified with column names list
        AliasedTableName, /// column qualified with table name (but table has an alias so its priority is lower than TableName)
        TableName,        /// column qualified with table name
        DbAndTable,       /// column qualified with database and table name
        TableAlias,       /// column qualified with table alias
        Ambiguous,
    };

    /// @returns name for column identifiers
    static std::optional<String> getColumnName(const ASTIdentifier & node);
    static std::optional<String> getColumnName(const ASTPtr & ast);

    /// @returns name for 'not a column' identifiers
    static std::optional<String> getTableName(const ASTIdentifier & node);
    static std::optional<String> getTableName(const ASTPtr & ast);
    static StorageID extractDatabaseAndTable(const ASTIdentifier & identifier);
    static std::optional<String> extractNestedName(const ASTIdentifier & identifier, const String & table_name);

    static ColumnMatch canReferColumnToTable(const ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table);
    static ColumnMatch canReferColumnToTable(const ASTIdentifier & identifier, const TableWithColumnNamesAndTypes & table_with_columns);

    static void setColumnShortName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table);
    static void setColumnLongName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table);
    static bool canBeAlias(const ASTIdentifier & identifier);
    static void setMembership(ASTIdentifier &, size_t table_pos);
    static void coverName(ASTIdentifier &, const String & alias);
    static std::optional<ASTIdentifier> uncover(const ASTIdentifier & identifier);
    static std::optional<size_t> getMembership(const ASTIdentifier & identifier);
    static std::optional<size_t> chooseTable(const ASTIdentifier &, const std::vector<DatabaseAndTableWithAlias> & tables,
                            bool allow_ambiguous = false);
    static std::optional<size_t> chooseTable(const ASTIdentifier &, const TablesWithColumns & tables,
                            bool allow_ambiguous = false);
    static std::optional<size_t> chooseTableColumnMatch(const ASTIdentifier &, const TablesWithColumns & tables,
                            bool allow_ambiguous = false);

private:
    static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table);
    static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table);
};

}

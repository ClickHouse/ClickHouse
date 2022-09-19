#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/getTableExpressions.h>

namespace DB
{

class ASTIdentifier;
class ASTSelectQuery;

struct IdentifierSemanticImpl
{
    bool special = false;              /// for now it's 'not a column': tables, subselects and some special stuff like FORMAT
    bool can_be_alias = true;          /// if it's a cropped name it could not be an alias
    bool covered = false;              /// real (compound) name is hidden by an alias (short name)
    std::optional<size_t> membership;  /// table position in join
    String table = {};                 /// store table name for columns just to support legacy logic.
    bool legacy_compound = false;      /// true if identifier supposed to be comply for legacy |compound()| behavior
};

/// Static class to manipulate IdentifierSemanticImpl via ASTIdentifier
struct IdentifierSemantic
{
    enum class ColumnMatch
    {
        NoMatch,
        ColumnName,       /// column qualified with column names list
        AliasedTableName, /// column qualified with table name (but table has an alias so its priority is lower than TableName)
        TableName,        /// column qualified with table name
        DBAndTable,       /// column qualified with database and table name
        TableAlias,       /// column qualified with table alias
        Ambiguous,
    };

    /// @returns name for column identifiers
    static std::optional<String> getColumnName(const ASTIdentifier & node);
    static std::optional<String> getColumnName(const ASTPtr & ast);

    /// @returns name for 'not a column' identifiers
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

    static std::optional<size_t> getIdentMembership(const ASTIdentifier & ident, const std::vector<TableWithColumnNamesAndTypes> & tables);

    /// Collect common table membership for identifiers in expression
    /// If membership cannot be established or there are several identifies from different tables, return empty optional
    static std::optional<size_t>
    getIdentsMembership(ASTPtr ast, const std::vector<TableWithColumnNamesAndTypes> & tables, const Aliases & aliases);

private:
    static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table);
    static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table);
};


/// Collect all identifies from AST recursively
class IdentifiersCollector
{
public:
    using ASTIdentifierPtr = const ASTIdentifier *;
    using ASTIdentifiers = std::vector<ASTIdentifierPtr>;
    struct Data
    {
        ASTIdentifiers idents;
    };

    static void visit(const ASTPtr & node, Data & data);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &);
    static ASTIdentifiers collect(const ASTPtr & node);
};

/// Collect identifier table membership considering aliases
class IdentifierMembershipCollector
{
public:
    IdentifierMembershipCollector(const ASTSelectQuery & select, ContextPtr context);
    std::optional<size_t> getIdentsMembership(ASTPtr ast) const;

private:
    std::vector<TableWithColumnNamesAndTypes> tables;
    Aliases aliases;
};

/// Split expression `expr_1 AND expr_2 AND ... AND expr_n` into vector `[expr_1, expr_2, ..., expr_n]`
std::vector<ASTPtr> collectConjunctions(const ASTPtr & node);

}

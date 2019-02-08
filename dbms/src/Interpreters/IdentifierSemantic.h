#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct IdentifierSemanticImpl
{
    bool special = false;
    bool need_long_name = false;
};

/// Static calss to manipulate IdentifierSemanticImpl via ASTIdentifier
struct IdentifierSemantic
{
    /// @returns name for column identifiers
    static std::optional<String> getColumnName(const ASTIdentifier & node);
    static std::optional<String> getColumnName(const ASTPtr & ast);

    /// @returns name for 'not a column' identifiers
    static std::optional<String> getTableName(const ASTIdentifier & node);
    static std::optional<String> getTableName(const ASTPtr & ast);
    static std::pair<String, String> extractDatabaseAndTable(const ASTIdentifier & identifier);

    static size_t canReferColumnToTable(const ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table);
    static String columnNormalName(const ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table);
    static void setColumnNormalName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table);
    static void setNeedLongName(ASTIdentifier & identifier, bool); /// if set setColumnNormalName makes qualified name

private:
    static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table);
    static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table);
    static void setColumnShortName(ASTIdentifier & identifier, size_t match);
};

}

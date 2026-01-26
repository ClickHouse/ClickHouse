#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST_erase.h>


namespace DB
{

String ASTQueryWithTableAndOutput::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(getDatabaseAst(), name);
    return name;
}

String ASTQueryWithTableAndOutput::getTable() const
{
    String name;
    tryGetIdentifierNameInto(getTableAst(), name);
    return name;
}

void ASTQueryWithTableAndOutput::setDatabase(const String & name)
{
    if (database_index != INVALID_INDEX)
    {
        /// Note: we don't remove from children for simplicity, just invalidate the index
        database_index = INVALID_INDEX;
    }

    if (!name.empty())
        setDatabaseAst(make_intrusive<ASTIdentifier>(name));
}

void ASTQueryWithTableAndOutput::setTable(const String & name)
{
    if (table_index != INVALID_INDEX)
    {
        /// Note: we don't remove from children for simplicity, just invalidate the index
        table_index = INVALID_INDEX;
    }

    if (!name.empty())
        setTableAst(make_intrusive<ASTIdentifier>(name));
}

void ASTQueryWithTableAndOutput::cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const
{
    /// Reset indices first since children was cleared
    cloned.resetTableIndices();

    if (auto database = getDatabaseAst())
        cloned.setDatabaseAst(database->clone());
    if (auto table = getTableAst())
        cloned.setTableAst(table->clone());
}

}

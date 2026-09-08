#include <Common/SipHash.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{

String ASTQueryWithTableAndOutput::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTQueryWithTableAndOutput::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTQueryWithTableAndOutput::setDatabase(const String & name)
{
    reset(database);
    if (!name.empty())
        set(database, make_intrusive<ASTIdentifier>(name));
}

void ASTQueryWithTableAndOutput::setTable(const String & name)
{
    reset(table);
    if (!name.empty())
        set(table, make_intrusive<ASTIdentifier>(name));
}

void ASTQueryWithTableAndOutput::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// Neither `TEMPORARY` nor `uuid` is a child, so without hashing them `DROP TABLE t` and
    /// `DROP TEMPORARY TABLE t` hash equally, as do two `CREATE`s with different explicit UUIDs.
    hash_state.update(isTemporary());
    hash_state.update(uuid);
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTQueryWithTableAndOutput::cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const
{
    if (database)
        cloned.set(cloned.database, database->clone());
    if (table)
        cloned.set(cloned.table, table->clone());
}

}

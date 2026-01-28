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
    if (!name.empty())
        setDatabaseAst(make_intrusive<ASTIdentifier>(name));
    else
    {
        /// Note: we don't remove from children for simplicity, just invalidate the index
        setDatabaseIndex(INVALID_INDEX);
    }
}

void ASTQueryWithTableAndOutput::setTable(const String & name)
{
    if (!name.empty())
        setTableAst(make_intrusive<ASTIdentifier>(name));
    else
    {
        /// Note: we don't remove from children for simplicity, just invalidate the index
        setTableIndex(INVALID_INDEX);
    }
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

void ASTQueryWithTableAndOutput::resetOutputAST()
{
    /// Collect output indices that will be removed
    std::vector<UInt8> indices_to_remove;
    if (out_file_index != INVALID_INDEX)
        indices_to_remove.push_back(out_file_index);
    if (format_ast_index != INVALID_INDEX)
        indices_to_remove.push_back(format_ast_index);
    if (settings_ast_index != INVALID_INDEX)
        indices_to_remove.push_back(settings_ast_index);
    if (compression_index != INVALID_INDEX)
        indices_to_remove.push_back(compression_index);
    if (compression_level_index != INVALID_INDEX)
        indices_to_remove.push_back(compression_level_index);

    /// Call base class to do the actual removal
    ASTQueryWithOutput::resetOutputAST();

    /// Adjust database_index and table_index after the other children were erased
    auto adjust_index = [&indices_to_remove](UInt8 index) -> UInt8
    {
        if (index == INVALID_INDEX)
            return index;
        UInt8 count_smaller = 0;
        for (UInt8 removed_idx : indices_to_remove)
            if (removed_idx < index)
                ++count_smaller;
        return index - count_smaller;
    };

    setDatabaseIndex(adjust_index(getDatabaseIndex()));
    setTableIndex(adjust_index(getTableIndex()));
}
}
